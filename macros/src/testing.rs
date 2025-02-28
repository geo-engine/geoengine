use proc_macro2::{Span, TokenStream};
use quote::quote;
use std::collections::HashMap;
use syn::{FnArg, Ident, ItemFn, Lit, Pat, TypePath, parse::Parser, punctuated::Punctuated};

pub type Result<T, E = syn::Error> = std::result::Result<T, E>;
pub type AttributeArgs = syn::punctuated::Punctuated<syn::Meta, syn::Token![,]>;
pub type ConfigArgs = HashMap<String, Lit>;

pub fn test(attr: TokenStream, item: &TokenStream) -> Result<TokenStream, syn::Error> {
    let input: ItemFn = syn::parse2(item.clone())?;

    let mut config_args = parse_config_args(attr)?;
    let test_config = TestConfig::from_args(&mut config_args)?;

    let inputs = parse_inputs(&input.sig.inputs)?;

    let test_name = input.sig.ident.clone();
    let test_output = input.sig.output.clone();

    let app_ctx_var = inputs.app_ctx_var();

    let db_config_var = inputs.db_config_var();

    let user_ctx = if let Some(input) = &inputs.user_ctx {
        test_config.user_context(&input.name, &input.ty, &app_ctx_var)
    } else {
        quote!()
    };

    let call_params = inputs.call_params();

    let tiling_spec = test_config.tiling_spec();
    let query_ctx_chunk_size = test_config.query_ctx_chunk_size();
    let test_execution = test_config.test_execution();
    let before = test_config.before();
    let expect_panic = test_config.expect_panic();
    let quota_config = test_config.quota_config();
    let oidc_db = test_config.oidc_db();

    let output = quote! {
        #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
        #expect_panic
        #test_execution
        async fn #test_name () #test_output {
            #input

            let tiling_spec = #tiling_spec;
            let query_ctx_chunk_size = #query_ctx_chunk_size;
            let quota_config = #quota_config;
            let (server, oidc_db) = #oidc_db;

            #before;

            crate::util::tests::with_temp_context_from_spec(
                tiling_spec,
                query_ctx_chunk_size,
                quota_config,
                oidc_db,
                #[allow(clippy::used_underscore_binding)]
                |#app_ctx_var, #db_config_var| async move {
                    #user_ctx
                    #[warn(clippy::used_underscore_binding)]
                    #test_name ( #call_params ).await
                },
            ).await
        }
    };

    Ok(output)
}

pub struct TestConfig {
    tiling_spec: Option<TokenStream>,
    query_ctx_chunk_size: Option<TokenStream>,
    test_execution: Option<TokenStream>,
    before: Option<TokenStream>,
    expect_panic: Option<TokenStream>,
    quota_config: Option<TokenStream>,
    oidc_db: Option<TokenStream>,
    user: UserConfig,
}

impl TestConfig {
    pub fn from_args(args: &mut ConfigArgs) -> Result<Self> {
        let mut this = Self {
            tiling_spec: None,
            query_ctx_chunk_size: None,
            test_execution: None,
            before: None,
            expect_panic: None,
            quota_config: None,
            oidc_db: None,
            user: UserConfig::Anonymous,
        };

        if let Some(lit) = args.remove("tiling_spec") {
            this.tiling_spec = Some(literal_to_fn(&lit)?);
        }

        if let Some(lit) = args.remove("query_ctx_chunk_size") {
            this.query_ctx_chunk_size = Some(literal_to_fn(&lit)?);
        }

        if let Some(lit) = args.remove("test_execution") {
            let Lit::Str(lit_str) = &lit else {
                return Err(syn::Error::new_spanned(
                    lit,
                    "test_execution must be a string",
                ));
            };

            match lit_str.value().as_str() {
                "parallel" => (), // leave as is
                "serial" => this.test_execution = Some(quote!(#[serial_test::serial])),
                _ => {
                    return Err(syn::Error::new_spanned(
                        lit,
                        "test_execution must be \"parallel\" or \"serial\"",
                    ));
                }
            }
        }

        if let Some(lit) = args.remove("before") {
            this.before = Some(literal_to_fn(&lit)?);
        }

        if let Some(lit) = args.remove("expect_panic") {
            let Lit::Str(lit_str) = &lit else {
                return Err(syn::Error::new_spanned(
                    lit,
                    "test_execution must be a string",
                ));
            };

            let expected_str = lit_str.value();

            this.expect_panic = Some(quote!(
                #[should_panic(expected = #expected_str)]
            ));
        }

        if let Some(lit) = args.remove("user") {
            this.user = lit.try_into()?;
        }

        if let Some(lit) = args.remove("quota_config") {
            this.quota_config = Some(literal_to_fn(&lit)?);
        }

        if let Some(lit) = args.remove("oidc_db") {
            let oidc_db_fn = literal_to_fn(&lit)?;
            this.oidc_db = Some(quote!(#oidc_db_fn));
        }

        Ok(this)
    }

    pub fn tiling_spec(&self) -> TokenStream {
        self.tiling_spec
            .clone()
            .unwrap_or_else(|| quote!(geoengine_datatypes::util::test::TestDefault::test_default()))
    }

    pub fn query_ctx_chunk_size(&self) -> TokenStream {
        self.query_ctx_chunk_size
            .clone()
            .unwrap_or_else(|| quote!(geoengine_datatypes::util::test::TestDefault::test_default()))
    }

    pub fn test_execution(&self) -> TokenStream {
        self.test_execution
            .clone()
            .unwrap_or_else(|| quote!(#[serial_test::parallel]))
    }

    pub fn before(&self) -> TokenStream {
        self.before.clone().unwrap_or_else(|| quote!((|| {})()))
    }

    pub fn expect_panic(&self) -> TokenStream {
        self.expect_panic.clone().unwrap_or_default()
    }

    pub fn quota_config(&self) -> TokenStream {
        self.quota_config.clone().unwrap_or_else(|| {
            quote!(crate::config::get_config_element::<crate::config::Quota>().unwrap())
        })
    }

    pub fn oidc_db(&self) -> TokenStream {
        self.oidc_db
            .clone()
            .unwrap_or_else(|| quote!(((), crate::users::OidcManager::default)))
    }

    pub fn user_context(
        &self,
        user_ctx_name: &Ident,
        user_ctx_ty: &TypePath,
        app_ctx_var: &Ident,
    ) -> TokenStream {
        match self.user {
            UserConfig::Anonymous => quote! {
                let #user_ctx_name: #user_ctx_ty = {
                    use crate::contexts::ApplicationContext;
                    use crate::users::UserAuth;
                    let session = #app_ctx_var.create_anonymous_session().await.unwrap();
                    #app_ctx_var.session_context(session)
                };
            },
            UserConfig::Admin => quote! {
                let #user_ctx_name: #user_ctx_ty = {
                    use crate::contexts::ApplicationContext;
                    let session = crate::util::tests::admin_login(&#app_ctx_var).await;
                    #app_ctx_var.session_context(session)
                };
            },
        }
    }
}

enum UserConfig {
    Anonymous,
    // Registered, TODO: impl
    Admin,
}

impl TryFrom<Lit> for UserConfig {
    type Error = syn::Error;

    fn try_from(value: Lit) -> std::result::Result<Self, Self::Error> {
        let syn::Lit::Str(user_str) = &value else {
            return Err(syn::Error::new_spanned(
                value,
                "`user` argument must be a string",
            ));
        };
        match user_str.value().as_str() {
            "admin" => Ok(Self::Admin),
            // "registered" => Self::Registered,
            "anonymous" => Ok(Self::Anonymous),
            other => Err(syn::Error::new_spanned(
                value,
                format!("Unknown UserConfig variant: {other}"),
            )),
        }
    }
}

/// use a literal of a function name as a function
pub fn literal_to_fn(lit: &Lit) -> Result<TokenStream> {
    match lit {
        Lit::Str(lit_str) => {
            let fn_ident = syn::Ident::new(&lit_str.value(), proc_macro2::Span::call_site());
            Ok(quote!(#fn_ident()))
        }
        _ => Err(syn::Error::new_spanned(lit, "Unsupported literal type")),
    }
}

pub fn parse_config_args(attr: TokenStream) -> Result<HashMap<String, Lit>> {
    let inputs = AttributeArgs::parse_terminated.parse2(attr)?;

    let mut args = HashMap::new();

    for input in inputs {
        let syn::Meta::NameValue(name_value) = input else {
            return Err(syn::Error::new_spanned(input, "expected name-value pair"));
        };

        let ident = name_value
            .path
            .get_ident()
            .ok_or_else(|| {
                syn::Error::new_spanned(name_value.clone(), "Must have specified ident")
            })?
            .to_string()
            .to_lowercase();
        let lit = match &name_value.value {
            syn::Expr::Lit(syn::ExprLit { lit, .. }) => lit,
            expr => return Err(syn::Error::new_spanned(expr, "Must be a literal")),
        };

        args.insert(ident, lit.clone());
    }

    Ok(args)
}

#[derive(Debug)]
struct Inputs {
    app_ctx: Option<Input>,
    user_ctx: Option<Input>,
    db_config: Option<Input>,
}

impl Inputs {
    pub fn app_ctx_var(&self) -> Ident {
        if let Some(app_ctx) = &self.app_ctx {
            app_ctx.name.clone()
        } else {
            Ident::new("_app_ctx", Span::call_site())
        }
    }

    pub fn db_config_var(&self) -> Ident {
        if let Some(db_config) = &self.db_config {
            db_config.name.clone()
        } else {
            Ident::new("_", Span::call_site())
        }
    }

    pub fn call_params(&self) -> TokenStream {
        let mut params = vec![];

        if let Some(app_ctx) = &self.app_ctx {
            params.push(app_ctx);
        }

        if let Some(user_ctx) = &self.user_ctx {
            params.push(user_ctx);
        }

        if let Some(db_config) = &self.db_config {
            params.push(db_config);
        }

        params.sort_by_key(|input| input.position);

        let param_names = params.iter().map(|input| &input.name);

        quote!(#(#param_names),*)
    }
}

#[derive(Debug)]
struct Input {
    pub name: Ident,
    pub ty: TypePath,
    pub position: usize,
}

fn parse_inputs(inputs: &Punctuated<FnArg, syn::token::Comma>) -> Result<Inputs> {
    let mut result = Inputs {
        app_ctx: None,
        user_ctx: None,
        db_config: None,
    };

    // check inputs by their type
    for (position, input) in inputs.iter().enumerate() {
        let FnArg::Typed(input) = input else {
            return Err(syn::Error::new_spanned(
                input,
                "Receiver not allowed in test function",
            ));
        };

        let Pat::Ident(ref ident) = *input.pat else {
            return Err(syn::Error::new_spanned(
                input,
                "Param name must be identifier",
            ));
        };

        let syn::Type::Path(ty) = &*input.ty else {
            return Err(syn::Error::new_spanned(
                input,
                "Type must be a path (e.g. `PostgresContext<NoTls>`)",
            ));
        };

        let last_segment = ty
            .path
            .segments
            .last()
            .expect("Type must not be empty")
            .ident
            .to_string();

        let var = match last_segment.as_str() {
            "PostgresContext" => &mut result.app_ctx,
            "PostgresSessionContext" => &mut result.user_ctx,
            "DatabaseConnectionConfig" => &mut result.db_config,
            other => {
                return Err(syn::Error::new_spanned(
                    input,
                    format!("Unknown input type: {other}"),
                ));
            }
        };

        *var = Some(Input {
            name: ident.ident.clone(),
            ty: ty.clone(),
            position,
        });
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_str_eq;

    // This macro is used to compare the pretty printed output of the expression parser.
    // We will use a macro instead of a function to get errors in the places where they occur.
    macro_rules! assert_eq_pretty {
        ( $left:expr, $right:expr ) => {{
            assert_str_eq!(
                prettyplease::unparse(&syn::parse_file(&$left).unwrap()),
                prettyplease::unparse(&syn::parse_file(&$right).unwrap()),
            );
        }};
    }

    #[test]
    fn test_codegen_no_attrs() {
        let input = quote! {
            async fn it_works(app_ctx: PostgresContext<NoTls>) {
                assert_eq!(1, 1);
            }
        };
        let attributes = quote! {};

        let expected = quote! {
            #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
            #[serial_test::parallel]
            async fn it_works() {
                async fn it_works(app_ctx: PostgresContext<NoTls>) {
                    assert_eq!(1, 1);
                }

                let tiling_spec = geoengine_datatypes::util::test::TestDefault::test_default();
                let query_ctx_chunk_size = geoengine_datatypes::util::test::TestDefault::test_default();
                let quota_config = crate::config::get_config_element::<crate::config::Quota>()
                    .unwrap();
                let (server, oidc_db) = ((), crate::users::OidcManager::default);

                (|| {})();

                crate::util::tests::with_temp_context_from_spec(
                    tiling_spec,
                    query_ctx_chunk_size,
                    quota_config,
                    oidc_db,
                    #[allow(clippy::used_underscore_binding)]
                    |app_ctx, _| async move {
                        #[warn(clippy::used_underscore_binding)]
                        it_works(app_ctx).await
                    },
                ).await
            }
        };

        let actual = test(attributes, &input).unwrap();

        assert_eq_pretty!(expected.to_string(), actual.to_string());
    }

    #[test]
    fn test_codegen_with_user() {
        let input = quote! {
            async fn it_works(app_ctx: PostgresContext<NoTls>, ctx: PostgresSessionContext<NoTls>) {
                assert_eq!(1, 1);
            }
        };
        let attributes = quote! {};

        let expected = quote! {
            #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
            #[serial_test::parallel]
            async fn it_works() {
                async fn it_works(app_ctx: PostgresContext<NoTls>, ctx: PostgresSessionContext<NoTls>) {
                    assert_eq!(1, 1);
                }

                let tiling_spec = geoengine_datatypes::util::test::TestDefault::test_default();
                let query_ctx_chunk_size = geoengine_datatypes::util::test::TestDefault::test_default();
                let quota_config = crate::config::get_config_element::<crate::config::Quota>()
                    .unwrap();
                let (server, oidc_db) = ((), crate::users::OidcManager::default);

                (|| {})();

                crate::util::tests::with_temp_context_from_spec(
                    tiling_spec,
                    query_ctx_chunk_size,
                    quota_config,
                    oidc_db,
                    #[allow(clippy::used_underscore_binding)]
                    |app_ctx, _| async move {
                        let ctx: PostgresSessionContext<NoTls> = {
                            use crate::contexts::ApplicationContext;
                            use crate::users::UserAuth;
                            let session = app_ctx.create_anonymous_session().await.unwrap();
                            app_ctx.session_context(session)
                        };
                        #[warn(clippy::used_underscore_binding)]
                        it_works(app_ctx, ctx).await
                    },
                ).await
            }
        };

        let actual = test(attributes, &input).unwrap();

        assert_eq_pretty!(expected.to_string(), actual.to_string());
    }

    #[test]
    fn test_codegen_with_admin_and_db() {
        let input = quote! {
            async fn it_works(db_config: DatabaseConnectionConfig, ctx: PostgresSessionContext<NoTls>) {
                assert_eq!(1, 1);
            }
        };
        let attributes = quote! {
            user = "admin"
        };

        let expected = quote! {
            #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
            #[serial_test::parallel]
            async fn it_works() {
                async fn it_works(db_config: DatabaseConnectionConfig, ctx: PostgresSessionContext<NoTls>) {
                    assert_eq!(1, 1);
                }

                let tiling_spec = geoengine_datatypes::util::test::TestDefault::test_default();
                let query_ctx_chunk_size = geoengine_datatypes::util::test::TestDefault::test_default();
                let quota_config = crate::config::get_config_element::<crate::config::Quota>()
                    .unwrap();
                let (server, oidc_db) = ((), crate::users::OidcManager::default);

                (|| {})();

                crate::util::tests::with_temp_context_from_spec(
                    tiling_spec,
                    query_ctx_chunk_size,
                    quota_config,
                    oidc_db,
                    #[allow(clippy::used_underscore_binding)]
                    |_app_ctx, db_config| async move {
                        let ctx: PostgresSessionContext<NoTls> = {
                            use crate::contexts::ApplicationContext;
                            let session = crate::util::tests::admin_login(&_app_ctx).await;
                            _app_ctx.session_context(session)
                        };
                        #[warn(clippy::used_underscore_binding)]
                        it_works(db_config, ctx).await
                    },
                ).await
            }
        };

        let actual = test(attributes, &input).unwrap();

        assert_eq_pretty!(expected.to_string(), actual.to_string());
    }

    #[test]
    fn test_codegen_with_attrs() {
        let input = quote! {
            async fn it_works(app_ctx: PostgresContext<NoTls>) {
                assert_eq!(1, 1);
            }
        };
        let attributes = quote! {
            tiling_spec = "foo",
            query_ctx_chunk_size = "bar",
            test_execution = "serial",
            before = "before_fn",
            expect_panic = "panic!!!",
            quota_config = "baz",
            oidc_db = "qux",
        };

        let expected = quote! {
            #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
            #[should_panic(expected = "panic!!!")]
            #[serial_test::serial]
            async fn it_works() {
                async fn it_works(app_ctx: PostgresContext<NoTls>) {
                    assert_eq!(1, 1);
                }

                let tiling_spec = foo();
                let query_ctx_chunk_size = bar();
                let quota_config = baz();
                let (server, oidc_db) = qux();

                before_fn();

                crate::util::tests::with_temp_context_from_spec(
                    tiling_spec,
                    query_ctx_chunk_size,
                    quota_config,
                    oidc_db,
                    #[allow(clippy::used_underscore_binding)]
                    |app_ctx, _| async move {
                        #[warn(clippy::used_underscore_binding)]
                        it_works(app_ctx).await
                    },
                ).await
            }
        };

        let actual = test(attributes, &input).unwrap();

        assert_eq_pretty!(expected.to_string(), actual.to_string());
    }
}
