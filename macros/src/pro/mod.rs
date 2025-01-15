use core::panic;

use crate::testing::{literal_to_fn, parse_config_args, ConfigArgs, Result, TestConfig};
use proc_macro2::TokenStream;
use quote::{quote, ToTokens};
use syn::{FnArg, Ident, ItemFn, Pat};

pub fn test(attr: TokenStream, item: &TokenStream) -> Result<TokenStream, syn::Error> {
    let input: ItemFn = syn::parse2(item.clone())?;

    let mut config_args = parse_config_args(attr)?;
    let test_config = ProTestConfig::from_args(&mut config_args)?;

    let test_name = input.sig.ident.clone();
    let test_output = input.sig.output.clone();

    let inputs = input.sig.inputs.iter().collect::<Vec<_>>();

    let input_params = input
        .sig
        .inputs
        .iter()
        .map(|input| {
            let pat = match input {
                syn::FnArg::Receiver(_) => {
                    return Err(syn::Error::new_spanned(
                        input,
                        "Receiver not allowed in test function",
                    ))
                }
                syn::FnArg::Typed(pat_type) => pat_type.pat.clone(),
            };
            Ok(pat)
        })
        .collect::<Result<Vec<Box<syn::Pat>>>>()?;

    let (app_ctx, user_ctx) = match inputs.as_slice() {
        [] => (quote!(), quote!()),
        [app_ctx] => (quote!(#app_ctx), quote!()),
        // TODO: make configurable for registered users and admins
        [app_ctx, ctx] => {
            let app_ctx_var = param_name(app_ctx);
            (
                quote!(#app_ctx),
                quote! {
                    let #ctx = {
                        use crate::contexts::ApplicationContext;
                        use crate::pro::users::UserAuth;
                        let session = #app_ctx_var.create_anonymous_session().await.unwrap();
                        #app_ctx_var.session_context(session)
                    };
                },
            )
        }
        [_app_ctx, _ctx, rest @ ..] => {
            let additional_names: Vec<String> = rest
                .iter()
                .map(|p| p.to_token_stream().to_string())
                .collect();
            panic!("Too many input params: {additional_names:?}")
        }
    };

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

            crate::pro::util::tests::with_pro_temp_context_from_spec(
                tiling_spec,
                query_ctx_chunk_size,
                quota_config,
                oidc_db,
                #[allow(clippy::used_underscore_binding)]
                |#app_ctx, _| async {
                    #user_ctx
                    #[warn(clippy::used_underscore_binding)]
                    #test_name ( #(#input_params),* ).await
                },
            ).await
        }
    };

    Ok(output)
}

fn param_name(arg: &FnArg) -> Ident {
    let FnArg::Typed(arg) = arg else {
        panic!("Param name must be typed");
    };
    let Pat::Ident(ref ident) = *arg.pat else {
        panic!("Param name must be identifier");
    };

    ident.ident.clone()
}

struct ProTestConfig {
    test_config: TestConfig,
    quota_config: Option<TokenStream>,
    oidc_db: Option<TokenStream>,
}

impl ProTestConfig {
    pub fn from_args(args: &mut ConfigArgs) -> Result<Self> {
        let mut this = Self {
            test_config: TestConfig::from_args(args)?,
            quota_config: None,
            oidc_db: None,
        };

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
        self.test_config.tiling_spec()
    }

    pub fn query_ctx_chunk_size(&self) -> TokenStream {
        self.test_config.query_ctx_chunk_size()
    }

    pub fn test_execution(&self) -> TokenStream {
        self.test_config.test_execution()
    }

    pub fn before(&self) -> TokenStream {
        self.test_config.before()
    }

    pub fn expect_panic(&self) -> TokenStream {
        self.test_config.expect_panic()
    }

    pub fn quota_config(&self) -> TokenStream {
        self.quota_config.clone().unwrap_or_else(|| {
            quote!(
                crate::util::config::get_config_element::<crate::pro::util::config::Quota>()
                    .unwrap()
            )
        })
    }

    pub fn oidc_db(&self) -> TokenStream {
        self.oidc_db
            .clone()
            .unwrap_or_else(|| quote!(((), crate::pro::users::OidcManager::default)))
    }
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
            async fn it_works(app_ctx: ProPostgresContext<NoTls>) {
                assert_eq!(1, 1);
            }
        };
        let attributes = quote! {};

        let expected = quote! {
            #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
            #[serial_test::parallel]
            async fn it_works() {
                async fn it_works(app_ctx: ProPostgresContext<NoTls>) {
                    assert_eq!(1, 1);
                }

                let tiling_spec = geoengine_datatypes::util::test::TestDefault::test_default();
                let query_ctx_chunk_size = geoengine_datatypes::util::test::TestDefault::test_default();
                let quota_config = crate::util::config::get_config_element::<crate::pro::util::config::Quota>()
                    .unwrap();
                let (server, oidc_db) = ((), crate::pro::users::OidcManager::default);

                (|| {})();

                crate::pro::util::tests::with_pro_temp_context_from_spec(
                    tiling_spec,
                    query_ctx_chunk_size,
                    quota_config,
                    oidc_db,
                    #[allow(clippy::used_underscore_binding)]
                    |app_ctx: ProPostgresContext<NoTls>, _| async {
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
            async fn it_works(app_ctx: ProPostgresContext<NoTls>, ctx: PostgresSessionContext<NoTls>) {
                assert_eq!(1, 1);
            }
        };
        let attributes = quote! {};

        let expected = quote! {
            #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
            #[serial_test::parallel]
            async fn it_works() {
                async fn it_works(app_ctx: ProPostgresContext<NoTls>, ctx: PostgresSessionContext<NoTls>) {
                    assert_eq!(1, 1);
                }

                let tiling_spec = geoengine_datatypes::util::test::TestDefault::test_default();
                let query_ctx_chunk_size = geoengine_datatypes::util::test::TestDefault::test_default();
                let quota_config = crate::util::config::get_config_element::<crate::pro::util::config::Quota>()
                    .unwrap();
                let (server, oidc_db) = ((), crate::pro::users::OidcManager::default);

                (|| {})();

                crate::pro::util::tests::with_pro_temp_context_from_spec(
                    tiling_spec,
                    query_ctx_chunk_size,
                    quota_config,
                    oidc_db,
                    #[allow(clippy::used_underscore_binding)]
                    |app_ctx: ProPostgresContext<NoTls>, _| async {
                        let ctx: PostgresSessionContext<NoTls> = {
                            use crate::contexts::ApplicationContext;
                            use crate::pro::users::UserAuth;
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
    fn test_codegen_with_attrs() {
        let input = quote! {
            async fn it_works(app_ctx: ProPostgresContext<NoTls>) {
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
                async fn it_works(app_ctx: ProPostgresContext<NoTls>) {
                    assert_eq!(1, 1);
                }

                let tiling_spec = foo();
                let query_ctx_chunk_size = bar();
                let quota_config = baz();
                let (server, oidc_db) = qux();

                before_fn();

                crate::pro::util::tests::with_pro_temp_context_from_spec(
                    tiling_spec,
                    query_ctx_chunk_size,
                    quota_config,
                    oidc_db,
                    #[allow(clippy::used_underscore_binding)]
                    |app_ctx: ProPostgresContext<NoTls>, _| async {
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
