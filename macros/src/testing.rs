use proc_macro2::TokenStream;
use quote::quote;
use std::collections::HashMap;
use syn::parse::Parser;
use syn::{ItemFn, Lit};

pub type Result<T, E = syn::Error> = std::result::Result<T, E>;
pub type AttributeArgs = syn::punctuated::Punctuated<syn::Meta, syn::Token![,]>;
pub type ConfigArgs = HashMap<String, Lit>;

pub fn test(attr: TokenStream, item: TokenStream) -> Result<TokenStream> {
    let input: ItemFn = syn::parse2(item.clone())?;

    let mut config_args = parse_config_args(attr)?;
    let test_config = TestConfig::from_args(&mut config_args)?;

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

    let (app_ctx, app_config) = match inputs.as_slice() {
        [] => (quote!(_), quote!(_)),
        [app_ctx] => (quote!(#app_ctx), quote!(_)),
        [app_ctx, app_config, ..] => (quote!(#app_ctx), quote!(#app_config)),
    };

    let tiling_spec = test_config.tiling_spec();
    let query_ctx_chunk_size = test_config.query_ctx_chunk_size();
    let test_execution = test_config.test_execution();

    let output = quote! {
        #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
        #test_execution
        async fn #test_name () #test_output {
            #input

            let tiling_spec = #tiling_spec;
            let query_ctx_chunk_size = #query_ctx_chunk_size;

            crate::util::tests::with_temp_context_from_spec(
                tiling_spec,
                query_ctx_chunk_size,
                |#app_ctx, #app_config| #test_name ( #(#input_params),* ),
            ).await
        }
    };

    Ok(output)
}

pub struct TestConfig {
    tiling_spec: Option<TokenStream>,
    query_ctx_chunk_size: Option<TokenStream>,
    test_execution: Option<TokenStream>,
}

impl TestConfig {
    pub fn from_args(args: &mut ConfigArgs) -> Result<Self> {
        let mut this = Self {
            tiling_spec: None,
            query_ctx_chunk_size: None,
            test_execution: None,
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
                    ))
                }
            }
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

#[cfg(test)]
mod tests {
    use super::*;

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

                crate::util::tests::with_temp_context_from_spec(
                    tiling_spec,
                    query_ctx_chunk_size,
                    |app_ctx: PostgresContext<NoTls>, _| it_works(app_ctx),
                ).await
            }
        };

        let actual = test(attributes, input).unwrap();

        assert_eq!(expected.to_string(), actual.to_string());
    }

    #[test]
    fn test_result() {
        let input = quote! {
            async fn it_works(app_ctx: PostgresContext<NoTls>) -> Result<(), Box<dyn std::error::Error>> {
                Ok(())
            }
        };
        let attributes = quote! {};

        let expected = quote! {
            #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
            #[serial_test::parallel]
            async fn it_works() -> Result<(), Box<dyn std::error::Error> > {
                async fn it_works(app_ctx: PostgresContext<NoTls>) -> Result<(), Box<dyn std::error::Error> > {
                    Ok(())
                }

                let tiling_spec = geoengine_datatypes::util::test::TestDefault::test_default();
                let query_ctx_chunk_size = geoengine_datatypes::util::test::TestDefault::test_default();

                crate::util::tests::with_temp_context_from_spec(
                    tiling_spec,
                    query_ctx_chunk_size,
                    |app_ctx: PostgresContext<NoTls>, _| it_works(app_ctx),
                ).await
            }
        };

        let actual = test(attributes, input).unwrap();

        assert_eq!(expected.to_string(), actual.to_string());
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
        };

        let expected = quote! {
            #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
            #[serial_test::serial]
            async fn it_works() {
                async fn it_works(app_ctx: PostgresContext<NoTls>) {
                    assert_eq!(1, 1);
                }

                let tiling_spec = foo();
                let query_ctx_chunk_size = bar();

                crate::util::tests::with_temp_context_from_spec(
                    tiling_spec,
                    query_ctx_chunk_size,
                    |app_ctx: PostgresContext<NoTls>, _| it_works(app_ctx),
                ).await
            }
        };

        let actual = test(attributes, input).unwrap();

        assert_eq!(expected.to_string(), actual.to_string());
    }
}
