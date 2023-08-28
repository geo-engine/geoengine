use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::token::Comma;
use syn::{ItemFn, Lit, Meta};

struct TestConfig {
    tiling_spec: TokenStream,
    query_ctx_chunk_size: TokenStream,
}

impl TestConfig {
    fn new() -> Self {
        Self {
            tiling_spec: quote!(geoengine_datatypes::util::test::TestDefault::test_default()),
            query_ctx_chunk_size: quote!(
                geoengine_datatypes::util::test::TestDefault::test_default()
            ),
        }
    }

    fn parse_args(&mut self, inputs: Punctuated<Meta, Comma>) -> Result<(), syn::Error> {
        for input in &inputs {
            match input {
                syn::Meta::NameValue(name_value) => {
                    let ident = name_value
                        .path
                        .get_ident()
                        .ok_or_else(|| {
                            syn::Error::new_spanned(name_value, "Must have specified ident")
                        })?
                        .to_string()
                        .to_lowercase();
                    let lit = match &name_value.value {
                        syn::Expr::Lit(syn::ExprLit { lit, .. }) => lit,
                        expr => return Err(syn::Error::new_spanned(expr, "Must be a literal")),
                    };
                    match ident.as_str() {
                        "tiling_spec" => {
                            self.tiling_spec = literal_to_fn(lit)?;
                        }
                        "query_ctx_chunk_size" => {
                            self.query_ctx_chunk_size = literal_to_fn(lit)?;
                        }
                        _ => {
                            return Err(syn::Error::new_spanned(
                                name_value,
                                "Unknown name-value pair",
                            ));
                        }
                    }
                }
                _ => {
                    return Err(syn::Error::new_spanned(input, "expected name-value pair"));
                }
            }
        }

        Ok(())
    }
}

/// use a literal of a function name as a function
fn literal_to_fn(lit: &Lit) -> Result<TokenStream, syn::Error> {
    match lit {
        Lit::Str(lit_str) => {
            let fn_ident = syn::Ident::new(&lit_str.value(), proc_macro2::Span::call_site());
            Ok(quote!(#fn_ident()))
        }
        _ => Err(syn::Error::new_spanned(lit, "Unsupported literal type")),
    }
}

#[proc_macro_attribute]
pub fn test(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let item: TokenStream = item.into();
    let attr: TokenStream = attr.into();

    let input: ItemFn = match syn::parse2(item.clone()) {
        Ok(it) => it,
        Err(e) => return token_stream_with_error(item, e).into(),
    };

    let attribute_parser =
        syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated;

    let mut test_config = TestConfig::new();
    if let Err(e) = attribute_parser
        .parse2(attr)
        .and_then(|inputs| test_config.parse_args(inputs))
    {
        return token_stream_with_error(item, e).into();
    }

    let test_name = input.sig.ident;

    let inputs = input.sig.inputs.iter().collect::<Vec<_>>();

    let (app_ctx, app_config) = match inputs.as_slice() {
        [] => (quote!(_), quote!(_)),
        [app_ctx] => (quote!(#app_ctx), quote!(_)),
        [app_ctx, app_config, ..] => (quote!(#app_ctx), quote!(#app_config)),
    };

    let body = input.block;
    let tiling_spec = test_config.tiling_spec;
    let query_ctx_chunk_size = test_config.query_ctx_chunk_size;

    let output = quote! {
        #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
        async fn #test_name () {
            let tiling_spec = #tiling_spec;
            let query_ctx_chunk_size = #query_ctx_chunk_size;
            let #test_name = |#app_ctx, #app_config| async move {
                #body
            };
            crate::util::tests::with_temp_context_from_spec(
                tiling_spec,
                query_ctx_chunk_size,
                #test_name,
            ).await;
        }
    };

    output.into()
}

fn token_stream_with_error(mut tokens: TokenStream, error: syn::Error) -> TokenStream {
    tokens.extend(error.into_compile_error());
    tokens
}
