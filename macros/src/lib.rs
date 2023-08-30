use proc_macro2::TokenStream;

/// Compiles Geo Engine Pro
#[cfg(feature = "pro")]
mod pro;
mod testing;

#[proc_macro_attribute]
pub fn test(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    match testing::test(attr.into(), item.clone().into()) {
        Ok(ts) => ts.into(),
        Err(e) => token_stream_with_error(item.into(), e).into(),
    }
}

#[cfg(feature = "pro")]
#[proc_macro_attribute]
pub fn pro_test(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    match pro::test(attr.into(), item.clone().into()) {
        Ok(ts) => ts.into(),
        Err(e) => token_stream_with_error(item.into(), e).into(),
    }
}

fn token_stream_with_error(mut tokens: TokenStream, error: syn::Error) -> TokenStream {
    tokens.extend(error.into_compile_error());
    tokens
}
