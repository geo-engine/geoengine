use proc_macro2::TokenStream;
use quote::quote;
use syn::ItemFn;

#[proc_macro_attribute]
pub fn test(
    _attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let item: TokenStream = item.into();

    let input: ItemFn = match syn::parse2(item.clone()) {
        Ok(it) => it,
        Err(e) => return token_stream_with_error(item, e).into(),
    };

    let test_name = input.sig.ident;

    let inputs = input.sig.inputs.iter().collect::<Vec<_>>();

    let (app_ctx, app_config) = match inputs.as_slice() {
        [] => (quote!(_), quote!(_)),
        [app_ctx] => (quote!(#app_ctx), quote!(_)),
        [app_ctx, app_config, ..] => (quote!(#app_ctx), quote!(#app_config)),
    };

    let body = input.block;

    let output = quote! {
        #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
        async fn #test_name () {
            with_temp_context(|#app_ctx, #app_config| async move {
                #body
            }).await;
        }
    };

    output.into()
}

fn token_stream_with_error(mut tokens: TokenStream, error: syn::Error) -> TokenStream {
    tokens.extend(error.into_compile_error());
    tokens
}
