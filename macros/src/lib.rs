use proc_macro2::TokenStream;

mod testing;
mod typetag;
mod util;

type Result<T, E = syn::Error> = std::result::Result<T, E>;

/// A macro to generate tests for Geo Engine services.
/// It automatically spins up a database context.
///
/// # Parameters
///
/// - `tiling_spec` - a function that returns a [`geoengine_datatypes::raster::TilingSpecification`] to use for the test
/// - `query_ctx_chunk_size` - a function that returns a [`geoengine_operators::engine::ChunkByteSize`] to use for the test
/// - `test_execution` - `parallel` (default) or `serial`, which isolates this test from other tests
/// - `before` - a function that is called before the context is created and the test is executed
/// - `expect_panic` - if the test is expected to panic
/// - `user` - `"admin"` if created session should be admin session
/// - `quota_config` - a function that returns a [`crate::config::Quota`] to use for the test
/// - `oidc_db` - a tuple `(handle, f)` with
///     - `handle` being a handle of an OpenID-Connect endpoint, preventing it from dropping too early, and
///     - `f` begin function that returns a [`crate::users::OidcManager`] to use for the test
///
#[proc_macro_attribute]
pub fn test(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    match testing::test(attr.into(), &item.clone().into()) {
        Ok(ts) => ts.into(),
        Err(e) => token_stream_with_error(item.into(), e).into(),
    }
}

/// This macro generates a type tag for a struct as part of the OpenAPI schema.
/// It creates a new enum with a single variant that is used as a type tag.
/// The type tag is used for serialization and deserialization as well as for schema generation.
///
/// # Parameters
/// - `tag` - the name of the type tag
/// - `discriminator` - (optional) the name of the field that is used as a discriminator (default: `"type"`)
///
#[proc_macro_attribute]
pub fn type_tag(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    match typetag::type_tag(attr.into(), &item.clone().into()) {
        Ok(ts) => ts.into(),
        Err(e) => token_stream_with_error(item.into(), e).into(),
    }
}

fn token_stream_with_error(mut tokens: TokenStream, error: syn::Error) -> TokenStream {
    tokens.extend(error.into_compile_error());
    tokens
}
