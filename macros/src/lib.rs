// configure default clippy lints
#![deny(clippy::correctness)]
#![warn(clippy::complexity, clippy::style, clippy::perf, clippy::pedantic)]
// disable some pedantic lints
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::default_trait_access,
    clippy::missing_errors_doc,
    clippy::module_name_repetitions,
    clippy::must_use_candidate,
    clippy::non_ascii_literal,
    clippy::option_if_let_else,
    clippy::result_large_err, // TODO: investigate this
    clippy::similar_names,
    clippy::single_match_else,
    clippy::type_repetition_in_bounds,
    clippy::wildcard_imports
)]
// enable some restriction lints
#![warn(
    clippy::dbg_macro,
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::unimplemented,
    clippy::unwrap_used
)]

use proc_macro2::TokenStream;

/// Compiles Geo Engine Pro
#[cfg(feature = "pro")]
mod pro;
mod testing;

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

#[cfg(feature = "pro")]
/// A macro to generate tests for Geo Engine pro services.
/// It automatically spins up a database context.
///
/// # Parameters
///
/// - `tiling_spec` - a function that returns a [`geoengine_datatypes::raster::TilingSpecification`] to use for the test
/// - `query_ctx_chunk_size` - a function that returns a [`geoengine_operators::engine::ChunkByteSize`] to use for the test
/// - `test_execution` - `parallel` (default) or `serial`, which isolates this test from other tests
/// - `before` - a function that is called before the context is created and the test is executed
/// - `expect_panic` - if the test is expected to panic
///
/// ## Pro Parameters
///
/// - `quota_config` - a function that returns a [`crate::pro::util::config::Quota`] to use for the test
/// - `oidc_db` - a tuple `(handle, f)` with
///     - `handle` being a handle of an OpenID-Connect endpoint, preventing it from dropping too early, and
///     - `f` begin function that returns a [`crate::pro::users::OidcRequestDb`] to use for the test
///
#[proc_macro_attribute]
pub fn pro_test(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    match pro::test(attr.into(), &item.clone().into()) {
        Ok(ts) => ts.into(),
        Err(e) => token_stream_with_error(item.into(), e).into(),
    }
}

fn token_stream_with_error(mut tokens: TokenStream, error: syn::Error) -> TokenStream {
    tokens.extend(error.into_compile_error());
    tokens
}
