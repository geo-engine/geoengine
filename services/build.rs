fn main() {
    vergen::EmitBuilder::builder()
        .build_date() // `VERGEN_BUILD_DATE`
        .git_sha(true) // `VERGEN_GIT_SHA`
        .cargo_features() // `VERGEN_CARGO_FEATURES`
        .emit_and_set()
        .expect("Unable to generate version info");
}
