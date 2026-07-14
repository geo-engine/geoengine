use anyhow::Context;
use vergen::{Build, Cargo, Emitter};
use vergen_gitcl::Gitcl;

fn main() -> anyhow::Result<()> {
    Emitter::default()
        .add_instructions(
            // `VERGEN_BUILD_DATE`
            &Build::builder().build_date(true).build(),
        )?
        .add_instructions(
            // `VERGEN_CARGO_FEATURES`
            &Cargo::all_cargo_builder().build(),
        )?
        .add_instructions(
            // `VERGEN_GIT_SHA`
            &Gitcl::builder().sha(true).build(),
        )?
        .emit_and_set()
        .context("Unable to generate version info")
}
