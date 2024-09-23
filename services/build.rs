use anyhow::Context;
use vergen::{BuildBuilder, CargoBuilder, Emitter};
use vergen_gitcl::GitclBuilder;

fn main() -> anyhow::Result<()> {
    Emitter::default()
        .add_instructions(
            // `VERGEN_BUILD_DATE`
            &BuildBuilder::default().build_date(true).build()?,
        )?
        .add_instructions(
            // `VERGEN_CARGO_FEATURES`
            &CargoBuilder::all_cargo()?,
        )?
        .add_instructions(
            // `VERGEN_GIT_SHA`
            &GitclBuilder::default().sha(true).build()?,
        )?
        .emit_and_set()
        .context("Unable to generate version info")
}
