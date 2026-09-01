//! New STAC harvester CLI that separates mapping generation from tile harvesting.
//!
//! # Subcommands
//!
//! - `discover-mapping`: Probes a STAC collection and items API to generate a
//!   `StacDataProviderDefinition` JSON that maps STAC assets to Geo Engine datasets.
//! - `harvest`: Reads a `StacDataProviderDefinition` and harvests tiles into Geo Engine,
//!   creating datasets, tiles, and layer collections.
//!
//! The mapping JSON matches the format of `StacDataProviderDefinition` as used by the
//! STAC provider and the EDV bootstrap scripts.

#![allow(clippy::print_stdout)]

mod discover;
mod harvest;

pub use discover::StacDiscoverMapping;
pub use harvest::StacHarvest;

use clap::{Parser, Subcommand};
use tracing_subscriber::{
    filter::{LevelFilter, Targets},
    layer::{Layer, SubscriberExt},
    util::SubscriberInitExt,
};

/// STAC harvester for Geo Engine
#[derive(Debug, Parser)]
pub struct StacHarvester {
    #[clap(subcommand)]
    pub command: StacHarvesterCommand,
}

#[derive(Debug, Subcommand)]
#[allow(clippy::enum_variant_names)]
pub enum StacHarvesterCommand {
    /// Probe a STAC API to auto-discover the dataset mapping
    DiscoverMapping(Box<StacDiscoverMapping>),
    /// Harvest tiles using a predefined dataset mapping
    Harvest(Box<StacHarvest>),
}

/// Run the STAC harvester
pub async fn stac_harvester(params: StacHarvester) -> Result<(), anyhow::Error> {
    match params.command {
        StacHarvesterCommand::DiscoverMapping(discover) => {
            init_stac_harvest_logging(discover.verbose);
            discover::discover_mapping(*discover).await
        }
        StacHarvesterCommand::Harvest(harvest) => {
            init_stac_harvest_logging(harvest.verbose);
            harvest::harvest_tiles(*harvest).await
        }
    }
}

/// Initialize the tracing subscriber for STAC harvesting.
///
/// Logs go to stderr so stdout stays clean (the `discover-mapping` command
/// emits the mapping JSON on stdout). Only the Geo Engine crates are logged —
/// dependency noise (hyper, reqwest, …) is filtered out. `--verbose` raises
/// the level to DEBUG; the default level is INFO so the progress `info!`
/// messages are visible.
fn init_stac_harvest_logging(verbose: bool) {
    let geoengine_level = if verbose {
        LevelFilter::DEBUG
    } else {
        LevelFilter::INFO
    };

    // `Targets` matches target prefixes hierarchically, so `geoengine_services`
    // also covers `geoengine_services::cli::stac_harvester::harvest`.
    let targets = Targets::new()
        .with_target("geoengine_services", geoengine_level)
        .with_target("geoengine_operators", geoengine_level)
        .with_target("geoengine_datatypes", geoengine_level)
        .with_target("geoengine_api_client", geoengine_level)
        .with_default(LevelFilter::OFF);

    let _ = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_filter(targets),
        )
        .try_init();
}
