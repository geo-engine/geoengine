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
            discover::discover_mapping(*discover).await
        }
        StacHarvesterCommand::Harvest(harvest) => harvest::harvest_tiles(*harvest).await,
    }
}
