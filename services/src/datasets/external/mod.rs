pub mod aruna;
mod copernicus_dataspace;
pub mod edr;
pub mod gbif;
pub mod gfbio_abcd;
pub mod gfbio_collections;
pub mod netcdfcf;
pub mod pangaea;
mod sentinel_s2_l2a_cogs;
mod wildlive;

pub use copernicus_dataspace::CopernicusDataspaceDataProviderDefinition;
pub use sentinel_s2_l2a_cogs::{
    GdalRetries, SentinelS2L2ACogsProviderDefinition, StacApiRetries, StacBand, StacQueryBuffer,
    StacZone,
};
pub use wildlive::{WildliveDataConnectorDefinition, WildliveError};
