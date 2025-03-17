use std::str::FromStr;

use geoengine_datatypes::{
    primitives::SpatialPartition2D,
    spatial_reference::{SpatialReference, SpatialReferenceAuthority},
};
use snafu::Snafu;
use strum::IntoStaticStr;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UtmZone {
    pub zone: u8,
    pub direction: UtmZoneDirection,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UtmZoneDirection {
    North,
    South,
}

#[derive(Debug, Snafu, IntoStaticStr)]
pub enum UtmZoneError {
    IdMustStartWithUtm,
    DirectionNotNorthOrSouth,
    ZoneSuffixNotANumber,
}

impl UtmZone {
    pub fn epsg_code(self) -> u32 {
        match self.direction {
            UtmZoneDirection::North => 32600 + u32::from(self.zone),
            UtmZoneDirection::South => 32700 + u32::from(self.zone),
        }
    }

    pub fn spatial_reference(self) -> SpatialReference {
        SpatialReference::new(SpatialReferenceAuthority::Epsg, self.epsg_code())
    }

    pub fn extent(self) -> Option<SpatialPartition2D> {
        // TODO: as Sentinel uses enlarged grids, we could return a larger extent
        self.spatial_reference().area_of_use().ok()
    }

    pub fn native_extent(self) -> SpatialPartition2D {
        self.spatial_reference().area_of_use_projected().unwrap() // TODO: replace with real real bounds
    }
}

impl FromStr for UtmZone {
    type Err = UtmZoneError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() < 5 || &s[..3] != "UTM" {
            return Err(UtmZoneError::IdMustStartWithUtm);
        }

        let (zone_str, dir_char) = s[3..].split_at(s.len() - 4);
        let zone = zone_str
            .parse::<u8>()
            .map_err(|_| UtmZoneError::ZoneSuffixNotANumber)?;

        // TODO: check if zone is in valid range

        let north = match dir_char {
            "N" => UtmZoneDirection::North,
            "S" => UtmZoneDirection::South,
            _ => return Err(UtmZoneError::DirectionNotNorthOrSouth),
        };

        Ok(Self {
            zone,
            direction: north,
        })
    }
}

impl std::fmt::Display for UtmZone {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "UTM{}{}",
            self.zone,
            match self.direction {
                UtmZoneDirection::North => "N",
                UtmZoneDirection::South => "S",
            }
        )
    }
}

impl UtmZone {
    pub fn zones() -> impl Iterator<Item = Self> {
        (1..=60).flat_map(|zone| {
            vec![
                UtmZone {
                    zone,
                    direction: UtmZoneDirection::North,
                },
                UtmZone {
                    zone,
                    direction: UtmZoneDirection::South,
                },
            ]
        })
    }
}
