use std::str::FromStr;

use geoengine_datatypes::{
    primitives::{Coordinate2D, SpatialPartition2D},
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
        match (self.zone, self.direction) {
            (32, UtmZoneDirection::North)
            | (34, UtmZoneDirection::North)
            | (36, UtmZoneDirection::North) => SpatialPartition2D::new_unchecked(
                Coordinate2D::new(199980.0, 8000040.0),
                Coordinate2D::new(909780.0, -9780.0),
            ),
            (60, UtmZoneDirection::North) => SpatialPartition2D::new_unchecked(
                Coordinate2D::new(199980.0, 9100020.0),
                Coordinate2D::new(809760.0, -9780.0),
            ),
            (_, UtmZoneDirection::North) => SpatialPartition2D::new_unchecked(
                Coordinate2D::new(199980.0, 9400020.0),
                Coordinate2D::new(909780.0, -9780.0),
            ),
            (1, UtmZoneDirection::South) => SpatialPartition2D::new_unchecked(
                Coordinate2D::new(99960.0, 10000000.0),
                Coordinate2D::new(909780.0, 690220.0),
            ),
            (60, UtmZoneDirection::South) => SpatialPartition2D::new_unchecked(
                Coordinate2D::new(199980.0, 10000000.0),
                Coordinate2D::new(809760.0, 890200.0),
            ),
            (_, UtmZoneDirection::South) => SpatialPartition2D::new_unchecked(
                Coordinate2D::new(199980.0, 10000000.0),
                Coordinate2D::new(909780.0, 690220.0),
            ),
        }
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
