use crate::error::{self, Result};
use crate::ogc::util::{
    parse_coordinate, parse_grid_offset_option, parse_time_option, parse_wcs_bbox, parse_wcs_crs,
    tuple_from_ogc_params,
};
use geoengine_datatypes::primitives::{Coordinate2D, SpatialPartition2D, SpatialResolution};
use geoengine_datatypes::{primitives::TimeInterval, spatial_reference::SpatialReference};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

// TODO: ignore case for field names

/// WCS 1.1.x
#[derive(PartialEq, Debug, Deserialize, Serialize)]
#[serde(tag = "request")]
pub enum WcsRequest {
    GetCapabilities(GetCapabilities),
    DescribeCoverage(DescribeCoverage),
    GetCoverage(GetCoverage),
}

// sample: SERVICE=WCS&request=GetCapabilities&VERSION=1.0.0
#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct GetCapabilities {
    #[serde(alias = "VERSION")]
    pub version: Option<String>,
}

// sample: SERVICE=WCS&request=DescribeCoverage&VERSION=1.1.1&IDENTIFIERS=nurc:Arc_Sample
#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct DescribeCoverage {
    #[serde(alias = "VERSION")]
    pub version: String,
    #[serde(alias = "IDENTIFIERS")]
    pub identifiers: String,
}

// sample: SERVICE=WCS&VERSION=1.1.1&request=GetCoverage&FORMAT=image/tiff&IDENTIFIER=nurc:Arc_Sample&BOUNDINGBOX=-81,-162,81,162,urn:ogc:def:crs:EPSG::4326&GRIDBASECRS=urn:ogc:def:crs:EPSG::4326&GRIDCS=urn:ogc:def:cs:OGC:0.0:Grid2dSquareCS&GRIDTYPE=urn:ogc:def:method:WCS:1.1:2dSimpleGrid&GRIDORIGIN=81,-162&GRIDOFFSETS=-18,36
#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct GetCoverage {
    #[serde(alias = "VERSION")]
    pub version: String,
    #[serde(alias = "FORMAT")]
    pub format: GetCoverageFormat,
    #[serde(alias = "IDENTIFIER")]
    pub identifier: String,
    #[serde(alias = "BOUNDINGBOX")]
    #[serde(deserialize_with = "parse_wcs_bbox")]
    pub boundingbox: WcsBoundingbox, // TODO: optional?
    #[serde(alias = "GRIDBASECRS")]
    #[serde(deserialize_with = "parse_wcs_crs")]
    pub gridbasecrs: SpatialReference,
    #[serde(alias = "GRIDORIGIN")]
    #[serde(deserialize_with = "parse_coordinate")]
    gridorigin: Coordinate2D,
    #[serde(alias = "GRIDOFFSETS")]
    #[serde(default)]
    #[serde(deserialize_with = "parse_grid_offset_option")]
    pub gridoffsets: Option<GridOffset>,

    // ignored for now:
    // GRIDCS=crs: The grid CRS (URN).
    // GridType=urn:ogc:def:method:WCS:1.1:2dGridIn2dCrs:
    // RangeSubset=selection: e.g. bands

    // vendor specific
    #[serde(default)]
    #[serde(deserialize_with = "parse_time_option")]
    pub time: Option<TimeInterval>,
}

impl GetCoverage {
    pub fn spatial_resolution(&self) -> Option<Result<SpatialResolution>> {
        if let Some(gridoffsets) = self.gridoffsets {
            let (x, y) =
                tuple_from_ogc_params(gridoffsets.x_step, gridoffsets.y_step, self.gridbasecrs);
            return Some(SpatialResolution::new(x.abs(), y.abs()).context(error::DataType));
        }
        None
    }

    pub fn grid_origin(&self) -> Coordinate2D {
        tuple_from_ogc_params(self.gridorigin.x, self.gridorigin.y, self.gridbasecrs).into()
    }
}

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub struct WcsBoundingbox {
    pub partition: SpatialPartition2D,
    pub spatial_reference: SpatialReference,
}

#[derive(PartialEq, Debug, Deserialize, Serialize, Clone, Copy)]
pub struct GridOffset {
    pub x_step: f64,
    pub y_step: f64,
}

impl From<GridOffset> for SpatialResolution {
    fn from(value: GridOffset) -> Self {
        SpatialResolution {
            x: value.x_step.abs(),
            y: value.y_step.abs(),
        }
    }
}

#[derive(PartialEq, Debug, Deserialize, Serialize)]
pub enum GetCoverageFormat {
    #[serde(rename = "image/tiff")]
    ImageTiff,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize() {
        let params = &[
            ("service", "WCS"),
            ("request", "GetCoverage"),
            ("version", "1.1.1"),
            ("identifier", "nurc:Arc_Sample"),
            ("boundingbox", "-81,-162,81,162,urn:ogc:def:crs:EPSG::4326"),
            ("format", "image/tiff"),
            ("gridbasecrs", "urn:ogc:def:crs:EPSG::4326"),
            ("gridcs", "urn:ogc:def:cs:OGC:0.0:Grid2dSquareCS"),
            ("gridtype", "urn:ogc:def:method:WCS:1.1:2dSimpleGrid"),
            ("gridorigin", "81,-162"),
            ("gridoffsets", "-18,36"),
            ("time", "2014-01-01T00:00:00.0Z"),
        ];
        let string = serde_urlencoded::to_string(params).unwrap();

        let coverage: WcsRequest = serde_urlencoded::from_str(&string).unwrap();
        assert_eq!(
            WcsRequest::GetCoverage(GetCoverage {
                version: "1.1.1".to_owned(),
                format: GetCoverageFormat::ImageTiff,
                identifier: "nurc:Arc_Sample".to_owned(),
                boundingbox: WcsBoundingbox {
                    partition: SpatialPartition2D::new_unchecked(
                        (-162., 81.).into(),
                        (162., -81.).into()
                    ),
                    spatial_reference: SpatialReference::epsg_4326(),
                },
                gridbasecrs: SpatialReference::epsg_4326(),
                gridorigin: Coordinate2D::new(81., -162.),
                gridoffsets: Some(GridOffset {
                    x_step: -18.,
                    y_step: 36.
                }),
                time: Some(TimeInterval::new_instant(1_388_534_400_000).unwrap()),
            }),
            coverage
        );
    }
}
