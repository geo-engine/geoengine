use std::{convert::TryInto, fmt::Debug, fs::File, io::BufReader, path::PathBuf};

use geo::{intersects::Intersects, prelude::Contains};
use geoengine_datatypes::{
    primitives::{Measurement::Continuous, TimeInstance},
    raster::{GeoTransform, RasterDataType},
    spatial_reference::SpatialReference,
};
use serde::{Deserialize, Serialize};

use crate::{
    engine::{MetaData, QueryRectangle, RasterResultDescriptor},
    error,
    source::gdal_source::{GdalLoadingInfoPart, GdalLoadingInfoPartIterator},
    util::Result,
};

use super::FeatureCollection as StacFeatureCollection;

use crate::source::gdal_source::{GdalDatasetParameters, GdalLoadingInfo};

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SentinelBandSpec {
    pub eo_band_id: String,
    // pub pixel_size: f64,
    pub no_data_value: Option<f64>,
    pub data_type: RasterDataType,
}

impl TryInto<GdalStacTiles> for GdalStacTilesReading {
    type Error = error::Error;

    fn try_into(self) -> Result<GdalStacTiles, Self::Error> {
        let mut sf_file = self.file_path;
        sf_file.push(self.stac_feature_collection_file_name);
        let sfc = serde_json::from_reader(BufReader::new(File::open(sf_file)?))?;
        Ok({
            GdalStacTiles {
                stac_tile_features: sfc,
                band_spec: self.band_spec,
                proj_epsg: self.proj_epsg,
            }
        })
    }
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GdalStacTilesReading {
    pub file_path: PathBuf,
    pub stac_feature_collection_file_name: String,
    pub band_spec: SentinelBandSpec,
    pub proj_epsg: u32,
}

impl MetaData<GdalLoadingInfo, RasterResultDescriptor> for GdalStacTilesReading {
    fn loading_info(&self, query: QueryRectangle) -> Result<GdalLoadingInfo> {
        let gst: GdalStacTiles = self.clone().try_into()?;
        gst.loading_info(query)
    }
    fn result_descriptor(&self) -> Result<RasterResultDescriptor> {
        let measurement = Continuous {
            measurement: "reflectance".to_string(),
            unit: None,
        };

        Ok(RasterResultDescriptor {
            data_type: self.band_spec.data_type,
            spatial_reference: SpatialReference::epsg(self.proj_epsg).into(),
            measurement,
            no_data_value: self.band_spec.no_data_value,
        })
    }

    fn box_clone(&self) -> Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor>> {
        Box::new(self.clone())
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GdalStacTiles {
    pub stac_tile_features: StacFeatureCollection,
    pub band_spec: SentinelBandSpec,
    pub proj_epsg: u32,
}

impl MetaData<GdalLoadingInfo, RasterResultDescriptor> for GdalStacTiles {
    fn loading_info(&self, query: QueryRectangle) -> Result<GdalLoadingInfo> {
        let geo_bbox: geo::Rect<f64> = query.bbox.into();

        let t_start = query.time_interval.start().inner();
        let t_end = query.time_interval.end().inner();
        let time_range = t_start..t_end;

        let mut tiles: Vec<GdalLoadingInfoPart> = self
            .stac_tile_features
            .features
            .iter()
            .filter(|&feature| {
                if let Some(tile_epsg) = feature.properties.proj_epsg {
                    tile_epsg == self.proj_epsg
                } else {
                    false
                }
            })
            .filter(|&feature| {
                let tn = feature.properties.datetime.timestamp_millis();
                tn == t_start || time_range.contains(&tn)
            })
            .filter_map(|f| {
                let ass = f
                    .assets
                    .get(&self.band_spec.eo_band_id)
                    .filter(|&ass| {
                        ass.native_bbox()
                            .map_or(false, |b| b.intersects(&geo_bbox) || b.contains(&geo_bbox))
                    })
                    .map(|a| (f.properties.datetime, a.clone()));

                ass
            })
            .map(|(dt, ass)| -> GdalLoadingInfoPart {
                let file_path = PathBuf::from("/vsicurl/".to_owned() + &ass.href);
                let geo_transform =
                    GeoTransform::from(ass.gdal_geotransform().expect("checked in filter"));
                let bbox = ass.native_bbox().expect("checked in filer").into();
                GdalLoadingInfoPart {
                    time: TimeInstance::from(dt).into(), //TODO: we need to a) add a duration and b) allow multiple tiles with the same time
                    params: GdalDatasetParameters {
                        file_path,
                        rasterband_channel: 1,
                        geo_transform,
                        bbox,
                        file_not_found_handling: crate::source::FileNotFoundHandling::NoData,
                        no_data_value: self.band_spec.no_data_value,
                    },
                }
            })
            .collect();

        tiles.sort_unstable_by_key(|f| f.time.start()); // would be nice to have an index for this

        Ok(GdalLoadingInfo {
            info: GdalLoadingInfoPartIterator::Static {
                parts: tiles.into_iter(),
            },
        })
    }

    fn result_descriptor(&self) -> Result<RasterResultDescriptor> {
        let measurement = Continuous {
            measurement: "reflectance".to_string(),
            unit: None,
        };

        Ok(RasterResultDescriptor {
            data_type: self.band_spec.data_type,
            spatial_reference: SpatialReference::epsg(self.proj_epsg).into(),
            measurement,
            no_data_value: self.band_spec.no_data_value,
        })
    }

    fn box_clone(&self) -> Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor>> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use futures::StreamExt;
    use geoengine_datatypes::{
        dataset::{DatasetId, InternalDatasetId},
        primitives::{BoundingBox2D, SpatialResolution, TimeInterval},
        raster::{GridShape2D, GridSize, RasterTile2D},
        util::Identifier,
    };

    use crate::{
        engine::{MockExecutionContext, MockQueryContext, QueryProcessor, RasterOperator},
        source::{GdalSource, GdalSourceParameters},
    };

    use super::StacFeatureCollection;
    use super::*;

    #[allow(clippy::missing_panics_doc)]
    pub fn create_stac_meta_data() -> GdalStacTilesReading {
        let t_folder = env!("CARGO_MANIFEST_DIR").to_string() + "/test-data/raster/stac/";
        let path = PathBuf::from_str(&t_folder).unwrap();
        let sbs = SentinelBandSpec {
            eo_band_id: "B03".to_string(),
            no_data_value: Some(0.),
            data_type: RasterDataType::U16,
        };

        GdalStacTilesReading {
            file_path: path,
            stac_feature_collection_file_name: "s2_items_1_1.json".to_string(),
            band_spec: sbs,
            proj_epsg: 32617,
        }
    }

    // TODO: move test helper somewhere else?
    pub fn add_stac_dataset(ctx: &mut MockExecutionContext) -> DatasetId {
        let id: DatasetId = InternalDatasetId::new().into();
        ctx.add_meta_data(id.clone(), Box::new(create_stac_meta_data()));
        id
    }

    async fn query_gdal_source(
        exe_ctx: &mut MockExecutionContext,
        query_ctx: &MockQueryContext,
        id: DatasetId,
        output_shape: GridShape2D,
        output_bounds: BoundingBox2D,
        time_interval: TimeInterval,
    ) -> Vec<Result<RasterTile2D<u16>>> {
        let op = GdalSource {
            params: GdalSourceParameters {
                dataset: id.clone(),
            },
        }
        .boxed();

        let x_query_resolution = output_bounds.size_x() / output_shape.axis_size_x() as f64;
        let y_query_resolution = output_bounds.size_y() / output_shape.axis_size_y() as f64;
        let spatial_resolution =
            SpatialResolution::new_unchecked(x_query_resolution, y_query_resolution);

        let o = op.initialize(exe_ctx).unwrap();

        o.query_processor()
            .unwrap()
            .get_u16()
            .unwrap()
            .query(
                QueryRectangle {
                    bbox: output_bounds,
                    time_interval,
                    spatial_resolution,
                },
                query_ctx,
            )
            .unwrap()
            .collect()
            .await
    }

    #[test]
    fn stac_to_meta_data() {
        let str = include_str!("../../../test-data/raster/stac/s2_items_1_1.json");
        let sfc: StacFeatureCollection = serde_json::from_str(str).unwrap();

        let sbs = SentinelBandSpec {
            eo_band_id: "B03".to_string(),
            no_data_value: Some(0.),
            data_type: RasterDataType::U16,
        };

        let gmdts = GdalStacTiles {
            stac_tile_features: sfc,
            band_spec: sbs,
            proj_epsg: 32617,
        };

        let query = QueryRectangle {
            bbox: BoundingBox2D::new((300000.0, 3390240.0).into(), (409800.0, 3500040.0).into())
                .unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution { x: 10., y: -10. },
        };

        let x = gmdts.loading_info(query).unwrap();
        let parts = match x.info {
            GdalLoadingInfoPartIterator::Static { parts } => parts,
            GdalLoadingInfoPartIterator::Regular { .. } => {
                panic!()
            }
        };

        assert_eq!(parts.len(), 1)
    }

    #[test]
    fn stac_to_meta_data_reading() {
        let t_folder = env!("CARGO_MANIFEST_DIR").to_string() + "/test-data/raster/stac/";
        let path = PathBuf::from_str(&t_folder).unwrap();

        let sbs = SentinelBandSpec {
            eo_band_id: "B03".to_string(),
            no_data_value: Some(0.),
            data_type: RasterDataType::U16,
        };

        let gmdts = GdalStacTilesReading {
            file_path: path,
            stac_feature_collection_file_name: "s2_items_1_1.json".to_string(),
            band_spec: sbs,
            proj_epsg: 32617,
        };

        let query = QueryRectangle {
            bbox: BoundingBox2D::new((300000.0, 3390240.0).into(), (409800.0, 3500040.0).into())
                .unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution { x: 10., y: -10. },
        };

        let x = gmdts.loading_info(query).unwrap();
        let parts = match x.info {
            GdalLoadingInfoPartIterator::Static { parts } => parts,
            GdalLoadingInfoPartIterator::Regular { .. } => {
                panic!()
            }
        };

        assert_eq!(parts.len(), 1)
    }

    #[test]
    fn stac_to_gdal_source() {
        let t_folder = env!("CARGO_MANIFEST_DIR").to_string() + "/test-data/raster/stac/";
        let path = PathBuf::from_str(&t_folder).unwrap();

        let sbs = SentinelBandSpec {
            eo_band_id: "B03".to_string(),
            no_data_value: Some(0.),
            data_type: RasterDataType::U16,
        };

        let gmdts = GdalStacTilesReading {
            file_path: path,
            stac_feature_collection_file_name: "s2_items_1_1.json".to_string(),
            band_spec: sbs,
            proj_epsg: 32617,
        };

        let query = QueryRectangle {
            bbox: BoundingBox2D::new((300000.0, 3390240.0).into(), (409800.0, 3500040.0).into())
                .unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution { x: 10., y: -10. },
        };

        let x = gmdts.loading_info(query).unwrap();
        let parts = match x.info {
            GdalLoadingInfoPartIterator::Static { parts } => parts,
            GdalLoadingInfoPartIterator::Regular { .. } => {
                panic!()
            }
        };
        dbg!(&parts);
        assert_eq!(parts.len(), 1)
    }

    #[tokio::test]
    async fn test_query_source() {
        let mut exe_ctx = MockExecutionContext::default();
        let query_ctx = MockQueryContext::default();
        let id = add_stac_dataset(&mut exe_ctx);

        let output_shape: GridShape2D = [1024, 1024].into();
        let output_bounds =
            BoundingBox2D::new((300000.0, 3390240.0).into(), (409800.0, 3500040.0).into()).unwrap();
        // let output_bounds = BoundingBox2D::new((499980., 7590240.).into(), (499999.0, 7590260.0).into()).unwrap();
        let time_interval = TimeInterval::default();

        let c = query_gdal_source(
            &mut exe_ctx,
            &query_ctx,
            id,
            output_shape,
            output_bounds,
            time_interval,
        )
        .await;
        let c: Vec<RasterTile2D<u16>> = c.into_iter().map(Result::unwrap).collect();
        assert_eq!(c.len(), 9);
        assert!(!c[0].is_empty())
    }
}
