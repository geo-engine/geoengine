use crate::engine::{
    ExecutionContext, InitializedRasterOperator, Operator, QueryContext, QueryProcessor,
    RasterOperator, RasterQueryProcessor, RasterQueryRectangle, RasterResultDescriptor,
    SingleRasterSource, TypedRasterQueryProcessor,
};
use crate::util::Result;
use async_trait::async_trait;
use TypedRasterQueryProcessor::F32 as QueryProcessorOut;

use crate::error::Error;
use futures::stream::BoxStream;
use futures::StreamExt;
use geoengine_datatypes::primitives::{Measurement, SpatialPartition2D};
use geoengine_datatypes::raster::{
    grid_idx_iter_2d, EmptyGrid, Grid2D, GridOrEmpty, GridShapeAccess, GridSize,
    GridSpaceToLinearSpace, NoDataValue, RasterDataType, RasterPropertiesKey, RasterTile2D,
};
use serde::{Deserialize, Serialize};

// Output type is always f32
type PixelOut = f32;
use crate::processing::meteosat::satellite::{Channel, Satellite};
use crate::util::sunpos::SunPos;
use chrono::{DateTime, Datelike, TimeZone, Utc};
use RasterDataType::F32 as RasterOut;

const OUT_NO_DATA_VALUE: PixelOut = PixelOut::NAN;

/// Parameters for the `Reflectance` operator.
/// * `solar_correction` switch to enable solar correction.
/// * `force_hrv` switch to force the use of the hrv channel.
/// * `force_satellite` forces the use of the satellite with the given name.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ReflectanceParams {
    pub solar_correction: bool,
    #[serde(rename = "forceHRV")]
    pub force_hrv: bool,
    pub force_satellite: String,
}

impl Default for ReflectanceParams {
    fn default() -> Self {
        ReflectanceParams {
            solar_correction: false,
            force_hrv: false,
            force_satellite: "".into(),
        }
    }
}

/// The reflectance operator consumes an MSG image preprocessed
/// via the radiance operator and computes the reflectance value
/// from a given radiance raster.
pub type Reflectance = Operator<ReflectanceParams, SingleRasterSource>;

pub struct InitializedReflectance {
    result_descriptor: RasterResultDescriptor,
    source: Box<dyn InitializedRasterOperator>,
    params: ReflectanceParams,
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for Reflectance {
    async fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let input = self.sources.raster.initialize(context).await?;

        let in_desc = input.result_descriptor();

        match &in_desc.measurement {
            Measurement::Continuous {
                measurement: m,
                unit: _,
            } if m != "radiance" => {
                return Err(Error::InvalidMeasurement {
                    expected: "radiance".into(),
                    found: m.clone(),
                })
            }
            Measurement::Classification {
                measurement: m,
                classes: _,
            } => {
                return Err(Error::InvalidMeasurement {
                    expected: "radiance".into(),
                    found: m.clone(),
                })
            }
            Measurement::Unitless => {
                return Err(Error::InvalidMeasurement {
                    expected: "radiance".into(),
                    found: "unitless".into(),
                })
            }
            // OK Case
            Measurement::Continuous {
                measurement: _,
                unit: _,
            } => {}
        }

        let out_desc = RasterResultDescriptor {
            spatial_reference: in_desc.spatial_reference,
            data_type: RasterOut,
            measurement: Measurement::Continuous {
                measurement: "reflectance".into(),
                unit: Some("fraction".into()),
            },
            no_data_value: Some(f64::from(OUT_NO_DATA_VALUE)),
        };

        let initialized_operator = InitializedReflectance {
            result_descriptor: out_desc,
            source: input,
            params: self.params,
        };

        Ok(initialized_operator.boxed())
    }
}

impl InitializedRasterOperator for InitializedReflectance {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor, Error> {
        let q = self.source.query_processor()?;

        // We only support f32 input rasters
        if let TypedRasterQueryProcessor::F32(p) = q {
            Ok(QueryProcessorOut(Box::new(ReflectanceProcessor::new(
                p,
                self.params.clone(),
            ))))
        } else {
            Err(Error::InvalidRasterDataType)
        }
    }
}

struct ReflectanceProcessor<Q>
where
    Q: RasterQueryProcessor<RasterType = PixelOut>,
{
    source: Q,
    params: ReflectanceParams,
    channel_key: RasterPropertiesKey,
    timestamp_key: RasterPropertiesKey,
    satellite_key: RasterPropertiesKey,
}

impl<Q> ReflectanceProcessor<Q>
where
    Q: RasterQueryProcessor<RasterType = PixelOut>,
{
    pub fn new(source: Q, params: ReflectanceParams) -> Self {
        Self {
            source,
            params,
            channel_key: RasterPropertiesKey {
                domain: Some("msg".into()),
                key: "Channel".into(),
            },
            timestamp_key: RasterPropertiesKey {
                domain: Some("msg".into()),
                key: "TimeStamp".into(),
            },
            satellite_key: RasterPropertiesKey {
                domain: Some("msg".into()),
                key: "Satellite".into(),
            },
        }
    }

    fn get_satellite(&self, tile: &RasterTile2D<PixelOut>) -> Result<&'static Satellite> {
        if self.params.force_satellite.is_empty() {
            let satellite_id = tile.properties.get_number_property(&self.satellite_key)?;
            Satellite::satellite_by_msg_id(satellite_id)
        } else {
            Satellite::satellite_by_name(self.params.force_satellite.as_str())
        }
    }

    fn get_channel<'a>(
        &self,
        tile: &RasterTile2D<PixelOut>,
        satellite: &'a Satellite,
    ) -> Result<&'a Channel> {
        if self.params.force_hrv {
            Ok(satellite.get_hrv())
        } else {
            let channel_id = tile
                .properties
                .get_number_property::<usize>(&self.channel_key)?
                - 1;
            satellite.get_channel(channel_id)
        }
    }

    fn get_timestamp(&self, tile: &RasterTile2D<PixelOut>) -> Result<DateTime<Utc>> {
        let time = tile.properties.get_string_property(&self.timestamp_key)?;
        Ok(Utc.datetime_from_str(time.as_str(), "%Y%m%d%H%M")?)
    }
}

fn calculate_esd(timestamp: &DateTime<Utc>) -> f64 {
    let perihelion = f64::from(timestamp.ordinal()) - 3.0;
    let e = 0.0167;
    let theta = std::f64::consts::TAU * (perihelion / 365.0);
    1.0 - e * theta.cos()
}

#[async_trait]
impl<Q> QueryProcessor for ReflectanceProcessor<Q>
where
    Q: QueryProcessor<Output = RasterTile2D<PixelOut>, SpatialBounds = SpatialPartition2D>,
{
    type Output = RasterTile2D<PixelOut>;
    type SpatialBounds = SpatialPartition2D;

    async fn query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let src = self.source.query(query, ctx).await?;

        let rs = src.map(move |tile| {
            let tile = tile?;
            match &tile.grid_array {
                GridOrEmpty::Empty(_) => Ok(RasterTile2D::new_with_properties(
                    tile.time,
                    tile.tile_position,
                    tile.global_geo_transform,
                    EmptyGrid::new(tile.grid_array.grid_shape(), OUT_NO_DATA_VALUE).into(),
                    tile.properties,
                )),
                GridOrEmpty::Grid(grid) => {
                    let satellite = self.get_satellite(&tile)?;
                    let channel = self.get_channel(&tile, satellite)?;
                    let timestamp = self.get_timestamp(&tile)?;

                    // get extra terrestrial solar radiation (...) and ESD (solar position)
                    let etsr = channel.etsr / std::f64::consts::PI;
                    let esd = calculate_esd(&timestamp);

                    // Create result raster
                    let mut out = Grid2D::new(
                        grid.grid_shape(),
                        vec![OUT_NO_DATA_VALUE; grid.number_of_elements()],
                        Some(OUT_NO_DATA_VALUE),
                    )
                    .expect("raster creation must succeed");

                    // Apply solar correction
                    if self.params.solar_correction {
                        let sunpos = SunPos::new(&timestamp);

                        for idx in grid_idx_iter_2d(&grid.grid_shape()) {
                            let geos_coord = tile
                                .global_geo_transform
                                .grid_idx_to_center_coordinate_2d(idx);

                            let (lat, lon) = channel.view_angle_lat_lon(geos_coord, 0.0);
                            let (_, zenith) = sunpos.solar_azimuth_zenith(lat, lon);

                            let idx = grid.shape.linear_space_index_unchecked(idx);
                            let pixel = grid.data[idx];
                            if !grid.is_no_data(pixel) {
                                out.data[idx] = (f64::from(pixel) * esd * esd
                                    / (etsr * zenith.min(80.0).to_radians().cos()))
                                    as PixelOut;
                            }
                        }
                    } else {
                        for (idx, &pixel) in grid.data.iter().enumerate() {
                            if !grid.is_no_data(pixel) {
                                out.data[idx] = (f64::from(pixel) * esd * esd / etsr) as PixelOut;
                            }
                        }
                    }
                    Ok(RasterTile2D::new_with_properties(
                        tile.time,
                        tile.tile_position,
                        tile.global_geo_transform,
                        out.into(),
                        tile.properties,
                    ))
                }
            }
        });
        Ok(rs.boxed())
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::{
        MockExecutionContext, MockQueryContext, QueryProcessor, RasterOperator,
        RasterQueryRectangle, RasterResultDescriptor, SingleRasterSource,
    };
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use crate::processing::meteosat::reflectance::{
        PixelOut, Reflectance, ReflectanceParams, OUT_NO_DATA_VALUE,
    };
    use crate::util::Result;
    use futures::StreamExt;
    use geoengine_datatypes::primitives::{
        Measurement, SpatialPartition2D, SpatialResolution, TimeInterval,
    };
    use geoengine_datatypes::raster::{
        EmptyGrid2D, Grid2D, GridOrEmpty, RasterDataType, RasterProperties, RasterPropertiesEntry,
        RasterPropertiesKey, RasterTile2D, TileInformation,
    };
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use num_traits::AsPrimitive;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_empty_ok() {
        let props = create_properties(Some(1), Some(1), Some("202108040838"));
        let params = ReflectanceParams::default();
        let res = process(props, params, true, None).await.unwrap();

        assert!(geoengine_datatypes::util::test::eq_with_no_data(
            &res.grid_array,
            &EmptyGrid2D::new([3, 2].into(), OUT_NO_DATA_VALUE,).into()
        ));
    }

    #[tokio::test]
    async fn test_ok_no_solar_correction() {
        let props = create_properties(Some(1), Some(1), Some("202108040838"));
        let params = ReflectanceParams::default();
        let res = process(props, params, false, None).await.unwrap();

        assert!(geoengine_datatypes::util::test::eq_with_no_data(
            &res.grid_array,
            &Grid2D::new(
                [3, 2].into(),
                vec![
                    0.049_564_075_f32,
                    0.099_128_15_f32,
                    0.148_692_22_f32,
                    0.198_256_3_f32,
                    0.247_820_36_f32,
                    OUT_NO_DATA_VALUE,
                ],
                Some(OUT_NO_DATA_VALUE),
            )
            .unwrap()
            .into()
        ));
    }

    #[tokio::test]
    async fn test_ok_force_satellite() {
        let props = create_properties(Some(1), Some(1), Some("202108040838"));
        let params = ReflectanceParams {
            force_satellite: "Meteosat-11".into(),
            ..Default::default()
        };
        let res = process(props, params, false, None).await.unwrap();

        assert!(geoengine_datatypes::util::test::eq_with_no_data(
            &res.grid_array,
            &Grid2D::new(
                [3, 2].into(),
                vec![
                    0.049_536_735_f32,
                    0.099_073_47_f32,
                    0.148_610_2_f32,
                    0.198_146_94_f32,
                    0.247_683_67_f32,
                    OUT_NO_DATA_VALUE
                ],
                Some(OUT_NO_DATA_VALUE),
            )
            .unwrap()
            .into()
        ));
    }

    #[tokio::test]
    async fn test_ok_force_hrv() {
        let props = create_properties(Some(1), Some(1), Some("202108040838"));
        let params = ReflectanceParams {
            force_hrv: true,
            ..Default::default()
        };
        let res = process(props, params, false, None).await.unwrap();

        assert!(geoengine_datatypes::util::test::eq_with_no_data(
            &res.grid_array,
            &Grid2D::new(
                [3, 2].into(),
                vec![
                    0.041_049_376,
                    0.082_098_75,
                    0.123_148_13,
                    0.164_197_5,
                    0.205_246_88,
                    OUT_NO_DATA_VALUE
                ],
                Some(OUT_NO_DATA_VALUE),
            )
            .unwrap()
            .into()
        ));
    }

    #[tokio::test]
    async fn test_ok_solar_correction() {
        let props = create_properties(Some(1), Some(1), Some("202108040838"));
        let params = ReflectanceParams {
            solar_correction: true,
            ..Default::default()
        };
        let res = process(props, params, false, None).await.unwrap();

        assert!(geoengine_datatypes::util::test::eq_with_no_data(
            &res.grid_array,
            &Grid2D::new(
                [3, 2].into(),
                vec![
                    0.285_428_14_f32,
                    0.570_856_3_f32,
                    0.856_284_4_f32,
                    1.141_712_5_f32,
                    1.427_140_6_f32,
                    OUT_NO_DATA_VALUE
                ],
                Some(OUT_NO_DATA_VALUE),
            )
            .unwrap()
            .into()
        ));
    }

    #[tokio::test]
    async fn test_invalid_force_satellite() {
        let props = create_properties(Some(1), Some(1), Some("202108040838"));
        let params = ReflectanceParams {
            force_satellite: "Meteosat-42".into(),
            ..Default::default()
        };
        let res = process(props, params, false, None).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_missing_timestamp() {
        let props = create_properties(Some(1), Some(1), None);
        let params = ReflectanceParams::default();
        let res = process(props, params, false, None).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_invalid_timestamp() {
        let props = create_properties(Some(1), Some(1), Some("2021-08-04 08:38"));
        let params = ReflectanceParams::default();
        let res = process(props, params, false, None).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_missing_satellite() {
        let props = create_properties(Some(1), None, Some("202108040838"));
        let params = ReflectanceParams::default();
        let res = process(props, params, false, None).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_invalid_satellite() {
        let props = create_properties(Some(1), Some(42), Some("202108040838"));
        let params = ReflectanceParams::default();
        let res = process(props, params, false, None).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_missing_channel() {
        let props = create_properties(None, Some(1), Some("202108040838"));
        let params = ReflectanceParams::default();
        let res = process(props, params, false, None).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_invalid_channel() {
        let props = create_properties(Some(42), Some(1), Some("202108040838"));
        let params = ReflectanceParams::default();
        let res = process(props, params, false, None).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_invalid_measurement_unitless() {
        let props = create_properties(Some(1), Some(1), Some("202108040838"));
        let params = ReflectanceParams::default();
        let res = process(props, params, false, Some(Measurement::Unitless)).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_invalid_measurement_continuous() {
        let props = create_properties(Some(1), Some(1), Some("202108040838"));
        let params = ReflectanceParams::default();
        let res = process(
            props,
            params,
            false,
            Some(Measurement::Continuous {
                measurement: "invalid".into(),
                unit: None,
            }),
        )
        .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_invalid_measurement_classification() {
        let props = create_properties(Some(1), Some(1), Some("202108040838"));
        let params = ReflectanceParams::default();
        let res = process(
            props,
            params,
            false,
            Some(Measurement::Classification {
                measurement: "invalid".into(),
                classes: HashMap::new(),
            }),
        )
        .await;
        assert!(res.is_err());
    }

    fn create_properties(
        channel: Option<u8>,
        satellite: Option<u8>,
        timestamp: Option<&str>,
    ) -> RasterProperties {
        let mut props = RasterProperties::default();

        if let Some(v) = channel {
            props.properties_map.insert(
                RasterPropertiesKey {
                    domain: Some("msg".into()),
                    key: "Channel".into(),
                },
                RasterPropertiesEntry::Number(v.as_()),
            );
        }

        if let Some(v) = satellite {
            props.properties_map.insert(
                RasterPropertiesKey {
                    domain: Some("msg".into()),
                    key: "Satellite".into(),
                },
                RasterPropertiesEntry::Number(v.as_()),
            );
        }

        if let Some(v) = timestamp {
            props.properties_map.insert(
                RasterPropertiesKey {
                    domain: Some("msg".into()),
                    key: "TimeStamp".into(),
                },
                RasterPropertiesEntry::String(v.into()),
            );
        }
        props
    }

    async fn process(
        props: RasterProperties,
        params: ReflectanceParams,
        empty_input: bool,
        measurement: Option<Measurement>,
    ) -> Result<RasterTile2D<PixelOut>> {
        let input = make_raster(props, empty_input, measurement);

        let op = Reflectance {
            sources: SingleRasterSource { raster: input },
            params,
        }
        .boxed()
        .initialize(&MockExecutionContext::default())
        .await?;

        let processor = op.query_processor().unwrap().get_f32().unwrap();

        let ctx = MockQueryContext::new(1);
        let result_stream = processor
            .query(
                RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (0., 4.).into(),
                        (3., 0.).into(),
                    ),
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &ctx,
            )
            .await
            .unwrap();

        let mut result: Vec<Result<RasterTile2D<PixelOut>>> = result_stream.collect().await;
        result.pop().unwrap()
    }

    fn make_raster(
        props: RasterProperties,
        empty_input: bool,
        measurement: Option<Measurement>,
    ) -> Box<dyn RasterOperator> {
        let no_data_value = Some(0);

        let raster = if empty_input {
            GridOrEmpty::Empty(EmptyGrid2D::new([3, 2].into(), no_data_value.unwrap()))
        } else {
            let data = vec![1, 2, 3, 4, 5, no_data_value.unwrap()];
            GridOrEmpty::Grid(Grid2D::new([3, 2].into(), data, no_data_value).unwrap())
        };

        let raster_tile = RasterTile2D::new_with_tile_info_and_properties(
            TimeInterval::default(),
            TileInformation {
                global_tile_position: [-1, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
                global_geo_transform: Default::default(),
            },
            raster,
            props,
        );

        MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::F32,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: measurement.unwrap_or_else(|| Measurement::Continuous {
                        measurement: "radiance".into(),
                        unit: Some("W·m^(-2)·sr^(-1)·cm^(-1)".into()),
                    }),
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed()
    }
}
