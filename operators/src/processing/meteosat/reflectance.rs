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
use futures::{FutureExt, StreamExt, TryStreamExt};
use geoengine_datatypes::primitives::{Measurement, SpatialPartition2D};
use geoengine_datatypes::raster::{
    grid_idx_iter_2d, EmptyGrid, Grid2D, GridOrEmpty, GridShapeAccess, GridSize,
    GridSpaceToLinearSpace, NoDataValue, RasterDataType, RasterPropertiesKey, RasterTile2D,
};
use serde::{Deserialize, Serialize};

// Output type is always f32
type PixelOut = f32;
use crate::processing::meteosat::satellite::{Channel, Satellite};
use crate::processing::meteosat::{new_channel_key, new_satellite_key};
use crate::util::sunpos::SunPos;
use chrono::{DateTime, Datelike, Utc};
use RasterDataType::F32 as RasterOut;

const OUT_NO_DATA_VALUE: PixelOut = PixelOut::NAN;

/// Parameters for the `Reflectance` operator.
/// * `solar_correction` switch to enable solar correction.
/// * `force_hrv` switch to force the use of the hrv channel.
/// * `force_satellite` forces the use of the satellite with the given name.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Default, Copy)]
#[serde(rename_all = "camelCase")]
pub struct ReflectanceParams {
    pub solar_correction: bool,
    #[serde(rename = "forceHRV")]
    pub force_hrv: bool,
    pub force_satellite: Option<u8>,
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
            channel_key: new_channel_key(),
            satellite_key: new_satellite_key(),
        }
    }
}

fn channel<'a>(
    tile: &RasterTile2D<PixelOut>,
    satellite: &'a Satellite,
    params: ReflectanceParams,
    channel_key: &RasterPropertiesKey,
) -> Result<&'a Channel> {
    if params.force_hrv {
        Ok(satellite.hrv())
    } else {
        let channel_id = tile.properties.number_property::<usize>(channel_key)? - 1;
        satellite.channel(channel_id)
    }
}

fn satellite(
    params: ReflectanceParams,
    tile: &RasterTile2D<PixelOut>,
    satellite_key: &RasterPropertiesKey,
) -> Result<&'static Satellite> {
    let id = match params.force_satellite {
        Some(id) => id,
        _ => tile.properties.number_property(satellite_key)?,
    };
    Satellite::satellite_by_msg_id(id)
}

fn transform_tile(
    tile: RasterTile2D<PixelOut>,
    params: ReflectanceParams,
    satellite_key: RasterPropertiesKey,
    channel_key: RasterPropertiesKey,
) -> Result<RasterTile2D<PixelOut>> {
    match &tile.grid_array {
        GridOrEmpty::Empty(_) => Ok(RasterTile2D::new_with_properties(
            tile.time,
            tile.tile_position,
            tile.global_geo_transform,
            EmptyGrid::new(tile.grid_array.grid_shape(), OUT_NO_DATA_VALUE).into(),
            tile.properties,
        )),
        GridOrEmpty::Grid(grid) => {
            let satellite = satellite(params, &tile, &satellite_key)?;
            let channel = channel(&tile, satellite, params, &channel_key)?;
            let timestamp = tile
                .time
                .start()
                .as_utc_date_time()
                .ok_or(Error::InvalidUTCTimestamp)?;

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
            if params.solar_correction {
                let sunpos = SunPos::new(&timestamp);

                for idx in grid_idx_iter_2d(&grid.grid_shape()) {
                    let flat_idx = grid.shape.linear_space_index_unchecked(idx);
                    let pixel = grid.data[flat_idx];

                    if !grid.is_no_data(pixel) {
                        let geos_coord = tile
                            .global_geo_transform
                            .grid_idx_to_center_coordinate_2d(idx);

                        let (lat, lon) = channel.view_angle_lat_lon(geos_coord, 0.0);
                        let (_, zenith) = sunpos.solar_azimuth_zenith(lat, lon);

                        out.data[flat_idx] = (f64::from(pixel) * esd * esd
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

        let rs = src.and_then(move |tile| {
            let sk = self.satellite_key.clone();
            let ck = self.channel_key.clone();
            let params = self.params;
            tokio::task::spawn_blocking(move || transform_tile(tile, params, sk, ck)).then(
                |x| async move {
                    match x {
                        Ok(r) => r,
                        Err(e) => Err(e.into()),
                    }
                },
            )
        });

        Ok(rs.boxed())
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::{MockExecutionContext, RasterOperator, SingleRasterSource};
    use crate::processing::meteosat::reflectance::{
        Reflectance, ReflectanceParams, OUT_NO_DATA_VALUE,
    };
    use crate::processing::meteosat::test_util;
    use crate::util::Result;
    use geoengine_datatypes::primitives::Measurement;
    use geoengine_datatypes::raster::{EmptyGrid2D, Grid2D, RasterTile2D};
    use std::collections::HashMap;

    async fn process_mock(
        params: ReflectanceParams,
        channel: Option<u8>,
        satellite: Option<u8>,
        empty: bool,
        measurement: Option<Measurement>,
    ) -> Result<RasterTile2D<f32>> {
        let ctx = MockExecutionContext::default();
        test_util::process(
            || {
                let props = test_util::create_properties(channel, satellite, None, None);
                let cc = if empty { Some(Vec::new()) } else { None };

                let m = measurement.or_else(|| {
                    Some(Measurement::Continuous {
                        measurement: "radiance".into(),
                        unit: Some("W·m^(-2)·sr^(-1)·cm^(-1)".into()),
                    })
                });

                let src = test_util::create_mock_source::<u8>(props, cc, m);

                RasterOperator::boxed(Reflectance {
                    params,
                    sources: SingleRasterSource {
                        raster: src.boxed(),
                    },
                })
            },
            test_util::create_mock_query(),
            &ctx,
        )
        .await
    }

    // #[tokio::test]
    // async fn test_msg_raster() {
    //     let mut ctx = MockExecutionContext::default();
    //     let src = test_util::_create_gdal_src(&mut ctx);
    //
    //     let rad = Radiance {
    //         sources: SingleRasterSource {
    //             raster: src.boxed(),
    //         },
    //         params: RadianceParams {},
    //     };
    //
    //     let result = test_util::process(
    //         move || {
    //             RasterOperator::boxed(Reflectance {
    //                 params: ReflectanceParams::default(),
    //                 sources: SingleRasterSource {
    //                     raster: RasterOperator::boxed(rad),
    //                 },
    //             })
    //         },
    //         test_util::_create_gdal_query(),
    //         &ctx,
    //     )
    //     .await;
    //     assert!(result.as_ref().is_ok());
    // }

    #[tokio::test]
    async fn test_empty_ok() {
        let result = process_mock(ReflectanceParams::default(), Some(1), Some(1), true, None).await;

        assert!(result.is_ok());
        assert!(geoengine_datatypes::util::test::eq_with_no_data(
            &result.as_ref().unwrap().grid_array,
            &EmptyGrid2D::new([3, 2].into(), OUT_NO_DATA_VALUE,).into()
        ));
    }

    #[tokio::test]
    async fn test_ok_no_solar_correction() {
        let result =
            process_mock(ReflectanceParams::default(), Some(1), Some(1), false, None).await;

        assert!(result.is_ok());
        assert!(geoengine_datatypes::util::test::eq_with_no_data(
            &result.as_ref().unwrap().grid_array,
            &Grid2D::new(
                [3, 2].into(),
                vec![
                    0.046_567_827_f32,
                    0.093_135_655_f32,
                    0.139_703_48_f32,
                    0.186_271_31_f32,
                    0.232_839_14_f32,
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
        let params = ReflectanceParams {
            force_satellite: Some(4),
            ..Default::default()
        };
        let result = process_mock(params, Some(1), Some(1), false, None).await;

        assert!(result.is_ok());
        assert!(geoengine_datatypes::util::test::eq_with_no_data(
            &result.as_ref().unwrap().grid_array,
            &Grid2D::new(
                [3, 2].into(),
                vec![
                    0.046_542_14_f32,
                    0.093_084_28_f32,
                    0.139_626_43_f32,
                    0.186_168_57_f32,
                    0.232_710_7_f32,
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
        let params = ReflectanceParams {
            force_hrv: true,
            ..Default::default()
        };
        let result = process_mock(params, Some(1), Some(1), false, None).await;

        assert!(result.is_ok());
        assert!(geoengine_datatypes::util::test::eq_with_no_data(
            &result.as_ref().unwrap().grid_array,
            &Grid2D::new(
                [3, 2].into(),
                vec![
                    0.038_567_86_f32,
                    0.077_135_72_f32,
                    0.115_703_575_f32,
                    0.154_271_44_f32,
                    0.192_839_3_f32,
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
        let params = ReflectanceParams {
            solar_correction: true,
            ..Default::default()
        };
        let result = process_mock(params, Some(1), Some(1), false, None).await;

        assert!(result.is_ok());
        assert!(geoengine_datatypes::util::test::eq_with_no_data(
            &result.as_ref().unwrap().grid_array,
            &Grid2D::new(
                [3, 2].into(),
                vec![
                    0.268_173_43_f32,
                    0.536_346_85_f32,
                    0.804_520_3_f32,
                    1.072_693_7_f32,
                    1.340_867_2_f32,
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
        let params = ReflectanceParams {
            force_satellite: Some(42),
            ..Default::default()
        };
        let result = process_mock(params, Some(1), Some(1), false, None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_missing_satellite() {
        let params = ReflectanceParams::default();
        let result = process_mock(params, Some(1), None, false, None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_invalid_satellite() {
        let params = ReflectanceParams::default();
        let result = process_mock(params, Some(42), None, false, None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_missing_channel() {
        let params = ReflectanceParams::default();
        let result = process_mock(params, None, Some(1), false, None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_invalid_channel() {
        let params = ReflectanceParams::default();
        let result = process_mock(params, Some(42), Some(1), false, None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_invalid_measurement_unitless() {
        let params = ReflectanceParams::default();
        let result =
            process_mock(params, Some(1), Some(1), false, Some(Measurement::Unitless)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_invalid_measurement_continuous() {
        let params = ReflectanceParams::default();
        let result = process_mock(
            params,
            Some(1),
            Some(1),
            false,
            Some(Measurement::Continuous {
                measurement: "invalid".into(),
                unit: None,
            }),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_invalid_measurement_classification() {
        let params = ReflectanceParams::default();
        let result = process_mock(
            params,
            Some(1),
            Some(1),
            false,
            Some(Measurement::Classification {
                measurement: "invalid".into(),
                classes: HashMap::new(),
            }),
        )
        .await;
        assert!(result.is_err());
    }
}
