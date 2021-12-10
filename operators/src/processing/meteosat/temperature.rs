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
use futures::{StreamExt, TryStreamExt};
use geoengine_datatypes::primitives::{Measurement, SpatialPartition2D};
use geoengine_datatypes::raster::{
    EmptyGrid, Grid2D, GridShapeAccess, GridSize, NoDataValue, Pixel, RasterDataType,
    RasterPropertiesKey, RasterTile2D,
};
use serde::{Deserialize, Serialize};

// Output type is always f32
type PixelOut = f32;
use crate::processing::meteosat::satellite::{Channel, Satellite};
use crate::processing::meteosat::{
    new_channel_key, new_offset_key, new_satellite_key, new_slope_key,
};
use RasterDataType::F32 as RasterOut;

const OUT_NO_DATA_VALUE: PixelOut = PixelOut::NAN;

/// Parameters for the `Temperature` operator.
/// * `force_satellite` forces the use of the satellite with the given name.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct TemperatureParams {
    force_satellite: Option<u8>,
}

/// The temperature operator approximates BT from
/// the raw MSG rasters.
pub type Temperature = Operator<TemperatureParams, SingleRasterSource>;

pub struct InitializedTemperature {
    result_descriptor: RasterResultDescriptor,
    source: Box<dyn InitializedRasterOperator>,
    params: TemperatureParams,
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for Temperature {
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
            } if m != "raw" => {
                return Err(Error::InvalidMeasurement {
                    expected: "raw".into(),
                    found: m.clone(),
                })
            }
            Measurement::Classification {
                measurement: m,
                classes: _,
            } => {
                return Err(Error::InvalidMeasurement {
                    expected: "raw".into(),
                    found: m.clone(),
                })
            }
            Measurement::Unitless => {
                return Err(Error::InvalidMeasurement {
                    expected: "raw".into(),
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
                measurement: "temperature".into(),
                unit: Some("k".into()),
            },
            no_data_value: Some(f64::from(OUT_NO_DATA_VALUE)),
        };

        let initialized_operator = InitializedTemperature {
            result_descriptor: out_desc,
            source: input,
            params: self.params,
        };

        Ok(initialized_operator.boxed())
    }
}

impl InitializedRasterOperator for InitializedTemperature {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor, Error> {
        let q = self.source.query_processor()?;

        Ok(match q {
            TypedRasterQueryProcessor::U8(p) => {
                QueryProcessorOut(Box::new(TemperatureProcessor::new(p, self.params.clone())))
            }
            TypedRasterQueryProcessor::U16(p) => {
                QueryProcessorOut(Box::new(TemperatureProcessor::new(p, self.params.clone())))
            }
            TypedRasterQueryProcessor::U32(p) => {
                QueryProcessorOut(Box::new(TemperatureProcessor::new(p, self.params.clone())))
            }
            TypedRasterQueryProcessor::U64(p) => {
                QueryProcessorOut(Box::new(TemperatureProcessor::new(p, self.params.clone())))
            }
            TypedRasterQueryProcessor::I8(p) => {
                QueryProcessorOut(Box::new(TemperatureProcessor::new(p, self.params.clone())))
            }
            TypedRasterQueryProcessor::I16(p) => {
                QueryProcessorOut(Box::new(TemperatureProcessor::new(p, self.params.clone())))
            }
            TypedRasterQueryProcessor::I32(p) => {
                QueryProcessorOut(Box::new(TemperatureProcessor::new(p, self.params.clone())))
            }
            TypedRasterQueryProcessor::I64(p) => {
                QueryProcessorOut(Box::new(TemperatureProcessor::new(p, self.params.clone())))
            }
            TypedRasterQueryProcessor::F32(p) => {
                QueryProcessorOut(Box::new(TemperatureProcessor::new(p, self.params.clone())))
            }
            TypedRasterQueryProcessor::F64(p) => {
                QueryProcessorOut(Box::new(TemperatureProcessor::new(p, self.params.clone())))
            }
        })
    }
}

struct TemperatureProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
{
    source: Q,
    params: TemperatureParams,
    satellite_key: RasterPropertiesKey,
    channel_key: RasterPropertiesKey,
    offset_key: RasterPropertiesKey,
    slope_key: RasterPropertiesKey,
}

impl<Q, P> TemperatureProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
{
    pub fn new(source: Q, params: TemperatureParams) -> Self {
        Self {
            source,
            params,
            satellite_key: new_satellite_key(),
            channel_key: new_channel_key(),
            offset_key: new_offset_key(),
            slope_key: new_slope_key(),
        }
    }

    fn satellite(&self, tile: &RasterTile2D<P>) -> Result<&'static Satellite> {
        let id = match self.params.force_satellite {
            Some(id) => id,
            _ => tile.properties.number_property(&self.satellite_key)?,
        };
        Satellite::satellite_by_msg_id(id)
    }

    fn channel<'a>(&self, tile: &RasterTile2D<P>, satellite: &'a Satellite) -> Result<&'a Channel> {
        let channel_id = tile
            .properties
            .number_property::<usize>(&self.channel_key)?
            - 1;
        if (3..=10).contains(&channel_id) {
            satellite.channel(channel_id)
        } else {
            Err(Error::InvalidChannel {
                channel: channel_id,
            })
        }
    }

    async fn process_tile_async(&self, tile: RasterTile2D<P>) -> Result<RasterTile2D<PixelOut>> {
        if tile.is_empty() {
            return Ok(RasterTile2D::new_with_properties(
                tile.time,
                tile.tile_position,
                tile.global_geo_transform,
                EmptyGrid::new(tile.grid_array.grid_shape(), OUT_NO_DATA_VALUE).into(),
                tile.properties,
            ));
        }

        let satellite = self.satellite(&tile)?;
        let channel = self.channel(&tile, satellite)?;
        let offset = tile.properties.number_property::<f64>(&self.offset_key)?;
        let slope = tile.properties.number_property::<f64>(&self.slope_key)?;
        let mat_tile = tile.into_materialized_tile(); // NOTE: the tile is already materialized.

        let temp_grid = tokio::task::spawn_blocking(move || {
            let lut = create_lookup_table(channel, offset, slope);
            process_tile(&mat_tile.grid_array, &lut)
        })
        .await??;

        Ok(RasterTile2D::new_with_properties(
            mat_tile.time,
            mat_tile.tile_position,
            mat_tile.global_geo_transform,
            temp_grid.into(),
            mat_tile.properties,
        ))
    }
}

fn create_lookup_table(channel: &Channel, offset: f64, slope: f64) -> Vec<f32> {
    let mut lut = Vec::with_capacity(1024);
    for i in 0..1024 {
        let radiance = offset + f64::from(i) * slope;
        let temp = channel.calculate_temperature_from_radiance(radiance);
        lut.push(temp as f32);
    }
    lut
}

fn process_tile<P: Pixel>(grid: &Grid2D<P>, lut: &[f32]) -> Result<Grid2D<PixelOut>> {
    // Create result raster
    let mut out = Grid2D::new(
        grid.grid_shape(),
        vec![OUT_NO_DATA_VALUE; grid.number_of_elements()],
        Some(OUT_NO_DATA_VALUE),
    )
    .expect("raster creation must succeed");

    for (idx, &pixel) in grid.data.iter().enumerate() {
        if !grid.is_no_data(pixel) {
            let lut_idx: u64 = pixel.as_();
            out.data[idx] = *lut
                .get(lut_idx as usize)
                .ok_or(Error::UnsupportedRasterValue)?;
        }
    }

    Ok(out)
}

#[async_trait]
impl<Q, P> QueryProcessor for TemperatureProcessor<Q, P>
where
    Q: QueryProcessor<Output = RasterTile2D<P>, SpatialBounds = SpatialPartition2D>,
    P: Pixel,
{
    type Output = RasterTile2D<PixelOut>;
    type SpatialBounds = SpatialPartition2D;

    async fn query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let src = self.source.query(query, ctx).await?;
        let rs = src.and_then(move |tile| self.process_tile_async(tile));
        Ok(rs.boxed())
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::{MockExecutionContext, RasterOperator, SingleRasterSource};
    use crate::processing::meteosat::temperature::{
        Temperature, TemperatureParams, OUT_NO_DATA_VALUE,
    };
    use crate::processing::meteosat::test_util;
    use geoengine_datatypes::primitives::Measurement;
    use geoengine_datatypes::raster::{EmptyGrid2D, Grid2D};
    use std::collections::HashMap;

    // #[tokio::test]
    // async fn test_msg_raster() {
    //     let mut ctx = MockExecutionContext::default();
    //     let src = test_util::_create_gdal_src(&mut ctx);
    //
    //     let result = test_util::process(
    //         move || {
    //             RasterOperator::boxed(Temperature {
    //                 params: TemperatureParams::default(),
    //                 sources: SingleRasterSource {
    //                     raster: src.boxed(),
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
        let ctx = MockExecutionContext::default();
        let res = test_util::process(
            || {
                let props = test_util::create_properties(Some(4), Some(1), Some(0.0), Some(1.0));
                let src = test_util::create_mock_source::<u8>(props, Some(vec![]), None);

                RasterOperator::boxed(Temperature {
                    params: TemperatureParams::default(),
                    sources: SingleRasterSource {
                        raster: src.boxed(),
                    },
                })
            },
            test_util::create_mock_query(),
            &ctx,
        )
        .await
        .unwrap();

        assert!(geoengine_datatypes::util::test::eq_with_no_data(
            &res.grid_array,
            &EmptyGrid2D::new([3, 2].into(), OUT_NO_DATA_VALUE,).into()
        ));
    }

    #[tokio::test]
    async fn test_ok() {
        let ctx = MockExecutionContext::default();
        let res = test_util::process(
            || {
                let props = test_util::create_properties(Some(4), Some(1), Some(0.0), Some(1.0));
                let src = test_util::create_mock_source::<u8>(props, None, None);

                RasterOperator::boxed(Temperature {
                    params: TemperatureParams::default(),
                    sources: SingleRasterSource {
                        raster: src.boxed(),
                    },
                })
            },
            test_util::create_mock_query(),
            &ctx,
        )
        .await
        .unwrap();

        assert!(geoengine_datatypes::util::test::eq_with_no_data(
            &res.grid_array,
            &Grid2D::new(
                [3, 2].into(),
                vec![
                    300.341_43,
                    318.617_65,
                    330.365_14,
                    339.233_64,
                    346.443_94,
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
        let ctx = MockExecutionContext::default();
        let res = test_util::process(
            || {
                let props = test_util::create_properties(Some(4), Some(1), Some(0.0), Some(1.0));
                let src = test_util::create_mock_source::<u8>(props, None, None);

                RasterOperator::boxed(Temperature {
                    params: TemperatureParams {
                        force_satellite: Some(4),
                    },
                    sources: SingleRasterSource {
                        raster: src.boxed(),
                    },
                })
            },
            test_util::create_mock_query(),
            &ctx,
        )
        .await
        .unwrap();

        assert!(geoengine_datatypes::util::test::eq_with_no_data(
            &res.grid_array,
            &Grid2D::new(
                [3, 2].into(),
                vec![
                    300.9428,
                    319.250_15,
                    331.019_04,
                    339.9044,
                    347.128_78,
                    OUT_NO_DATA_VALUE
                ],
                Some(OUT_NO_DATA_VALUE),
            )
            .unwrap()
            .into()
        ));
    }

    #[tokio::test]
    async fn test_fail_illegal_input() {
        let ctx = MockExecutionContext::default();

        let res = test_util::process(
            || {
                let props = test_util::create_properties(Some(4), Some(1), Some(0.0), Some(1.0));
                let src = test_util::create_mock_source::<u16>(
                    props,
                    Some(vec![1, 2, 3, 4, 1024, 0]),
                    None,
                );

                RasterOperator::boxed(Temperature {
                    params: TemperatureParams::default(),
                    sources: SingleRasterSource {
                        raster: src.boxed(),
                    },
                })
            },
            test_util::create_mock_query(),
            &ctx,
        )
        .await;

        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_invalid_force_satellite() {
        let ctx = MockExecutionContext::default();
        let res = test_util::process(
            || {
                let props = test_util::create_properties(Some(4), Some(1), Some(0.0), Some(1.0));
                let src = test_util::create_mock_source::<u8>(props, None, None);

                RasterOperator::boxed(Temperature {
                    params: TemperatureParams {
                        force_satellite: Some(13),
                    },
                    sources: SingleRasterSource {
                        raster: src.boxed(),
                    },
                })
            },
            test_util::create_mock_query(),
            &ctx,
        )
        .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_missing_satellite() {
        let ctx = MockExecutionContext::default();
        let res = test_util::process(
            || {
                let props = test_util::create_properties(Some(4), None, Some(0.0), Some(1.0));
                let src = test_util::create_mock_source::<u8>(props, None, None);

                RasterOperator::boxed(Temperature {
                    params: TemperatureParams::default(),
                    sources: SingleRasterSource {
                        raster: src.boxed(),
                    },
                })
            },
            test_util::create_mock_query(),
            &ctx,
        )
        .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_invalid_satellite() {
        let ctx = MockExecutionContext::default();
        let res = test_util::process(
            || {
                let props = test_util::create_properties(Some(4), Some(42), Some(0.0), Some(1.0));
                let src = test_util::create_mock_source::<u8>(props, None, None);

                RasterOperator::boxed(Temperature {
                    params: TemperatureParams::default(),
                    sources: SingleRasterSource {
                        raster: src.boxed(),
                    },
                })
            },
            test_util::create_mock_query(),
            &ctx,
        )
        .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_missing_channel() {
        let ctx = MockExecutionContext::default();
        let res = test_util::process(
            || {
                let props = test_util::create_properties(None, Some(1), Some(0.0), Some(1.0));
                let src = test_util::create_mock_source::<u8>(props, None, None);

                RasterOperator::boxed(Temperature {
                    params: TemperatureParams::default(),
                    sources: SingleRasterSource {
                        raster: src.boxed(),
                    },
                })
            },
            test_util::create_mock_query(),
            &ctx,
        )
        .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_invalid_channel() {
        let ctx = MockExecutionContext::default();
        let res = test_util::process(
            || {
                let props = test_util::create_properties(Some(1), Some(1), Some(0.0), Some(1.0));
                let src = test_util::create_mock_source::<u8>(props, None, None);

                RasterOperator::boxed(Temperature {
                    params: TemperatureParams::default(),
                    sources: SingleRasterSource {
                        raster: src.boxed(),
                    },
                })
            },
            test_util::create_mock_query(),
            &ctx,
        )
        .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_missing_slope() {
        let ctx = MockExecutionContext::default();
        let res = test_util::process(
            || {
                let props = test_util::create_properties(Some(4), Some(1), Some(0.0), None);
                let src = test_util::create_mock_source::<u8>(props, None, None);

                RasterOperator::boxed(Temperature {
                    params: TemperatureParams::default(),
                    sources: SingleRasterSource {
                        raster: src.boxed(),
                    },
                })
            },
            test_util::create_mock_query(),
            &ctx,
        )
        .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_missing_offset() {
        let ctx = MockExecutionContext::default();
        let res = test_util::process(
            || {
                let props = test_util::create_properties(Some(4), Some(1), None, Some(1.0));
                let src = test_util::create_mock_source::<u8>(props, None, None);

                RasterOperator::boxed(Temperature {
                    params: TemperatureParams::default(),
                    sources: SingleRasterSource {
                        raster: src.boxed(),
                    },
                })
            },
            test_util::create_mock_query(),
            &ctx,
        )
        .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_invalid_measurement_unitless() {
        let ctx = MockExecutionContext::default();
        let res = test_util::process(
            || {
                let props = test_util::create_properties(Some(4), Some(1), Some(0.0), Some(1.0));
                let src =
                    test_util::create_mock_source::<u8>(props, None, Some(Measurement::Unitless));

                RasterOperator::boxed(Temperature {
                    params: TemperatureParams::default(),
                    sources: SingleRasterSource {
                        raster: src.boxed(),
                    },
                })
            },
            test_util::create_mock_query(),
            &ctx,
        )
        .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_invalid_measurement_continuous() {
        let ctx = MockExecutionContext::default();
        let res = test_util::process(
            || {
                let props = test_util::create_properties(Some(4), Some(1), Some(0.0), Some(1.0));
                let src = test_util::create_mock_source::<u8>(
                    props,
                    None,
                    Some(Measurement::Continuous {
                        measurement: "invalid".into(),
                        unit: None,
                    }),
                );

                RasterOperator::boxed(Temperature {
                    params: TemperatureParams::default(),
                    sources: SingleRasterSource {
                        raster: src.boxed(),
                    },
                })
            },
            test_util::create_mock_query(),
            &ctx,
        )
        .await;

        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_invalid_measurement_classification() {
        let ctx = MockExecutionContext::default();
        let res = test_util::process(
            || {
                let props = test_util::create_properties(Some(4), Some(1), Some(0.0), Some(1.0));
                let src = test_util::create_mock_source::<u8>(
                    props,
                    None,
                    Some(Measurement::Classification {
                        measurement: "invalid".into(),
                        classes: HashMap::new(),
                    }),
                );

                RasterOperator::boxed(Temperature {
                    params: TemperatureParams::default(),
                    sources: SingleRasterSource {
                        raster: src.boxed(),
                    },
                })
            },
            test_util::create_mock_query(),
            &ctx,
        )
        .await;
        assert!(res.is_err());
    }
}
