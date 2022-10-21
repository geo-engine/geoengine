use std::sync::Arc;

use crate::engine::{
    CreateSpan, ExecutionContext, InitializedRasterOperator, Operator, OperatorName, QueryContext,
    QueryProcessor, RasterOperator, RasterQueryProcessor, RasterResultDescriptor,
    SingleRasterSource, TypedRasterQueryProcessor,
};
use crate::util::Result;
use async_trait::async_trait;
use rayon::ThreadPool;
use tracing::{span, Level};
use TypedRasterQueryProcessor::F32 as QueryProcessorOut;

use crate::error::Error;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use geoengine_datatypes::primitives::{
    ClassificationMeasurement, ContinuousMeasurement, Measurement, RasterQueryRectangle,
    SpatialPartition2D,
};
use geoengine_datatypes::raster::{
    MapElementsParallel, Pixel, RasterDataType, RasterPropertiesKey, RasterTile2D,
};
use serde::{Deserialize, Serialize};

// Output type is always f32
type PixelOut = f32;
use crate::processing::meteosat::satellite::{Channel, Satellite};
use crate::processing::meteosat::{
    new_channel_key, new_offset_key, new_satellite_key, new_slope_key,
};
use RasterDataType::F32 as RasterOut;

/// Parameters for the `Temperature` operator.
/// * `force_satellite` forces the use of the satellite with the given name.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct TemperatureParams {
    force_satellite: Option<u8>,
}

/// The temperature operator approximates BT from
/// the raw MSG rasters.
pub type Temperature = Operator<TemperatureParams, SingleRasterSource>;

impl OperatorName for Temperature {
    const TYPE_NAME: &'static str = "Temperature";
}

pub struct InitializedTemperature {
    result_descriptor: RasterResultDescriptor,
    source: Box<dyn InitializedRasterOperator>,
    params: TemperatureParams,
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for Temperature {
    async fn _initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let input = self.sources.raster.initialize(context).await?;

        let in_desc = input.result_descriptor();

        match &in_desc.measurement {
            Measurement::Continuous(ContinuousMeasurement {
                measurement: m,
                unit: _,
            }) if m != "raw" => {
                return Err(Error::InvalidMeasurement {
                    expected: "raw".into(),
                    found: m.clone(),
                })
            }
            Measurement::Classification(ClassificationMeasurement {
                measurement: m,
                classes: _,
            }) => {
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
            Measurement::Continuous(ContinuousMeasurement {
                measurement: _,
                unit: _,
            }) => {}
        }

        let out_desc = RasterResultDescriptor {
            spatial_reference: in_desc.spatial_reference,
            data_type: RasterOut,
            measurement: Measurement::Continuous(ContinuousMeasurement {
                measurement: "temperature".into(),
                unit: Some("k".into()),
            }),
            time: in_desc.time,
            bbox: in_desc.bbox,
            resolution: in_desc.resolution,
        };

        let initialized_operator = InitializedTemperature {
            result_descriptor: out_desc,
            source: input,
            params: self.params,
        };

        Ok(initialized_operator.boxed())
    }

    fn span(&self) -> CreateSpan {
        || span!(Level::TRACE, Temperature::TYPE_NAME)
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

    async fn process_tile_async(
        &self,
        tile: RasterTile2D<P>,
        pool: Arc<ThreadPool>,
    ) -> Result<RasterTile2D<PixelOut>> {
        let satellite = self.satellite(&tile)?;
        let channel = self.channel(&tile, satellite)?;
        let offset = tile.properties.number_property::<f64>(&self.offset_key)?;
        let slope = tile.properties.number_property::<f64>(&self.slope_key)?;

        let temp_tile = crate::util::spawn_blocking_with_thread_pool(pool.clone(), move || {
            let lut = create_lookup_table(channel, offset, slope, &pool);

            let map_fn = move |pixel_option: Option<P>| {
                pixel_option.and_then(|p| {
                    let lut_idx: u64 = p.as_();
                    lut.get(lut_idx as usize).copied()
                })
            };

            tile.map_elements_parallel(map_fn)
        })
        .await?;

        Ok(temp_tile)
    }
}

fn create_lookup_table(channel: &Channel, offset: f64, slope: f64, _pool: &ThreadPool) -> Vec<f32> {
    // this should propably be done with SIMD not a threadpool
    (0..1024)
        .into_iter()
        .map(|i| {
            let radiance = offset + f64::from(i) * slope;
            channel.calculate_temperature_from_radiance(radiance) as f32
        })
        .collect::<Vec<f32>>()
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
        let rs = src.and_then(move |tile| self.process_tile_async(tile, ctx.thread_pool().clone()));
        Ok(rs.boxed())
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::{MockExecutionContext, RasterOperator, SingleRasterSource};
    use crate::processing::meteosat::temperature::{Temperature, TemperatureParams};
    use crate::processing::meteosat::test_util;
    use geoengine_datatypes::primitives::{
        ClassificationMeasurement, ContinuousMeasurement, Measurement,
    };
    use geoengine_datatypes::raster::{EmptyGrid2D, Grid2D, MaskedGrid2D, TilingSpecification};
    use std::collections::HashMap;

    // #[tokio::test]
    // async fn test_msg_raster() {
    //     let mut ctx = MockExecutionContext::test_default();
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
        let tiling_specification = TilingSpecification::new([0.0, 0.0].into(), [3, 2].into());
        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let res = test_util::process(
            || {
                let props = test_util::create_properties(Some(4), Some(1), Some(0.0), Some(1.0));
                let src = test_util::create_mock_source::<u8>(
                    props,
                    Some(EmptyGrid2D::new([3, 2].into()).into()),
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
        .await
        .unwrap();

        assert!(geoengine_datatypes::util::test::grid_or_empty_grid_eq(
            &res.grid_array,
            &EmptyGrid2D::new([3, 2].into()).into()
        ));
    }

    #[tokio::test]
    async fn test_ok() {
        let tiling_specification = TilingSpecification::new([0.0, 0.0].into(), [3, 2].into());
        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

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

        assert!(geoengine_datatypes::util::test::grid_or_empty_grid_eq(
            &res.grid_array,
            &MaskedGrid2D::new(
                Grid2D::new(
                    [3, 2].into(),
                    vec![300.341_43, 318.617_65, 330.365_14, 339.233_64, 346.443_94, 0.,],
                )
                .unwrap(),
                Grid2D::new([3, 2].into(), vec![true, true, true, true, true, false,],).unwrap(),
            )
            .unwrap()
            .into()
        ));

        // TODO: add assert to check mask
    }

    #[tokio::test]
    async fn test_ok_force_satellite() {
        let tiling_specification = TilingSpecification::new([0.0, 0.0].into(), [3, 2].into());
        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

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

        assert!(geoengine_datatypes::util::test::grid_or_empty_grid_eq(
            &res.grid_array,
            &MaskedGrid2D::new(
                Grid2D::new(
                    [3, 2].into(),
                    vec![300.9428, 319.250_15, 331.019_04, 339.9044, 347.128_78, 0.],
                )
                .unwrap(),
                Grid2D::new([3, 2].into(), vec![true, true, true, true, true, false,],).unwrap(),
            )
            .unwrap()
            .into()
        ));
    }

    #[tokio::test]
    async fn test_ok_illegal_input_to_masked() {
        let tiling_specification = TilingSpecification::new([0.0, 0.0].into(), [3, 2].into());
        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let res = test_util::process(
            || {
                let props = test_util::create_properties(Some(4), Some(1), Some(0.0), Some(1.0));
                let src = test_util::create_mock_source::<u16>(
                    props,
                    Some(
                        MaskedGrid2D::new(
                            Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 1024, 0]).unwrap(),
                            Grid2D::new([3, 2].into(), vec![true, true, true, true, true, false])
                                .unwrap(),
                        )
                        .unwrap()
                        .into(),
                    ),
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
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(geoengine_datatypes::util::test::grid_or_empty_grid_eq(
            &res.grid_array,
            &MaskedGrid2D::new(
                Grid2D::new(
                    [3, 2].into(),
                    vec![300.341_43, 318.617_65, 330.365_14, 339.233_64, 0., 0.],
                )
                .unwrap(),
                Grid2D::new([3, 2].into(), vec![true, true, true, true, false, false,],).unwrap(),
            )
            .unwrap()
            .into()
        ));
    }

    #[tokio::test]
    async fn test_invalid_force_satellite() {
        let tiling_specification = TilingSpecification::new([0.0, 0.0].into(), [3, 2].into());
        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

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
        let tiling_specification = TilingSpecification::new([0.0, 0.0].into(), [3, 2].into());
        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

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
        let tiling_specification = TilingSpecification::new([0.0, 0.0].into(), [3, 2].into());
        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

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
        let tiling_specification = TilingSpecification::new([0.0, 0.0].into(), [3, 2].into());
        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

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
        let tiling_specification = TilingSpecification::new([0.0, 0.0].into(), [3, 2].into());
        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

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
        let tiling_specification = TilingSpecification::new([0.0, 0.0].into(), [3, 2].into());
        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

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
        let tiling_specification = TilingSpecification::new([0.0, 0.0].into(), [3, 2].into());
        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

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
        let tiling_specification = TilingSpecification::new([0.0, 0.0].into(), [3, 2].into());
        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

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
        let tiling_specification = TilingSpecification::new([0.0, 0.0].into(), [3, 2].into());
        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let res = test_util::process(
            || {
                let props = test_util::create_properties(Some(4), Some(1), Some(0.0), Some(1.0));
                let src = test_util::create_mock_source::<u8>(
                    props,
                    None,
                    Some(Measurement::Continuous(ContinuousMeasurement {
                        measurement: "invalid".into(),
                        unit: None,
                    })),
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
        let tiling_specification = TilingSpecification::new([0.0, 0.0].into(), [3, 2].into());

        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let res = test_util::process(
            || {
                let props = test_util::create_properties(Some(4), Some(1), Some(0.0), Some(1.0));
                let src = test_util::create_mock_source::<u8>(
                    props,
                    None,
                    Some(Measurement::Classification(ClassificationMeasurement {
                        measurement: "invalid".into(),
                        classes: HashMap::new(),
                    })),
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
