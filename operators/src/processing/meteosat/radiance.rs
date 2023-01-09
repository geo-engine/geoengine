use std::sync::Arc;

use crate::engine::{
    CreateSpan, ExecutionContext, InitializedRasterOperator, Operator, OperatorName, QueryContext,
    QueryProcessor, RasterOperator, RasterQueryProcessor, RasterResultDescriptor,
    SingleRasterSource, TypedRasterQueryProcessor,
};
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use geoengine_datatypes::primitives::{
    ClassificationMeasurement, ContinuousMeasurement, Measurement, RasterQueryRectangle,
    RasterSpatialQueryRectangle,
};
use geoengine_datatypes::raster::{
    MapElementsParallel, Pixel, RasterDataType, RasterPropertiesKey, RasterTile2D,
};
use rayon::ThreadPool;
use serde::{Deserialize, Serialize};
use tracing::{span, Level};

// Output type is always f32
type PixelOut = f32;
use crate::error::Error;
use crate::processing::meteosat::{new_offset_key, new_slope_key};
use RasterDataType::F32 as RasterOut;
use TypedRasterQueryProcessor::F32 as QueryProcessorOut;

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "camelCase")]
pub struct RadianceParams {}

/// The radiance operator converts a raw MSG raster into radiance.
/// This is done by applying the following formula to every pixel:
///
/// `p_new = offset + p_old * slope`
///
/// Here, `p_old` and `p_new` refer to the old and new pixel values,
/// while slope and offset are properties attached to the input
/// raster.
/// The exact names of the properties are:
///
/// - offset: `msg.calibration_offset`
/// - slope: `msg.calibration_slope`
pub type Radiance = Operator<RadianceParams, SingleRasterSource>;

impl OperatorName for Radiance {
    const TYPE_NAME: &'static str = "Radiance";
}

pub struct InitializedRadiance {
    result_descriptor: RasterResultDescriptor,
    source: Box<dyn InitializedRasterOperator>,
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for Radiance {
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
                measurement: "radiance".into(),
                unit: Some("W·m^(-2)·sr^(-1)·cm^(-1)".into()),
            }),
            time: in_desc.time,
            bbox: in_desc.bbox,
            resolution: in_desc.resolution,
        };

        let initialized_operator = InitializedRadiance {
            result_descriptor: out_desc,
            source: input,
        };

        Ok(initialized_operator.boxed())
    }

    span_fn!(Radiance);
}

impl InitializedRasterOperator for InitializedRadiance {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let q = self.source.query_processor()?;

        Ok(match q {
            TypedRasterQueryProcessor::U8(p) => {
                QueryProcessorOut(Box::new(RadianceProcessor::new(p)))
            }
            TypedRasterQueryProcessor::U16(p) => {
                QueryProcessorOut(Box::new(RadianceProcessor::new(p)))
            }
            TypedRasterQueryProcessor::U32(p) => {
                QueryProcessorOut(Box::new(RadianceProcessor::new(p)))
            }
            TypedRasterQueryProcessor::U64(p) => {
                QueryProcessorOut(Box::new(RadianceProcessor::new(p)))
            }
            TypedRasterQueryProcessor::I8(p) => {
                QueryProcessorOut(Box::new(RadianceProcessor::new(p)))
            }
            TypedRasterQueryProcessor::I16(p) => {
                QueryProcessorOut(Box::new(RadianceProcessor::new(p)))
            }
            TypedRasterQueryProcessor::I32(p) => {
                QueryProcessorOut(Box::new(RadianceProcessor::new(p)))
            }
            TypedRasterQueryProcessor::I64(p) => {
                QueryProcessorOut(Box::new(RadianceProcessor::new(p)))
            }
            TypedRasterQueryProcessor::F32(p) => {
                QueryProcessorOut(Box::new(RadianceProcessor::new(p)))
            }
            TypedRasterQueryProcessor::F64(p) => {
                QueryProcessorOut(Box::new(RadianceProcessor::new(p)))
            }
        })
    }
}

struct RadianceProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
{
    source: Q,
    offset_key: RasterPropertiesKey,
    slope_key: RasterPropertiesKey,
}

impl<Q, P> RadianceProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
{
    pub fn new(source: Q) -> Self {
        Self {
            source,
            offset_key: new_offset_key(),
            slope_key: new_slope_key(),
        }
    }

    async fn process_tile_async(
        &self,
        tile: RasterTile2D<P>,
        pool: Arc<ThreadPool>,
    ) -> Result<RasterTile2D<PixelOut>> {
        let offset = tile.properties.number_property::<f32>(&self.offset_key)?;
        let slope = tile.properties.number_property::<f32>(&self.slope_key)?;

        let map_fn = move |raw_value_option: Option<P>| {
            raw_value_option.map(|raw_value| {
                let raw_f32: f32 = raw_value.as_();
                offset + raw_f32 * slope
            })
        };

        let result_tile = crate::util::spawn_blocking_with_thread_pool(pool, move || {
            tile.map_elements_parallel(map_fn)
        })
        .await?;

        Ok(result_tile)
    }
}

#[async_trait]
impl<Q, P> QueryProcessor for RadianceProcessor<Q, P>
where
    Q: QueryProcessor<Output = RasterTile2D<P>, SpatialQuery = RasterSpatialQueryRectangle>,
    P: Pixel,
{
    type Output = RasterTile2D<PixelOut>;
    type SpatialQuery = RasterSpatialQueryRectangle;

    async fn _query<'a>(
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
    use crate::processing::meteosat::radiance::{Radiance, RadianceParams};
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
    //             RasterOperator::boxed(Radiance {
    //                 params: RadianceParams {},
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
    async fn test_ok() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };

        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let result = test_util::process(
            || {
                let props = test_util::create_properties(None, None, Some(11.0), Some(2.0));
                let src = test_util::create_mock_source::<u8>(props, None, None);
                RasterOperator::boxed(Radiance {
                    sources: SingleRasterSource {
                        raster: src.boxed(),
                    },
                    params: RadianceParams {},
                })
            },
            test_util::create_mock_query(),
            &ctx,
        )
        .await;

        assert!(geoengine_datatypes::util::test::grid_or_empty_grid_eq(
            &result.as_ref().unwrap().grid_array,
            &MaskedGrid2D::new(
                Grid2D::new([3, 2].into(), vec![13.0, 15.0, 17.0, 19.0, 21.0, 0.],).unwrap(),
                Grid2D::new([3, 2].into(), vec![true, true, true, true, true, false]).unwrap()
            )
            .unwrap()
            .into()
        ));

        // TODO: add assert to check mask
    }

    #[tokio::test]
    async fn test_empty_raster() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };

        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let result = test_util::process(
            || {
                let props = test_util::create_properties(None, None, Some(11.0), Some(2.0));
                let src = test_util::create_mock_source::<u8>(
                    props,
                    Some(EmptyGrid2D::new([3, 2].into()).into()),
                    None,
                );
                RasterOperator::boxed(Radiance {
                    sources: SingleRasterSource {
                        raster: src.boxed(),
                    },
                    params: RadianceParams {},
                })
            },
            test_util::create_mock_query(),
            &ctx,
        )
        .await;

        assert!(geoengine_datatypes::util::test::grid_or_empty_grid_eq(
            &result.as_ref().unwrap().grid_array,
            &EmptyGrid2D::new([3, 2].into()).into()
        ));
    }

    #[tokio::test]
    async fn test_missing_offset() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };

        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);
        let result = test_util::process(
            || {
                let props = test_util::create_properties(None, None, None, Some(2.0));
                let src = test_util::create_mock_source::<u8>(props, None, None);
                RasterOperator::boxed(Radiance {
                    sources: SingleRasterSource {
                        raster: src.boxed(),
                    },
                    params: RadianceParams {},
                })
            },
            test_util::create_mock_query(),
            &ctx,
        )
        .await;

        assert!(&result.is_err());
    }

    #[tokio::test]
    async fn test_missing_slope() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };

        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let result = test_util::process(
            || {
                let props = test_util::create_properties(None, None, Some(11.0), None);
                let src = test_util::create_mock_source::<u8>(props, None, None);
                RasterOperator::boxed(Radiance {
                    sources: SingleRasterSource {
                        raster: src.boxed(),
                    },
                    params: RadianceParams {},
                })
            },
            test_util::create_mock_query(),
            &ctx,
        )
        .await;

        assert!(&result.is_err());
    }

    #[tokio::test]
    async fn test_invalid_measurement_unitless() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };

        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let res = test_util::process(
            || {
                let props = test_util::create_properties(None, None, Some(11.0), Some(2.0));
                let src =
                    test_util::create_mock_source::<u8>(props, None, Some(Measurement::Unitless));

                RasterOperator::boxed(Radiance {
                    params: RadianceParams {},
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
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };

        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let res = test_util::process(
            || {
                let props = test_util::create_properties(None, None, Some(11.0), Some(2.0));
                let src = test_util::create_mock_source::<u8>(
                    props,
                    None,
                    Some(Measurement::Continuous(ContinuousMeasurement {
                        measurement: "invalid".into(),
                        unit: None,
                    })),
                );

                RasterOperator::boxed(Radiance {
                    params: RadianceParams {},
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
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };

        let ctx = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let res = test_util::process(
            || {
                let props = test_util::create_properties(None, None, Some(11.0), Some(2.0));
                let src = test_util::create_mock_source::<u8>(
                    props,
                    None,
                    Some(Measurement::Classification(ClassificationMeasurement {
                        measurement: "invalid".into(),
                        classes: HashMap::new(),
                    })),
                );

                RasterOperator::boxed(Radiance {
                    params: RadianceParams {},
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
