use crate::engine::{
    ExecutionContext, InitializedRasterOperator, Operator, QueryContext, QueryProcessor,
    RasterOperator, RasterQueryProcessor, RasterQueryRectangle, RasterResultDescriptor,
    SingleRasterSource, TypedRasterQueryProcessor,
};
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use geoengine_datatypes::primitives::{Measurement, SpatialPartition2D};
use geoengine_datatypes::raster::{
    EmptyGrid, Grid2D, GridShapeAccess, NoDataValue, Pixel, RasterDataType, RasterPropertiesKey,
    RasterTile2D,
};
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};

// Output type is always f32
type PixelOut = f32;
use crate::error::Error;
use crate::processing::meteosat::{offset_key, slope_key};
use RasterDataType::F32 as RasterOut;
use TypedRasterQueryProcessor::F32 as QueryProcessorOut;

const OUT_NO_DATA_VALUE: PixelOut = PixelOut::NAN;

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

pub struct InitializedRadiance {
    result_descriptor: RasterResultDescriptor,
    source: Box<dyn InitializedRasterOperator>,
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for Radiance {
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
                measurement: "radiance".into(),
                unit: Some("W·m^(-2)·sr^(-1)·cm^(-1)".into()),
            },
            no_data_value: Some(f64::from(OUT_NO_DATA_VALUE)),
        };

        let initialized_operator = InitializedRadiance {
            result_descriptor: out_desc,
            source: input,
        };

        Ok(initialized_operator.boxed())
    }
}

impl InitializedRasterOperator for InitializedRadiance {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let q = self.source.query_processor()?;

        Ok(match q {
            TypedRasterQueryProcessor::U8(p) => {
                QueryProcessorOut(Box::new(RadianceProcessor::new(p, OUT_NO_DATA_VALUE.as_())))
            }
            TypedRasterQueryProcessor::U16(p) => {
                QueryProcessorOut(Box::new(RadianceProcessor::new(p, OUT_NO_DATA_VALUE.as_())))
            }
            TypedRasterQueryProcessor::U32(p) => {
                QueryProcessorOut(Box::new(RadianceProcessor::new(p, OUT_NO_DATA_VALUE.as_())))
            }
            TypedRasterQueryProcessor::U64(p) => {
                QueryProcessorOut(Box::new(RadianceProcessor::new(p, OUT_NO_DATA_VALUE.as_())))
            }
            TypedRasterQueryProcessor::I8(p) => {
                QueryProcessorOut(Box::new(RadianceProcessor::new(p, OUT_NO_DATA_VALUE.as_())))
            }
            TypedRasterQueryProcessor::I16(p) => {
                QueryProcessorOut(Box::new(RadianceProcessor::new(p, OUT_NO_DATA_VALUE.as_())))
            }
            TypedRasterQueryProcessor::I32(p) => {
                QueryProcessorOut(Box::new(RadianceProcessor::new(p, OUT_NO_DATA_VALUE.as_())))
            }
            TypedRasterQueryProcessor::I64(p) => {
                QueryProcessorOut(Box::new(RadianceProcessor::new(p, OUT_NO_DATA_VALUE.as_())))
            }
            TypedRasterQueryProcessor::F32(p) => {
                QueryProcessorOut(Box::new(RadianceProcessor::new(p, OUT_NO_DATA_VALUE.as_())))
            }
            TypedRasterQueryProcessor::F64(p) => {
                QueryProcessorOut(Box::new(RadianceProcessor::new(p, OUT_NO_DATA_VALUE.as_())))
            }
        })
    }
}

pub struct RadianceProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
{
    source: Q,
    no_data_value: PixelOut,
    offset_key: RasterPropertiesKey,
    slope_key: RasterPropertiesKey,
}

impl<Q, P> RadianceProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
{
    pub fn new(source: Q, no_data_value: PixelOut) -> Self {
        Self {
            source,
            no_data_value,
            offset_key: offset_key(),
            slope_key: slope_key(),
        }
    }
}

#[async_trait]
impl<Q, P> QueryProcessor for RadianceProcessor<Q, P>
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
        Ok(src
            .map(move |tile| match tile {
                Ok(tile) if tile.grid_array.is_empty() => Ok(RasterTile2D::new_with_properties(
                    tile.time,
                    tile.tile_position,
                    tile.global_geo_transform,
                    EmptyGrid::new(tile.grid_array.grid_shape(), self.no_data_value).into(),
                    tile.properties,
                )),
                Ok(tile) => {
                    let mg = tile.grid_array.into_materialized_grid();

                    let mut out = Grid2D::new(
                        mg.grid_shape(),
                        vec![self.no_data_value; mg.data.len()],
                        Some(self.no_data_value),
                    )
                    .expect("raster creation must succeed");

                    let offset = tile.properties.number_property::<f32>(&self.offset_key)?;
                    let slope = tile.properties.number_property::<f32>(&self.slope_key)?;
                    let tgt = &mut out.data;

                    for (idx, v) in mg.data.iter().enumerate() {
                        if !mg.is_no_data(*v) {
                            let val: PixelOut = (*v).as_();
                            tgt[idx] = offset + val * slope;
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
                Err(e) => Err(e),
            })
            .boxed())
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::{MockExecutionContext, RasterOperator, SingleRasterSource};
    use crate::processing::meteosat::radiance::{Radiance, RadianceParams};
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
        let no_data_value_option = Some(super::OUT_NO_DATA_VALUE);
        let ctx = MockExecutionContext::default();

        let result = test_util::process(
            || {
                let props = test_util::create_properties(None, None, Some(11.0), Some(2.0));
                let src = test_util::create_mock_source(props, None, None);
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

        assert!(geoengine_datatypes::util::test::eq_with_no_data(
            &result.as_ref().unwrap().grid_array,
            &Grid2D::new(
                [3, 2].into(),
                vec![13.0, 15.0, 17.0, 19.0, 21.0, no_data_value_option.unwrap()],
                no_data_value_option,
            )
            .unwrap()
            .into()
        ));
    }

    #[tokio::test]
    async fn test_empty_raster() {
        let no_data_value_option = Some(super::OUT_NO_DATA_VALUE);
        let ctx = MockExecutionContext::default();

        let result = test_util::process(
            || {
                let props = test_util::create_properties(None, None, Some(11.0), Some(2.0));
                let src = test_util::create_mock_source(props, Some(vec![]), None);
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

        assert!(geoengine_datatypes::util::test::eq_with_no_data(
            &result.as_ref().unwrap().grid_array,
            &EmptyGrid2D::new([3, 2].into(), no_data_value_option.unwrap(),).into()
        ));
    }

    #[tokio::test]
    async fn test_missing_offset() {
        let ctx = MockExecutionContext::default();

        let result = test_util::process(
            || {
                let props = test_util::create_properties(None, None, None, Some(2.0));
                let src = test_util::create_mock_source(props, None, None);
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
        let ctx = MockExecutionContext::default();

        let result = test_util::process(
            || {
                let props = test_util::create_properties(None, None, Some(11.0), None);
                let src = test_util::create_mock_source(props, None, None);
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
        let ctx = MockExecutionContext::default();

        let res = test_util::process(
            || {
                let props = test_util::create_properties(None, None, Some(11.0), Some(2.0));
                let src = test_util::create_mock_source(props, None, Some(Measurement::Unitless));

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
        let ctx = MockExecutionContext::default();

        let res = test_util::process(
            || {
                let props = test_util::create_properties(None, None, Some(11.0), Some(2.0));
                let src = test_util::create_mock_source(
                    props,
                    None,
                    Some(Measurement::Continuous {
                        measurement: "invalid".into(),
                        unit: None,
                    }),
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
        let ctx = MockExecutionContext::default();

        let res = test_util::process(
            || {
                let props = test_util::create_properties(None, None, Some(11.0), Some(2.0));
                let src = test_util::create_mock_source(
                    props,
                    None,
                    Some(Measurement::Classification {
                        measurement: "invalid".into(),
                        classes: HashMap::new(),
                    }),
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
