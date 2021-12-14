use std::sync::Arc;

use crate::engine::{
    ExecutionContext, InitializedRasterOperator, Operator, QueryContext, QueryProcessor,
    RasterOperator, RasterQueryProcessor, RasterQueryRectangle, RasterResultDescriptor,
    SingleRasterSource, TypedRasterQueryProcessor,
};
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use geoengine_datatypes::primitives::{Measurement, SpatialPartition2D};
use geoengine_datatypes::raster::{
    EmptyGrid, Grid2D, GridShapeAccess, GridSize, NoDataValue, Pixel, RasterDataType,
    RasterPropertiesKey, RasterTile2D,
};
use rayon::iter::ParallelIterator;
use rayon::slice::ParallelSlice;
use rayon::ThreadPool;
use serde::{Deserialize, Serialize};

// Output type is always f32
type PixelOut = f32;
use crate::error::Error;
use crate::processing::meteosat::{new_offset_key, new_slope_key};
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
        if tile.is_empty() {
            return Ok(RasterTile2D::new_with_properties(
                tile.time,
                tile.tile_position,
                tile.global_geo_transform,
                EmptyGrid::new(tile.grid_array.grid_shape(), OUT_NO_DATA_VALUE).into(),
                tile.properties,
            ));
        }

        let offset = tile.properties.number_property::<f32>(&self.offset_key)?;
        let slope = tile.properties.number_property::<f32>(&self.slope_key)?;
        let mat_tile = tile.into_materialized_tile(); // NOTE: the tile is already materialized.

        let rad_grid = tokio::task::spawn_blocking(move || {
            process_tile(&mat_tile.grid_array, offset, slope, &pool)
        })
        .await?;

        Ok(RasterTile2D::new_with_properties(
            mat_tile.time,
            mat_tile.tile_position,
            mat_tile.global_geo_transform,
            rad_grid.into(),
            mat_tile.properties,
        ))
    }
}

fn process_tile<P: Pixel>(
    grid: &Grid2D<P>,
    offset: f32,
    slope: f32,
    pool: &ThreadPool,
) -> Grid2D<PixelOut> {
    pool.install(|| {
        let rad_array = grid
            .data
            .par_chunks(1.max(grid.axis_size_y() / pool.current_num_threads()))
            .map(|row| {
                row.iter().map(|p| {
                    if grid.is_no_data(*p) {
                        OUT_NO_DATA_VALUE
                    } else {
                        let val: PixelOut = (p).as_();
                        offset + val * slope
                    }
                })
            })
            .flatten_iter()
            .collect::<Vec<PixelOut>>();

        Grid2D::new(grid.grid_shape(), rad_array, Some(OUT_NO_DATA_VALUE))
            .expect("raster creation must succeed")
    })
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
        let rs = src.and_then(move |tile| self.process_tile_async(tile, ctx.thread_pool().clone()));
        Ok(rs.boxed())
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
                let src = test_util::create_mock_source::<u8>(props, Some(vec![]), None);
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
        let ctx = MockExecutionContext::default();

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
        let ctx = MockExecutionContext::default();

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
        let ctx = MockExecutionContext::default();

        let res = test_util::process(
            || {
                let props = test_util::create_properties(None, None, Some(11.0), Some(2.0));
                let src = test_util::create_mock_source::<u8>(
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
                let src = test_util::create_mock_source::<u8>(
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
