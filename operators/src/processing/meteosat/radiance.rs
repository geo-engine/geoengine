use crate::engine::{
    ExecutionContext, InitializedRasterOperator, Operator, QueryContext, QueryProcessor,
    RasterOperator, RasterQueryProcessor, RasterQueryRectangle, RasterResultDescriptor,
    SingleRasterSource, TypedRasterQueryProcessor,
};
use crate::util::Result;
use futures::stream::BoxStream;
use futures::StreamExt;
use geoengine_datatypes::primitives::SpatialPartition2D;
use geoengine_datatypes::raster::{
    EmptyGrid, Grid2D, GridShapeAccess, Pixel, RasterDataType, RasterPropertiesKey, RasterTile2D,
};
use num_traits::AsPrimitive;
use std::convert::TryFrom;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

//TODO Check
type PixelOut = f32;
use RasterDataType::F32 as RasterOut;
use TypedRasterQueryProcessor::F32 as QueryProcessorOut;

//TODO: For NAN the tests fail due to == comparisons
const OUT_NO_DATA_VALUE: PixelOut = PixelOut::NEG_INFINITY;

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "camelCase")]
pub struct RadianceParams {}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct RadianceState {
    //TODO: Could be removed, if no data is a constant
    out_no_data_value: PixelOut,
}

pub type Radiance = Operator<RadianceParams, SingleRasterSource>;

pub struct InitializedRadiance {
    result_descriptor: RasterResultDescriptor,
    source: Box<dyn InitializedRasterOperator>,
    state: RadianceState,
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

        let out_desc = RasterResultDescriptor {
            spatial_reference: in_desc.spatial_reference,
            data_type: RasterOut,
            // TODO:
            measurement: in_desc.measurement.clone(),
            no_data_value: in_desc.no_data_value.map(|_| f64::from(OUT_NO_DATA_VALUE)),
        };

        let state = RadianceState {
            out_no_data_value: OUT_NO_DATA_VALUE,
        };

        let initialized_operator = InitializedRadiance {
            result_descriptor: out_desc,
            source: input,
            state,
        };

        Ok(initialized_operator.boxed())
    }
}

impl InitializedRasterOperator for InitializedRadiance {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    // i know there is a macro somewhere. we need to re-work this when we have the no-data value anyway.
    #[allow(clippy::too_many_lines)]
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let q = self.source.query_processor()?;

        let s = self.state;

        // TODO: Any type of input is mapped to f32 output. Correct?
        Ok(match q {
            TypedRasterQueryProcessor::U8(p) => QueryProcessorOut(Box::new(
                RadianceProcessor::new(p, s.out_no_data_value.as_()),
            )),
            TypedRasterQueryProcessor::U16(p) => QueryProcessorOut(Box::new(
                RadianceProcessor::new(p, s.out_no_data_value.as_()),
            )),
            TypedRasterQueryProcessor::U32(p) => QueryProcessorOut(Box::new(
                RadianceProcessor::new(p, s.out_no_data_value.as_()),
            )),
            TypedRasterQueryProcessor::U64(p) => QueryProcessorOut(Box::new(
                RadianceProcessor::new(p, s.out_no_data_value.as_()),
            )),
            TypedRasterQueryProcessor::I8(p) => QueryProcessorOut(Box::new(
                RadianceProcessor::new(p, s.out_no_data_value.as_()),
            )),
            TypedRasterQueryProcessor::I16(p) => QueryProcessorOut(Box::new(
                RadianceProcessor::new(p, s.out_no_data_value.as_()),
            )),
            TypedRasterQueryProcessor::I32(p) => QueryProcessorOut(Box::new(
                RadianceProcessor::new(p, s.out_no_data_value.as_()),
            )),
            TypedRasterQueryProcessor::I64(p) => QueryProcessorOut(Box::new(
                RadianceProcessor::new(p, s.out_no_data_value.as_()),
            )),
            TypedRasterQueryProcessor::F32(p) => QueryProcessorOut(Box::new(
                RadianceProcessor::new(p, s.out_no_data_value.as_()),
            )),
            TypedRasterQueryProcessor::F64(p) => QueryProcessorOut(Box::new(
                RadianceProcessor::new(p, s.out_no_data_value.as_()),
            )),
        })
    }
}

struct RadianceProcessor<Q, P>
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
            offset_key: RasterPropertiesKey {
                domain: Some("msg".into()),
                key: "CalibrationOffset".into(),
            },
            slope_key: RasterPropertiesKey {
                domain: Some("msg".into()),
                key: "CalibrationSlope".into(),
            },
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
            .map(move |tile| {
                match tile {
                    Ok(tile) if tile.grid_array.is_empty() => Ok(RasterTile2D::new(
                        tile.time,
                        tile.tile_position,
                        tile.global_geo_transform,
                        EmptyGrid::new(tile.grid_array.grid_shape(), self.no_data_value).into(),
                    )),
                    Ok(tile) => {
                        let mg = tile.grid_array.into_materialized_grid();

                        let mut out = Grid2D::new(
                            mg.grid_shape(),
                            vec![self.no_data_value; mg.data.len()],
                            Some(self.no_data_value), // TODO
                        )
                        .expect("raster creation must succeed");

                        let offset = f64::try_from(
                            tile.properties
                                .properties_map
                                .get(&self.offset_key)
                                .ok_or(crate::error::Error::MissingRasterProperty {
                                    property: "msg.CalibrationOffset".into(),
                                })?
                                .clone(),
                        )? as PixelOut;
                        let slope = f64::try_from(
                            tile.properties
                                .properties_map
                                .get(&self.slope_key)
                                .ok_or(crate::error::Error::MissingRasterProperty {
                                    property: "msg.CalibrationSlope".into(),
                                })?
                                .clone(),
                        )? as PixelOut;

                        let tgt = &mut out.data;

                        for (idx, v) in mg.data.iter().enumerate() {
                            if mg.no_data_value.map_or(true, |ndv| ndv != *v) {
                                let val: PixelOut = (*v).as_();
                                tgt[idx] = offset + val * slope;
                            }
                        }

                        Ok(RasterTile2D::new(
                            tile.time,
                            tile.tile_position,
                            tile.global_geo_transform,
                            out.into(),
                        ))
                    }
                    Err(e) => Err(e),
                }
            })
            .boxed())
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::{
        MockExecutionContext, MockQueryContext, QueryProcessor, RasterOperator,
        RasterQueryRectangle, RasterResultDescriptor, SingleRasterSource,
    };
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use crate::processing::meteosat::radiance::{PixelOut, Radiance, RadianceParams};
    use crate::util::Result;
    use futures::StreamExt;
    use geoengine_datatypes::primitives::{
        Measurement, SpatialPartition2D, SpatialResolution, TimeInterval,
    };
    use geoengine_datatypes::raster::{
        EmptyGrid2D, Grid2D, RasterDataType, RasterProperties, RasterPropertiesEntry,
        RasterPropertiesKey, RasterTile2D, TileInformation,
    };
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use num_traits::AsPrimitive;

    #[tokio::test]
    async fn test_ok() -> Result<()> {
        let no_data_value_option = Some(super::OUT_NO_DATA_VALUE);

        let input = make_raster(Some(11.0), Some(2.0));

        let op = Radiance {
            sources: SingleRasterSource { raster: input },
            params: RadianceParams {},
        }
        .boxed()
        .initialize(&MockExecutionContext::default())
        .await
        .unwrap();

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

        let result: Vec<Result<RasterTile2D<PixelOut>>> = result_stream.collect().await;

        assert_eq!(1, result.len());
        assert_eq!(
            result[0].as_ref().unwrap().grid_array,
            Grid2D::new(
                [3, 2].into(),
                vec![13.0, 15.0, 17.0, 19.0, 21.0, no_data_value_option.unwrap()],
                no_data_value_option,
            )
            .unwrap()
            .into()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_raster() -> Result<()> {
        let no_data_value_option = Some(super::OUT_NO_DATA_VALUE);

        let input = make_empty_raster();

        let op = Radiance {
            sources: SingleRasterSource { raster: input },
            params: RadianceParams {},
        }
        .boxed()
        .initialize(&MockExecutionContext::default())
        .await
        .unwrap();

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

        let result: Vec<Result<RasterTile2D<PixelOut>>> = result_stream.collect().await;

        assert_eq!(
            result[0].as_ref().unwrap().grid_array,
            EmptyGrid2D::new([3, 2].into(), no_data_value_option.unwrap(),).into()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_missing_offset() -> std::result::Result<(), &'static str> {
        let input = make_raster(None, Some(2.0));

        let op = Radiance {
            sources: SingleRasterSource { raster: input },
            params: RadianceParams {},
        }
        .boxed()
        .initialize(&MockExecutionContext::default())
        .await
        .unwrap();

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

        let result: Vec<Result<RasterTile2D<PixelOut>>> = result_stream.collect().await;

        assert_eq!(1, result.len());
        match &result[0] {
            Err(_) => Ok(()),
            Ok(_) => Err("Should fail on missing \"msg.CalibrationOffset\" property."),
        }
    }

    #[tokio::test]
    async fn test_missing_slope() -> std::result::Result<(), &'static str> {
        let input = make_raster(Some(11.0), None);

        let op = Radiance {
            sources: SingleRasterSource { raster: input },
            params: RadianceParams {},
        }
        .boxed()
        .initialize(&MockExecutionContext::default())
        .await
        .unwrap();

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

        let result: Vec<Result<RasterTile2D<PixelOut>>> = result_stream.collect().await;

        assert_eq!(1, result.len());
        match &result[0] {
            Err(_) => Ok(()),
            Ok(_) => Err("Should fail on missing \"msg.CalibrationOffset\" property."),
        }
    }

    fn make_empty_raster() -> Box<dyn RasterOperator> {
        let no_data_value = Some(0_u8);

        let raster_tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_tile_position: [-1, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
                global_geo_transform: Default::default(),
            },
            EmptyGrid2D::new([3, 2].into(), no_data_value.unwrap()).into(),
        );

        MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed()
    }

    fn make_raster(offset: Option<f64>, slope: Option<f64>) -> Box<dyn RasterOperator> {
        let no_data_value = Some(0);

        let data = vec![1, 2, 3, 4, 5, no_data_value.unwrap()];
        let raster = Grid2D::new([3, 2].into(), data, no_data_value).unwrap();

        let mut props = RasterProperties::default();

        if let Some(p) = offset {
            props.properties_map.insert(
                RasterPropertiesKey {
                    domain: Some("msg".into()),
                    key: "CalibrationOffset".into(),
                },
                RasterPropertiesEntry::Number(p),
            );
        }

        if let Some(p) = slope {
            props.properties_map.insert(
                RasterPropertiesKey {
                    domain: Some("msg".into()),
                    key: "CalibrationSlope".into(),
                },
                RasterPropertiesEntry::Number(p),
            );
        }

        let raster_tile = RasterTile2D::new_with_tile_info_and_properties(
            TimeInterval::default(),
            TileInformation {
                global_tile_position: [-1, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
                global_geo_transform: Default::default(),
            },
            raster.into(),
            props,
        );

        MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed()
    }
}
