use futures::StreamExt;
use geoengine_datatypes::{
    operations::reproject::{
        suggest_pixel_size_from_diag_cross, CoordinateProjection, CoordinateProjector, Reproject,
    },
    raster::{Pixel, TilingSpecification},
    spatial_reference::SpatialReference,
};
use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::{
    adapters::{fold_by_coordinate_lookup_future, RasterOverlapAdapter, TileReprojectionSubQuery},
    engine::{
        ExecutionContext, InitializedOperator, InitializedOperatorImpl, InitializedRasterOperator,
        InitializedVectorOperator, Operator, QueryContext, QueryRectangle, RasterOperator,
        RasterQueryProcessor, RasterResultDescriptor, TypedRasterQueryProcessor,
        TypedVectorQueryProcessor, VectorOperator, VectorQueryProcessor, VectorResultDescriptor,
    },
    error,
    util::Result,
};

use super::map_query::MapQueryProcessor;
use futures::stream::BoxStream;

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct ReprojectionParams {
    pub target_spatial_reference: SpatialReference,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct VectorReprojectionState {
    source_srs: SpatialReference,
    target_srs: SpatialReference,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct RasterReprojectionState {
    source_srs: SpatialReference,
    target_srs: SpatialReference,
    tiling_spec: TilingSpecification,
}

pub type Reprojection = Operator<ReprojectionParams>;
pub type InitializedVectorReprojection =
    InitializedOperatorImpl<VectorResultDescriptor, VectorReprojectionState>;

pub type InitializedRasterReprojection =
    InitializedOperatorImpl<RasterResultDescriptor, RasterReprojectionState>;

#[typetag::serde]
impl VectorOperator for Reprojection {
    fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<InitializedVectorOperator>> {
        ensure!(
            self.vector_sources.len() == 1,
            error::InvalidNumberOfVectorInputs {
                expected: 1..2,
                found: self.vector_sources.len()
            }
        );
        ensure!(
            self.raster_sources.is_empty(),
            error::InvalidNumberOfRasterInputs {
                expected: 0..1,
                found: self.raster_sources.len()
            }
        );

        let initialized_vector_sources = self
            .vector_sources
            .into_iter()
            .map(|o| o.initialize(context))
            .collect::<Result<Vec<Box<InitializedVectorOperator>>>>()?;

        let in_desc: &VectorResultDescriptor = initialized_vector_sources[0].result_descriptor();
        let out_desc = VectorResultDescriptor {
            spatial_reference: self.params.target_spatial_reference.into(),
            data_type: in_desc.data_type,
            columns: in_desc.columns.clone(),
        };

        let state = VectorReprojectionState {
            source_srs: Option::from(in_desc.spatial_reference).unwrap(),
            target_srs: self.params.target_spatial_reference,
        };

        Ok(
            InitializedVectorReprojection::new(out_desc, vec![], initialized_vector_sources, state)
                .boxed(),
        )
    }
}

impl InitializedOperator<VectorResultDescriptor, TypedVectorQueryProcessor>
    for InitializedVectorReprojection
{
    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        let state = self.state;
        match self.vector_sources[0].query_processor()? {
            TypedVectorQueryProcessor::Data(source) => Ok(TypedVectorQueryProcessor::Data(
                MapQueryProcessor::new(source, move |query| {
                    query_rewrite_fn(query, state.source_srs, state.target_srs)
                })
                .boxed(),
            )),
            TypedVectorQueryProcessor::MultiPoint(source) => {
                Ok(TypedVectorQueryProcessor::MultiPoint(
                    VectorReprojectionProcessor::new(
                        source,
                        self.state.source_srs,
                        self.state.target_srs,
                    )
                    .boxed(),
                ))
            }
            TypedVectorQueryProcessor::MultiLineString(source) => {
                Ok(TypedVectorQueryProcessor::MultiLineString(
                    VectorReprojectionProcessor::new(
                        source,
                        self.state.source_srs,
                        self.state.target_srs,
                    )
                    .boxed(),
                ))
            }
            TypedVectorQueryProcessor::MultiPolygon(source) => {
                Ok(TypedVectorQueryProcessor::MultiPolygon(
                    VectorReprojectionProcessor::new(
                        source,
                        self.state.source_srs,
                        self.state.target_srs,
                    )
                    .boxed(),
                ))
            }
        }
    }
}

struct VectorReprojectionProcessor<Q, G>
where
    Q: VectorQueryProcessor<VectorType = G>,
{
    source: Q,
    from: SpatialReference,
    to: SpatialReference,
}

impl<Q, G> VectorReprojectionProcessor<Q, G>
where
    Q: VectorQueryProcessor<VectorType = G>,
{
    pub fn new(source: Q, from: SpatialReference, to: SpatialReference) -> Self {
        Self { source, from, to }
    }
}

/// this method performs the reverse transformation of a query rectangle
pub fn query_rewrite_fn(
    query: QueryRectangle,
    from: SpatialReference,
    to: SpatialReference,
) -> Result<QueryRectangle> {
    let projector = CoordinateProjector::from_known_srs(to, from)?;
    let p_bbox = query.bbox.reproject(&projector)?;
    let p_spatial_resolution =
        suggest_pixel_size_from_diag_cross(query.bbox, query.spatial_resolution, &projector)?;
    Ok(QueryRectangle {
        bbox: p_bbox,
        spatial_resolution: p_spatial_resolution,
        time_interval: query.time_interval,
    })
}

impl<Q, G> VectorQueryProcessor for VectorReprojectionProcessor<Q, G>
where
    Q: VectorQueryProcessor<VectorType = G>,
    G: Reproject<CoordinateProjector> + Sync + Send,
{
    type VectorType = G::Out;

    fn vector_query<'a>(
        &'a self,
        query: QueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::VectorType>>> {
        let rewritten_query = query_rewrite_fn(query, self.from, self.to)?;

        Ok(self
            .source
            .vector_query(rewritten_query, ctx)?
            .map(move |collection_result| {
                collection_result.and_then(|collection| {
                    CoordinateProjector::from_known_srs(self.from, self.to)
                        .and_then(|projector| collection.reproject(projector.as_ref()))
                        .map_err(Into::into)
                })
            })
            .boxed())
    }
}

#[typetag::serde]
impl RasterOperator for Reprojection {
    fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<InitializedRasterOperator>> {
        ensure!(
            self.vector_sources.is_empty(),
            crate::error::InvalidNumberOfVectorInputs {
                expected: 0..0,
                found: self.vector_sources.len()
            }
        );
        ensure!(
            !self.raster_sources.is_empty(),
            crate::error::InvalidNumberOfRasterInputs {
                expected: 1..1,
                found: self.raster_sources.len()
            }
        );

        let initialized_raster_sources = self
            .raster_sources
            .into_iter()
            .map(|o| o.initialize(context))
            .collect::<Result<Vec<Box<InitializedRasterOperator>>>>()?;

        let in_desc: &RasterResultDescriptor = initialized_raster_sources[0].result_descriptor();
        let out_desc = RasterResultDescriptor {
            spatial_reference: self.params.target_spatial_reference.into(),
            data_type: in_desc.data_type,
            measurement: in_desc.measurement.clone(),
        };

        let state = RasterReprojectionState {
            source_srs: Option::from(in_desc.spatial_reference).unwrap(),
            target_srs: self.params.target_spatial_reference,
            tiling_spec: context.tiling_specification(),
        };

        Ok(
            InitializedRasterReprojection::new(out_desc, initialized_raster_sources, vec![], state)
                .boxed(),
        )
    }
}

impl InitializedOperator<RasterResultDescriptor, TypedRasterQueryProcessor>
    for InitializedRasterReprojection
{
    // i know there is a macro somewhere. we need to re-work this when we have the no-data value anyway.
    #[allow(clippy::clippy::too_many_lines)]
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let q = self.raster_sources[0].query_processor()?;

        let s = self.state;

        Ok(match self.result_descriptor.data_type {
            geoengine_datatypes::raster::RasterDataType::U8 => {
                let qt = q.get_u8().unwrap();
                TypedRasterQueryProcessor::U8(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    s.source_srs,
                    s.target_srs,
                    s.tiling_spec,
                    0_u8,
                )))
            }
            geoengine_datatypes::raster::RasterDataType::U16 => {
                let qt = q.get_u16().unwrap();
                TypedRasterQueryProcessor::U16(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    s.source_srs,
                    s.target_srs,
                    s.tiling_spec,
                    0_u16,
                )))
            }

            geoengine_datatypes::raster::RasterDataType::U32 => {
                let qt = q.get_u32().unwrap();
                TypedRasterQueryProcessor::U32(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    s.source_srs,
                    s.target_srs,
                    s.tiling_spec,
                    0_u32,
                )))
            }
            geoengine_datatypes::raster::RasterDataType::U64 => {
                let qt = q.get_u64().unwrap();
                TypedRasterQueryProcessor::U64(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    s.source_srs,
                    s.target_srs,
                    s.tiling_spec,
                    0_u64,
                )))
            }
            geoengine_datatypes::raster::RasterDataType::I8 => {
                let qt = q.get_i8().unwrap();
                TypedRasterQueryProcessor::I8(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    s.source_srs,
                    s.target_srs,
                    s.tiling_spec,
                    0_i8,
                )))
            }
            geoengine_datatypes::raster::RasterDataType::I16 => {
                let qt = q.get_i16().unwrap();
                TypedRasterQueryProcessor::I16(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    s.source_srs,
                    s.target_srs,
                    s.tiling_spec,
                    0_i16,
                )))
            }
            geoengine_datatypes::raster::RasterDataType::I32 => {
                let qt = q.get_i32().unwrap();
                TypedRasterQueryProcessor::I32(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    s.source_srs,
                    s.target_srs,
                    s.tiling_spec,
                    0_i32,
                )))
            }
            geoengine_datatypes::raster::RasterDataType::I64 => {
                let qt = q.get_i64().unwrap();
                TypedRasterQueryProcessor::I64(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    s.source_srs,
                    s.target_srs,
                    s.tiling_spec,
                    0_i64,
                )))
            }
            geoengine_datatypes::raster::RasterDataType::F32 => {
                let qt = q.get_f32().unwrap();
                TypedRasterQueryProcessor::F32(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    s.source_srs,
                    s.target_srs,
                    s.tiling_spec,
                    0_f32,
                )))
            }
            geoengine_datatypes::raster::RasterDataType::F64 => {
                let qt = q.get_f64().unwrap();
                TypedRasterQueryProcessor::F64(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    s.source_srs,
                    s.target_srs,
                    s.tiling_spec,
                    0_f64,
                )))
            }
        })
    }
}

struct RasterReprojectionProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
{
    source: Q,
    from: SpatialReference,
    to: SpatialReference,
    tiling_spec: TilingSpecification,
    no_data_and_fill_value: P,
}

impl<Q, P> RasterReprojectionProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
{
    pub fn new(
        source: Q,
        from: SpatialReference,
        to: SpatialReference,
        tiling_spec: TilingSpecification,
        no_data_and_fill_value: P,
    ) -> Self {
        Self {
            source,
            from,
            to,
            tiling_spec,
            no_data_and_fill_value,
        }
    }
}

impl<Q, P> RasterQueryProcessor for RasterReprojectionProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
{
    type RasterType = P;

    fn raster_query<'a>(
        &'a self,
        query: QueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<geoengine_datatypes::raster::RasterTile2D<Self::RasterType>>>>
    {
        // we need a resolution for the sub-querys. And since we don't want this to change for tiles, we precompute it for the complete bbox and pass it to the sub-query spec.
        let projector = CoordinateProjector::from_known_srs(self.to, self.from)?;
        let p_spatial_resolution =
            suggest_pixel_size_from_diag_cross(query.bbox, query.spatial_resolution, &projector)?;

        let sub_query_spec = TileReprojectionSubQuery {
            in_srs: self.from,
            out_srs: self.to,
            no_data_and_fill_value: self.no_data_and_fill_value,
            fold_fn: fold_by_coordinate_lookup_future,
            in_spatial_res: p_spatial_resolution,
        };
        let s = RasterOverlapAdapter::<'a, P, _, _>::new(
            &self.source,
            query,
            self.tiling_spec,
            ctx,
            sub_query_spec,
        );

        Ok(s.boxed())
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        engine::VectorOperator,
        source::{GdalSource, GdalSourceParameters},
        util::gdal::add_ndvi_data_set,
    };
    use geoengine_datatypes::{
        collections::{MultiLineStringCollection, MultiPointCollection, MultiPolygonCollection},
        operations::image::{Colorizer, RgbaColor, ToPng},
        primitives::{
            BoundingBox2D, Measurement, MultiLineString, MultiPoint, MultiPolygon,
            SpatialResolution, TimeInterval,
        },
        raster::{Grid, GridShape, GridShape2D, GridSize, RasterDataType, RasterTile2D},
        spatial_reference::SpatialReferenceAuthority,
        util::well_known_data::{
            COLOGNE_EPSG_4326, COLOGNE_EPSG_900_913, HAMBURG_EPSG_4326, HAMBURG_EPSG_900_913,
            MARBURG_EPSG_4326, MARBURG_EPSG_900_913,
        },
    };

    use crate::engine::{MockExecutionContext, MockQueryContext, VectorQueryProcessor};
    use crate::mock::MockFeatureCollectionSource;
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use futures::StreamExt;

    use std::convert::TryInto;

    use super::*;

    #[tokio::test]
    async fn multi_point() -> Result<()> {
        let points = MultiPointCollection::from_data(
            MultiPoint::many(vec![
                MARBURG_EPSG_4326,
                COLOGNE_EPSG_4326,
                HAMBURG_EPSG_4326,
            ])
            .unwrap(),
            vec![TimeInterval::new_unchecked(0, 1); 3],
            Default::default(),
        )?;

        let projected_points = MultiPointCollection::from_data(
            MultiPoint::many(vec![
                MARBURG_EPSG_900_913,
                COLOGNE_EPSG_900_913,
                HAMBURG_EPSG_900_913,
            ])
            .unwrap(),
            vec![TimeInterval::new_unchecked(0, 1); 3],
            Default::default(),
        )?;

        let point_source = MockFeatureCollectionSource::single(points.clone()).boxed();

        let target_spatial_reference =
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 900_913);

        let initialized_operator = VectorOperator::boxed(Reprojection {
            vector_sources: vec![point_source],
            raster_sources: vec![],
            params: ReprojectionParams {
                target_spatial_reference,
            },
        })
        .initialize(&MockExecutionContext::default())?;

        let query_processor = initialized_operator.query_processor()?;

        let query_processor = query_processor.multi_point().unwrap();

        let query_rectangle = QueryRectangle {
            bbox: BoundingBox2D::new(
                (COLOGNE_EPSG_4326.x, MARBURG_EPSG_4326.y).into(),
                (MARBURG_EPSG_4326.x, HAMBURG_EPSG_4326.y).into(),
            )
            .unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let ctx = MockQueryContext::new(usize::MAX);

        let query = query_processor.vector_query(query_rectangle, &ctx).unwrap();

        let result = query
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        assert_eq!(result[0], projected_points);

        Ok(())
    }

    #[tokio::test]
    async fn multi_lines() -> Result<()> {
        let lines = MultiLineStringCollection::from_data(
            vec![MultiLineString::new(vec![vec![
                MARBURG_EPSG_4326,
                COLOGNE_EPSG_4326,
                HAMBURG_EPSG_4326,
            ]])
            .unwrap()],
            vec![TimeInterval::new_unchecked(0, 1); 1],
            Default::default(),
        )?;

        let projected_lines = MultiLineStringCollection::from_data(
            vec![MultiLineString::new(vec![vec![
                MARBURG_EPSG_900_913,
                COLOGNE_EPSG_900_913,
                HAMBURG_EPSG_900_913,
            ]])
            .unwrap()],
            vec![TimeInterval::new_unchecked(0, 1); 1],
            Default::default(),
        )?;

        let lines_source = MockFeatureCollectionSource::single(lines.clone()).boxed();

        let target_spatial_reference =
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 900_913);

        let initialized_operator = VectorOperator::boxed(Reprojection {
            vector_sources: vec![lines_source],
            raster_sources: vec![],
            params: ReprojectionParams {
                target_spatial_reference,
            },
        })
        .initialize(&MockExecutionContext::default())?;

        let query_processor = initialized_operator.query_processor()?;

        let query_processor = query_processor.multi_line_string().unwrap();

        let query_rectangle = QueryRectangle {
            bbox: BoundingBox2D::new(
                (COLOGNE_EPSG_4326.x, MARBURG_EPSG_4326.y).into(),
                (MARBURG_EPSG_4326.x, HAMBURG_EPSG_4326.y).into(),
            )
            .unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let ctx = MockQueryContext::new(usize::MAX);

        let query = query_processor.vector_query(query_rectangle, &ctx).unwrap();

        let result = query
            .map(Result::unwrap)
            .collect::<Vec<MultiLineStringCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        assert_eq!(result[0], projected_lines);

        Ok(())
    }

    #[tokio::test]
    async fn multi_polygons() -> Result<()> {
        let polygons = MultiPolygonCollection::from_data(
            vec![MultiPolygon::new(vec![vec![vec![
                MARBURG_EPSG_4326,
                COLOGNE_EPSG_4326,
                HAMBURG_EPSG_4326,
                MARBURG_EPSG_4326,
            ]]])
            .unwrap()],
            vec![TimeInterval::new_unchecked(0, 1); 1],
            Default::default(),
        )?;

        let projected_polygons = MultiPolygonCollection::from_data(
            vec![MultiPolygon::new(vec![vec![vec![
                MARBURG_EPSG_900_913,
                COLOGNE_EPSG_900_913,
                HAMBURG_EPSG_900_913,
                MARBURG_EPSG_900_913,
            ]]])
            .unwrap()],
            vec![TimeInterval::new_unchecked(0, 1); 1],
            Default::default(),
        )?;

        let polygon_source = MockFeatureCollectionSource::single(polygons.clone()).boxed();

        let target_spatial_reference =
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 900_913);

        let initialized_operator = VectorOperator::boxed(Reprojection {
            vector_sources: vec![polygon_source],
            raster_sources: vec![],
            params: ReprojectionParams {
                target_spatial_reference,
            },
        })
        .initialize(&MockExecutionContext::default())?;

        let query_processor = initialized_operator.query_processor()?;

        let query_processor = query_processor.multi_polygon().unwrap();

        let query_rectangle = QueryRectangle {
            bbox: BoundingBox2D::new(
                (COLOGNE_EPSG_4326.x, MARBURG_EPSG_4326.y).into(),
                (MARBURG_EPSG_4326.x, HAMBURG_EPSG_4326.y).into(),
            )
            .unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let ctx = MockQueryContext::new(usize::MAX);

        let query = query_processor.vector_query(query_rectangle, &ctx).unwrap();

        let result = query
            .map(Result::unwrap)
            .collect::<Vec<MultiPolygonCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        assert_eq!(result[0], projected_polygons);

        Ok(())
    }

    #[tokio::test]
    async fn raster_identity() -> Result<()> {
        let projection = SpatialReference::new(
            geoengine_datatypes::spatial_reference::SpatialReferenceAuthority::Epsg,
            4326,
        );

        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                global_geo_transform: Default::default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4], Some(0)).unwrap(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                global_geo_transform: Default::default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10], Some(0)).unwrap(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                global_geo_transform: Default::default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16], Some(0)).unwrap(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                global_geo_transform: Default::default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22], Some(0)).unwrap(),
            },
        ];

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                },
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            // we need a smaller tile size
            shape_array: [2, 2],
        };

        let query_ctx = MockQueryContext {
            chunk_byte_size: 1024 * 1024,
        };

        let initialized_operator = RasterOperator::boxed(Reprojection {
            vector_sources: vec![],
            raster_sources: vec![mrs1],
            params: ReprojectionParams {
                target_spatial_reference: projection, // This test will do a identity reprojhection
            },
        })
        .initialize(&exe_ctx)?;

        let qp = initialized_operator
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let query_rect = QueryRectangle {
            bbox: BoundingBox2D::new_unchecked((0., 0.).into(), (3., 1.).into()),
            time_interval: TimeInterval::new_unchecked(0, 10),
            spatial_resolution: SpatialResolution::one(),
        };

        let a = qp.raster_query(query_rect, &query_ctx)?;

        let res = a
            .map(Result::unwrap)
            .collect::<Vec<RasterTile2D<u8>>>()
            .await;
        assert_eq!(data, res);

        Ok(())
    }

    #[tokio::test]
    async fn raster_ndvi_3857() -> Result<()> {
        let mut exe_ctx = MockExecutionContext::default();
        let query_ctx = MockQueryContext::default();
        let id = add_ndvi_data_set(&mut exe_ctx);
        exe_ctx.tiling_specification =
            TilingSpecification::new((0.0, 0.0).into(), [450, 450].into());

        let output_shape: GridShape2D = [1800, 3600].into();
        let output_bounds = BoundingBox2D::new_unchecked(
            (-20_000_000., -20_000_000.).into(),
            (20_000_000., 20_000_000.).into(),
        );
        let time_interval = TimeInterval::new_unchecked(1_388_534_400_000, 1_388_534_400_001);
        // 2014-01-01

        let gdal_op = GdalSource {
            params: GdalSourceParameters {
                data_set: id.clone(),
            },
        }
        .boxed();

        let projection = SpatialReference::new(
            geoengine_datatypes::spatial_reference::SpatialReferenceAuthority::Epsg,
            3857,
        );

        let initialized_operator = RasterOperator::boxed(Reprojection {
            vector_sources: vec![],
            raster_sources: vec![gdal_op],
            params: ReprojectionParams {
                target_spatial_reference: projection,
            },
        })
        .initialize(&exe_ctx)?;

        let x_query_resolution = output_bounds.size_x() / output_shape.axis_size_x() as f64;
        let y_query_resolution = output_bounds.size_y() / (output_shape.axis_size_y() * 2) as f64; // *2 to account for the dataset aspect ratio 2:1
        let spatial_resolution =
            SpatialResolution::new_unchecked(x_query_resolution, y_query_resolution);

        let qp = initialized_operator
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let qs = qp
            .raster_query(
                QueryRectangle {
                    bbox: output_bounds,
                    time_interval,
                    spatial_resolution,
                },
                &query_ctx,
            )
            .unwrap();

        let res = qs
            .map(Result::unwrap)
            .collect::<Vec<RasterTile2D<u8>>>()
            .await;

        let colorizer = Colorizer::linear_gradient(
            vec![
                (0.0, RgbaColor::new(0, 0, 0, 255)).try_into().unwrap(),
                (255.0, RgbaColor::new(255, 255, 255, 255))
                    .try_into()
                    .unwrap(),
            ],
            RgbaColor::transparent(),
            RgbaColor::pink(),
        )
        .unwrap();

        // TODO: check against reference  data
        for (i, t) in res.iter().enumerate() {
            dbg!(&t.tile_information());
            dbg!(&t.grid_array.shape);

            let (min, max) = t
                .grid_array
                .data
                .iter()
                .fold((255_u8, 0_u8), |x, &a| (a.min(x.0), a.max(x.1)));

            dbg!(min, max);

            let tile_shape = &t.grid_array.shape;

            let png = t.to_png(
                tile_shape.axis_size_y() as u32,
                tile_shape.axis_size_x() as u32,
                &colorizer,
            );

            let mut p = std::path::PathBuf::from("/tmp/foo");
            p.set_file_name(format!("meh_{}.png", i));
            std::fs::write(p.as_path(), png.unwrap()).expect("Unable to write file");
            p.set_file_name(format!("meh_{}.pgw", i));
            let loc_geo = t.tile_geo_transform();
            std::fs::write(p.as_path(), loc_geo.worldfile_string()).expect("Unable to write file");
        }

        Ok(())
    }
}
