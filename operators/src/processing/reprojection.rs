use super::map_query::MapQueryProcessor;
use crate::{
    adapters::{fold_by_coordinate_lookup_future, RasterSubQueryAdapter, TileReprojectionSubQuery},
    engine::{
        ExecutionContext, InitializedRasterOperator, InitializedVectorOperator, Operator,
        QueryContext, QueryProcessor, QueryRectangle, RasterOperator, RasterQueryProcessor,
        RasterQueryRectangle, RasterResultDescriptor, SingleRasterOrVectorSource,
        TypedRasterQueryProcessor, TypedVectorQueryProcessor, VectorOperator, VectorQueryProcessor,
        VectorQueryRectangle, VectorResultDescriptor,
    },
    error::{self, Error},
    util::{input::RasterOrVectorOperator, Result},
};
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use geoengine_datatypes::primitives::Coordinate2D;
use geoengine_datatypes::{
    operations::reproject::{
        suggest_pixel_size_from_diag_cross_projected, CoordinateProjection, CoordinateProjector,
        Reproject, ReprojectClipped,
    },
    primitives::{AxisAlignedRectangle, SpatialResolution},
    primitives::{BoundingBox2D, SpatialPartition2D},
    raster::{Pixel, RasterTile2D, TilingSpecification},
    spatial_reference::SpatialReference,
};
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "camelCase")]
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
    out_no_data_value: f64,
}

pub type Reprojection = Operator<ReprojectionParams, SingleRasterOrVectorSource>;
pub struct InitializedVectorReprojection {
    result_descriptor: VectorResultDescriptor,
    source: Box<dyn InitializedVectorOperator>,
    state: VectorReprojectionState,
}

pub struct InitializedRasterReprojection {
    result_descriptor: RasterResultDescriptor,
    source: Box<dyn InitializedRasterOperator>,
    state: RasterReprojectionState,
}

#[typetag::serde]
#[async_trait]
impl VectorOperator for Reprojection {
    async fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedVectorOperator>> {
        let vector_operator = match self.sources.source {
            RasterOrVectorOperator::Vector(operator) => operator,
            RasterOrVectorOperator::Raster(_) => return Err(Error::InvalidOperatorType),
        };

        let vector_operator = vector_operator.initialize(context).await?;

        let in_desc: &VectorResultDescriptor = vector_operator.result_descriptor();
        let out_desc = VectorResultDescriptor {
            spatial_reference: self.params.target_spatial_reference.into(),
            data_type: in_desc.data_type,
            columns: in_desc.columns.clone(),
        };

        let state = VectorReprojectionState {
            source_srs: Option::from(in_desc.spatial_reference).unwrap(),
            target_srs: self.params.target_spatial_reference,
        };

        let initialized_operator = InitializedVectorReprojection {
            result_descriptor: out_desc,
            source: vector_operator,
            state,
        };

        Ok(initialized_operator.boxed())
    }
}

impl InitializedVectorOperator for InitializedVectorReprojection {
    fn result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        let state = self.state;
        match self.source.query_processor()? {
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

/// this method performs the transformation of a query rectangle in `target` projection
/// to a new query rectangle with coordinates in the `source` projection
pub fn query_rewrite_fn(
    query: VectorQueryRectangle,
    source: SpatialReference,
    target: SpatialReference,
) -> Result<VectorQueryRectangle> {
    let projector_source_target = CoordinateProjector::from_known_srs(source, target)?;
    let projector_target_source = CoordinateProjector::from_known_srs(target, source)?;

    let p_bbox = query
        .spatial_bounds
        .reproject_clipped(&projector_target_source)?;
    let s_bbox = p_bbox.reproject(&projector_source_target)?;

    let p_spatial_resolution =
        suggest_pixel_size_from_diag_cross_projected(s_bbox, p_bbox, query.spatial_resolution)?;
    Ok(VectorQueryRectangle {
        spatial_bounds: p_bbox,
        spatial_resolution: p_spatial_resolution,
        time_interval: query.time_interval,
    })
}

#[async_trait]
impl<Q, G> QueryProcessor for VectorReprojectionProcessor<Q, G>
where
    Q: QueryProcessor<Output = G, SpatialBounds = BoundingBox2D>,
    G: Reproject<CoordinateProjector> + Sync + Send,
{
    type Output = G::Out;
    type SpatialBounds = BoundingBox2D;

    async fn query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let rewritten_query = query_rewrite_fn(query, self.from, self.to)?;

        Ok(self
            .source
            .query(rewritten_query, ctx)
            .await?
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
#[async_trait]
impl RasterOperator for Reprojection {
    async fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let raster_operator = match self.sources.source {
            RasterOrVectorOperator::Raster(operator) => operator,
            RasterOrVectorOperator::Vector(_) => return Err(Error::InvalidOperatorType),
        };

        let raster_operator = raster_operator.initialize(context).await?;

        let in_desc: &RasterResultDescriptor = raster_operator.result_descriptor();
        let out_no_data_value = in_desc.no_data_value.unwrap_or(0.); // TODO: add option to force a no_data_value

        let out_desc = RasterResultDescriptor {
            spatial_reference: self.params.target_spatial_reference.into(),
            data_type: in_desc.data_type,
            measurement: in_desc.measurement.clone(),
            no_data_value: Some(out_no_data_value),
        };

        let state = RasterReprojectionState {
            source_srs: Option::from(in_desc.spatial_reference).unwrap(),
            target_srs: self.params.target_spatial_reference,
            tiling_spec: context.tiling_specification(),
            out_no_data_value,
        };

        let initialized_operator = InitializedRasterReprojection {
            result_descriptor: out_desc,
            source: raster_operator,
            state,
        };

        Ok(initialized_operator.boxed())
    }
}

impl InitializedRasterOperator for InitializedRasterReprojection {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    // i know there is a macro somewhere. we need to re-work this when we have the no-data value anyway.
    #[allow(clippy::too_many_lines)]
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let q = self.source.query_processor()?;

        let s = self.state;

        Ok(match self.result_descriptor.data_type {
            geoengine_datatypes::raster::RasterDataType::U8 => {
                let qt = q.get_u8().unwrap();
                TypedRasterQueryProcessor::U8(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    s.source_srs,
                    s.target_srs,
                    s.tiling_spec,
                    s.out_no_data_value.as_(),
                )))
            }
            geoengine_datatypes::raster::RasterDataType::U16 => {
                let qt = q.get_u16().unwrap();
                TypedRasterQueryProcessor::U16(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    s.source_srs,
                    s.target_srs,
                    s.tiling_spec,
                    s.out_no_data_value.as_(),
                )))
            }

            geoengine_datatypes::raster::RasterDataType::U32 => {
                let qt = q.get_u32().unwrap();
                TypedRasterQueryProcessor::U32(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    s.source_srs,
                    s.target_srs,
                    s.tiling_spec,
                    s.out_no_data_value.as_(),
                )))
            }
            geoengine_datatypes::raster::RasterDataType::U64 => {
                let qt = q.get_u64().unwrap();
                TypedRasterQueryProcessor::U64(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    s.source_srs,
                    s.target_srs,
                    s.tiling_spec,
                    s.out_no_data_value.as_(),
                )))
            }
            geoengine_datatypes::raster::RasterDataType::I8 => {
                let qt = q.get_i8().unwrap();
                TypedRasterQueryProcessor::I8(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    s.source_srs,
                    s.target_srs,
                    s.tiling_spec,
                    s.out_no_data_value.as_(),
                )))
            }
            geoengine_datatypes::raster::RasterDataType::I16 => {
                let qt = q.get_i16().unwrap();
                TypedRasterQueryProcessor::I16(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    s.source_srs,
                    s.target_srs,
                    s.tiling_spec,
                    s.out_no_data_value.as_(),
                )))
            }
            geoengine_datatypes::raster::RasterDataType::I32 => {
                let qt = q.get_i32().unwrap();
                TypedRasterQueryProcessor::I32(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    s.source_srs,
                    s.target_srs,
                    s.tiling_spec,
                    s.out_no_data_value.as_(),
                )))
            }
            geoengine_datatypes::raster::RasterDataType::I64 => {
                let qt = q.get_i64().unwrap();
                TypedRasterQueryProcessor::I64(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    s.source_srs,
                    s.target_srs,
                    s.tiling_spec,
                    s.out_no_data_value.as_(),
                )))
            }
            geoengine_datatypes::raster::RasterDataType::F32 => {
                let qt = q.get_f32().unwrap();
                TypedRasterQueryProcessor::F32(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    s.source_srs,
                    s.target_srs,
                    s.tiling_spec,
                    s.out_no_data_value.as_(),
                )))
            }
            geoengine_datatypes::raster::RasterDataType::F64 => {
                let qt = q.get_f64().unwrap();
                TypedRasterQueryProcessor::F64(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    s.source_srs,
                    s.target_srs,
                    s.tiling_spec,
                    s.out_no_data_value.as_(),
                )))
            }
        })
    }
}

/// compute the resolution for the source. For this we subdivide the valid area into a grid and compute the resulting resolution as
/// the minimum of the required resolutions for all tiles. This gives us a global, reproducible resolution. We don't use the global
/// tiling strategy for subdivision because we want to avoid partially valid tiles (where a tile is partially outside of the area
/// of use of the source or target projection). Rather we split divide it into a configurable number of homogeneous `rows` and `cols`
/// that align with the intersection of source and target area of use.
fn query_source_resolution<A: AxisAlignedRectangle>(
    query_resolution: SpatialResolution,
    projector_source_target: &CoordinateProjector,
    projector_target_source: &CoordinateProjector,
    rows: usize,
    cols: usize,
) -> Result<SpatialResolution> {
    let use_area = projector_source_target.to.area_of_use_projected::<A>()?;
    let source_use_area_clipped = use_area.reproject_clipped(projector_target_source)?;
    let use_area_clipped = source_use_area_clipped.reproject_clipped(projector_source_target)?;

    let width = use_area_clipped.size_x() / cols as f64;
    let height = use_area_clipped.size_y() / rows as f64;
    let mut resolution: Option<SpatialResolution> = None;
    for row in 0..rows {
        for col in 0..cols {
            let target_bbox = A::from_min_max(
                use_area_clipped.lower_left()
                    + Coordinate2D::new(col as f64 * width, row as f64 * height),
                use_area_clipped.lower_left()
                    + Coordinate2D::new((col + 1) as f64 * width, (row + 1) as f64 * height),
            )?;

            let source_bbox = target_bbox.reproject_clipped(projector_target_source)?;

            let tile_resolution = suggest_pixel_size_from_diag_cross_projected(
                target_bbox,
                source_bbox,
                query_resolution,
            )
            .context(error::DataType)?;

            if let Some(current_resolution) = resolution {
                resolution = Some(SpatialResolution::new(
                    f64::min(current_resolution.x, tile_resolution.x),
                    f64::min(current_resolution.y, tile_resolution.y),
                )?);
            } else {
                resolution = Some(tile_resolution);
            }
        }
    }

    resolution.ok_or(error::Error::NoSourceResolution)
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

    /// clip query s.th. it is valid in the source projection
    fn clip_query<T: AxisAlignedRectangle>(
        query: QueryRectangle<T>,
        projector_source_target: &CoordinateProjector,
        projector_target_source: &CoordinateProjector,
    ) -> Result<QueryRectangle<T>> {
        let p_bbox = query
            .spatial_bounds
            .reproject_clipped(projector_target_source)?;
        let s_bbox = p_bbox.reproject(projector_source_target)?;

        Ok(QueryRectangle {
            spatial_bounds: s_bbox,
            time_interval: query.time_interval,
            spatial_resolution: query.spatial_resolution,
        })
    }
}

#[async_trait]
impl<Q, P> QueryProcessor for RasterReprojectionProcessor<Q, P>
where
    Q: QueryProcessor<Output = RasterTile2D<P>, SpatialBounds = SpatialPartition2D>,
    P: Pixel,
{
    type Output = RasterTile2D<P>;
    type SpatialBounds = SpatialPartition2D;

    async fn query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let projector_source_target = CoordinateProjector::from_known_srs(self.from, self.to)?;
        let projector_target_source = CoordinateProjector::from_known_srs(self.to, self.from)?;

        let sub_query_spec = TileReprojectionSubQuery {
            in_srs: self.from,
            out_srs: self.to,
            no_data_and_fill_value: self.no_data_and_fill_value,
            fold_fn: fold_by_coordinate_lookup_future,
            in_spatial_res: query_source_resolution::<SpatialPartition2D>(
                query.spatial_resolution,
                &projector_source_target,
                &projector_target_source,
                1, // TODO: use a configurable number of rows and cols, but only a resolution computed on a single tile seems to not lead to over estimates
                1,
            )?,
        };
        let s = RasterSubQueryAdapter::<'a, P, _, _>::new(
            &self.source,
            Self::clip_query(query, &projector_source_target, &projector_target_source)?,
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
        engine::{QueryRectangle, VectorOperator},
        source::{
            FileNotFoundHandling, GdalDatasetParameters, GdalMetaDataRegular, GdalSource,
            GdalSourceParameters,
        },
        util::gdal::{add_ndvi_dataset, raster_dir},
    };
    use gdal::Dataset;
    use geoengine_datatypes::{
        collections::{MultiLineStringCollection, MultiPointCollection, MultiPolygonCollection},
        dataset::{DatasetId, InternalDatasetId},
        primitives::{
            BoundingBox2D, Measurement, MultiLineString, MultiPoint, MultiPolygon,
            SpatialResolution, TimeGranularity, TimeInstance, TimeInterval, TimeStep,
        },
        raster::{
            GeoTransform, Grid, GridShape, GridShape2D, GridSize, RasterDataType, RasterTile2D,
        },
        spatial_reference::SpatialReferenceAuthority,
        util::{
            well_known_data::{
                COLOGNE_EPSG_4326, COLOGNE_EPSG_900_913, HAMBURG_EPSG_4326, HAMBURG_EPSG_900_913,
                MARBURG_EPSG_4326, MARBURG_EPSG_900_913,
            },
            Identifier,
        },
    };

    use crate::engine::{MockExecutionContext, MockQueryContext};
    use crate::mock::MockFeatureCollectionSource;
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use futures::StreamExt;

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
            params: ReprojectionParams {
                target_spatial_reference,
            },
            sources: SingleRasterOrVectorSource {
                source: point_source.into(),
            },
        })
        .initialize(&MockExecutionContext::default())
        .await?;

        let query_processor = initialized_operator.query_processor()?;

        let query_processor = query_processor.multi_point().unwrap();

        let query_rectangle = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new(
                (COLOGNE_EPSG_4326.x, MARBURG_EPSG_4326.y).into(),
                (MARBURG_EPSG_4326.x, HAMBURG_EPSG_4326.y).into(),
            )
            .unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let ctx = MockQueryContext::new(usize::MAX);

        let query = query_processor.query(query_rectangle, &ctx).await.unwrap();

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
            params: ReprojectionParams {
                target_spatial_reference,
            },
            sources: SingleRasterOrVectorSource {
                source: lines_source.into(),
            },
        })
        .initialize(&MockExecutionContext::default())
        .await?;

        let query_processor = initialized_operator.query_processor()?;

        let query_processor = query_processor.multi_line_string().unwrap();

        let query_rectangle = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new(
                (COLOGNE_EPSG_4326.x, MARBURG_EPSG_4326.y).into(),
                (MARBURG_EPSG_4326.x, HAMBURG_EPSG_4326.y).into(),
            )
            .unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let ctx = MockQueryContext::new(usize::MAX);

        let query = query_processor.query(query_rectangle, &ctx).await.unwrap();

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
            params: ReprojectionParams {
                target_spatial_reference,
            },
            sources: SingleRasterOrVectorSource {
                source: polygon_source.into(),
            },
        })
        .initialize(&MockExecutionContext::default())
        .await?;

        let query_processor = initialized_operator.query_processor()?;

        let query_processor = query_processor.multi_polygon().unwrap();

        let query_rectangle = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new(
                (COLOGNE_EPSG_4326.x, MARBURG_EPSG_4326.y).into(),
                (MARBURG_EPSG_4326.x, HAMBURG_EPSG_4326.y).into(),
            )
            .unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let ctx = MockQueryContext::new(usize::MAX);

        let query = query_processor.query(query_rectangle, &ctx).await.unwrap();

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

        let no_data_value = Some(0);

        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                global_geo_transform: Default::default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                global_geo_transform: Default::default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                global_geo_transform: Default::default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                global_geo_transform: Default::default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
        ];

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
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
            params: ReprojectionParams {
                target_spatial_reference: projection, // This test will do a identity reprojection
            },
            sources: SingleRasterOrVectorSource {
                source: mrs1.into(),
            },
        })
        .initialize(&exe_ctx)
        .await?;

        let qp = initialized_operator
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 1.).into(), (3., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 10),
            spatial_resolution: SpatialResolution::one(),
        };

        let a = qp.raster_query(query_rect, &query_ctx).await?;

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
        let id = add_ndvi_dataset(&mut exe_ctx);
        exe_ctx.tiling_specification =
            TilingSpecification::new((0.0, 0.0).into(), [450, 450].into());

        let output_shape: GridShape2D = [900, 1800].into();
        let output_bounds =
            SpatialPartition2D::new_unchecked((0., 20_000_000.).into(), (20_000_000., 0.).into());
        let time_interval = TimeInterval::new_unchecked(1_388_534_400_000, 1_388_534_400_001);
        // 2014-01-01

        let gdal_op = GdalSource {
            params: GdalSourceParameters {
                dataset: id.clone(),
            },
        }
        .boxed();

        let projection = SpatialReference::new(
            geoengine_datatypes::spatial_reference::SpatialReferenceAuthority::Epsg,
            3857,
        );

        let initialized_operator = RasterOperator::boxed(Reprojection {
            params: ReprojectionParams {
                target_spatial_reference: projection,
            },
            sources: SingleRasterOrVectorSource {
                source: gdal_op.into(),
            },
        })
        .initialize(&exe_ctx)
        .await?;

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
                RasterQueryRectangle {
                    spatial_bounds: output_bounds,
                    time_interval,
                    spatial_resolution,
                },
                &query_ctx,
            )
            .await
            .unwrap();

        let res = qs
            .map(Result::unwrap)
            .collect::<Vec<RasterTile2D<u8>>>()
            .await;

        // This check is against a tile produced by the operator itself. It was visually validated. TODO: rebuild when open issues are solved.
        // A perfect validation would be against a GDAL output generated like this:
        // gdalwarp -t_srs EPSG:3857 -tr 11111.11111111 11111.11111111 -r near -te 0.0 5011111.111111112 5000000.0 10011111.111111112 -te_srs EPSG:3857 -of GTiff ./MOD13A2_M_NDVI_2014-04-01.TIFF ./MOD13A2_M_NDVI_2014-04-01_tile-20.rst
        assert_eq!(
            include_bytes!(
                "../../test-data/raster/modis_ndvi/projected_3857/MOD13A2_M_NDVI_2014-04-01_tile-20.rst"
            ) as &[u8],
            res[8].clone().into_materialized_tile().grid_array.data.as_slice()
        );

        Ok(())
    }

    #[test]
    fn query_rewrite_4326_3857() {
        let query = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new_unchecked((-180., -90.).into(), (180., 90.).into()),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };

        let expected = BoundingBox2D::new_unchecked(
            (-20_037_508.342_789_244, -20_048_966.104_014_6).into(),
            (20_037_508.342_789_244, 20_048_966.104_014_594).into(),
        );

        assert_eq!(
            expected,
            query_rewrite_fn(
                query,
                SpatialReference::new(SpatialReferenceAuthority::Epsg, 3857),
                SpatialReference::epsg_4326(),
            )
            .unwrap()
            .spatial_bounds
        );
    }

    #[tokio::test]
    async fn raster_ndvi_3857_to_4326() -> Result<()> {
        let mut exe_ctx = MockExecutionContext::default();
        let query_ctx = MockQueryContext::default();

        let m = GdalMetaDataRegular {
            start: TimeInstance::from_millis(1_388_534_400_000).unwrap(),
            step: TimeStep {
                granularity: TimeGranularity::Months,
                step: 1,
            },
            placeholder: "%%%_START_TIME_%%%".to_string(),
            time_format: "%Y-%m-%d".to_string(),
            params: GdalDatasetParameters {
                file_path: raster_dir()
                    .join("modis_ndvi/projected_3857/MOD13A2_M_NDVI_%%%_START_TIME_%%%.TIFF"),
                rasterband_channel: 1,
                geo_transform: GeoTransform {
                    origin_coordinate: (20_037_508.342_789_244, 19_971_868.880_408_563).into(),
                    x_pixel_size: 14_052.950_258_048_739,
                    y_pixel_size: -14_057.881_117_788_405,
                },
                width: 1000,
                height: 1000,
                file_not_found_handling: FileNotFoundHandling::Error,
                no_data_value: Some(0.),
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options: None,
            },
            result_descriptor: RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReference::new(SpatialReferenceAuthority::Epsg, 3857)
                    .into(),
                measurement: Measurement::Unitless,
                no_data_value: Some(0.),
            },
        };

        let id: DatasetId = InternalDatasetId::new().into();
        exe_ctx.add_meta_data(id.clone(), Box::new(m));

        exe_ctx.tiling_specification =
            TilingSpecification::new((0.0, 0.0).into(), [600, 600].into());

        let output_shape: GridShape2D = [1000, 1000].into();
        let output_bounds =
            SpatialPartition2D::new_unchecked((-180., 90.).into(), (180., -90.).into());
        let time_interval = TimeInterval::new_unchecked(1_388_534_400_000, 1_388_534_400_001);
        // 2014-01-01

        let gdal_op = GdalSource {
            params: GdalSourceParameters {
                dataset: id.clone(),
            },
        }
        .boxed();

        let initialized_operator = RasterOperator::boxed(Reprojection {
            params: ReprojectionParams {
                target_spatial_reference: SpatialReference::epsg_4326(),
            },
            sources: SingleRasterOrVectorSource {
                source: gdal_op.into(),
            },
        })
        .initialize(&exe_ctx)
        .await?;

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
                    spatial_bounds: output_bounds,
                    time_interval,
                    spatial_resolution,
                },
                &query_ctx,
            )
            .await
            .unwrap();

        qs.map(Result::unwrap)
            .collect::<Vec<RasterTile2D<u8>>>()
            .await;

        Ok(())
    }

    #[test]
    fn source_resolution() {
        let epsg_4326 = SpatialReference::epsg_4326();
        let epsg_3857 = SpatialReference::new(SpatialReferenceAuthority::Epsg, 3857);

        let projector_source_target =
            CoordinateProjector::from_known_srs(epsg_4326, epsg_3857).unwrap();
        let projector_target_source =
            CoordinateProjector::from_known_srs(epsg_3857, epsg_4326).unwrap();

        // use ndvi dataset that was reprojected using gdal as ground truth
        let dataset_4326 =
            Dataset::open(&raster_dir().join("modis_ndvi/MOD13A2_M_NDVI_2014-04-01.TIFF")).unwrap();
        let geotransform_4326 = dataset_4326.geo_transform().unwrap();
        let res_4326 = SpatialResolution::new(geotransform_4326[1], -geotransform_4326[5]).unwrap();

        let dataset_3857 = Dataset::open(
            &raster_dir().join("modis_ndvi/projected_3857/MOD13A2_M_NDVI_2014-04-01.TIFF"),
        )
        .unwrap();
        let geotransform_3857 = dataset_3857.geo_transform().unwrap();
        let res_3857 = SpatialResolution::new(geotransform_3857[1], -geotransform_3857[5]).unwrap();

        // ndvi was projected from 4326 to 3857. The calculated source_resolution for getting the raster in 3857 with `res_3857`
        // should thus roughly be like the original `res_4326`
        let result_res = query_source_resolution::<SpatialPartition2D>(
            res_3857,
            &projector_source_target,
            &projector_target_source,
            1,
            1,
        )
        .unwrap();
        assert!(1. - (result_res.x / res_4326.x).abs() < 0.02);
        assert!(1. - (result_res.y / res_4326.y).abs() < 0.02);
    }
}
