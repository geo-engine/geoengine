use futures::StreamExt;
use geoengine_datatypes::{
    operations::reproject::{
        suggest_pixel_size_from_diag_cross, CoordinateProjection, CoordinateProjector, Reproject,
    },
    raster::Pixel,
    spatial_reference::SpatialReference,
};
use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::{
    engine::{
        ExecutionContext, InitializedOperator, InitializedOperatorImpl, InitializedVectorOperator,
        Operator, QueryContext, QueryRectangle, RasterQueryProcessor, TypedVectorQueryProcessor,
        VectorOperator, VectorQueryProcessor, VectorResultDescriptor,
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
pub struct ReprojectionState {
    source_srs: SpatialReference,
    target_srs: SpatialReference,
}

pub type Reprojection = Operator<ReprojectionParams>;
pub type InitializedReprojection =
    InitializedOperatorImpl<VectorResultDescriptor, ReprojectionState>;

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

        let state = ReprojectionState {
            source_srs: Option::from(in_desc.spatial_reference).unwrap(),
            target_srs: self.params.target_spatial_reference,
        };

        Ok(
            InitializedReprojection::new(out_desc, vec![], initialized_vector_sources, state)
                .boxed(),
        )
    }
}

impl InitializedOperator<VectorResultDescriptor, TypedVectorQueryProcessor>
    for InitializedReprojection
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
fn query_rewrite_fn(
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

struct RasterReprojectionProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
{
    source: Q,
    from: SpatialReference,
    to: SpatialReference,
}

impl<Q, P> RasterReprojectionProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
{
    pub fn new(source: Q, from: SpatialReference, to: SpatialReference) -> Self {
        Self { source, from, to }
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
        todo!()
    }
}

#[cfg(test)]
mod tests {

    use geoengine_datatypes::{
        collections::{MultiLineStringCollection, MultiPointCollection, MultiPolygonCollection},
        primitives::{
            BoundingBox2D, MultiLineString, MultiPoint, MultiPolygon, SpatialResolution,
            TimeInterval,
        },
        spatial_reference::SpatialReferenceAuthority,
        util::well_known_data::{
            COLOGNE_EPSG_4326, COLOGNE_EPSG_900_913, HAMBURG_EPSG_4326, HAMBURG_EPSG_900_913,
            MARBURG_EPSG_4326, MARBURG_EPSG_900_913,
        },
    };

    use crate::engine::{MockExecutionContext, MockQueryContext, QueryProcessor};
    use crate::mock::MockFeatureCollectionSource;
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

        let initialized_operator = Reprojection {
            vector_sources: vec![point_source],
            raster_sources: vec![],
            params: ReprojectionParams {
                target_spatial_reference,
            },
        }
        .boxed()
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

        let query = query_processor.query(query_rectangle, &ctx).unwrap();

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

        let initialized_operator = Reprojection {
            vector_sources: vec![lines_source],
            raster_sources: vec![],
            params: ReprojectionParams {
                target_spatial_reference,
            },
        }
        .boxed()
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

        let query = query_processor.query(query_rectangle, &ctx).unwrap();

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

        let initialized_operator = Reprojection {
            vector_sources: vec![polygon_source],
            raster_sources: vec![],
            params: ReprojectionParams {
                target_spatial_reference,
            },
        }
        .boxed()
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

        let query = query_processor.query(query_rectangle, &ctx).unwrap();

        let result = query
            .map(Result::unwrap)
            .collect::<Vec<MultiPolygonCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        assert_eq!(result[0], projected_polygons);

        Ok(())
    }
}
