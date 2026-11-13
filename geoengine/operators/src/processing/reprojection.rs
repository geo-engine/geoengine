use std::marker::PhantomData;

use super::map_query::MapQueryProcessor;
use crate::processing::{InitializedWarp, WarpMethod, WarpOrigin, WarpParams};
use crate::{
    adapters::{
        RasterSubQueryAdapter, TileReprojectionSubQuery, TileReprojectionSubqueryGridInfo,
        fold_by_coordinate_lookup_future,
    },
    engine::{
        CanonicOperatorName, ExecutionContext, InitializedRasterOperator, InitializedSources,
        InitializedVectorOperator, Operator, OperatorName, ProcessingGraphOperatorPath,
        QueryContext, QueryProcessor, RasterOperator, RasterQueryProcessor, RasterResultDescriptor,
        SingleRasterOrVectorSource, SpatialGridDescriptor, TypedVectorOperator,
        TypedVectorQueryProcessor, VectorOperator, VectorQueryProcessor, VectorResultDescriptor,
    },
    error::{self, Error},
    optimization::OptimizationError,
    util::{Result, input::RasterOrVectorOperator},
};
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{StreamExt, stream};
use geoengine_datatypes::{
    collections::FeatureCollection,
    operations::reproject::{Reproject, ReprojectClipped, reproject_spatial_query},
    primitives::{
        BandSelection, BoundingBox2D, ColumnSelection, Geometry, RasterQueryRectangle,
        SpatialPartition2D, SpatialResolution, VectorQueryRectangle,
    },
    raster::{GridBoundingBox2D, Pixel, RasterTile2D},
    spatial_reference::{CoordinateProjection, DefaultCoordinateProjector, SpatialReference},
    util::arrow::ArrowTyped,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "camelCase")]
#[derive(Default)]
pub enum DeriveOutRasterSpecsSource {
    DataBounds,
    #[default]
    ProjectionBounds,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "camelCase")]
pub struct ReprojectionParams {
    pub target_spatial_reference: SpatialReference,
    #[serde(default)]
    pub derive_out_spec: DeriveOutRasterSpecsSource,
}

pub type Reprojection = Operator<ReprojectionParams, SingleRasterOrVectorSource>;

impl Reprojection {}

impl OperatorName for Reprojection {
    const TYPE_NAME: &'static str = "Reprojection";
}

pub struct InitializedVectorReprojection {
    name: CanonicOperatorName,
    path: ProcessingGraphOperatorPath,
    result_descriptor: VectorResultDescriptor,
    params: ReprojectionParams,
    source: Box<dyn InitializedVectorOperator>,
    source_srs: SpatialReference,
    target_srs: SpatialReference,
}

impl InitializedVectorReprojection {
    /// Create a new `InitializedVectorReprojection` instance.
    /// The `source` must have the same `SpatialReference` as `source_srs`.
    ///
    /// # Errors
    /// This function errors if the `source`'s `SpatialReference` is `None`.
    /// This function errors if the source's bounding box cannot be reprojected to the target's `SpatialReference`.
    pub fn try_new_with_input(
        name: CanonicOperatorName,
        path: ProcessingGraphOperatorPath,
        params: ReprojectionParams,
        source_vector_operator: Box<dyn InitializedVectorOperator>,
    ) -> Result<Self> {
        let in_desc: VectorResultDescriptor = source_vector_operator.result_descriptor().clone();

        let in_srs = Into::<Option<SpatialReference>>::into(in_desc.spatial_reference)
            .ok_or(Error::AllSourcesMustHaveSameSpatialReference)?;

        let bbox = if let Some(bbox) = in_desc.bbox {
            let projector = DefaultCoordinateProjector::from_known_srs(
                in_srs,
                params.target_spatial_reference,
            )?;

            bbox.reproject_clipped(&projector)? // TODO: if this is none then we could skip the whole reprojection similar to raster?
        } else {
            None
        };

        let out_desc = VectorResultDescriptor {
            spatial_reference: params.target_spatial_reference.into(),
            data_type: in_desc.data_type,
            columns: in_desc.columns.clone(),
            time: in_desc.time,
            bbox,
        };

        Ok(InitializedVectorReprojection {
            name,
            path,
            result_descriptor: out_desc,
            params,
            source: source_vector_operator,
            source_srs: in_srs,
            target_srs: params.target_spatial_reference,
        })
    }
}

pub(crate) fn compute_output_spatial_grid(
    in_spatial_grid_descriptor: SpatialGridDescriptor,
    in_spatial_bounds: SpatialPartition2D,
    in_srs: SpatialReference,
    params: ReprojectionParams,
) -> Result<SpatialGridDescriptor> {
    let proj_from_to =
        DefaultCoordinateProjector::from_known_srs(in_srs, params.target_spatial_reference)?;
    let out_spatial_grid = match params.derive_out_spec {
        DeriveOutRasterSpecsSource::DataBounds => {
            in_spatial_grid_descriptor.reproject_clipped(&proj_from_to)?
        }
        DeriveOutRasterSpecsSource::ProjectionBounds => {
            let in_srs_area: SpatialPartition2D = in_srs.area_of_use_projected()?; // TODO: since we clip in projection anyway, we could use the AOU of the source projection?
            let target_proj_total_grid = in_spatial_grid_descriptor
                .spatial_bounds_to_compatible_spatial_grid(in_srs_area)
                .reproject_clipped(&proj_from_to)?;
            // First, move grid to actual dataset grid. Then reproject and then map projected bounds to grid.
            let spatial_bounds_proj = in_spatial_bounds.reproject_clipped(&proj_from_to)?;
            target_proj_total_grid.and_then(|x| {
                spatial_bounds_proj.map(|spb| x.spatial_bounds_to_compatible_spatial_grid(spb))
            })
        }
    };
    let out_spatial_grid = out_spatial_grid.ok_or(error::Error::ReprojectionFailed)?;
    Ok(out_spatial_grid)
}

#[async_trait]
impl VectorOperator for Reprojection {
    async fn _initialize(
        self: Box<Self>,
        path: ProcessingGraphOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedVectorOperator>> {
        let name = CanonicOperatorName::from(&self);

        let vector_source =
            self.sources
                .vector()
                .ok_or_else(|| error::Error::InvalidOperatorType {
                    expected: "Vector".to_owned(),
                    found: "Raster".to_owned(),
                })?;

        let initialized_source = vector_source
            .initialize_sources(path.clone(), context)
            .await?;

        let initialized_operator = InitializedVectorReprojection::try_new_with_input(
            name,
            path,
            self.params,
            initialized_source.vector,
        )?;

        Ok(initialized_operator.boxed())
    }

    span_fn!(Reprojection);
}

impl InitializedVectorOperator for InitializedVectorReprojection {
    fn result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        let source_srs = self.source_srs;
        let target_srs = self.target_srs;
        match self.source.query_processor()? {
            TypedVectorQueryProcessor::Data(source) => Ok(TypedVectorQueryProcessor::Data(
                MapQueryProcessor::new(source, move |query: VectorQueryRectangle| {
                    reproject_spatial_query::<_, DefaultCoordinateProjector>(
                        query.spatial_bounds(),
                        source_srs,
                        target_srs,
                        false,
                    ) // NOTE: this uses the `DefaultCoordinateProjector`, which may change.
                    .map(|sqr| {
                        sqr.map(|x| {
                            VectorQueryRectangle::new(x, query.time_interval(), *query.attributes())
                        })
                    })
                    .map_err(From::from)
                })
                .boxed(),
            )),
            TypedVectorQueryProcessor::MultiPoint(source) => {
                Ok(TypedVectorQueryProcessor::MultiPoint(
                    VectorReprojectionProcessor::new(
                        source,
                        self.result_descriptor.clone(),
                        source_srs,
                        target_srs,
                    )
                    .boxed(),
                ))
            }
            TypedVectorQueryProcessor::MultiLineString(source) => {
                Ok(TypedVectorQueryProcessor::MultiLineString(
                    VectorReprojectionProcessor::new(
                        source,
                        self.result_descriptor.clone(),
                        source_srs,
                        target_srs,
                    )
                    .boxed(),
                ))
            }
            TypedVectorQueryProcessor::MultiPolygon(source) => {
                Ok(TypedVectorQueryProcessor::MultiPolygon(
                    VectorReprojectionProcessor::new(
                        source,
                        self.result_descriptor.clone(),
                        source_srs,
                        target_srs,
                    )
                    .boxed(),
                ))
            }
        }
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn name(&self) -> &'static str {
        Reprojection::TYPE_NAME
    }

    fn path(&self) -> ProcessingGraphOperatorPath {
        self.path.clone()
    }

    fn optimize(
        &self,
        target_resolution: SpatialResolution,
    ) -> Result<TypedVectorOperator, OptimizationError> {
        self.source
            .optimize(target_resolution)
            .map(|optimized_source| {
                VectorOperator::typed(Reprojection {
                    params: self.params,
                    sources: SingleRasterOrVectorSource {
                        source: RasterOrVectorOperator::Vector(optimized_source),
                    },
                })
            })
    }
}

struct VectorReprojectionProcessor<Q, G>
where
    Q: VectorQueryProcessor<VectorType = FeatureCollection<G>>,
{
    source: Q,
    result_descriptor: VectorResultDescriptor,
    from: SpatialReference,
    to: SpatialReference,
}

impl<Q, G> VectorReprojectionProcessor<Q, G>
where
    Q: VectorQueryProcessor<VectorType = FeatureCollection<G>>,
{
    pub fn new(
        source: Q,
        result_descriptor: VectorResultDescriptor,
        from: SpatialReference,
        to: SpatialReference,
    ) -> Self {
        Self {
            source,
            result_descriptor,
            from,
            to,
        }
    }
}

#[async_trait]
impl<Q, G> QueryProcessor for VectorReprojectionProcessor<Q, G>
where
    Q: QueryProcessor<
            Output = FeatureCollection<G>,
            SpatialBounds = BoundingBox2D,
            Selection = ColumnSelection,
            ResultDescription = VectorResultDescriptor,
        >,
    FeatureCollection<G>: Reproject<DefaultCoordinateProjector, Out = FeatureCollection<G>>, // NOTE: this uses the `DefaultCoordinateProjector`, which may change.
    G: Geometry + ArrowTyped,
{
    type Output = FeatureCollection<G>;
    type SpatialBounds = BoundingBox2D;
    type Selection = ColumnSelection;
    type ResultDescription = VectorResultDescriptor;

    async fn _query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let rewritten_spatial_query = reproject_spatial_query::<_, DefaultCoordinateProjector>(
            query.spatial_bounds(),
            self.from,
            self.to,
            false,
        )?;

        let rewritten_query = rewritten_spatial_query.map(|rwq| query.select_spatial_bounds(rwq));

        if let Some(rewritten_query) = rewritten_query {
            Ok(self
                .source
                .query(rewritten_query, ctx)
                .await?
                .map(move |collection_result| {
                    collection_result.and_then(|collection| {
                        DefaultCoordinateProjector::from_known_srs(self.from, self.to)
                            .and_then(|projector| collection.reproject(&projector))
                            .map_err(Into::into)
                    })
                })
                .boxed())
        } else {
            let res = Ok(FeatureCollection::empty());
            Ok(Box::pin(stream::once(async { res })))
        }
    }

    fn result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }
}

#[async_trait]
impl RasterOperator for Reprojection {
    async fn _initialize(
        self: Box<Self>,
        path: ProcessingGraphOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let name = CanonicOperatorName::from(&self);

        let raster_source =
            self.sources
                .raster()
                .ok_or_else(|| error::Error::InvalidOperatorType {
                    expected: "Raster".to_owned(),
                    found: "Vector".to_owned(),
                })?;

        let initialized_source = raster_source
            .initialize_sources(path.clone(), context)
            .await?;

        let raster_source = initialized_source.raster;
        let in_descriptor = raster_source.result_descriptor();
        let in_srs = Option::<SpatialReference>::from(in_descriptor.spatial_reference)
            .ok_or(Error::AllSourcesMustHaveSameSpatialReference)?;
        let out_grid = compute_output_spatial_grid(
            *in_descriptor.spatial_grid_descriptor(),
            in_descriptor.spatial_bounds(),
            in_srs,
            self.params,
        )?;

        // `Warp` with nearest neighbour does the same, onto the grid of `Reprojection`, which may
        // be derived from the data bounds; it projects the coordinates even within the same spatial
        // reference, so the output is clipped to the area of use
        let warp_params = WarpParams {
            target_spatial_reference: Some(self.params.target_spatial_reference),
            output_resolution: None,
            output_origin_reference: None,
            method: WarpMethod::NearestNeighbor,
        };

        InitializedWarp::with_output_grid(
            WarpOrigin {
                name,
                path,
                operator_name: Reprojection::TYPE_NAME,
                always_reproject: true,
            },
            raster_source,
            warp_params,
            out_grid,
            context.tiling_specification(),
        )
        .map(InitializedRasterOperator::boxed)
    }

    span_fn!(Reprojection);
}

pub struct RasterReprojectionProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
{
    source: Q,
    result_descriptor: RasterResultDescriptor,
    from: SpatialReference,
    to: SpatialReference,
    state: TileReprojectionSubqueryGridInfo,
    _phantom_data: PhantomData<P>,
}

impl<Q, P> RasterReprojectionProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
{
    pub fn new(
        source: Q,
        result_descriptor: RasterResultDescriptor,
        from: SpatialReference,
        to: SpatialReference,
        state: TileReprojectionSubqueryGridInfo,
    ) -> Self {
        Self {
            source,
            result_descriptor,
            from,
            to,
            state,
            _phantom_data: PhantomData,
        }
    }
}

#[async_trait]
impl<Q, P> QueryProcessor for RasterReprojectionProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P> + Send + Sync,
    P: Pixel,
{
    type Output = RasterTile2D<P>;
    type SpatialBounds = GridBoundingBox2D;
    type Selection = BandSelection;
    type ResultDescription = RasterResultDescriptor;

    async fn _query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let state = self.state;

        // setup the subquery
        let sub_query_spec = TileReprojectionSubQuery {
            in_srs: self.from,
            out_srs: self.to,
            fold_fn: fold_by_coordinate_lookup_future,
            state,
            _phantom_data: PhantomData,
        };

        let tiling_strat = self
            .result_descriptor
            .spatial_grid_descriptor()
            .tiling_grid_definition(ctx.tiling_specification())
            .generate_data_tiling_strategy();

        let time_stream = self.time_query(query.time_interval(), ctx).await?;

        // return the adapter which will reproject the tiles and uses the fill adapter to inject missing tiles
        Ok(RasterSubQueryAdapter::<'a, P, _, _, _>::new(
            &self.source,
            query,
            tiling_strat,
            ctx,
            sub_query_spec,
            time_stream,
        )
        .box_pin())
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }
}

#[async_trait]
impl<Q, P> RasterQueryProcessor for RasterReprojectionProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P> + Send + Sync,
    P: Pixel,
{
    type RasterType = P;

    async fn _time_query<'a>(
        &'a self,
        query: geoengine_datatypes::primitives::TimeInterval,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<geoengine_datatypes::primitives::TimeInterval>>> {
        self.source.time_query(query, ctx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{MockExecutionContext, RasterBandDescriptors, SpatialGridDescriptor};
    use crate::mock::MockFeatureCollectionSource;
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use crate::source::{
        FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters, GdalMetaDataRegular,
        GdalMetaDataStatic, GdalSourceTimePlaceholder, TimeReference,
    };
    use crate::util::gdal::add_ndvi_dataset;
    use crate::{
        engine::{ChunkByteSize, VectorOperator},
        source::{GdalSource, GdalSourceParameters},
        test_data,
    };
    use float_cmp::{approx_eq, assert_approx_eq};
    use futures::StreamExt;
    use geoengine_datatypes::collections::IntoGeometryIterator;
    use geoengine_datatypes::dataset::{DataId, DatasetId, NamedData};
    use geoengine_datatypes::hashmap;
    use geoengine_datatypes::primitives::{
        CacheHint, DateTimeParseFormat, SpatialResolution, TimeGranularity, TimeInstance,
    };
    use geoengine_datatypes::primitives::{Coordinate2D, TimeStep};
    use geoengine_datatypes::raster::TilingSpecification;
    use geoengine_datatypes::raster::{
        GeoTransform, GridBoundingBox2D, GridShape2D, GridSize, SpatialGridDefinition,
        TilesEqualIgnoringCacheHint,
    };
    use geoengine_datatypes::util::NonZeroFiniteF64;
    use geoengine_datatypes::{
        collections::{
            GeometryCollection, MultiLineStringCollection, MultiPointCollection,
            MultiPolygonCollection,
        },
        primitives::{BoundingBox2D, MultiLineString, MultiPoint, MultiPolygon, TimeInterval},
        raster::{Grid, RasterDataType, RasterTile2D},
        spatial_reference::SpatialReferenceAuthority,
        util::{
            Identifier,
            test::TestDefault,
            well_known_data::{
                COLOGNE_EPSG_3857, COLOGNE_EPSG_4326, HAMBURG_EPSG_3857, HAMBURG_EPSG_4326,
                MARBURG_EPSG_3857, MARBURG_EPSG_4326,
            },
        },
    };
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::str::FromStr;

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
            CacheHint::no_cache(),
        )?;

        let expected = MultiPoint::many(vec![
            MARBURG_EPSG_3857,
            COLOGNE_EPSG_3857,
            HAMBURG_EPSG_3857,
        ])
        .unwrap();

        let point_source = MockFeatureCollectionSource::single(points.clone()).typed();

        let target_spatial_reference =
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 900_913);

        let exe_ctx = MockExecutionContext::test_default();

        let initialized_operator = VectorOperator::typed(Reprojection {
            params: ReprojectionParams {
                target_spatial_reference,
                derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
            },
            sources: SingleRasterOrVectorSource {
                source: point_source.into(),
            },
        })
        .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
        .await?;

        let query_processor = initialized_operator.query_processor()?;

        let query_processor = query_processor.multi_point().unwrap();

        let query_rectangle = VectorQueryRectangle::new(
            BoundingBox2D::new(
                (COLOGNE_EPSG_4326.x, MARBURG_EPSG_4326.y).into(),
                (MARBURG_EPSG_4326.x, HAMBURG_EPSG_4326.y).into(),
            )
            .unwrap(),
            TimeInterval::default(),
            ColumnSelection::all(),
        );
        let ctx = exe_ctx.mock_query_context(ChunkByteSize::MAX);

        let query = query_processor.query(query_rectangle, &ctx).await.unwrap();

        let result = query
            .map(Result::unwrap)
            .collect::<Vec<MultiPointCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        result[0]
            .geometries()
            .zip(expected.iter())
            .for_each(|(a, e)| {
                assert!(approx_eq!(&MultiPoint, &a.into(), e, epsilon = 0.00001));
            });

        Ok(())
    }

    #[tokio::test]
    async fn multi_lines() -> Result<()> {
        let lines = MultiLineStringCollection::from_data(
            vec![
                MultiLineString::new(vec![vec![
                    MARBURG_EPSG_4326,
                    COLOGNE_EPSG_4326,
                    HAMBURG_EPSG_4326,
                ]])
                .unwrap(),
            ],
            vec![TimeInterval::new_unchecked(0, 1); 1],
            Default::default(),
            CacheHint::no_cache(),
        )?;

        let expected = [MultiLineString::new(vec![vec![
            MARBURG_EPSG_3857,
            COLOGNE_EPSG_3857,
            HAMBURG_EPSG_3857,
        ]])
        .unwrap()];

        let lines_source = MockFeatureCollectionSource::single(lines.clone()).typed();

        let target_spatial_reference =
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 900_913);

        let exe_ctx = MockExecutionContext::test_default();

        let initialized_operator = VectorOperator::typed(Reprojection {
            params: ReprojectionParams {
                target_spatial_reference,
                derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
            },
            sources: SingleRasterOrVectorSource {
                source: lines_source.into(),
            },
        })
        .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
        .await?;

        let query_processor = initialized_operator.query_processor()?;

        let query_processor = query_processor.multi_line_string().unwrap();

        let query_rectangle = VectorQueryRectangle::new(
            BoundingBox2D::new(
                (COLOGNE_EPSG_4326.x, MARBURG_EPSG_4326.y).into(),
                (MARBURG_EPSG_4326.x, HAMBURG_EPSG_4326.y).into(),
            )
            .unwrap(),
            TimeInterval::default(),
            ColumnSelection::all(),
        );
        let ctx = exe_ctx.mock_query_context(ChunkByteSize::MAX);

        let query = query_processor.query(query_rectangle, &ctx).await.unwrap();

        let result = query
            .map(Result::unwrap)
            .collect::<Vec<MultiLineStringCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        result[0]
            .geometries()
            .zip(expected.iter())
            .for_each(|(a, e)| {
                assert!(approx_eq!(
                    &MultiLineString,
                    &a.into(),
                    e,
                    epsilon = 0.00001
                ));
            });

        Ok(())
    }

    #[tokio::test]
    async fn multi_polygons() -> Result<()> {
        let polygons = MultiPolygonCollection::from_data(
            vec![
                MultiPolygon::new(vec![vec![vec![
                    MARBURG_EPSG_4326,
                    COLOGNE_EPSG_4326,
                    HAMBURG_EPSG_4326,
                    MARBURG_EPSG_4326,
                ]]])
                .unwrap(),
            ],
            vec![TimeInterval::new_unchecked(0, 1); 1],
            Default::default(),
            CacheHint::no_cache(),
        )?;

        let expected = [MultiPolygon::new(vec![vec![vec![
            MARBURG_EPSG_3857,
            COLOGNE_EPSG_3857,
            HAMBURG_EPSG_3857,
            MARBURG_EPSG_3857,
        ]]])
        .unwrap()];

        let polygon_source = MockFeatureCollectionSource::single(polygons.clone()).typed();

        let target_spatial_reference =
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 900_913);

        let exe_ctx = MockExecutionContext::test_default();

        let initialized_operator = VectorOperator::typed(Reprojection {
            params: ReprojectionParams {
                target_spatial_reference,
                derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
            },
            sources: SingleRasterOrVectorSource {
                source: polygon_source.into(),
            },
        })
        .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
        .await?;

        let query_processor = initialized_operator.query_processor()?;

        let query_processor = query_processor.multi_polygon().unwrap();

        let query_rectangle = VectorQueryRectangle::new(
            BoundingBox2D::new(
                (COLOGNE_EPSG_4326.x, MARBURG_EPSG_4326.y).into(),
                (MARBURG_EPSG_4326.x, HAMBURG_EPSG_4326.y).into(),
            )
            .unwrap(),
            TimeInterval::default(),
            ColumnSelection::all(),
        );
        let ctx = exe_ctx.mock_query_context(ChunkByteSize::MAX);

        let query = query_processor.query(query_rectangle, &ctx).await.unwrap();

        let result = query
            .map(Result::unwrap)
            .collect::<Vec<MultiPolygonCollection>>()
            .await;

        assert_eq!(result.len(), 1);

        result[0]
            .geometries()
            .zip(expected.iter())
            .for_each(|(a, e)| {
                assert!(approx_eq!(&MultiPolygon, &a.into(), e, epsilon = 0.00001));
            });

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn raster_identity() -> Result<()> {
        let projection = SpatialReference::epsg_4326();

        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1_u8, 2, 3, 4])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::no_cache(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::no_cache(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1_u8, 2, 3, 4])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::no_cache(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [0, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::no_cache(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::no_cache(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::no_cache(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::no_cache(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [0, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::no_cache(),
            },
        ];

        let geo_transform = GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 1., -1.);

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: crate::engine::TimeDescriptor::new_regular_with_epoch(
                Some(TimeInterval::new_unchecked(0, 10)),
                TimeStep::millis(5).unwrap(),
            ),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                geo_transform,
                GridBoundingBox2D::new([-2, 0], [1, 3]).unwrap(),
            ),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let exe_ctx =
            MockExecutionContext::new_with_tiling_spec(TilingSpecification::new([2, 2].into()));

        let query_ctx = exe_ctx.mock_query_context(TestDefault::test_default());

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor,
            },
        }
        .typed();

        let initialized_operator = RasterOperator::typed(Reprojection {
            params: ReprojectionParams {
                target_spatial_reference: projection, // This test will do a identity reprojection
                derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
            },
            sources: SingleRasterOrVectorSource {
                source: mrs1.into(),
            },
        })
        .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
        .await?;

        let qp = initialized_operator
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-2, 0], [1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 10),
            BandSelection::first(),
        );

        let a = qp.raster_query(query_rect, &query_ctx).await?;

        let res = a
            .map(Result::unwrap)
            .collect::<Vec<RasterTile2D<u8>>>()
            .await;
        assert!(data.tiles_equal_ignoring_cache_hint(&res));

        Ok(())
    }

    #[tokio::test]
    async fn raster_ndvi_3857() -> Result<()> {
        let mut exe_ctx = MockExecutionContext::test_default();
        let id = add_ndvi_dataset(&mut exe_ctx);

        let tile_size = GridShape2D::new_2d(512, 512);
        exe_ctx.tiling_specification = TilingSpecification::new(tile_size);

        let query_ctx = exe_ctx.mock_query_context(TestDefault::test_default());

        let time_interval = TimeInterval::new_unchecked(1_396_303_200_000, 1_396_389_600_000);
        // 2014-04-01

        let gdal_op = GdalSource {
            params: GdalSourceParameters::new(id.clone()),
        }
        .typed();

        let projection = SpatialReference::new(
            geoengine_datatypes::spatial_reference::SpatialReferenceAuthority::Epsg,
            3857,
        );

        let initialized_operator = RasterOperator::typed(Reprojection {
            params: ReprojectionParams {
                target_spatial_reference: projection,
                derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
            },
            sources: SingleRasterOrVectorSource {
                source: gdal_op.into(),
            },
        })
        .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
        .await?;

        let qp = initialized_operator
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result_descritptor = qp.result_descriptor();

        assert_approx_eq!(
            f64,
            14_255.015_508_816_849, // TODO: GDAL output is 14228.560819126376373
            result_descritptor
                .spatial_grid_descriptor()
                .spatial_resolution()
                .x()
                .get(),
            epsilon = 0.000_001
        );

        assert_approx_eq!(
            f64,
            14_255.015_508_816_849, // TODO: GDAL output is -14233.615370039031404
            result_descritptor
                .spatial_grid_descriptor()
                .spatial_resolution()
                .y()
                .get(),
            epsilon = 0.000_001
        );

        let tlz = result_descritptor
            .spatial_grid_descriptor()
            .tiling_grid_definition(query_ctx.tiling_specification())
            .generate_data_tiling_strategy();
        let query_tl_pixel = tlz.tile_idx_to_global_pixel_idx([-1, 0].into());
        let query_bounds =
            GridBoundingBox2D::new(query_tl_pixel, query_tl_pixel + [511, 511]).unwrap();

        let qrect = RasterQueryRectangle::new(query_bounds, time_interval, BandSelection::first());

        let qs = qp.raster_query(qrect.clone(), &query_ctx).await.unwrap();

        let res = qs
            .map(Result::unwrap)
            .collect::<Vec<RasterTile2D<u8>>>()
            .await;

        // get the worldfile
        // println!("{}", res[0].tile_geo_transform().worldfile_string());

        // Write the tile to a file

        /*
        let mut buffer = std::fs::File::create("MOD13A2_M_NDVI_2014-04-01_tile-20_v6.rst")?;

        std::io::Write::write(
            &mut buffer,
            res[0]
                .clone()
                .into_materialized_tile()
                .grid_array
                .inner_grid
                .data
                .as_slice(),
        )?;
        */

        // This check is against a tile produced by the operator itself. It was visually validated. TODO: rebuild when open issues are solved.
        // A perfect validation would be against a GDAL output generated like this:
        // gdalwarp -t_srs EPSG:3857 -r near -te_srs EPSG:3857 -of GTiff ./MOD13A2_M_NDVI_2014-04-01.TIFF ./MOD13A2_M_NDVI_2014-04-01.TIFF

        assert_eq!(
            include_bytes!(
                "../../../test_data/raster/modis_ndvi/projected_3857/MOD13A2_M_NDVI_2014-04-01_tile-20_v6.rst"
            ) as &[u8],
            res[0]
                .clone()
                .into_materialized_tile()
                .grid_array
                .inner_grid
                .data
                .as_slice()
        );

        Ok(())
    }

    #[test]
    fn query_rewrite_4326_3857() {
        let query = VectorQueryRectangle::new(
            BoundingBox2D::new_unchecked((-180., -90.).into(), (180., 90.).into()),
            TimeInterval::default(),
            ColumnSelection::all(),
        );

        let expected = BoundingBox2D::new_unchecked(
            (-20_037_508.342_789_244, -20_048_966.104_014_594).into(),
            (20_037_508.342_789_244, 20_048_966.104_014_594).into(),
        );

        // NOTE: this uses the `DefaultCoordinateProjector`, which may change.
        let reprojected = reproject_spatial_query::<_, DefaultCoordinateProjector>(
            query.spatial_bounds(),
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 3857),
            SpatialReference::epsg_4326(),
            true,
        )
        .unwrap()
        .unwrap();

        assert!(approx_eq!(
            BoundingBox2D,
            expected,
            reprojected,
            epsilon = 0.000_001
        ));
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn raster_ndvi_3857_to_4326() -> Result<()> {
        let tile_size_in_pixels = [200, 200].into();
        let data_geo_transform = GeoTransform::new_unchecked(
            Coordinate2D::new(-20_037_508.342_789_244, 19_971_868.880_408_562),
            14_052.950_258_048_738,
            -14_057.881_117_788_405,
        );
        let data_bounds = GridBoundingBox2D::new([0, 0], [2840, 2850]).unwrap();
        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::new(SpatialReferenceAuthority::Epsg, 3857).into(),
            time: crate::engine::TimeDescriptor::new_regular_with_epoch(
                Some(TimeInterval::new_unchecked(
                    TimeInstance::from_str("2014-01-01T00:00:00.000Z").unwrap(),
                    TimeInstance::from_str("2014-07-01T00:00:00.000Z").unwrap(),
                )),
                TimeStep::months(1).unwrap(),
            ),
            spatial_grid: SpatialGridDescriptor::source_from_parts(data_geo_transform, data_bounds),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let m = GdalMetaDataRegular {
            data_time: TimeInterval::new_unchecked(
                TimeInstance::from_str("2014-01-01T00:00:00.000Z").unwrap(),
                TimeInstance::from_str("2014-07-01T00:00:00.000Z").unwrap(),
            ),
            step: TimeStep {
                granularity: TimeGranularity::Months,
                step: 1,
            },
            time_placeholders: hashmap! {
                "%_START_TIME_%".to_string() => GdalSourceTimePlaceholder {
                    format: DateTimeParseFormat::custom("%Y-%m-%d".to_string()),
                    reference: TimeReference::Start,
                },
            },
            params: GdalDatasetParameters {
                file_path: test_data!(
                    "raster/modis_ndvi/projected_3857/MOD13A2_M_NDVI_%_START_TIME_%.TIFF"
                )
                .into(),
                rasterband_channel: 1,
                geo_transform: GdalDatasetGeoTransform {
                    origin_coordinate: data_geo_transform.origin_coordinate,
                    x_pixel_size: NonZeroFiniteF64::new_unchecked(
                        data_geo_transform.x_pixel_size().get(),
                    ),
                    y_pixel_size: NonZeroFiniteF64::new_unchecked(
                        data_geo_transform.y_pixel_size().get(),
                    ),
                },
                width: data_bounds.axis_size_x(),
                height: data_bounds.axis_size_y(),
                file_not_found_handling: FileNotFoundHandling::Error,
                no_data_value: Some(0.),
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options: None,
                allow_alphaband_as_mask: true,
                retry: None,
            },
            result_descriptor: result_descriptor.clone(),
            cache_ttl: None,
        };

        let mut exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            tile_size_in_pixels,
        ));

        let id: DataId = DatasetId::new().into();
        let name = NamedData::with_system_name("ndvi");
        exe_ctx.add_meta_data(id.clone(), name.clone(), Box::new(m));

        let time_interval = TimeInterval::new_unchecked(1_396_310_400_000, 1_396_310_400_000); // 2014-04-01

        let gdal_op = GdalSource {
            params: GdalSourceParameters::new(name),
        }
        .typed();

        let initialized_operator = RasterOperator::typed(Reprojection {
            params: ReprojectionParams {
                target_spatial_reference: SpatialReference::epsg_4326(),
                derive_out_spec: DeriveOutRasterSpecsSource::DataBounds,
            },
            sources: SingleRasterOrVectorSource {
                source: gdal_op.into(),
            },
        })
        .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
        .await?;

        let qp = initialized_operator
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let qr = qp.result_descriptor();
        let query_ctx = exe_ctx.mock_query_context(TestDefault::test_default());

        let qs = qp
            .raster_query(
                RasterQueryRectangle::new(
                    qr.spatial_grid_descriptor()
                        .tiling_grid_definition(query_ctx.tiling_specification())
                        .tiling_grid_bounds(),
                    time_interval,
                    BandSelection::first(),
                ),
                &query_ctx,
            )
            .await
            .unwrap();

        let tiles = qs
            .map(Result::unwrap)
            .collect::<Vec<RasterTile2D<u8>>>()
            .await;

        // the test should generate 18x10 tiles. However, since the real procucrd pixel size is < 0.1 we will get 20 tiles on the x-axis
        assert_eq!(tiles.len(), /*18*/ 20 * 10);

        // none of the tiles should be empty
        assert!(tiles.iter().all(|t| !t.is_empty()));
        Ok(())
    }

    #[tokio::test]
    async fn query_outside_projection_area_of_use_produces_empty_tiles() {
        let tile_size_in_pixels = [600, 600].into();
        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::new(SpatialReferenceAuthority::Epsg, 32636).into(),
            time: crate::engine::TimeDescriptor::new_irregular(None),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::new_unchecked(
                    Coordinate2D::new(166_021.44, 9_329_005.188),
                    534_994.66 - 166_021.444,
                    -9_329_005.18,
                ),
                GridBoundingBox2D::new_min_max(0, 100, 0, 100).unwrap(),
            ),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let m = GdalMetaDataStatic {
            time: Some(TimeInterval::default()),
            params: GdalDatasetParameters {
                file_path: PathBuf::new(),
                rasterband_channel: 1,
                geo_transform: GdalDatasetGeoTransform {
                    origin_coordinate: (166_021.44, 9_329_005.188).into(),
                    x_pixel_size: NonZeroFiniteF64::new_unchecked(
                        (534_994.66 - 166_021.444) / 100.,
                    ),
                    y_pixel_size: NonZeroFiniteF64::new_unchecked(-9_329_005.18 / 100.),
                },
                width: 100,
                height: 100,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value: Some(0.),
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options: None,
                allow_alphaband_as_mask: true,
                retry: None,
            },
            result_descriptor: result_descriptor.clone(),
            cache_ttl: None,
        };

        let mut exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            tile_size_in_pixels,
        ));
        let query_ctx = exe_ctx.mock_query_context(TestDefault::test_default());

        let id: DataId = DatasetId::new().into();
        let name = NamedData::with_system_name("ndvi");
        exe_ctx.add_meta_data(id.clone(), name.clone(), Box::new(m));

        let time_interval = TimeInterval::new_instant(1_388_534_400_000).unwrap(); // 2014-01-01

        let gdal_op = GdalSource {
            params: GdalSourceParameters::new(name),
        }
        .typed();

        let initialized_operator = RasterOperator::typed(Reprojection {
            params: ReprojectionParams {
                target_spatial_reference: SpatialReference::epsg_4326(),
                derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
            },
            sources: SingleRasterOrVectorSource {
                source: gdal_op.into(),
            },
        })
        .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
        .await
        .unwrap();

        let qp = initialized_operator
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .raster_query(
                RasterQueryRectangle::new(
                    GridBoundingBox2D::new_min_max(500, 1000, 500, 1000).unwrap(),
                    time_interval,
                    BandSelection::first(),
                ),
                &query_ctx,
            )
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 4);

        for r in result {
            assert!(r.is_empty());
        }
    }

    #[tokio::test]
    async fn points_from_wgs84_to_utm36n() {
        let exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();

        let point_source = MockFeatureCollectionSource::single(
            MultiPointCollection::from_data(
                MultiPoint::many(vec![
                    vec![(30.0, 0.0)], // lower left of utm36n area of use
                    vec![(36.0, 84.0)],
                    vec![(33.0, 42.0)], // upper right of utm36n area of use
                ])
                .unwrap(),
                vec![TimeInterval::default(); 3],
                HashMap::default(),
                CacheHint::no_cache(),
            )
            .unwrap(),
        )
        .typed();

        let initialized_operator = VectorOperator::typed(Reprojection {
            params: ReprojectionParams {
                target_spatial_reference: SpatialReference::new(
                    SpatialReferenceAuthority::Epsg,
                    32636, // utm36n
                ),
                derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
            },
            sources: SingleRasterOrVectorSource {
                source: point_source.into(),
            },
        })
        .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
        .await
        .unwrap();

        let qp = initialized_operator
            .query_processor()
            .unwrap()
            .multi_point()
            .unwrap();

        let spatial_bounds = BoundingBox2D::new(
            (166_021.44, 0.00).into(), // lower left of projected utm36n area of use
            (534_994.666_6, 9_329_005.18).into(), // upper right of projected utm36n area of use
        )
        .unwrap();

        let qs = qp
            .vector_query(
                VectorQueryRectangle::new(
                    spatial_bounds,
                    TimeInterval::default(),
                    ColumnSelection::all(),
                ),
                &query_ctx,
            )
            .await
            .unwrap();

        let points = qs.map(Result::unwrap).collect::<Vec<_>>().await;

        assert_eq!(points.len(), 1);

        let points = &points[0];

        assert_approx_eq!(
            &[Coordinate2D],
            points.coordinates(),
            &[
                (166_021.443_080_538_42, 0.0).into(),
                (534_994.655_061_136_1, 9_329_005.182_447_437).into(),
                (499_999.999_999_999_5, 4_649_776.224_819_178).into(),
            ],
            epsilon = 0.001 // in m...
        );
    }

    #[tokio::test]
    async fn points_from_utm36n_to_wgs84() {
        let exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context(TestDefault::test_default());

        let point_source = MockFeatureCollectionSource::with_collections_and_sref(
            vec![
                MultiPointCollection::from_data(
                    MultiPoint::many(vec![
                        vec![(166_021.443_080_538_42, 0.0)],
                        vec![(534_994.655_061_136_1, 9_329_005.182_447_437)],
                        vec![(499_999.999_999_999_5, 4_649_776.224_819_178)],
                    ])
                    .unwrap(),
                    vec![TimeInterval::default(); 3],
                    HashMap::default(),
                    CacheHint::no_cache(),
                )
                .unwrap(),
            ],
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 32636), //utm36n
        )
        .typed();

        let initialized_operator = VectorOperator::typed(Reprojection {
            params: ReprojectionParams {
                target_spatial_reference: SpatialReference::new(
                    SpatialReferenceAuthority::Epsg,
                    4326, // utm36n
                ),
                derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
            },
            sources: SingleRasterOrVectorSource {
                source: point_source.into(),
            },
        })
        .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
        .await
        .unwrap();

        let qp = initialized_operator
            .query_processor()
            .unwrap()
            .multi_point()
            .unwrap();

        let spatial_bounds = BoundingBox2D::new(
            (30.0, 0.0).into(),  // lower left of utm36n area of use
            (33.0, 42.0).into(), // upper right of utm36n area of use
        )
        .unwrap();

        let qs = qp
            .vector_query(
                VectorQueryRectangle::new(
                    spatial_bounds,
                    TimeInterval::default(),
                    ColumnSelection::all(),
                ),
                &query_ctx,
            )
            .await
            .unwrap();

        let points = qs.map(Result::unwrap).collect::<Vec<_>>().await;

        assert_eq!(points.len(), 1);

        let points = &points[0];

        assert_approx_eq!(
            &[Coordinate2D],
            points.coordinates(),
            &[
                (30.0, 0.0).into(), // lower left of utm36n area of use
                (36.0, 84.0).into(),
                (33.0, 42.0).into(), // upper right of utm36n area of use
            ],
            epsilon = 0.000_000_01 // ~ 0.001 m
        );
    }

    #[tokio::test]
    async fn points_from_utm36n_to_wgs84_out_of_area() {
        // TODO this test becomes obsolete in this form, it should be removed or we test for 'correct' reprojection here

        // This test checks that points that are outside the area of use of the target spatial reference are not projected and an empty collection is returned

        let exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();

        let point_source = MockFeatureCollectionSource::with_collections_and_sref(
            vec![
                MultiPointCollection::from_data(
                    MultiPoint::many(vec![
                        vec![(758_565., 4_928_353.)], // (12.25, 44,46)
                    ])
                    .unwrap(),
                    vec![TimeInterval::default(); 1],
                    HashMap::default(),
                    CacheHint::no_cache(),
                )
                .unwrap(),
            ],
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 32636), //utm36n
        )
        .typed();

        let initialized_operator = VectorOperator::typed(Reprojection {
            params: ReprojectionParams {
                target_spatial_reference: SpatialReference::new(
                    SpatialReferenceAuthority::Epsg,
                    4326, // utm36n
                ),
                derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
            },
            sources: SingleRasterOrVectorSource {
                source: point_source.into(),
            },
        })
        .initialize(ProcessingGraphOperatorPath::initialize_root(), &exe_ctx)
        .await
        .unwrap();

        let qp = initialized_operator
            .query_processor()
            .unwrap()
            .multi_point()
            .unwrap();

        let spatial_bounds = BoundingBox2D::new(
            (10.0, 0.0).into(),  // -20 x values left of lower left of utm36n area of use
            (13.0, 42.0).into(), // -20 x values left of upper right of utm36n area of use
        )
        .unwrap();

        let qs = qp
            .vector_query(
                VectorQueryRectangle::new(
                    spatial_bounds,
                    TimeInterval::default(),
                    ColumnSelection::all(),
                ),
                &query_ctx,
            )
            .await
            .unwrap();

        let points = qs.map(Result::unwrap).collect::<Vec<_>>().await;

        assert_eq!(points.len(), 1);

        //let points = &points[0];

        //assert!(geoengine_datatypes::collections::FeatureCollectionInfos::is_empty(points));
        //assert!(points.coordinates().is_empty());
    }

    /* TODO resolve the problem with empty intersections
     */
    #[test]
    fn it_derives_raster_result_descriptor() {
        let in_proj = SpatialReference::epsg_4326();
        let out_proj = SpatialReference::from_str("EPSG:3857").unwrap();

        let geo_transform = GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 0.1, -0.1);
        let grid_bounds = GridBoundingBox2D::new_min_max(-850, 849, -1800, 1799).unwrap();
        let spatial_grid = SpatialGridDefinition::new(geo_transform, grid_bounds);

        // NOTE: this uses the `DefaultCoordinateProjector`, which may change.
        let projector = DefaultCoordinateProjector::from_known_srs(in_proj, out_proj).unwrap();

        let out_spatial_grid = spatial_grid.reproject(&projector).unwrap();

        assert_approx_eq!(
            Coordinate2D,
            out_spatial_grid.geo_transform.origin_coordinate(),
            Coordinate2D::new(0., 0.),
            epsilon = 0.000_000_01
        );

        assert_approx_eq!(
            SpatialResolution,
            out_spatial_grid.geo_transform.spatial_resolution(),
            SpatialResolution::new_unchecked(14_212.246_793_017_477, 14_212.246_793_017_477)
        );

        /*
        Projected bounds:
        -20037508.34 -20048966.1
        20037508.34 20048966.1
        */

        assert_eq!(
            out_spatial_grid.grid_bounds,
            GridBoundingBox2D::new_min_max(-1405, 1405, -1410, 1409).unwrap()
        );
    }
}
