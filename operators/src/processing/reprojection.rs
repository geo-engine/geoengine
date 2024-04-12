use std::marker::PhantomData;

use super::map_query::MapQueryProcessor;
use crate::{
    adapters::{
        fold_by_coordinate_lookup_future, FillerTileCacheExpirationStrategy, RasterSubQueryAdapter,
        TileReprojectionSubQuery, TileReprojectionSubqueryGridInfo,
    },
    engine::{
        CanonicOperatorName, ExecutionContext, InitializedRasterOperator, InitializedSources,
        InitializedVectorOperator, Operator, OperatorName, QueryContext, QueryProcessor,
        RasterOperator, RasterQueryProcessor, RasterResultDescriptor, SingleRasterOrVectorSource,
        TypedRasterQueryProcessor, TypedVectorQueryProcessor, VectorOperator, VectorQueryProcessor,
        VectorResultDescriptor, WorkflowOperatorPath,
    },
    error::{self, Error},
    util::Result,
};
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{stream, StreamExt};
use geoengine_datatypes::{
    collections::FeatureCollection,
    operations::reproject::{
        reproject_and_unify_bbox, reproject_and_unify_proj_bounds, reproject_spatial_query,
        suggest_pixel_size_from_diag_cross_projected, CoordinateProjection, CoordinateProjector,
        Reproject, ReprojectClipped,
    },
    primitives::{
        AxisAlignedRectangle, BandSelection, ColumnSelection, Geometry, RasterQueryRectangle,
        RasterSpatialQueryRectangle, SpatialPartition2D, VectorQueryRectangle,
        VectorSpatialQueryRectangle,
    },
    raster::{GeoTransform, GridBoundingBox2D, Pixel, RasterTile2D, TilingSpecification},
    spatial_reference::SpatialReference,
    util::arrow::ArrowTyped,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "camelCase")]
pub enum DeriveOutRasterSpecsSource {
    DataBounds,
    ProjectionBounds,
}
impl Default for DeriveOutRasterSpecsSource {
    fn default() -> Self {
        Self::ProjectionBounds
    }
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
    result_descriptor: VectorResultDescriptor,
    source: Box<dyn InitializedVectorOperator>,
    source_srs: SpatialReference,
    target_srs: SpatialReference,
}

pub struct InitializedRasterReprojection {
    name: CanonicOperatorName,
    result_descriptor: RasterResultDescriptor,
    source: Box<dyn InitializedRasterOperator>,
    state: TileReprojectionSubqueryGridInfo,
    source_srs: SpatialReference,
    target_srs: SpatialReference,
    tiling_spec: TilingSpecification,
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
        params: ReprojectionParams,
        source_vector_operator: Box<dyn InitializedVectorOperator>,
    ) -> Result<Self> {
        let in_desc: VectorResultDescriptor = source_vector_operator.result_descriptor().clone();

        let in_srs = Into::<Option<SpatialReference>>::into(in_desc.spatial_reference)
            .ok_or(Error::AllSourcesMustHaveSameSpatialReference)?;

        let bbox = if let Some(bbox) = in_desc.bbox {
            let projector =
                CoordinateProjector::from_known_srs(in_srs, params.target_spatial_reference)?;

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
            result_descriptor: out_desc,
            source: source_vector_operator,
            source_srs: in_srs,
            target_srs: params.target_spatial_reference,
        })
    }
}

impl InitializedRasterReprojection {
    pub fn try_new_with_input(
        name: CanonicOperatorName,
        params: ReprojectionParams,
        source_raster_operator: Box<dyn InitializedRasterOperator>,
        tiling_spec: TilingSpecification,
    ) -> Result<Self> {
        let in_desc: RasterResultDescriptor = source_raster_operator.result_descriptor().clone();

        let in_srs = Into::<Option<SpatialReference>>::into(in_desc.spatial_reference)
            .ok_or(Error::AllSourcesMustHaveSameSpatialReference)?;

        // calculate the intersection of input and output srs in both coordinate systems
        // FIXME: the projecten result might be empty. We need to handle this case in result descriptors. So it should be able to return None?
        let (out_geo_transform, out_bounds, _in_bounds) = match params.derive_out_spec {
            DeriveOutRasterSpecsSource::DataBounds => {
                Self::derive_out_bounds_and_res_from_data_bounds(
                    in_srs,
                    params.target_spatial_reference,
                    in_desc.tiling_geo_transform(), // TOOO: maybe we should use the tiling geo transform here? It might be not as accurate if the input reports real origin
                    in_desc.tiling_pixel_bounds(),
                )
            }
            DeriveOutRasterSpecsSource::ProjectionBounds => {
                Self::derive_out_bounds_and_res_from_proj_bounds(
                    in_srs,
                    params.target_spatial_reference,
                    in_desc.tiling_geo_transform(), // TOOO: maybe we should use the tiling geo transform here? It might be not as accurate if the input reports real origin
                    in_desc.tiling_pixel_bounds(),
                )
            }
        }
        .map_err(|_| error::Error::ReprojectionFailed)?
        .ok_or(error::Error::ReprojectionFailed)?; // TODO: better error handling

        let out_bounds = out_geo_transform.spatial_to_grid_bounds(&out_bounds);

        let out_desc = RasterResultDescriptor {
            spatial_reference: params.target_spatial_reference.into(),
            data_type: in_desc.data_type,
            time: in_desc.time,
            geo_transform_x: out_geo_transform,
            pixel_bounds_x: out_bounds,
            bands: in_desc.bands.clone(),
        };

        let state = TileReprojectionSubqueryGridInfo {
            out_geo_tansform: out_geo_transform.nearest_pixel_to_zero_based(),
            out_pixel_bounds: out_geo_transform.shape_to_nearest_to_zero_based(&out_bounds),
            in_geo_tansform: in_desc.tiling_geo_transform(),
            in_pixel_bounds: in_desc.tiling_pixel_bounds(),
        };

        Ok(InitializedRasterReprojection {
            name,
            result_descriptor: out_desc,
            source: source_raster_operator,
            state,
            source_srs: in_srs,
            target_srs: params.target_spatial_reference,
            tiling_spec,
        })
    }

    fn derive_out_bounds_and_res_from_proj_bounds(
        source_srs: SpatialReference,
        target_srs: SpatialReference,
        source_geo_transform: GeoTransform,
        source_bounds: GridBoundingBox2D,
    ) -> Result<Option<(GeoTransform, SpatialPartition2D, SpatialPartition2D)>> {
        let source_bounds = source_geo_transform.grid_to_spatial_bounds(&source_bounds);

        // TODO: maybe we can move that higher up to avoid the double calculation
        if source_srs == target_srs {
            return Ok(Some((source_geo_transform, source_bounds, source_bounds)));
        }

        // we need the projection bounds to calculate the resolution
        let (usable_in_bounds, usable_out_bounds) =
            reproject_and_unify_proj_bounds::<SpatialPartition2D>(source_srs, target_srs)?;

        let pixel_size = // now we can calculate the resolution based on the bounds. If we have no bounds we can't calculate the resolution...
        match (usable_in_bounds, usable_out_bounds) {
            (Some(real_in_bounds), Some(out_bounds)) => {
                let out_res: geoengine_datatypes::primitives::SpatialResolution = suggest_pixel_size_from_diag_cross_projected(
                    real_in_bounds,
                    out_bounds,
                    source_geo_transform.spatial_resolution(), // FIXME: sign should go through method
                )?;
                Some(out_res)
            }
            _ => None,
        };

        // we need the data bounds to calculate the extend of the output
        let (usable_data_in_bounds, usable_data_out_bounds) =
            reproject_and_unify_bbox(source_bounds, source_srs, target_srs)?;

        match (usable_data_in_bounds, usable_data_out_bounds, pixel_size) {
            (Some(real_in_bounds), Some(out_bounds), Some(out_res)) => {
                let out_geo_transform = GeoTransform::new(
                    out_bounds.upper_left(),
                    out_res.x,
                    -out_res.y, // FIXME: sign should go through method
                );

                Ok(Some((out_geo_transform, out_bounds, real_in_bounds)))
            }
            _ => Ok(None),
        }
    }

    fn derive_out_bounds_and_res_from_data_bounds(
        source_srs: SpatialReference,
        target_srs: SpatialReference,
        source_geo_transform: GeoTransform,
        source_bounds: GridBoundingBox2D,
    ) -> Result<Option<(GeoTransform, SpatialPartition2D, SpatialPartition2D)>> {
        let source_bounds = source_geo_transform.grid_to_spatial_bounds(&source_bounds);

        // TODO: maybe we can move that higher up to avoid the double calculation
        if source_srs == target_srs {
            return Ok(Some((source_geo_transform, source_bounds, source_bounds)));
        }

        let (usable_in_bounds, usable_out_bounds) =
            reproject_and_unify_bbox(source_bounds, source_srs, target_srs)?;

        // now we can calculate the resolution based on the bounds. If we have no bounds we can't calculate the resolution...
        match (usable_in_bounds, usable_out_bounds) {
            (Some(real_in_bounds), Some(out_bounds)) => {
                let out_res: geoengine_datatypes::primitives::SpatialResolution =
                    suggest_pixel_size_from_diag_cross_projected(
                        real_in_bounds,
                        out_bounds,
                        source_geo_transform.spatial_resolution(), // FIXME: sign should go through method
                    )?;

                let out_geo_transform = GeoTransform::new(
                    out_bounds.upper_left(),
                    out_res.x,
                    -out_res.y, // FIXME: sign should go through method
                );

                Ok(Some((out_geo_transform, out_bounds, real_in_bounds)))
            }
            _ => Ok(None),
        }
    }
}

#[typetag::serde]
#[async_trait]
impl VectorOperator for Reprojection {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
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

        let initialized_source = vector_source.initialize_sources(path, context).await?;

        let initialized_operator = InitializedVectorReprojection::try_new_with_input(
            name,
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
                MapQueryProcessor::new(
                    source,
                    self.result_descriptor.clone(),
                    move |query: VectorQueryRectangle| {
                        reproject_spatial_query(query.spatial_query(), source_srs, target_srs)
                            .map(|sqr| {
                                sqr.map(|x| {
                                    VectorQueryRectangle::new(
                                        x,
                                        query.time_interval,
                                        ColumnSelection::all(),
                                    )
                                })
                            })
                            .map_err(From::from)
                    },
                    (),
                )
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
        SpatialQuery = VectorSpatialQueryRectangle,
        Selection = ColumnSelection,
        ResultDescription = VectorResultDescriptor,
    >,
    FeatureCollection<G>: Reproject<CoordinateProjector, Out = FeatureCollection<G>>,
    G: Geometry + ArrowTyped,
{
    type Output = FeatureCollection<G>;
    type SpatialQuery = VectorSpatialQueryRectangle;
    type Selection = ColumnSelection;
    type ResultDescription = VectorResultDescriptor;

    async fn _query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let rewritten_spatial_query =
            reproject_spatial_query(query.spatial_query(), self.from, self.to)?;

        let rewritten_query = rewritten_spatial_query
            .map(|rwq| VectorQueryRectangle::new(rwq, query.time_interval, query.attributes));

        if let Some(rewritten_query) = rewritten_query {
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
        } else {
            let res = Ok(FeatureCollection::empty());
            Ok(Box::pin(stream::once(async { res })))
        }
    }

    fn result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for Reprojection {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
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

        let initialized_source = raster_source.initialize_sources(path, context).await?;

        let initialized_operator = InitializedRasterReprojection::try_new_with_input(
            name,
            self.params,
            initialized_source.raster,
            context.tiling_specification(),
        )?;

        Ok(initialized_operator.boxed())
    }

    span_fn!(Reprojection);
}

impl InitializedRasterOperator for InitializedRasterReprojection {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    // i know there is a macro somewhere. we need to re-work this when we have the no-data value anyway.
    #[allow(clippy::too_many_lines)]
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let q = self.source.query_processor()?;

        Ok(match self.result_descriptor.data_type {
            geoengine_datatypes::raster::RasterDataType::U8 => {
                let qt = q
                    .get_u8()
                    .expect("the result descriptor and query processor type should match");
                TypedRasterQueryProcessor::U8(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    self.result_descriptor.clone(),
                    self.source_srs,
                    self.target_srs,
                    self.tiling_spec,
                    self.state,
                )))
            }
            geoengine_datatypes::raster::RasterDataType::U16 => {
                let qt = q
                    .get_u16()
                    .expect("the result descriptor and query processor type should match");
                TypedRasterQueryProcessor::U16(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    self.result_descriptor.clone(),
                    self.source_srs,
                    self.target_srs,
                    self.tiling_spec,
                    self.state,
                )))
            }

            geoengine_datatypes::raster::RasterDataType::U32 => {
                let qt = q
                    .get_u32()
                    .expect("the result descriptor and query processor type should match");
                TypedRasterQueryProcessor::U32(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    self.result_descriptor.clone(),
                    self.source_srs,
                    self.target_srs,
                    self.tiling_spec,
                    self.state,
                )))
            }
            geoengine_datatypes::raster::RasterDataType::U64 => {
                let qt = q
                    .get_u64()
                    .expect("the result descriptor and query processor type should match");
                TypedRasterQueryProcessor::U64(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    self.result_descriptor.clone(),
                    self.source_srs,
                    self.target_srs,
                    self.tiling_spec,
                    self.state,
                )))
            }
            geoengine_datatypes::raster::RasterDataType::I8 => {
                let qt = q
                    .get_i8()
                    .expect("the result descriptor and query processor type should match");
                TypedRasterQueryProcessor::I8(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    self.result_descriptor.clone(),
                    self.source_srs,
                    self.target_srs,
                    self.tiling_spec,
                    self.state,
                )))
            }
            geoengine_datatypes::raster::RasterDataType::I16 => {
                let qt = q
                    .get_i16()
                    .expect("the result descriptor and query processor type should match");
                TypedRasterQueryProcessor::I16(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    self.result_descriptor.clone(),
                    self.source_srs,
                    self.target_srs,
                    self.tiling_spec,
                    self.state,
                )))
            }
            geoengine_datatypes::raster::RasterDataType::I32 => {
                let qt = q
                    .get_i32()
                    .expect("the result descriptor and query processor type should match");
                TypedRasterQueryProcessor::I32(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    self.result_descriptor.clone(),
                    self.source_srs,
                    self.target_srs,
                    self.tiling_spec,
                    self.state,
                )))
            }
            geoengine_datatypes::raster::RasterDataType::I64 => {
                let qt = q
                    .get_i64()
                    .expect("the result descriptor and query processor type should match");
                TypedRasterQueryProcessor::I64(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    self.result_descriptor.clone(),
                    self.source_srs,
                    self.target_srs,
                    self.tiling_spec,
                    self.state,
                )))
            }
            geoengine_datatypes::raster::RasterDataType::F32 => {
                let qt = q
                    .get_f32()
                    .expect("the result descriptor and query processor type should match");
                TypedRasterQueryProcessor::F32(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    self.result_descriptor.clone(),
                    self.source_srs,
                    self.target_srs,
                    self.tiling_spec,
                    self.state,
                )))
            }
            geoengine_datatypes::raster::RasterDataType::F64 => {
                let qt = q
                    .get_f64()
                    .expect("the result descriptor and query processor type should match");
                TypedRasterQueryProcessor::F64(Box::new(RasterReprojectionProcessor::new(
                    qt,
                    self.result_descriptor.clone(),
                    self.source_srs,
                    self.target_srs,
                    self.tiling_spec,
                    self.state,
                )))
            }
        })
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }
}

pub struct RasterReprojectionProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
{
    source: Q,
    result_descriptor: RasterResultDescriptor,
    from: SpatialReference,
    to: SpatialReference,
    tiling_spec: TilingSpecification,
    state: TileReprojectionSubqueryGridInfo,
    _phantom_data: PhantomData<P>,
}

impl<Q, P> RasterReprojectionProcessor<Q, P>
where
    Q: QueryProcessor<
        Output = RasterTile2D<P>,
        SpatialBounds = SpatialPartition2D,
        Selection = BandSelection,
        ResultDescription = RasterResultDescriptor,
    >,
    P: Pixel,
{
    pub fn new(
        source: Q,
        result_descriptor: RasterResultDescriptor,
        from: SpatialReference,
        to: SpatialReference,
        tiling_spec: TilingSpecification,
        state: TileReprojectionSubqueryGridInfo,
    ) -> Self {
        Self {
            source,
            result_descriptor,
            from,
            to,
            tiling_spec,
            state,
            _phantom_data: PhantomData,
        }
    }
}

#[async_trait]
impl<Q, P> QueryProcessor for RasterReprojectionProcessor<Q, P>
where
    Q: QueryProcessor<
        Output = RasterTile2D<P>,
        SpatialQuery = RasterSpatialQueryRectangle,
        Selection = BandSelection,
        ResultDescription = RasterResultDescriptor,
    >,
    P: Pixel,
{
    type Output = RasterTile2D<P>;
    type SpatialQuery = RasterSpatialQueryRectangle;
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
            .tiling_spec
            .strategy(self.result_descriptor().tiling_geo_transform());

        // return the adapter which will reproject the tiles and uses the fill adapter to inject missing tiles
        Ok(RasterSubQueryAdapter::<'a, P, _, _>::new(
            &self.source,
            query,
            tiling_strat,
            ctx,
            sub_query_spec,
        )
        .filter_and_fill(FillerTileCacheExpirationStrategy::DerivedFromSurroundingTiles))
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{MockExecutionContext, MockQueryContext, RasterBandDescriptors};
    use crate::mock::MockFeatureCollectionSource;
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use crate::source::{
        FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters, GdalMetaDataRegular,
        GdalSourceTimePlaceholder, TimeReference,
    };
    use crate::util::gdal::{
        add_ndvi_dataset_cropped_to_valid_webmercator_bounds, gdal_open_dataset,
    };
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
        CacheHint, CacheTtlSeconds, DateTimeParseFormat, TimeGranularity, TimeInstance,
    };
    use geoengine_datatypes::primitives::{Coordinate2D, TimeStep};
    use geoengine_datatypes::raster::TilesEqualIgnoringCacheHint;
    use geoengine_datatypes::util::Identifier;
    use geoengine_datatypes::{
        collections::{
            GeometryCollection, MultiLineStringCollection, MultiPointCollection,
            MultiPolygonCollection,
        },
        primitives::{
            BoundingBox2D, MultiLineString, MultiPoint, MultiPolygon, SpatialResolution,
            TimeInterval,
        },
        raster::{Grid, RasterDataType, RasterTile2D},
        spatial_reference::SpatialReferenceAuthority,
        util::{
            test::TestDefault,
            well_known_data::{
                COLOGNE_EPSG_4326, COLOGNE_EPSG_900_913, HAMBURG_EPSG_4326, HAMBURG_EPSG_900_913,
                MARBURG_EPSG_4326, MARBURG_EPSG_900_913,
            },
        },
    };
    use std::collections::HashMap;
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
            CacheHint::default(),
        )?;

        let expected = MultiPoint::many(vec![
            MARBURG_EPSG_900_913,
            COLOGNE_EPSG_900_913,
            HAMBURG_EPSG_900_913,
        ])
        .unwrap();

        let point_source = MockFeatureCollectionSource::single(points.clone()).boxed();

        let target_spatial_reference =
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 900_913);

        let initialized_operator = VectorOperator::boxed(Reprojection {
            params: ReprojectionParams {
                target_spatial_reference,
                derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
            },
            sources: SingleRasterOrVectorSource {
                source: point_source.into(),
            },
        })
        .initialize(
            WorkflowOperatorPath::initialize_root(),
            &MockExecutionContext::test_default(),
        )
        .await?;

        let query_processor = initialized_operator.query_processor()?;

        let query_processor = query_processor.multi_point().unwrap();

        let query_rectangle = VectorQueryRectangle::with_bounds(
            BoundingBox2D::new(
                (COLOGNE_EPSG_4326.x, MARBURG_EPSG_4326.y).into(),
                (MARBURG_EPSG_4326.x, HAMBURG_EPSG_4326.y).into(),
            )
            .unwrap(),
            TimeInterval::default(),
            ColumnSelection::all(),
        );
        let ctx = MockQueryContext::new(ChunkByteSize::MAX);

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
            vec![MultiLineString::new(vec![vec![
                MARBURG_EPSG_4326,
                COLOGNE_EPSG_4326,
                HAMBURG_EPSG_4326,
            ]])
            .unwrap()],
            vec![TimeInterval::new_unchecked(0, 1); 1],
            Default::default(),
            CacheHint::default(),
        )?;

        let expected = [MultiLineString::new(vec![vec![
            MARBURG_EPSG_900_913,
            COLOGNE_EPSG_900_913,
            HAMBURG_EPSG_900_913,
        ]])
        .unwrap()];

        let lines_source = MockFeatureCollectionSource::single(lines.clone()).boxed();

        let target_spatial_reference =
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 900_913);

        let initialized_operator = VectorOperator::boxed(Reprojection {
            params: ReprojectionParams {
                target_spatial_reference,
                derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
            },
            sources: SingleRasterOrVectorSource {
                source: lines_source.into(),
            },
        })
        .initialize(
            WorkflowOperatorPath::initialize_root(),
            &MockExecutionContext::test_default(),
        )
        .await?;

        let query_processor = initialized_operator.query_processor()?;

        let query_processor = query_processor.multi_line_string().unwrap();

        let query_rectangle = VectorQueryRectangle::with_bounds(
            BoundingBox2D::new(
                (COLOGNE_EPSG_4326.x, MARBURG_EPSG_4326.y).into(),
                (MARBURG_EPSG_4326.x, HAMBURG_EPSG_4326.y).into(),
            )
            .unwrap(),
            TimeInterval::default(),
            ColumnSelection::all(),
        );
        let ctx = MockQueryContext::new(ChunkByteSize::MAX);

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
            vec![MultiPolygon::new(vec![vec![vec![
                MARBURG_EPSG_4326,
                COLOGNE_EPSG_4326,
                HAMBURG_EPSG_4326,
                MARBURG_EPSG_4326,
            ]]])
            .unwrap()],
            vec![TimeInterval::new_unchecked(0, 1); 1],
            Default::default(),
            CacheHint::default(),
        )?;

        let expected = [MultiPolygon::new(vec![vec![vec![
            MARBURG_EPSG_900_913,
            COLOGNE_EPSG_900_913,
            HAMBURG_EPSG_900_913,
            MARBURG_EPSG_900_913,
        ]]])
        .unwrap()];

        let polygon_source = MockFeatureCollectionSource::single(polygons.clone()).boxed();

        let target_spatial_reference =
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 900_913);

        let initialized_operator = VectorOperator::boxed(Reprojection {
            params: ReprojectionParams {
                target_spatial_reference,
                derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
            },
            sources: SingleRasterOrVectorSource {
                source: polygon_source.into(),
            },
        })
        .initialize(
            WorkflowOperatorPath::initialize_root(),
            &MockExecutionContext::test_default(),
        )
        .await?;

        let query_processor = initialized_operator.query_processor()?;

        let query_processor = query_processor.multi_polygon().unwrap();

        let query_rectangle = VectorQueryRectangle::with_bounds(
            BoundingBox2D::new(
                (COLOGNE_EPSG_4326.x, MARBURG_EPSG_4326.y).into(),
                (MARBURG_EPSG_4326.x, HAMBURG_EPSG_4326.y).into(),
            )
            .unwrap(),
            TimeInterval::default(),
            ColumnSelection::all(),
        );
        let ctx = MockQueryContext::new(ChunkByteSize::MAX);

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
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
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
                cache_hint: CacheHint::default(),
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
                cache_hint: CacheHint::default(),
            },
        ];

        let geo_transform = GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.);

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: None,
            geo_transform_x: geo_transform,
            pixel_bounds_x: GridBoundingBox2D::new([-1, 0], [-1, 3]).unwrap(),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let exe_ctx =
            MockExecutionContext::new_with_tiling_spec(TilingSpecification::new([2, 2].into()));

        let query_ctx = MockQueryContext::test_default();

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor,
            },
        }
        .boxed();

        let initialized_operator = RasterOperator::boxed(Reprojection {
            params: ReprojectionParams {
                target_spatial_reference: projection, // This test will do a identity reprojection
                derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
            },
            sources: SingleRasterOrVectorSource {
                source: mrs1.into(),
            },
        })
        .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
        .await?;

        let qp = initialized_operator
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let query_rect = RasterQueryRectangle::new_with_grid_bounds(
            GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
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
        let query_ctx = MockQueryContext::test_default();
        let id = add_ndvi_dataset_cropped_to_valid_webmercator_bounds(&mut exe_ctx);

        exe_ctx.tiling_specification = TilingSpecification::new([450, 450].into());

        let output_bounds =
            SpatialPartition2D::new_unchecked((0., 20_000_000.).into(), (20_000_000., 0.).into());
        let time_interval = TimeInterval::new_unchecked(1_388_534_400_000, 1_388_534_400_001);
        // 2014-01-01

        let gdal_op = GdalSource {
            params: GdalSourceParameters::new(id.clone()),
        }
        .boxed();

        let projection = SpatialReference::new(
            geoengine_datatypes::spatial_reference::SpatialReferenceAuthority::Epsg,
            3857,
        );

        let initialized_operator = RasterOperator::boxed(Reprojection {
            params: ReprojectionParams {
                target_spatial_reference: projection,
                derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
            },
            sources: SingleRasterOrVectorSource {
                source: gdal_op.into(),
            },
        })
        .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
        .await?;

        let qp = initialized_operator
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result_descritptor = qp.result_descriptor();
        println!("{:?}", result_descritptor);

        assert_approx_eq!(
            f64,
            14236.77502413757, // TODO: GDAL output is 14228.560819126376373
            result_descritptor
                .tiling_geo_transform()
                .spatial_resolution()
                .x,
            epsilon = 0.000_001
        );

        assert_approx_eq!(
            f64,
            14236.77502413757, // TODO: GDAL output is -14233.615370039031404
            result_descritptor
                .tiling_geo_transform()
                .spatial_resolution()
                .y,
            epsilon = 0.000_001
        );
        /*
                let qs = qp
                    .raster_query(
                        RasterQueryRectangle::new_with_grid_bounds(
                            output_bounds,
                            result_descritptor
                                .tiling_geo_transform()
                                .spatial_resolution(),
                            result_descritptor.tiling_origin(),
                            time_interval,
                            BandSelection::first(),
                        ),
                        &query_ctx,
                    )
                    .await
                    .unwrap();

                let _res = qs
                    .map(Result::unwrap)
                    .collect::<Vec<RasterTile2D<u8>>>()
                    .await;
        */
        // FIXME: add check against tile data

        // Write the tiles to a file
        /*
        let mut buffer = std::fs::File::create("MOD13A2_M_NDVI_2014-04-01_tile-20_v3.rst")?;
        std::io::Write::write(
            &mut buffer,
            res[8]
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
        // gdalwarp -t_srs EPSG:3857 -r near -te 0.0 20000000.0 0. 20000000.0 -te_srs EPSG:3857 -of GTiff ./MOD13A2_M_NDVI_2014-04-01.TIFF ./MOD13A2_M_NDVI_2014-04-01_tile-20_g1.rst
        /*
        assert_eq!(
            include_bytes!(
                "../../../test_data/raster/modis_ndvi/projected_3857/MOD13A2_M_NDVI_2014-04-01_tile-20.rst"
            ) as &[u8],
            res[8].clone().into_materialized_tile().grid_array.inner_grid.data.as_slice()
        );
        */

        Ok(())
    }

    #[test]
    fn query_rewrite_4326_3857() {
        let query = VectorQueryRectangle::with_bounds(
            BoundingBox2D::new_unchecked((-180., -90.).into(), (180., 90.).into()),
            TimeInterval::default(),
            ColumnSelection::all(),
        );

        let expected = BoundingBox2D::new_unchecked(
            (-20_037_508.342_789_244, -20_048_966.104_014_594).into(),
            (20_037_508.342_789_244, 20_048_966.104_014_594).into(),
        );

        let reprojected = reproject_spatial_query(
            query.spatial_query(),
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 3857),
            SpatialReference::epsg_4326(),
        )
        .unwrap()
        .unwrap();

        assert!(approx_eq!(
            BoundingBox2D,
            expected,
            reprojected.spatial_bounds,
            epsilon = 0.000_001
        ));
    }

    #[tokio::test]
    async fn raster_ndvi_3857_to_4326() -> Result<()> {
        let tile_size_in_pixels = [200, 200].into();
        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::new(SpatialReferenceAuthority::Epsg, 3857).into(),
            time: None,
            geo_transform_x: GeoTransform::new(
                Coordinate2D::new(-20_037_508.342_789_244, 19_971_868.880_408_562),
                14_052.950_258_048_739,
                -14_057.881_117_788_405,
            ),
            pixel_bounds_x: GridBoundingBox2D::new([0, 0], [2840, 2850]).unwrap(),
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
                    origin_coordinate: (-20_037_508.342_789_244, 19_971_868.880_408_563).into(),
                    x_pixel_size: 14_052.950_258_048_739,
                    y_pixel_size: -14_057.881_117_788_405,
                },
                width: 2851,
                height: 2841,
                file_not_found_handling: FileNotFoundHandling::Error,
                no_data_value: Some(0.),
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options: None,
                allow_alphaband_as_mask: true,
                retry: None,
            },
            result_descriptor: result_descriptor.clone(),
            cache_ttl: CacheTtlSeconds::default(),
        };

        let mut exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            tile_size_in_pixels,
        ));
        let query_ctx = MockQueryContext::test_default();

        let id: DataId = DatasetId::new().into();
        let name = NamedData::with_system_name("ndvi");
        exe_ctx.add_meta_data(id.clone(), name.clone(), Box::new(m));

        let time_interval = TimeInterval::new_unchecked(1_396_310_400_000, 1_396_310_400_000);
        // 2014-04-01

        let gdal_op = GdalSource {
            params: GdalSourceParameters::new(name),
        }
        .boxed();

        let initialized_operator = RasterOperator::boxed(Reprojection {
            params: ReprojectionParams {
                target_spatial_reference: SpatialReference::epsg_4326(),
                derive_out_spec: DeriveOutRasterSpecsSource::ProjectionBounds,
            },
            sources: SingleRasterOrVectorSource {
                source: gdal_op.into(),
            },
        })
        .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
        .await?;

        let qp = initialized_operator
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let qr = qp.result_descriptor();

        let qs = qp
            .raster_query(
                RasterQueryRectangle::new_with_grid_bounds(
                    qr.tiling_pixel_bounds(),
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

        // the test must generate 18x10 tiles
        assert_eq!(tiles.len(), 18 * 10);

        // none of the tiles should be empty
        assert!(tiles.iter().all(|t| !t.is_empty()));

        Ok(())
    }

    #[test]
    fn source_resolution() {
        let epsg_4326 = SpatialReference::epsg_4326();
        let epsg_3857 = SpatialReference::new(SpatialReferenceAuthority::Epsg, 3857);

        // use ndvi dataset that was reprojected using gdal as ground truth
        let dataset_4326 = gdal_open_dataset(test_data!(
            "raster/modis_ndvi/MOD13A2_M_NDVI_2014-04-01.TIFF"
        ))
        .unwrap();
        let geotransform_4326 = dataset_4326.geo_transform().unwrap();
        let res_4326 = SpatialResolution::new(geotransform_4326[1], -geotransform_4326[5]).unwrap();

        let dataset_3857 = gdal_open_dataset(test_data!(
            "raster/modis_ndvi/projected_3857/MOD13A2_M_NDVI_2014-04-01.TIFF"
        ))
        .unwrap();
        let geotransform_3857 = dataset_3857.geo_transform().unwrap();
        let res_3857 = SpatialResolution::new(geotransform_3857[1], -geotransform_3857[5]).unwrap();

        // ndvi was projected from 4326 to 3857. The calculated source_resolution for getting the raster in 3857 with `res_3857`
        // should thus roughly be like the original `res_4326`
        let result_res = suggest_pixel_size_from_diag_cross_projected::<SpatialPartition2D>(
            epsg_3857.area_of_use_projected().unwrap(),
            epsg_4326.area_of_use_projected().unwrap(),
            res_3857,
        )
        .unwrap();
        assert!(1. - (result_res.x / res_4326.x).abs() < 0.02);
        assert!(1. - (result_res.y / res_4326.y).abs() < 0.02);
    }

    /*
    #[tokio::test]
    async fn query_outside_projection_area_of_use_produces_empty_tiles() {
        let tile_size_in_pixels = [600, 600].into();
        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::new(SpatialReferenceAuthority::Epsg, 32636).into(),
            time: None,
            geo_transform: GeoTransform::new(
                Coordinate2D::new(166_021.44, 9_329_005.188),
                534_994.66 - 166_021.444,
                -9_329_005.18,
            ),
            pixel_bounds: GridBoundingBox2D::new_min_max(0, 0, 100, 100).unwrap(),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let m = GdalMetaDataStatic {
            time: Some(TimeInterval::default()),
            params: GdalDatasetParameters {
                file_path: PathBuf::new(),
                rasterband_channel: 1,
                geo_transform: GdalDatasetGeoTransform {
                    origin_coordinate: (166_021.44, 9_329_005.188).into(),
                    x_pixel_size: (534_994.66 - 166_021.444) / 100.,
                    y_pixel_size: -9_329_005.18 / 100.,
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
            cache_ttl: CacheTtlSeconds::default(),
        };

        let mut exe_ctx = MockExecutionContext::new_with_tiling_spec(
            TilingSpecification::new(tile_size_in_pixels),
        );
        let query_ctx = MockQueryContext::test_default();

        let id: DataId = DatasetId::new().into();
        let name = NamedData::with_system_name("ndvi");
        exe_ctx.add_meta_data(id.clone(), name.clone(), Box::new(m));

        let output_shape: GridShape2D = [1000, 1000].into();
        let output_bounds =
            SpatialPartition2D::new_unchecked((-180., 0.).into(), (180., -90.).into());
        let time_interval = TimeInterval::new_instant(1_388_534_400_000).unwrap(); // 2014-01-01

        let gdal_op = GdalSource {
            params: GdalSourceParameters { data: name },
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
        .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
        .await
        .unwrap();

        let x_query_resolution = output_bounds.size_x() / output_shape.axis_size_x() as f64;
        let y_query_resolution = output_bounds.size_y() / (output_shape.axis_size_y()) as f64;
        let spatial_resolution =
            SpatialResolution::new_unchecked(x_query_resolution, y_query_resolution);

        let qp = initialized_operator
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .raster_query(
                RasterQueryRectangle::with_partition_and_resolution_and_origin(
                    output_bounds,
                    spatial_resolution,
                    exe_ctx.tiling_specification.origin_coordinate,
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
     */

    #[tokio::test]
    async fn points_from_wgs84_to_utm36n() {
        let exe_ctx = MockExecutionContext::test_default();
        let query_ctx = MockQueryContext::test_default();

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
                CacheHint::default(),
            )
            .unwrap(),
        )
        .boxed();

        let initialized_operator = VectorOperator::boxed(Reprojection {
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
        .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
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
                VectorQueryRectangle::with_bounds(
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

        assert_eq!(
            points.coordinates(),
            &[
                (166_021.443_080_538_42, 0.0).into(),
                (534_994.655_061_136_1, 9_329_005.182_447_437).into(),
                (499_999.999_999_999_5, 4_649_776.224_819_178).into()
            ]
        );
    }

    #[tokio::test]
    async fn points_from_utm36n_to_wgs84() {
        let exe_ctx = MockExecutionContext::test_default();
        let query_ctx = MockQueryContext::test_default();

        let point_source = MockFeatureCollectionSource::with_collections_and_sref(
            vec![MultiPointCollection::from_data(
                MultiPoint::many(vec![
                    vec![(166_021.443_080_538_42, 0.0)],
                    vec![(534_994.655_061_136_1, 9_329_005.182_447_437)],
                    vec![(499_999.999_999_999_5, 4_649_776.224_819_178)],
                ])
                .unwrap(),
                vec![TimeInterval::default(); 3],
                HashMap::default(),
                CacheHint::default(),
            )
            .unwrap()],
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 32636), //utm36n
        )
        .boxed();

        let initialized_operator = VectorOperator::boxed(Reprojection {
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
        .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
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
                VectorQueryRectangle::with_bounds(
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

        assert!(approx_eq!(
            &[Coordinate2D],
            points.coordinates(),
            &[
                (30.0, 0.0).into(), // lower left of utm36n area of use
                (36.0, 84.0).into(),
                (33.0, 42.0).into(), // upper right of utm36n area of use
            ]
        ));
    }

    #[tokio::test]
    async fn points_from_utm36n_to_wgs84_out_of_area() {
        // This test checks that points that are outside the area of use of the target spatial reference are not projected and an empty collection is returned

        let exe_ctx = MockExecutionContext::test_default();
        let query_ctx = MockQueryContext::test_default();

        let point_source = MockFeatureCollectionSource::with_collections_and_sref(
            vec![MultiPointCollection::from_data(
                MultiPoint::many(vec![
                    vec![(758_565., 4_928_353.)], // (12.25, 44,46)
                ])
                .unwrap(),
                vec![TimeInterval::default(); 1],
                HashMap::default(),
                CacheHint::default(),
            )
            .unwrap()],
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 32636), //utm36n
        )
        .boxed();

        let initialized_operator = VectorOperator::boxed(Reprojection {
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
        .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
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
                VectorQueryRectangle::with_bounds(
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

        assert!(geoengine_datatypes::collections::FeatureCollectionInfos::is_empty(points));
        assert!(points.coordinates().is_empty());
    }

    /* FIXME resolve the problem with empty intersections
    #[test]
    fn it_derives_raster_result_descriptor() {
        let in_proj = SpatialReference::epsg_4326();
        let out_proj = SpatialReference::from_str("EPSG:3857").unwrap();
        let bbox = Some(SpatialPartition2D::new_unchecked(
            (-180., 90.).into(),
            (180., -90.).into(),
        ));

        let resolution = Some(SpatialResolution::new_unchecked(0.1, 0.1));

        let (in_bounds, out_bounds, out_res) =
            InitializedRasterReprojection::derive_raster_in_bounds_out_bounds_out_res(
                in_proj, out_proj, resolution, bbox,
            )
            .unwrap();

        assert_eq!(
            in_bounds.unwrap(),
            SpatialPartition2D::new_unchecked((-180., 85.06).into(), (180., -85.06).into(),)
        );

        assert_eq!(
            out_bounds.unwrap(),
            out_proj
                .area_of_use_projected::<SpatialPartition2D>()
                .unwrap()
        );

        // TODO: y resolution should be double the x resolution, but currently we only compute a uniform resolution
        assert_eq!(
            out_res.unwrap(),
            SpatialResolution::new_unchecked(14_237.781_884_528_267, 14_237.781_884_528_267),
        );
    }
    */
}
