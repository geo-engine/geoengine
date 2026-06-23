use crate::engine::{
    BoxRasterQueryProcessor, CanonicOperatorName, ExecutionContext, InitializedRasterOperator,
    InitializedSources, InitializedVectorOperator, Operator, OperatorData, OperatorName,
    QueryContext, QueryProcessor, RasterOperator, RasterQueryProcessor, RasterResultDescriptor,
    TypedRasterQueryProcessor, TypedVectorQueryProcessor, VectorOperator, WorkflowOperatorPath,
};
use crate::error::{self, Error};
use crate::processing::point_in_polygon::PointInPolygonTesterWithCollection;
use crate::util::Result;
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use geoengine_datatypes::collections::{
    FeatureCollectionInfos, MultiPolygonCollection, VectorDataType,
};
use geoengine_datatypes::dataset::NamedData;
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BandSelection, ColumnSelection, RasterQueryRectangle, SpatialPartitioned,
    VectorQueryRectangle,
};
use geoengine_datatypes::raster::{
    GridBoundingBox2D, GridIdx2D, GridIndexAccess, GridIndexAccessMut, GridShapeAccess, Pixel,
    RasterTile2D,
};
use serde::{Deserialize, Serialize};
use snafu::ensure;

/// An operator that keeps raster pixels only where at least one polygon contains the pixel center.
pub type PolygonalRasterExtraction =
    Operator<PolygonalRasterExtractionParams, PolygonalRasterExtractionSource>;

impl OperatorName for PolygonalRasterExtraction {
    const TYPE_NAME: &'static str = "PolygonalRasterExtraction";
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PolygonalRasterExtractionParams {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PolygonalRasterExtractionSource {
    pub raster: Box<dyn RasterOperator>,
    pub polygons: Box<dyn VectorOperator>,
}

impl OperatorData for PolygonalRasterExtractionSource {
    fn data_names_collect(&self, data_names: &mut Vec<NamedData>) {
        self.raster.data_names_collect(data_names);
        self.polygons.data_names_collect(data_names);
    }
}

struct InitializedPolygonalRasterExtractionSource {
    raster: Box<dyn InitializedRasterOperator>,
    polygons: Box<dyn crate::engine::InitializedVectorOperator>,
}

#[async_trait]
impl InitializedSources<InitializedPolygonalRasterExtractionSource>
    for PolygonalRasterExtractionSource
{
    async fn initialize_sources(
        self,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<InitializedPolygonalRasterExtractionSource> {
        let raster_path = path.clone_and_append(0);
        let polygons_path = path.clone_and_append(1);

        Ok(InitializedPolygonalRasterExtractionSource {
            raster: self.raster.initialize(raster_path, context).await?,
            polygons: self.polygons.initialize(polygons_path, context).await?,
        })
    }
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for PolygonalRasterExtraction {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let name = CanonicOperatorName::from(&self);

        let initialized_sources = self
            .sources
            .initialize_sources(path.clone(), context)
            .await?;

        let raster_descriptor = initialized_sources.raster.result_descriptor().clone();
        let polygon_descriptor = initialized_sources.polygons.result_descriptor();

        ensure!(
            polygon_descriptor.data_type == VectorDataType::MultiPolygon,
            error::InvalidType {
                expected: VectorDataType::MultiPolygon.to_string(),
                found: polygon_descriptor.data_type.to_string(),
            }
        );

        ensure!(
            raster_descriptor.spatial_reference == polygon_descriptor.spatial_reference,
            crate::error::InvalidSpatialReference {
                expected: raster_descriptor.spatial_reference,
                found: polygon_descriptor.spatial_reference,
            }
        );

        Ok(InitializedPolygonalRasterExtraction {
            name,
            path,
            result_descriptor: raster_descriptor,
            raster: initialized_sources.raster,
            polygons: initialized_sources.polygons,
        }
        .boxed())
    }

    span_fn!(PolygonalRasterExtraction);
}

struct InitializedPolygonalRasterExtraction {
    name: CanonicOperatorName,
    path: WorkflowOperatorPath,
    result_descriptor: RasterResultDescriptor,
    raster: Box<dyn InitializedRasterOperator>,
    polygons: Box<dyn crate::engine::InitializedVectorOperator>,
}

impl InitializedRasterOperator for InitializedPolygonalRasterExtraction {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let raster_processor = self.raster.query_processor()?;
        let TypedVectorQueryProcessor::MultiPolygon(polygons) = self.polygons.query_processor()?
        else {
            return Err(Error::InvalidType {
                expected: VectorDataType::MultiPolygon.to_string(),
                found: self.polygons.result_descriptor().data_type.to_string(),
            });
        };

        Ok(
            call_on_generic_raster_processor!(raster_processor, raster_processor => {
                TypedRasterQueryProcessor::from(Box::new(PolygonalRasterExtractionQueryProcessor::new(
                    raster_processor,
                    polygons,
                    self.result_descriptor.clone(),
                )) as BoxRasterQueryProcessor<_>)
            }),
        )
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn name(&self) -> &'static str {
        PolygonalRasterExtraction::TYPE_NAME
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.path.clone()
    }

    fn optimize(
        &self,
        target_resolution: geoengine_datatypes::primitives::SpatialResolution,
    ) -> Result<Box<dyn RasterOperator>, crate::optimization::OptimizationError> {
        Ok(PolygonalRasterExtraction {
            params: PolygonalRasterExtractionParams::default(),
            sources: PolygonalRasterExtractionSource {
                raster: self.raster.optimize(target_resolution)?,
                polygons: self.polygons.optimize(target_resolution)?,
            },
        }
        .boxed())
    }
}

struct PolygonalRasterExtractionQueryProcessor<P>
where
    P: Pixel,
{
    raster_processor: BoxRasterQueryProcessor<P>,
    polygons: Box<dyn crate::engine::VectorQueryProcessor<VectorType = MultiPolygonCollection>>,
    result_descriptor: RasterResultDescriptor,
}

impl<P> PolygonalRasterExtractionQueryProcessor<P>
where
    P: Pixel,
{
    fn new(
        raster_processor: BoxRasterQueryProcessor<P>,
        polygons: Box<dyn crate::engine::VectorQueryProcessor<VectorType = MultiPolygonCollection>>,
        result_descriptor: RasterResultDescriptor,
    ) -> Self {
        Self {
            raster_processor,
            polygons,
            result_descriptor,
        }
    }

    fn set_pixels_inside_polygons(tile: &mut RasterTile2D<P>, polygons: MultiPolygonCollection) {
        let wrapper = PointInPolygonTesterWithCollection::new(polygons);
        let tester = wrapper.tester();
        let tile_geo_transform = tile.tile_geo_transform();
        let [height, width] = tile.grid_shape_array();
        let masked_grid = tile
            .grid_array
            .as_masked_grid_mut()
            .expect("tile should have been materialized before masking");

        for row in 0..height {
            for col in 0..width {
                let grid_idx: GridIdx2D = [row as isize, col as isize].into();
                let coordinate =
                    tile_geo_transform.grid_idx_to_pixel_center_coordinate_2d(grid_idx);

                if tester.any_polygon_contains_coordinate(&coordinate, &tile.time) {
                    let value = masked_grid.inner_grid.get_at_grid_index_unchecked(grid_idx);
                    masked_grid.set_at_grid_index_unchecked(grid_idx, Some(value));
                }
            }
        }
    }
}

#[async_trait]
impl<P> QueryProcessor for PolygonalRasterExtractionQueryProcessor<P>
where
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
        let raster_stream = self.raster_processor.raster_query(query, ctx).await?;

        let polygons = &self.polygons;

        let stream = raster_stream
            .and_then(move |tile| async move {
                let vector_query = VectorQueryRectangle::new(
                    tile.spatial_partition().as_bbox(),
                    tile.time,
                    ColumnSelection::all(),
                );

                let mut polygon_stream = polygons.query(vector_query, ctx).await?;

                let mut output = RasterTile2D {
                    time: tile.time,
                    tile_position: tile.tile_position,
                    band: tile.band,
                    global_geo_transform: tile.global_geo_transform,
                    grid_array: tile.grid_array.into_materialized_masked_grid().into(),
                    properties: tile.properties,
                    cache_hint: tile.cache_hint,
                };

                output
                    .grid_array
                    .as_masked_grid_mut()
                    .expect("materialized tile must be masked")
                    .validity_mask
                    .data
                    .fill(false);

                while let Some(chunk) = polygon_stream.next().await {
                    let chunk = chunk?;
                    if chunk.len() == 0 {
                        continue;
                    }

                    Self::set_pixels_inside_polygons(&mut output, chunk);
                }

                Ok(output)
            })
            .boxed();

        Ok(stream)
    }

    fn result_descriptor(&self) -> &Self::ResultDescription {
        &self.result_descriptor
    }
}

#[async_trait]
impl<P> RasterQueryProcessor for PolygonalRasterExtractionQueryProcessor<P>
where
    P: Pixel,
{
    type RasterType = P;

    async fn _time_query<'a>(
        &'a self,
        query: geoengine_datatypes::primitives::TimeInterval,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<geoengine_datatypes::primitives::TimeInterval>>> {
        self.raster_processor.time_query(query, ctx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::engine::{
        ChunkByteSize, MockExecutionContext, RasterBandDescriptors, RasterOperator,
        RasterResultDescriptor, SpatialGridDescriptor, TimeDescriptor, VectorOperator,
        WorkflowOperatorPath,
    };
    use crate::mock::{MockFeatureCollectionSource, MockRasterSource, MockRasterSourceParams};
    use futures::StreamExt;
    use geoengine_datatypes::primitives::{
        BandSelection, CacheHint, Coordinate2D, MultiPolygon, RasterQueryRectangle, TimeInterval,
    };
    use geoengine_datatypes::raster::{
        GeoTransform, Grid2D, GridBoundingBox2D, TileInformation, TilingSpecification,
    };
    use geoengine_datatypes::spatial_reference::SpatialReference;

    fn make_raster_source() -> Box<dyn RasterOperator> {
        let raster_tile = RasterTile2D::<u8>::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_geo_transform: GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            0,
            Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                .unwrap()
                .into(),
            CacheHint::default(),
        );

        let result_descriptor = RasterResultDescriptor {
            data_type: geoengine_datatypes::raster::RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_irregular(Some(TimeInterval::default())),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
                GridBoundingBox2D::new([0, 0], [2, 1]).unwrap(),
            ),
            bands: RasterBandDescriptors::new_single_band(),
        };

        MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor,
            },
        }
        .boxed()
    }

    fn make_polygon_collection(
        coordinates: Vec<(f64, f64)>,
    ) -> geoengine_datatypes::collections::MultiPolygonCollection {
        MultiPolygonCollection::from_data(
            vec![
                MultiPolygon::new(vec![vec![
                    coordinates.into_iter().map(Into::into).collect::<Vec<_>>(),
                ]])
                .unwrap(),
            ],
            vec![TimeInterval::default()],
            Default::default(),
            CacheHint::default(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn masks_pixels_inside_polygon() {
        let execution_context =
            MockExecutionContext::new_with_tiling_spec(TilingSpecification::new([3, 2].into()));

        let operator = PolygonalRasterExtraction {
            params: PolygonalRasterExtractionParams::default(),
            sources: PolygonalRasterExtractionSource {
                raster: make_raster_source(),
                polygons: MockFeatureCollectionSource::single(make_polygon_collection(vec![
                    (0.0, 0.0),
                    (2.0, 0.0),
                    (2.0, -2.0),
                    (0.0, -2.0),
                    (0.0, 0.0),
                ]))
                .boxed(),
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
        .await
        .unwrap();

        let query = RasterQueryRectangle::new(
            GridBoundingBox2D::new([0, 0], [1, 2]).unwrap(),
            TimeInterval::default(),
            BandSelection::first(),
        );

        let query_context = execution_context.mock_query_context(ChunkByteSize::MIN);
        let tiles = operator
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap()
            .query(query, &query_context)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        let tiles = tiles.into_iter().map(Result::unwrap).collect::<Vec<_>>();
        assert!(!tiles.is_empty());

        let tile = tiles
            .iter()
            .find(|tile| {
                tile.grid_array
                    .as_masked_grid()
                    .unwrap()
                    .masked_element_deref_iterator()
                    .any(|value| value.is_some())
            })
            .expect("expected at least one tile with masked data");

        assert_eq!(tile.band, 0);
        let grid = tile.grid_array.as_masked_grid().unwrap();
        let values = grid.masked_element_deref_iterator().collect::<Vec<_>>();
        assert_eq!(values, vec![Some(1), Some(2), Some(3), Some(4), None, None]);
    }

    #[tokio::test]
    async fn clears_pixels_outside_polygon() {
        let execution_context =
            MockExecutionContext::new_with_tiling_spec(TilingSpecification::new([3, 2].into()));

        let operator = PolygonalRasterExtraction {
            params: PolygonalRasterExtractionParams::default(),
            sources: PolygonalRasterExtractionSource {
                raster: make_raster_source(),
                polygons: MockFeatureCollectionSource::single(make_polygon_collection(vec![
                    (100.0, 100.0),
                    (102.0, 100.0),
                    (102.0, 98.0),
                    (100.0, 98.0),
                    (100.0, 100.0),
                ]))
                .boxed(),
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
        .await
        .unwrap();

        let query = RasterQueryRectangle::new(
            GridBoundingBox2D::new([0, 0], [1, 2]).unwrap(),
            TimeInterval::default(),
            BandSelection::first(),
        );

        let query_context = execution_context.mock_query_context(ChunkByteSize::MIN);
        let tiles = operator
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap()
            .query(query, &query_context)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        let tiles = tiles.into_iter().map(Result::unwrap).collect::<Vec<_>>();
        assert!(!tiles.is_empty());

        assert!(tiles.iter().all(|tile| {
            tile.grid_array
                .as_masked_grid()
                .unwrap()
                .masked_element_deref_iterator()
                .all(|value| value.is_none())
        }));
    }
}
