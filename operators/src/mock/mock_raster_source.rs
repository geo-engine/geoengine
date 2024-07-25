use crate::adapters::{
    FillerTileCacheExpirationStrategy, FillerTimeBounds, SparseTilesFillAdapter,
};
use crate::engine::{
    CanonicOperatorName, InitializedRasterOperator, OperatorData, OperatorName, RasterOperator,
    RasterQueryProcessor, RasterResultDescriptor, SourceOperator, TypedRasterQueryProcessor,
    WorkflowOperatorPath,
};
use crate::util::Result;
use async_trait::async_trait;
use futures::{stream, stream::StreamExt};
use geoengine_datatypes::dataset::NamedData;
use geoengine_datatypes::primitives::{CacheExpiration, TimeInstance};
use geoengine_datatypes::primitives::{RasterQueryRectangle, SpatialPartitioned};
use geoengine_datatypes::raster::{
    GridShape2D, GridShapeAccess, GridSize, Pixel, RasterTile2D, TilingSpecification,
};
use serde::{Deserialize, Serialize};
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum MockRasterSourceError {
    #[snafu(display(
        "A tile has a shape [y: {}, x: {}] which does not match the tiling speciications tile shape (y,x) [y: {}, x: {}].",
        tiling_specification_yx.axis_size()[0],
        tiling_specification_yx.axis_size()[1],
        tile_size_yx.axis_size()[0],
        tile_size_yx.axis_size()[1],
    ))]
    TileSizeDiffersFromTilingSpecification {
        tiling_specification_yx: GridShape2D,
        tile_size_yx: GridShape2D,
    },
}

#[derive(Debug, Clone)]
pub struct MockRasterSourceProcessor<T>
where
    T: Pixel,
{
    pub result_descriptor: RasterResultDescriptor,
    pub data: Vec<RasterTile2D<T>>,
    pub tiling_specification: TilingSpecification,
}

impl<T> MockRasterSourceProcessor<T>
where
    T: Pixel,
{
    fn new_unchecked(
        result_descriptor: RasterResultDescriptor,
        data: Vec<RasterTile2D<T>>,
        tiling_specification: TilingSpecification,
    ) -> Self {
        Self {
            result_descriptor,
            data,
            tiling_specification,
        }
    }

    fn _new(
        result_descriptor: RasterResultDescriptor,
        data: Vec<RasterTile2D<T>>,
        tiling_specification: TilingSpecification,
    ) -> Result<Self, MockRasterSourceError> {
        if let Some(tile_shape) =
            first_tile_shape_not_matching_tiling_spec(&data, tiling_specification)
        {
            return Err(
                MockRasterSourceError::TileSizeDiffersFromTilingSpecification {
                    tiling_specification_yx: tiling_specification.grid_shape(),
                    tile_size_yx: tile_shape,
                },
            );
        };

        Ok(Self {
            result_descriptor,
            data,
            tiling_specification,
        })
    }
}

fn first_tile_shape_not_matching_tiling_spec<T>(
    tiles: &[RasterTile2D<T>],
    tiling_spec: TilingSpecification,
) -> Option<GridShape2D>
where
    T: Pixel,
{
    for tile in tiles {
        if tile.grid_shape() != tiling_spec.grid_shape() {
            return Some(tile.grid_shape());
        }
    }

    None
}

#[async_trait]
impl<T> RasterQueryProcessor for MockRasterSourceProcessor<T>
where
    T: Pixel,
{
    type RasterType = T;
    async fn raster_query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        _ctx: &'a dyn crate::engine::QueryContext,
    ) -> Result<futures::stream::BoxStream<crate::util::Result<RasterTile2D<Self::RasterType>>>>
    {
        let mut known_time_start: Option<TimeInstance> = None;
        let mut known_time_end: Option<TimeInstance> = None;
        let parts: Vec<RasterTile2D<T>> = self
            .data
            .iter()
            .inspect(|m| {
                let time_interval = m.time;

                if time_interval.start() <= query.time_interval.start() {
                    let t = if time_interval.end() > query.time_interval.start() {
                        time_interval.start()
                    } else {
                        time_interval.end()
                    };
                    known_time_start = known_time_start.map(|old| old.max(t)).or(Some(t));
                }

                if time_interval.end() >= query.time_interval.end() {
                    let t = if time_interval.start() < query.time_interval.end() {
                        time_interval.end()
                    } else {
                        time_interval.start()
                    };
                    known_time_end = known_time_end.map(|old| old.min(t)).or(Some(t));
                }
            })
            .filter(move |t| {
                t.time.intersects(&query.time_interval)
                    && t.tile_information()
                        .spatial_partition()
                        .intersects(&query.spatial_bounds)
            })
            .cloned()
            .collect();

        // if we found no time bound we can assume that there is no data
        let known_time_before = known_time_start.unwrap_or(TimeInstance::MIN);
        let known_time_after = known_time_end.unwrap_or(TimeInstance::MAX);

        let inner_stream = stream::iter(parts.into_iter().map(Result::Ok));

        // TODO: evaluate if there are GeoTransforms with positive y-axis
        // The "Pixel-space" starts at the top-left corner of a `GeoTransform`.
        // Therefore, the pixel size on the x-axis is always increasing
        let spatial_resolution = query.spatial_resolution;

        let pixel_size_x = spatial_resolution.x;
        debug_assert!(pixel_size_x.is_sign_positive());
        // and the pixel size on  the y-axis is always decreasing
        let pixel_size_y = spatial_resolution.y * -1.0;
        debug_assert!(pixel_size_y.is_sign_negative());

        let tiling_strategy = self
            .tiling_specification
            .strategy(pixel_size_x, pixel_size_y);

        // use SparseTilesFillAdapter to fill all the gaps
        Ok(SparseTilesFillAdapter::new(
            inner_stream,
            tiling_strategy.tile_grid_box(query.spatial_partition()),
            self.result_descriptor.bands.count(),
            tiling_strategy.geo_transform,
            tiling_strategy.tile_size_in_pixels,
            FillerTileCacheExpirationStrategy::FixedValue(CacheExpiration::max()), // cache forever because we know all mock data
            FillerTimeBounds::new(known_time_before, known_time_after),
        )
        .boxed())
    }

    fn raster_result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MockRasterSourceParams<T: Pixel> {
    pub data: Vec<RasterTile2D<T>>,
    pub result_descriptor: RasterResultDescriptor,
}

pub type MockRasterSource<T> = SourceOperator<MockRasterSourceParams<T>>;

impl<T: Pixel> OperatorData for MockRasterSource<T> {
    fn data_names_collect(&self, _data_names: &mut Vec<NamedData>) {}
}

/// Implement a mock raster source with typetag for a specific generic type
///
/// TODO: use single implementation once
///      "deserialization of generic impls is not supported yet; use `#[typetag::serialize]` to generate serialization only"
///      is solved
///
/// TODO: implementation is done with `paste!`, but we can use `core::concat_idents` once its stable
///
/// ```ignore
/// #[typetag::serde]
/// #[async_trait]
/// impl<T: Pixel> RasterOperator for MockRasterSource<T> {
///     async fn initialize(
///         self: Box<Self>,
///         _path: WorkflowOperatorPath,
///         _context: &dyn crate::engine::ExecutionContext,
///     ) -> Result<Box<dyn InitializedRasterOperator>> {
///         Ok(InitializedMockRasterSource {
///             result_descriptor: self.params.result_descriptor,
///             data: self.params.data,
///         }
///         .boxed())
///     }
/// }
/// ```
///
macro_rules! impl_mock_raster_source {
    ($pixel_type:ty) => {
        paste::paste! {
            impl_mock_raster_source!(
                $pixel_type,
                [<MockRasterSource$pixel_type>]
            );
        }
    };

    ($pixel_type:ty, $newtype:ident) => {
        type $newtype = MockRasterSource<$pixel_type>;

        #[typetag::serde]
        #[async_trait]
        impl RasterOperator for $newtype {
            async fn _initialize(
                self: Box<Self>,
                _path: WorkflowOperatorPath,
                context: &dyn crate::engine::ExecutionContext,
            ) -> Result<Box<dyn InitializedRasterOperator>> {
                let name = CanonicOperatorName::from(&self);

                let data = self.params.data;
                let tiling_specification = context.tiling_specification();

                if let Some(tile_shape) =
                    first_tile_shape_not_matching_tiling_spec(&data, tiling_specification)
                {
                    return Err(
                        MockRasterSourceError::TileSizeDiffersFromTilingSpecification {
                            tiling_specification_yx: tiling_specification.grid_shape(),
                            tile_size_yx: tile_shape,
                        }
                        .into(),
                    );
                };

                Ok(InitializedMockRasterSource {
                    name,
                    result_descriptor: self.params.result_descriptor,
                    data,
                    tiling_specification,
                }
                .boxed())
            }

            span_fn!($newtype);
        }

        impl OperatorName for $newtype {
            const TYPE_NAME: &'static str = "MockRasterSource";
        }
    };
}

impl_mock_raster_source!(u8);
impl_mock_raster_source!(u16);
impl_mock_raster_source!(u32);
impl_mock_raster_source!(u64);
impl_mock_raster_source!(i8);
impl_mock_raster_source!(i16);
impl_mock_raster_source!(i32);
impl_mock_raster_source!(i64);
impl_mock_raster_source!(f32);
impl_mock_raster_source!(f64);

pub struct InitializedMockRasterSource<T: Pixel> {
    name: CanonicOperatorName,
    result_descriptor: RasterResultDescriptor,
    data: Vec<RasterTile2D<T>>,
    tiling_specification: TilingSpecification,
}

impl<T: Pixel> InitializedRasterOperator for InitializedMockRasterSource<T>
where
    TypedRasterQueryProcessor: From<std::boxed::Box<dyn RasterQueryProcessor<RasterType = T>>>,
{
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let processor = TypedRasterQueryProcessor::from(
            MockRasterSourceProcessor::new_unchecked(
                self.result_descriptor.clone(),
                self.data.clone(),
                self.tiling_specification,
            )
            .boxed(),
        );

        Ok(processor)
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{
        MockExecutionContext, MockQueryContext, QueryProcessor, RasterBandDescriptors,
    };
    use geoengine_datatypes::primitives::{BandSelection, CacheHint};
    use geoengine_datatypes::primitives::{SpatialPartition2D, SpatialResolution};
    use geoengine_datatypes::raster::{Grid, MaskedGrid, RasterDataType, RasterProperties};
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_datatypes::{
        primitives::TimeInterval,
        raster::{Grid2D, TileInformation},
        spatial_reference::SpatialReference,
    };

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn serde() {
        let raster =
            MaskedGrid::from(Grid2D::new([3, 2].into(), vec![1_u8, 2, 3, 4, 5, 6]).unwrap());

        let cache_hint = CacheHint::default();
        let raster_tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            0,
            raster.into(),
            cache_hint,
        );

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let serialized = serde_json::to_value(&mrs).unwrap();

        let spec = serde_json::json!({
            "type": "MockRasterSourceu8",
            "params": {
                "data": [{
                    "time": {
                        "start": -8_334_601_228_800_000_i64,
                        "end": 8_210_266_876_799_999_i64
                    },
                    "tilePosition": [0, 0],
                    "band": 0,
                    "globalGeoTransform": {
                        "originCoordinate": {
                            "x": 0.0,
                            "y": 0.0
                        },
                        "xPixelSize": 1.0,
                        "yPixelSize": -1.0
                    },
                    "gridArray": {
                        "type": "grid",
                        "innerGrid" : {
                            "shape": {
                                "shapeArray": [3, 2]
                            },
                            "data": [1, 2, 3, 4, 5, 6],
                        },
                        "validityMask": {
                            "shape": {
                                "shapeArray": [3, 2]
                            },
                            "data": [true, true, true, true, true, true],
                        }
                    },
                    "properties":{
                        "scale":null,
                        "offset":null,
                        "description":null,
                        "propertiesMap": []
                    },
                    "cacheHint": cache_hint,
                }],
                "resultDescriptor": {
                    "dataType": "U8",
                    "spatialReference": "EPSG:4326",
                    "time": null,
                    "bbox": null,
                    "resolution": null,
                    "bands": [
                        {
                            "name": "band",
                            "measurement":  {
                                "type": "unitless"
                            }
                        }
                    ],
                }
            }
        });
        assert_eq!(serialized, spec);

        let deserialized: Box<dyn RasterOperator> = serde_json::from_value(serialized).unwrap();

        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };

        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let initialized = deserialized
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap();

        match initialized.query_processor().unwrap() {
            crate::engine::TypedRasterQueryProcessor::U8(..) => {}
            _ => panic!("wrong raster type"),
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn query_interval_larger_then_data_range() {
        let raster_source = MockRasterSource {
            params: MockRasterSourceParams::<u8> {
                data: vec![
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(1, 2),
                        tile_position: [-1, 0].into(),
                        band: 0,
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                            .unwrap()
                            .into(),
                        properties: RasterProperties::default(),
                        cache_hint: CacheHint::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(1, 2),
                        tile_position: [-1, 1].into(),
                        band: 0,
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12])
                            .unwrap()
                            .into(),
                        properties: RasterProperties::default(),
                        cache_hint: CacheHint::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(2, 3),
                        tile_position: [-1, 0].into(),
                        band: 0,
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new([3, 2].into(), vec![13, 14, 15, 16, 17, 18])
                            .unwrap()
                            .into(),
                        properties: RasterProperties::default(),
                        cache_hint: CacheHint::default(),
                    },
                    RasterTile2D {
                        time: TimeInterval::new_unchecked(2, 3),
                        tile_position: [-1, 1].into(),
                        band: 0,
                        global_geo_transform: TestDefault::test_default(),
                        grid_array: Grid::new([3, 2].into(), vec![19, 20, 21, 22, 23, 24])
                            .unwrap()
                            .into(),
                        properties: RasterProperties::default(),
                        cache_hint: CacheHint::default(),
                    },
                ],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let execution_context = MockExecutionContext::new_with_tiling_spec(
            TilingSpecification::new((0., 0.).into(), [3, 2].into()),
        );

        let query_processor = raster_source
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let query_ctx = MockQueryContext::test_default();

        // QUERY 1

        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(-3, 7),
            spatial_resolution: SpatialResolution::one(),
            attributes: BandSelection::first(),
        };

        let result_stream = query_processor.query(query_rect, &query_ctx).await.unwrap();

        let result = result_stream.map(Result::unwrap).collect::<Vec<_>>().await;

        assert_eq!(
            result.iter().map(|tile| tile.time).collect::<Vec<_>>(),
            [
                TimeInterval::new_unchecked(TimeInstance::MIN, 1),
                TimeInterval::new_unchecked(TimeInstance::MIN, 1),
                TimeInterval::new_unchecked(1, 2),
                TimeInterval::new_unchecked(1, 2),
                TimeInterval::new_unchecked(2, 3),
                TimeInterval::new_unchecked(2, 3),
                TimeInterval::new_unchecked(3, TimeInstance::MAX),
                TimeInterval::new_unchecked(3, TimeInstance::MAX),
            ]
        );

        // QUERY 2

        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(2, 4),
            spatial_resolution: SpatialResolution::one(),
            attributes: BandSelection::first(),
        };

        let result_stream = query_processor.query(query_rect, &query_ctx).await.unwrap();

        let result = result_stream.map(Result::unwrap).collect::<Vec<_>>().await;

        assert_eq!(
            result.iter().map(|tile| tile.time).collect::<Vec<_>>(),
            [
                TimeInterval::new_unchecked(2, 3),
                TimeInterval::new_unchecked(2, 3),
                TimeInterval::new_unchecked(3, TimeInstance::MAX),
                TimeInterval::new_unchecked(3, TimeInstance::MAX),
            ]
        );
    }
}
