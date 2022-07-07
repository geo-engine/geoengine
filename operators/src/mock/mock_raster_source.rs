use crate::adapters::SparseTilesFillAdapter;
use crate::engine::{
    InitializedRasterOperator, OperatorDatasets, RasterOperator, RasterQueryProcessor,
    RasterResultDescriptor, SourceOperator, TypedRasterQueryProcessor,
};
use crate::util::Result;
use async_trait::async_trait;
use futures::{stream, stream::StreamExt};
use geoengine_datatypes::dataset::DatasetId;
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
    pub data: Vec<RasterTile2D<T>>,
    pub tiling_specification: TilingSpecification,
}

impl<T> MockRasterSourceProcessor<T>
where
    T: Pixel,
{
    fn new_unchecked(
        data: Vec<RasterTile2D<T>>,
        tiling_specification: TilingSpecification,
    ) -> Self {
        Self {
            data,
            tiling_specification,
        }
    }

    fn _new(
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
        let inner_stream = stream::iter(
            self.data
                .iter()
                .filter(move |t| {
                    t.time.intersects(&query.time_interval)
                        && t.tile_information()
                            .spatial_partition()
                            .intersects(&query.spatial_bounds)
                })
                .cloned()
                .map(Result::Ok),
        );

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
            tiling_strategy.geo_transform,
            tiling_strategy.tile_size_in_pixels,
        )
        .boxed())
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MockRasterSourceParams<T: Pixel> {
    pub data: Vec<RasterTile2D<T>>,
    pub result_descriptor: RasterResultDescriptor,
}

pub type MockRasterSource<T> = SourceOperator<MockRasterSourceParams<T>>;

impl<T: Pixel> OperatorDatasets for MockRasterSource<T> {
    fn datasets_collect(&self, _datasets: &mut Vec<DatasetId>) {}
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
            async fn initialize(
                self: Box<Self>,
                context: &dyn crate::engine::ExecutionContext,
            ) -> Result<Box<dyn InitializedRasterOperator>> {
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
                    result_descriptor: self.params.result_descriptor,
                    data,
                    tiling_specification,
                }
                .boxed())
            }
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
            MockRasterSourceProcessor::new_unchecked(self.data.clone(), self.tiling_specification)
                .boxed(),
        );

        Ok(processor)
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::MockExecutionContext;
    use geoengine_datatypes::primitives::Measurement;
    use geoengine_datatypes::raster::{MaskedGrid, RasterDataType};
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_datatypes::{
        primitives::TimeInterval,
        raster::{Grid2D, TileInformation},
        spatial_reference::SpatialReference,
    };

    #[tokio::test]
    async fn serde() {
        let raster =
            MaskedGrid::from(Grid2D::new([3, 2].into(), vec![1_u8, 2, 3, 4, 5, 6]).unwrap());

        let raster_tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            raster.into(),
        );

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    time: None,
                    bbox: None,
                },
            },
        }
        .boxed();

        let serialized = serde_json::to_string(&mrs).unwrap();

        let spec = serde_json::json!({
            "type": "MockRasterSourceu8",
            "params": {
                "data": [{
                    "time": {
                        "start": -8_334_632_851_200_000_i64,
                        "end": 8_210_298_412_799_999_i64
                    },
                    "tilePosition": [0, 0],
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
                        "band_name":null,
                        "properties_map":{}
                    }
                }],
                "resultDescriptor": {
                    "dataType": "U8",
                    "spatialReference": "EPSG:4326",
                    "measurement": {
                        "type": "unitless"
                    },
                    "time": null,
                    "bbox": null
                }
            }
        })
        .to_string();
        assert_eq!(serialized, spec);

        let deserialized: Box<dyn RasterOperator> = serde_json::from_str(&serialized).unwrap();

        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };

        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let initialized = deserialized.initialize(&execution_context).await.unwrap();

        match initialized.query_processor().unwrap() {
            crate::engine::TypedRasterQueryProcessor::U8(..) => {}
            _ => panic!("wrong raster type"),
        }
    }
}
