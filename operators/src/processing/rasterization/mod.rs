use crate::engine::TypedVectorQueryProcessor::MultiPoint;
use crate::engine::{
    CreateSpan, ExecutionContext, InitializedRasterOperator, InitializedVectorOperator, Operator,
    OperatorName, QueryContext, QueryProcessor, RasterOperator, RasterQueryProcessor,
    RasterResultDescriptor, SingleVectorSource, TypedRasterQueryProcessor,
    TypedVectorQueryProcessor,
};
use arrow::datatypes::ArrowNativeTypeOp;

use crate::error;
use crate::processing::rasterization::GridOrDensity::Grid;
use crate::util;

use async_trait::async_trait;

use futures::stream::BoxStream;
use futures::{stream, StreamExt};
use geoengine_datatypes::collections::GeometryCollection;

use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BoundingBox2D, Coordinate2D, Measurement, RasterQueryRectangle,
    SpatialPartition2D, SpatialPartitioned, SpatialResolution, VectorQueryRectangle,
};
use geoengine_datatypes::raster::{
    EmptyGrid, GeoTransform, Grid2D, GridOrEmpty, GridSize, GridSpaceToLinearSpace, RasterDataType,
    RasterTile2D, TilingSpecification,
};

use num_traits::FloatConst;
use rayon::prelude::*;

use serde::{Deserialize, Serialize};
use snafu::ensure;

use tracing::span;
use tracing::Level;
use typetag::serde;

/// An operator that rasterizes vector data
pub type Rasterization = Operator<RasterizationParams, SingleVectorSource>;

impl OperatorName for Rasterization {
    const TYPE_NAME: &'static str = "Rasterization";
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum GridSizeMode {
    Fixed,
    Relative,
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "type")]
pub enum GridOrDensity {
    Grid(GridParams),
    Density(DensityParams),
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub struct DensityParams {
    radius: f64,
    stddev: f64,
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GridParams {
    spatial_resolution: SpatialResolution,
    origin_coordinate: Coordinate2D,
    grid_size_mode: GridSizeMode,
}

/// The parameter spec for `Rasterization`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RasterizationParams {
    grid_or_density: GridOrDensity,
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for Rasterization {
    async fn _initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> util::Result<Box<dyn InitializedRasterOperator>> {
        let vector_source = self.sources.vector.initialize(context).await?;
        let in_desc = vector_source.result_descriptor();

        let tiling_specification = context.tiling_specification();

        let out_desc = RasterResultDescriptor {
            spatial_reference: in_desc.spatial_reference,
            data_type: RasterDataType::F64,
            measurement: Measurement::default(),
            bbox: None,
            time: in_desc.time,
            resolution: None,
        };

        match self.params.grid_or_density {
            Grid(params) => Ok(InitializedGridRasterization {
                source: vector_source,
                result_descriptor: out_desc,
                spatial_resolution: params.spatial_resolution,
                grid_size_mode: params.grid_size_mode,
                tiling_specification,
                origin_coordinate: params.origin_coordinate,
            }
            .boxed()),
            GridOrDensity::Density(params) => InitializedDensityRasterization::new(
                vector_source,
                out_desc,
                tiling_specification,
                params.radius,
                params.stddev,
            )
            .map(InitializedRasterOperator::boxed),
        }
    }

    span_fn!(Rasterization);
}

pub struct InitializedGridRasterization {
    source: Box<dyn InitializedVectorOperator>,
    result_descriptor: RasterResultDescriptor,
    spatial_resolution: SpatialResolution,
    grid_size_mode: GridSizeMode,
    tiling_specification: TilingSpecification,
    origin_coordinate: Coordinate2D,
}

impl InitializedRasterOperator for InitializedGridRasterization {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> util::Result<TypedRasterQueryProcessor> {
        Ok(TypedRasterQueryProcessor::F64(
            GridRasterizationQueryProcessor {
                input: self.source.query_processor()?,
                spatial_resolution: self.spatial_resolution,
                grid_size_mode: self.grid_size_mode,
                tiling_specification: self.tiling_specification,
                origin_coordinate: self.origin_coordinate,
            }
            .boxed(),
        ))
    }
}

pub struct InitializedDensityRasterization {
    source: Box<dyn InitializedVectorOperator>,
    result_descriptor: RasterResultDescriptor,
    tiling_specification: TilingSpecification,
    radius: f64,
    stddev: f64,
}

impl InitializedDensityRasterization {
    fn new(
        source: Box<dyn InitializedVectorOperator>,
        result_descriptor: RasterResultDescriptor,
        tiling_specification: TilingSpecification,
        radius: f64,
        stddev: f64,
    ) -> Result<Self, error::Error> {
        ensure!(
            radius > 0.,
            error::InvalidOperatorSpec {
                reason: "The radius for density rasterization must be strictly positive."
                    .to_string()
            }
        );

        Ok(InitializedDensityRasterization {
            source,
            result_descriptor,
            tiling_specification,
            radius,
            stddev,
        })
    }
}

impl InitializedRasterOperator for InitializedDensityRasterization {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> util::Result<TypedRasterQueryProcessor> {
        Ok(TypedRasterQueryProcessor::F64(
            DensityRasterizationQueryProcessor {
                input: self.source.query_processor()?,
                tiling_specification: self.tiling_specification,
                radius: self.radius,
                stddev: self.stddev,
            }
            .boxed(),
        ))
    }
}

pub struct GridRasterizationQueryProcessor {
    input: TypedVectorQueryProcessor,
    spatial_resolution: SpatialResolution,
    grid_size_mode: GridSizeMode,
    tiling_specification: TilingSpecification,
    origin_coordinate: Coordinate2D,
}

#[async_trait]
impl RasterQueryProcessor for GridRasterizationQueryProcessor {
    type RasterType = f64;

    async fn raster_query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> util::Result<BoxStream<'a, util::Result<RasterTile2D<Self::RasterType>>>> {
        let grid_resolution = match self.grid_size_mode {
            GridSizeMode::Fixed => SpatialResolution {
                x: f64::max(self.spatial_resolution.x, query.spatial_resolution.x),
                y: f64::max(self.spatial_resolution.y, query.spatial_resolution.y),
            },
            GridSizeMode::Relative => SpatialResolution {
                x: f64::max(
                    self.spatial_resolution.x * query.spatial_resolution.x,
                    query.spatial_resolution.x,
                ),
                y: f64::max(
                    self.spatial_resolution.y * query.spatial_resolution.y,
                    query.spatial_resolution.y,
                ),
            },
        };

        let tiling_strategy = self
            .tiling_specification
            .strategy(query.spatial_resolution.x, -query.spatial_resolution.y);
        let tile_shape = tiling_strategy.tile_size_in_pixels;

        let tiles = ctx.thread_pool().install(|| {
            stream::iter(tiling_strategy.tile_information_iterator(query.spatial_bounds)).then(
                move |tile_info| async move {
                    let grid_spatial_bounds = tile_info
                        .spatial_partition()
                        .snap_to_grid(self.origin_coordinate, grid_resolution);

                    let grid_size_x =
                        f64::ceil(grid_spatial_bounds.size_x() / grid_resolution.x) as usize;
                    let grid_size_y =
                        f64::ceil(grid_spatial_bounds.size_y() / grid_resolution.y) as usize;

                    let vector_query = VectorQueryRectangle {
                        spatial_bounds: grid_spatial_bounds.as_bbox(),
                        time_interval: query.time_interval,
                        spatial_resolution: grid_resolution,
                    };

                    let grid_geo_transform = GeoTransform::new(
                        grid_spatial_bounds.upper_left(),
                        grid_resolution.x,
                        -grid_resolution.y,
                    );

                    let mut grid_data;

                    if let MultiPoint(processor) = &self.input {
                        let mut chunks = processor.query(vector_query, ctx).await?;

                        grid_data = vec![0.; grid_size_x * grid_size_y];
                        while let Some(chunk) = chunks.next().await {
                            for &coord in chunk?.coordinates() {
                                if !grid_spatial_bounds.contains_coordinate(&coord) {
                                    continue;
                                }
                                let [y, x] = grid_geo_transform.coordinate_to_grid_idx_2d(coord).0;
                                grid_data[x as usize + y as usize * grid_size_x] += 1.;
                            }
                        }
                    } else {
                        return Ok(RasterTile2D::new_with_tile_info(
                            query.time_interval,
                            tile_info,
                            GridOrEmpty::Empty(EmptyGrid::new(tile_shape)),
                        ));
                    }

                    let mut tile_data = Vec::with_capacity(tile_shape.number_of_elements());
                    for tile_y in 0..tile_shape.axis_size_y() as isize {
                        for tile_x in 0..tile_shape.axis_size_x() as isize {
                            let pixel_coordinate = tile_info
                                .tile_geo_transform()
                                .grid_idx_to_pixel_center_coordinate_2d([tile_y, tile_x].into());
                            if query.spatial_bounds.contains_coordinate(&pixel_coordinate) {
                                let [grid_y, grid_x] = grid_geo_transform
                                    .coordinate_to_grid_idx_2d(pixel_coordinate)
                                    .0;
                                tile_data.push(
                                    grid_data[grid_x as usize + grid_y as usize * grid_size_x],
                                );
                            } else {
                                tile_data.push(0.);
                            }
                        }
                    }
                    let tile_grid = Grid2D::new(tile_shape, tile_data)
                        .expect("Data vector length should match the number of pixels in the tile");

                    Ok(RasterTile2D::new_with_tile_info(
                        query.time_interval,
                        tile_info,
                        GridOrEmpty::Grid(tile_grid.into()),
                    ))
                },
            )
        });

        Ok(tiles.boxed())
    }
}

pub struct DensityRasterizationQueryProcessor {
    input: TypedVectorQueryProcessor,
    tiling_specification: TilingSpecification,
    radius: f64,
    stddev: f64,
}

#[async_trait]
#[allow(clippy::too_many_lines)]
impl RasterQueryProcessor for DensityRasterizationQueryProcessor {
    type RasterType = f64;

    async fn raster_query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> util::Result<BoxStream<'a, util::Result<RasterTile2D<Self::RasterType>>>> {
        let tiling_strategy = self
            .tiling_specification
            .strategy(query.spatial_resolution.x, -query.spatial_resolution.y);

        let tile_size_x = tiling_strategy.tile_size_in_pixels.axis_size_x();
        let tile_size_y = tiling_strategy.tile_size_in_pixels.axis_size_y();

        let tiles = stream::iter(tiling_strategy.tile_information_iterator(query.spatial_bounds))
            .then(move |tile_info| async move {
                let tile_bounds = tile_info.spatial_partition();

                let vector_query = VectorQueryRectangle {
                    spatial_bounds: extended_bounding_box_from_spatial_partition(
                        tile_bounds,
                        self.radius,
                    ),
                    time_interval: query.time_interval,
                    spatial_resolution: query.spatial_resolution,
                };

                let tile_geo_transform = tile_info.tile_geo_transform();

                let mut tile_data;

                if let MultiPoint(processor) = &self.input {
                    let mut chunks = processor.query(vector_query, ctx).await?;

                    tile_data = vec![0.; tile_size_x * tile_size_y];

                    while let Some(chunk) = chunks.next().await {
                        let chunk = chunk?;
                        ctx.thread_pool().install(|| {
                            rayon::scope(|s| {
                                let (send, receive) = std::sync::mpsc::channel();

                                s.spawn(move |_s1| {
                                    chunk
                                        .coordinates()
                                        .par_iter()
                                        .filter_map(|&coord| {
                                            let spatial_partition =
                                                spatial_partition_around_point(coord, self.radius)
                                                    .intersection(&tile_info.spatial_partition());

                                            let grid_bounds = match spatial_partition {
                                                None => {
                                                    return None;
                                                }
                                                Some(spatial_partition) => tile_geo_transform
                                                    .spatial_to_grid_bounds(&spatial_partition),
                                            };

                                            Some(
                                                (0..grid_bounds.number_of_elements())
                                                    .into_par_iter()
                                                    .filter_map(move |linear_index| {
                                                        let grid_idx = grid_bounds
                                                            .grid_idx_unchecked(linear_index);
                                                        let pixel_coordinate = tile_geo_transform
                                                            .grid_idx_to_pixel_center_coordinate_2d(
                                                                grid_idx,
                                                            );
                                                        let distance = coord
                                                            .euclidean_distance(&pixel_coordinate);
                                                        if distance <= self.radius {
                                                            let [y, x] = grid_idx.0;
                                                            Some((
                                                                x as usize
                                                                    + y as usize * tile_size_x,
                                                                gaussian(distance, self.stddev),
                                                            ))
                                                        } else {
                                                            None
                                                        }
                                                    })
                                                    .collect::<Vec<_>>(),
                                            )
                                        })
                                        .for_each_with(send, |send, val| {
                                            send.send(val).expect(
                                                "Receiver should be allocated for scope duration",
                                            );
                                        });
                                });

                                for result in receive.iter() {
                                    for (index, value) in result {
                                        tile_data[index] += value;
                                    }
                                }
                            });
                        });
                    }
                } else {
                    return Ok(RasterTile2D::new_with_tile_info(
                        query.time_interval,
                        tile_info,
                        GridOrEmpty::Empty(EmptyGrid::new(tiling_strategy.tile_size_in_pixels)),
                    ));
                };

                Ok(RasterTile2D::new_with_tile_info(
                    query.time_interval,
                    tile_info,
                    GridOrEmpty::Grid(
                        Grid2D::new(tiling_strategy.tile_size_in_pixels, tile_data)
                            .expect(
                                "Data vector length should match the number of pixels in the tile",
                            )
                            .into(),
                    ),
                ))
            });

        Ok(tiles.boxed())
    }
}

fn spatial_partition_around_point(coordinate: Coordinate2D, extent: f64) -> SpatialPartition2D {
    SpatialPartition2D::new_unchecked(
        Coordinate2D::new(
            coordinate.x.sub_checked(extent).unwrap_or(f64::MIN),
            coordinate.y.add_checked(extent).unwrap_or(f64::MAX),
        ),
        Coordinate2D::new(
            coordinate.x.add_checked(extent).unwrap_or(f64::MAX),
            coordinate.y.sub_checked(extent).unwrap_or(f64::MIN),
        ),
    )
}

fn extended_bounding_box_from_spatial_partition(
    spatial_partition: SpatialPartition2D,
    extent: f64,
) -> BoundingBox2D {
    BoundingBox2D::new_unchecked(
        Coordinate2D::new(
            spatial_partition
                .lower_left()
                .x
                .sub_checked(extent)
                .unwrap_or(f64::MIN),
            spatial_partition
                .lower_left()
                .y
                .sub_checked(extent)
                .unwrap_or(f64::MIN),
        ),
        Coordinate2D::new(
            spatial_partition
                .upper_right()
                .x
                .add_checked(extent)
                .unwrap_or(f64::MAX),
            spatial_partition
                .upper_right()
                .y
                .add_checked(extent)
                .unwrap_or(f64::MAX),
        ),
    )
}

fn gaussian(x: f64, stddev: f64) -> f64 {
    (1. / f64::sqrt(2. * f64::PI() * stddev * stddev)) * f64::exp(-(x * x) / (2. * stddev * stddev))
}

#[cfg(test)]
mod tests {
    use crate::engine::{
        InitializedRasterOperator, MockExecutionContext, MockQueryContext, QueryProcessor,
        RasterOperator, SingleVectorSource, VectorOperator,
    };
    use crate::mock::{MockPointSource, MockPointSourceParams};
    use crate::processing::rasterization::GridSizeMode::{Fixed, Relative};
    use crate::processing::rasterization::{
        gaussian, DensityParams, GridOrDensity, GridParams, Rasterization, RasterizationParams,
    };
    use futures::StreamExt;
    use geoengine_datatypes::primitives::{
        Coordinate2D, RasterQueryRectangle, SpatialPartition2D, SpatialResolution,
    };
    use geoengine_datatypes::raster::TilingSpecification;
    use geoengine_datatypes::util::test::TestDefault;

    async fn get_results(
        rasterization: Box<dyn InitializedRasterOperator>,
        query: RasterQueryRectangle,
    ) -> Vec<Vec<f64>> {
        rasterization
            .query_processor()
            .unwrap()
            .get_f64()
            .unwrap()
            .query(query, &MockQueryContext::test_default())
            .await
            .unwrap()
            .map(|res| {
                res.unwrap()
                    .grid_array
                    .into_materialized_masked_grid()
                    .inner_grid
                    .data
            })
            .collect()
            .await
    }

    #[tokio::test]
    async fn fixed_grid_basic() {
        let execution_context = MockExecutionContext::new_with_tiling_spec(
            TilingSpecification::new([0., 0.].into(), [2, 2].into()),
        );
        let rasterization = Rasterization {
            params: RasterizationParams {
                grid_or_density: GridOrDensity::Grid(GridParams {
                    spatial_resolution: SpatialResolution { x: 1.0, y: 1.0 },
                    origin_coordinate: [0., 0.].into(),
                    grid_size_mode: Fixed,
                }),
            },
            sources: SingleVectorSource {
                vector: MockPointSource {
                    params: MockPointSourceParams {
                        points: vec![
                            (-1., 1.).into(),
                            (1., 1.).into(),
                            (-1., -1.).into(),
                            (1., -1.).into(),
                        ],
                    },
                }
                .boxed(),
            },
        }
        .boxed()
        .initialize(&execution_context)
        .await
        .unwrap();

        let query = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new([-2., 2.].into(), [2., -2.].into()).unwrap(),
            time_interval: Default::default(),
            spatial_resolution: SpatialResolution { x: 1.0, y: 1.0 },
        };

        let res = get_results(rasterization, query).await;

        assert_eq!(
            res,
            vec![
                vec![0., 0., 0., 1.],
                vec![0., 0., 0., 1.],
                vec![0., 0., 0., 1.],
                vec![0., 0., 0., 1.],
            ]
        );
    }

    #[tokio::test]
    async fn fixed_grid_with_shift() {
        let execution_context = MockExecutionContext::new_with_tiling_spec(
            TilingSpecification::new([0., 0.].into(), [2, 2].into()),
        );
        let rasterization = Rasterization {
            params: RasterizationParams {
                grid_or_density: GridOrDensity::Grid(GridParams {
                    spatial_resolution: SpatialResolution { x: 1.0, y: 1.0 },
                    origin_coordinate: [0.5, -0.5].into(),
                    grid_size_mode: Fixed,
                }),
            },
            sources: SingleVectorSource {
                vector: MockPointSource {
                    params: MockPointSourceParams {
                        points: vec![
                            (-1., 1.).into(),
                            (1., 1.).into(),
                            (-1., -1.).into(),
                            (1., -1.).into(),
                        ],
                    },
                }
                .boxed(),
            },
        }
        .boxed()
        .initialize(&execution_context)
        .await
        .unwrap();

        let query = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new([-2., 2.].into(), [2., -2.].into()).unwrap(),
            time_interval: Default::default(),
            spatial_resolution: SpatialResolution { x: 1.0, y: 1.0 },
        };

        let res = get_results(rasterization, query).await;

        assert_eq!(
            res,
            vec![
                vec![1., 0., 0., 0.],
                vec![1., 0., 0., 0.],
                vec![1., 0., 0., 0.],
                vec![1., 0., 0., 0.],
            ]
        );
    }

    #[tokio::test]
    async fn fixed_grid_with_upsampling() {
        let execution_context = MockExecutionContext::new_with_tiling_spec(
            TilingSpecification::new([0., 0.].into(), [3, 3].into()),
        );
        let rasterization = Rasterization {
            params: RasterizationParams {
                grid_or_density: GridOrDensity::Grid(GridParams {
                    spatial_resolution: SpatialResolution { x: 2.0, y: 2.0 },
                    origin_coordinate: [0., 0.].into(),
                    grid_size_mode: Fixed,
                }),
            },
            sources: SingleVectorSource {
                vector: MockPointSource {
                    params: MockPointSourceParams {
                        points: vec![
                            (-1., 1.).into(),
                            (1., 1.).into(),
                            (-1., -1.).into(),
                            (1., -1.).into(),
                        ],
                    },
                }
                .boxed(),
            },
        }
        .boxed()
        .initialize(&execution_context)
        .await
        .unwrap();

        let query = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new([-3., 3.].into(), [3., -3.].into()).unwrap(),
            time_interval: Default::default(),
            spatial_resolution: SpatialResolution { x: 1.0, y: 1.0 },
        };

        let res = get_results(rasterization, query).await;

        assert_eq!(
            res,
            vec![
                vec![0., 0., 0., 0., 1., 1., 0., 1., 1.],
                vec![0., 0., 0., 1., 1., 0., 1., 1., 0.],
                vec![0., 1., 1., 0., 1., 1., 0., 0., 0.],
                vec![1., 1., 0., 1., 1., 0., 0., 0., 0.],
            ]
        );
    }

    #[tokio::test]
    async fn relative_grid_basic() {
        let execution_context = MockExecutionContext::new_with_tiling_spec(
            TilingSpecification::new([0., 0.].into(), [3, 3].into()),
        );
        let rasterization = Rasterization {
            params: RasterizationParams {
                grid_or_density: GridOrDensity::Grid(GridParams {
                    spatial_resolution: SpatialResolution { x: 1.0, y: 1.0 },
                    origin_coordinate: [0., 0.].into(),
                    grid_size_mode: Relative,
                }),
            },
            sources: SingleVectorSource {
                vector: MockPointSource {
                    params: MockPointSourceParams {
                        points: vec![
                            (-1., 1.).into(),
                            (1., 1.).into(),
                            (-1., -1.).into(),
                            (1., -1.).into(),
                        ],
                    },
                }
                .boxed(),
            },
        }
        .boxed()
        .initialize(&execution_context)
        .await
        .unwrap();

        let query = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new([-1.5, 1.5].into(), [1.5, -1.5].into())
                .unwrap(),
            time_interval: Default::default(),
            spatial_resolution: SpatialResolution { x: 0.5, y: 0.5 },
        };

        let res = get_results(rasterization, query).await;

        assert_eq!(
            res,
            vec![
                vec![0., 0., 0., 0., 1., 0., 0., 0., 0.],
                vec![0., 0., 0., 0., 0., 1., 0., 0., 0.],
                vec![0., 0., 0., 0., 0., 0., 0., 1., 0.],
                vec![0., 0., 0., 0., 0., 0., 0., 0., 1.],
            ]
        );
    }

    #[tokio::test]
    async fn relative_grid_with_shift() {
        let execution_context = MockExecutionContext::new_with_tiling_spec(
            TilingSpecification::new([0., 0.].into(), [3, 3].into()),
        );
        let rasterization = Rasterization {
            params: RasterizationParams {
                grid_or_density: GridOrDensity::Grid(GridParams {
                    spatial_resolution: SpatialResolution { x: 1.0, y: 1.0 },
                    origin_coordinate: [0.25, -0.25].into(),
                    grid_size_mode: Relative,
                }),
            },
            sources: SingleVectorSource {
                vector: MockPointSource {
                    params: MockPointSourceParams {
                        points: vec![
                            (-1., 1.).into(),
                            (1., 1.).into(),
                            (-1., -1.).into(),
                            (1., -1.).into(),
                        ],
                    },
                }
                .boxed(),
            },
        }
        .boxed()
        .initialize(&execution_context)
        .await
        .unwrap();

        let query = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new([-1.5, 1.5].into(), [1.5, -1.5].into())
                .unwrap(),
            time_interval: Default::default(),
            spatial_resolution: SpatialResolution { x: 0.5, y: 0.5 },
        };

        let res = get_results(rasterization, query).await;

        assert_eq!(
            res,
            vec![
                vec![1., 0., 0., 0., 0., 0., 0., 0., 0.],
                vec![0., 1., 0., 0., 0., 0., 0., 0., 0.],
                vec![0., 0., 0., 1., 0., 0., 0., 0., 0.],
                vec![0., 0., 0., 0., 1., 0., 0., 0., 0.],
            ]
        );
    }

    #[tokio::test]
    async fn relative_grid_with_upsampling() {
        let execution_context = MockExecutionContext::new_with_tiling_spec(
            TilingSpecification::new([0., 0.].into(), [2, 2].into()),
        );
        let rasterization = Rasterization {
            params: RasterizationParams {
                grid_or_density: GridOrDensity::Grid(GridParams {
                    spatial_resolution: SpatialResolution { x: 2.0, y: 2.0 },
                    origin_coordinate: [0., 0.].into(),
                    grid_size_mode: Relative,
                }),
            },
            sources: SingleVectorSource {
                vector: MockPointSource {
                    params: MockPointSourceParams {
                        points: vec![
                            (-1., 1.).into(),
                            (1., 1.).into(),
                            (-1., -1.).into(),
                            (1., -1.).into(),
                        ],
                    },
                }
                .boxed(),
            },
        }
        .boxed()
        .initialize(&execution_context)
        .await
        .unwrap();

        let query = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new([-1., 1.].into(), [1., -1.].into()).unwrap(),
            time_interval: Default::default(),
            spatial_resolution: SpatialResolution { x: 0.5, y: 0.5 },
        };

        let res = get_results(rasterization, query).await;

        assert_eq!(
            res,
            vec![
                vec![1., 1., 1., 1.],
                vec![0., 0., 0., 0.],
                vec![0., 0., 0., 0.],
                vec![0., 0., 0., 0.]
            ]
        );
    }

    #[tokio::test]
    async fn density_basic() {
        let execution_context = MockExecutionContext::new_with_tiling_spec(
            TilingSpecification::new([0., 0.].into(), [2, 2].into()),
        );
        let rasterization = Rasterization {
            params: RasterizationParams {
                grid_or_density: GridOrDensity::Density(DensityParams {
                    radius: 1.0,
                    stddev: 1.0,
                }),
            },
            sources: SingleVectorSource {
                vector: MockPointSource {
                    params: MockPointSourceParams {
                        points: vec![(-1., 1.).into(), (1., 1.).into()],
                    },
                }
                .boxed(),
            },
        }
        .boxed()
        .initialize(&execution_context)
        .await
        .unwrap();

        let query = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new([-2., 2.].into(), [2., 0.].into()).unwrap(),
            time_interval: Default::default(),
            spatial_resolution: SpatialResolution { x: 1.0, y: 1.0 },
        };

        let res = get_results(rasterization, query).await;

        assert_eq!(
            res,
            vec![
                vec![
                    gaussian(
                        Coordinate2D::new(-1., 1.)
                            .euclidean_distance(&Coordinate2D::new(-1.5, 1.5)),
                        1.0
                    ),
                    gaussian(
                        Coordinate2D::new(-1., 1.)
                            .euclidean_distance(&Coordinate2D::new(-0.5, 1.5)),
                        1.0
                    ),
                    gaussian(
                        Coordinate2D::new(-1., 1.)
                            .euclidean_distance(&Coordinate2D::new(-1.5, 0.5)),
                        1.0
                    ),
                    gaussian(
                        Coordinate2D::new(-1., 1.)
                            .euclidean_distance(&Coordinate2D::new(-0.5, 0.5)),
                        1.0
                    )
                ],
                vec![
                    gaussian(
                        Coordinate2D::new(1., 1.).euclidean_distance(&Coordinate2D::new(0.5, 1.5)),
                        1.0
                    ),
                    gaussian(
                        Coordinate2D::new(1., 1.).euclidean_distance(&Coordinate2D::new(1.5, 1.5)),
                        1.0
                    ),
                    gaussian(
                        Coordinate2D::new(1., 1.).euclidean_distance(&Coordinate2D::new(0.5, 0.5)),
                        1.0
                    ),
                    gaussian(
                        Coordinate2D::new(1., 1.).euclidean_distance(&Coordinate2D::new(1.5, 0.5)),
                        1.0
                    )
                ],
            ]
        );
    }

    #[tokio::test]
    async fn density_radius_overlap() {
        let execution_context = MockExecutionContext::new_with_tiling_spec(
            TilingSpecification::new([0., 0.].into(), [2, 2].into()),
        );
        let rasterization = Rasterization {
            params: RasterizationParams {
                grid_or_density: GridOrDensity::Density(DensityParams {
                    radius: 2.0,
                    stddev: 1.0,
                }),
            },
            sources: SingleVectorSource {
                vector: MockPointSource {
                    params: MockPointSourceParams {
                        points: vec![(-1., 1.).into(), (1., 1.).into()],
                    },
                }
                .boxed(),
            },
        }
        .boxed()
        .initialize(&execution_context)
        .await
        .unwrap();

        let query = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new([-2., 2.].into(), [2., 0.].into()).unwrap(),
            time_interval: Default::default(),
            spatial_resolution: SpatialResolution { x: 1.0, y: 1.0 },
        };

        let res = get_results(rasterization, query).await;

        assert_eq!(
            res,
            vec![
                vec![
                    gaussian(
                        Coordinate2D::new(-1., 1.)
                            .euclidean_distance(&Coordinate2D::new(-1.5, 1.5)),
                        1.0
                    ),
                    gaussian(
                        Coordinate2D::new(-1., 1.)
                            .euclidean_distance(&Coordinate2D::new(-0.5, 1.5)),
                        1.0
                    ) + gaussian(
                        Coordinate2D::new(1., 1.).euclidean_distance(&Coordinate2D::new(-0.5, 1.5)),
                        1.0
                    ),
                    gaussian(
                        Coordinate2D::new(-1., 1.)
                            .euclidean_distance(&Coordinate2D::new(-1.5, 0.5)),
                        1.0
                    ),
                    gaussian(
                        Coordinate2D::new(-1., 1.)
                            .euclidean_distance(&Coordinate2D::new(-0.5, 0.5)),
                        1.0
                    ) + gaussian(
                        Coordinate2D::new(1., 1.).euclidean_distance(&Coordinate2D::new(-0.5, 0.5)),
                        1.0
                    )
                ],
                vec![
                    gaussian(
                        Coordinate2D::new(1., 1.).euclidean_distance(&Coordinate2D::new(0.5, 1.5)),
                        1.0
                    ) + gaussian(
                        Coordinate2D::new(-1., 1.).euclidean_distance(&Coordinate2D::new(0.5, 1.5)),
                        1.0
                    ),
                    gaussian(
                        Coordinate2D::new(1., 1.).euclidean_distance(&Coordinate2D::new(1.5, 1.5)),
                        1.0
                    ),
                    gaussian(
                        Coordinate2D::new(1., 1.).euclidean_distance(&Coordinate2D::new(0.5, 0.5)),
                        1.0
                    ) + gaussian(
                        Coordinate2D::new(-1., 1.).euclidean_distance(&Coordinate2D::new(0.5, 0.5)),
                        1.0
                    ),
                    gaussian(
                        Coordinate2D::new(1., 1.).euclidean_distance(&Coordinate2D::new(1.5, 0.5)),
                        1.0
                    )
                ],
            ]
        );
    }
}