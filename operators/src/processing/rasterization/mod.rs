use crate::engine::TypedVectorQueryProcessor::MultiPoint;
use crate::engine::{
    CanonicOperatorName, ExecutionContext, InitializedRasterOperator, InitializedSources,
    InitializedVectorOperator, Operator, OperatorName, QueryContext, QueryProcessor,
    RasterBandDescriptors, RasterOperator, RasterQueryProcessor, RasterResultDescriptor,
    ResultDescriptor, SingleVectorSource, SpatialGridDescriptor, TypedRasterQueryProcessor,
    TypedVectorQueryProcessor, WorkflowOperatorPath,
};
use crate::error;
use crate::util::spawn_blocking;
use crate::util::{self, spawn_blocking_with_thread_pool};
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{StreamExt, stream};
use geoengine_datatypes::collections::GeometryCollection;
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BandSelection, BoundingBox2D, Coordinate2D, RasterQueryRectangle,
    SpatialPartition2D, SpatialPartitioned, SpatialResolution, VectorQueryRectangle,
};
use geoengine_datatypes::primitives::{CacheHint, ColumnSelection};
use geoengine_datatypes::raster::{
    ChangeGridBounds, GeoTransform, Grid as GridWithFlexibleBoundType, Grid2D, GridBoundingBox2D,
    GridIdx, GridOrEmpty, GridSize, RasterDataType, RasterTile2D, TilingSpecification,
    TilingStrategy, UpdateIndexedElementsParallel,
};
use geoengine_datatypes::spatial_reference::SpatialReference;
use num_traits::FloatConst;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use typetag::serde;

/// An operator that rasterizes vector data
pub type Rasterization = Operator<RasterizationParams, SingleVectorSource>;

impl OperatorName for Rasterization {
    const TYPE_NAME: &'static str = "Rasterization";
}
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub struct DensityParams {
    /// Defines the cutoff (as percentage of maximum density) down to which a point is taken
    /// into account for an output pixel density value
    cutoff: f64,
    /// The standard deviation parameter for the gaussian function
    stddev: f64,
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RasterizationParams {
    /// The size of grid cells, interpreted depending on the chosen grid size mode
    spatial_resolution: SpatialResolution,
    /// The origin coordinate which aligns the grid bounds
    origin_coordinate: Coordinate2D,
    // Heatmap calculated from a gaussian density function
    density_params: Option<DensityParams>,
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for Rasterization {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> util::Result<Box<dyn InitializedRasterOperator>> {
        let name = CanonicOperatorName::from(&self);

        let initialized_source = self
            .sources
            .initialize_sources(path.clone(), context)
            .await?;
        let vector_source = initialized_source.vector;
        let in_desc = vector_source.result_descriptor();

        let tiling_specification = context.tiling_specification();

        let resolution = self.params.spatial_resolution;
        let origin = self.params.origin_coordinate;

        let geo_transform = GeoTransform::new(origin, resolution.x, -resolution.y);

        let spatial_bounds = in_desc
            .bbox
            .ok_or_else(|| {
                in_desc
                    .spatial_reference()
                    .as_option()
                    .map(SpatialReference::area_of_use_projected::<BoundingBox2D>)
            })
            .map_err(|_| error::Error::NoSpatialBoundsAvailable)?;

        let pixel_bounds =
            geo_transform.spatial_to_grid_bounds(&SpatialPartition2D::new_unchecked(
                spatial_bounds.upper_left(),
                spatial_bounds.lower_right(),
            ));

        let out_desc = RasterResultDescriptor {
            spatial_reference: in_desc.spatial_reference,
            data_type: RasterDataType::F64,
            time: in_desc.time,
            spatial_grid: SpatialGridDescriptor::source_from_parts(geo_transform, pixel_bounds),
            bands: RasterBandDescriptors::new_single_band(),
        };

        if let Some(density_params) = self.params.density_params {
            return InitializedDensityRasterization::new(
                name,
                path,
                vector_source,
                out_desc,
                tiling_specification,
                density_params.cutoff,
                density_params.stddev,
            )
            .map(InitializedRasterOperator::boxed);
        }

        Ok(InitializedGridRasterization {
            name,
            path,
            source: vector_source,
            result_descriptor: out_desc,
            tiling_specification,
        }
        .boxed())
    }

    span_fn!(Rasterization);
}

pub struct InitializedGridRasterization {
    name: CanonicOperatorName,
    path: WorkflowOperatorPath,
    source: Box<dyn InitializedVectorOperator>,
    result_descriptor: RasterResultDescriptor,
    tiling_specification: TilingSpecification,
}

impl InitializedRasterOperator for InitializedGridRasterization {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> util::Result<TypedRasterQueryProcessor> {
        Ok(TypedRasterQueryProcessor::F64(
            GridRasterizationQueryProcessor {
                input: self.source.query_processor()?,
                result_descriptor: self.result_descriptor.clone(),
                tiling_specification: self.tiling_specification,
            }
            .boxed(),
        ))
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn name(&self) -> &'static str {
        Rasterization::TYPE_NAME
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.path.clone()
    }
}

pub struct InitializedDensityRasterization {
    name: CanonicOperatorName,
    path: WorkflowOperatorPath,
    source: Box<dyn InitializedVectorOperator>,
    result_descriptor: RasterResultDescriptor,
    tiling_specification: TilingSpecification,
    radius: f64,
    stddev: f64,
}

impl InitializedDensityRasterization {
    fn new(
        name: CanonicOperatorName,
        path: WorkflowOperatorPath,
        source: Box<dyn InitializedVectorOperator>,
        result_descriptor: RasterResultDescriptor,
        tiling_specification: TilingSpecification,
        cutoff: f64,
        stddev: f64,
    ) -> Result<Self, error::Error> {
        ensure!(
            (0. ..1.).contains(&cutoff),
            error::InvalidOperatorSpec {
                reason: "The cutoff for density rasterization must be in [0, 1).".to_string()
            }
        );
        ensure!(
            stddev >= 0.,
            error::InvalidOperatorSpec {
                reason: "The standard deviation for density rasterization must be greater than or equal to zero."
                    .to_string()
            }
        );

        // Determine radius from cutoff percentage
        let radius = gaussian_inverse(cutoff * gaussian(0., stddev), stddev);

        Ok(InitializedDensityRasterization {
            name,
            path,
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
                result_descriptor: self.result_descriptor.clone(),
                input: self.source.query_processor()?,
                tiling_specification: self.tiling_specification,
                radius: self.radius,
                stddev: self.stddev,
            }
            .boxed(),
        ))
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn name(&self) -> &'static str {
        Rasterization::TYPE_NAME
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.path.clone()
    }
}

pub struct GridRasterizationQueryProcessor {
    input: TypedVectorQueryProcessor,
    result_descriptor: RasterResultDescriptor,
    tiling_specification: TilingSpecification,
}

#[async_trait]
impl QueryProcessor for GridRasterizationQueryProcessor {
    type Output = RasterTile2D<f64>;
    type SpatialBounds = GridBoundingBox2D;
    type ResultDescription = RasterResultDescriptor;
    type Selection = BandSelection;

    /// Performs a grid rasterization by first determining the grid resolution to use.
    /// The grid resolution is limited to the query resolution, because a finer granularity
    /// would not be visible in the resulting raster.
    /// Then, for each tile, a grid, aligned to the configured origin coordinate, is created.
    /// All points within the spatial bounds of the grid are queried and counted in the
    /// grid cells.
    /// Finally, the grid resolution is upsampled (if necessary) to the tile resolution.
    #[allow(clippy::too_many_lines)]
    async fn _query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> util::Result<BoxStream<'a, util::Result<Self::Output>>> {
        let spatial_grid_desc = self
            .result_descriptor
            .tiling_grid_definition(ctx.tiling_specification());

        let tiling_strategy = spatial_grid_desc.generate_data_tiling_strategy();
        let tiling_geo_transform = spatial_grid_desc.tiling_geo_transform();
        let query_time = query.time_interval();

        if let MultiPoint(points_processor) = &self.input {
            let query_grid_bounds = query.spatial_bounds();
            let query_spatial_partition =
                tiling_geo_transform.grid_to_spatial_bounds(&query_grid_bounds);

            let tiles = stream::iter(
                tiling_strategy.tile_information_iterator_from_grid_bounds(query.spatial_bounds()),
            )
            .then(move |tile_info| async move {
                let tile_spatial_bounds = tile_info.spatial_partition();

                let grid_size_x = tile_info.tile_size_in_pixels().axis_size_x();

                let vector_query = VectorQueryRectangle::new(
                    tile_spatial_bounds.as_bbox(),
                    query_time,
                    ColumnSelection::all(), // FIXME: should be configurable
                );

                let mut chunks = points_processor.query(vector_query, ctx).await?;

                let mut cache_hint = CacheHint::max_duration();

                let mut grid_data =
                    GridWithFlexibleBoundType::new_filled(tile_info.global_pixel_bounds(), 0.);
                while let Some(chunk) = chunks.next().await {
                    let chunk = chunk?;

                    cache_hint.merge_with(&chunk.cache_hint);

                    grid_data = spawn_blocking(move || {
                        for &coord in chunk.coordinates() {
                            if !tile_spatial_bounds.contains_coordinate(&coord)
                                || !query_spatial_partition.contains_coordinate(&coord)
                            // TODO: old code checks if the pixel center is in the query bounds.
                            {
                                continue;
                            }
                            let GridIdx([y, x]) = tiling_geo_transform
                                .coordinate_to_grid_idx_2d(coord)
                                - tile_info.global_upper_left_pixel_idx();
                            grid_data.data[x as usize + y as usize * grid_size_x] += 1.;
                        }
                        grid_data
                    })
                    .await
                    .expect("Should only forward panics from spawned task");
                }

                let tile_grid = grid_data.unbounded();

                Ok(RasterTile2D::new_with_tile_info(
                    query_time,
                    tile_info,
                    0,
                    GridOrEmpty::Grid(tile_grid.into()),
                    cache_hint,
                ))
            });
            Ok(tiles.boxed())
        } else {
            Ok(generate_zeroed_tiles(
                tiling_geo_transform,
                self.tiling_specification,
                &query,
            ))
        }
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }
}

impl RasterQueryProcessor for GridRasterizationQueryProcessor {
    type RasterType = f64;
}

pub struct DensityRasterizationQueryProcessor {
    input: TypedVectorQueryProcessor,
    result_descriptor: RasterResultDescriptor,
    tiling_specification: TilingSpecification,
    radius: f64,
    stddev: f64,
}

#[async_trait]
impl QueryProcessor for DensityRasterizationQueryProcessor {
    type Output = RasterTile2D<f64>;
    type SpatialBounds = GridBoundingBox2D;
    type ResultDescription = RasterResultDescriptor;
    type Selection = BandSelection;

    /// Performs a gaussian density rasterization.
    /// For each tile, the spatial bounds are extended by `radius` in x and y direction.
    /// All points within these extended bounds are then queried. For each point, the distance to
    /// its surrounding tile pixels (up to `radius` distance) is measured and input into the
    /// gaussian density function with the configured standard deviation. The density values
    /// for each pixel are then summed to result in the tile pixel grid.
    async fn _query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> util::Result<BoxStream<'a, util::Result<Self::Output>>> {
        let spatial_grid_desc = self
            .result_descriptor
            .tiling_grid_definition(ctx.tiling_specification());

        let tiling_strategy = spatial_grid_desc.generate_data_tiling_strategy();
        let tiling_geo_transform = spatial_grid_desc.tiling_geo_transform();
        let query_time = query.time_interval();

        if let MultiPoint(points_processor) = &self.input {
            // Use rounding factor calculated from query resolution to extend in full pixel units
            let rounding_factor = f64::max(
                1. / tiling_geo_transform.x_pixel_size(),
                1. / tiling_geo_transform.y_pixel_size(),
            );
            let radius = (self.radius * rounding_factor).ceil() / rounding_factor;

            let query_grid_bounds = query.spatial_bounds();
            let query_spatial_partition =
                tiling_geo_transform.grid_to_spatial_bounds(&query_grid_bounds);

            let tiles = stream::iter(
                tiling_strategy.tile_information_iterator_from_grid_bounds(query.spatial_bounds()),
            )
            .then(move |tile_info| async move {
                let tile_spatial_bounds = tile_info.spatial_partition();

                let vector_query = VectorQueryRectangle::new(
                    extended_bounding_box_from_spatial_partition(tile_spatial_bounds, radius),
                    query_time,
                    ColumnSelection::all(), // FIXME: should be configurable
                );

                let tile_geo_transform = tile_info.tile_geo_transform();

                let mut chunks = points_processor.query(vector_query, ctx).await?;

                let mut tile_data = Grid2D::new_filled(tiling_strategy.tile_size_in_pixels, 0.0);

                let mut cache_hint = CacheHint::max_duration();

                while let Some(chunk) = chunks.next().await {
                    let chunk = chunk?;

                    cache_hint.merge_with(&chunk.cache_hint);

                    let stddev = self.stddev;
                    tile_data =
                        spawn_blocking_with_thread_pool(ctx.thread_pool().clone(), move || {
                            tile_data.update_indexed_elements_parallel(|pixel_idx, pixel| {
                                let pixel_coordinate = tile_geo_transform
                                    .grid_idx_to_pixel_center_coordinate_2d(pixel_idx);

                                if !query_spatial_partition.contains_coordinate(&pixel_coordinate) {
                                    // TODO: this is propably wrong to do since it produces different tiles depending on the query bounds
                                    return pixel;
                                }

                                let mut pixel_tmp = pixel;

                                for coord in chunk.coordinates() {
                                    let distance = coord.euclidean_distance(&pixel_coordinate);

                                    if distance <= radius {
                                        pixel_tmp += gaussian(distance, stddev);
                                    }
                                }

                                pixel_tmp
                            });

                            tile_data
                        })
                        .await?;
                }

                Ok(RasterTile2D::new_with_tile_info(
                    query_time, // FIXME this breaks the semantics we usually require
                    tile_info,
                    0,
                    tile_data.into(),
                    cache_hint,
                ))
            });

            Ok(tiles.boxed())
        } else {
            Ok(generate_zeroed_tiles(
                tiling_geo_transform,
                self.tiling_specification,
                &query,
            ))
        }
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }
}

impl RasterQueryProcessor for DensityRasterizationQueryProcessor {
    type RasterType = f64;
}

fn generate_zeroed_tiles<'a>(
    tiling_geo_transform: GeoTransform,
    tiling_specification: TilingSpecification,
    query: &RasterQueryRectangle,
) -> BoxStream<'a, util::Result<RasterTile2D<f64>>> {
    let tile_shape = tiling_specification.tile_size_in_pixels;
    let time_interval = query.time_interval();

    let tiling_strategy = TilingStrategy::new(tile_shape, tiling_geo_transform);

    stream::iter(
        tiling_strategy
            .tile_information_iterator_from_grid_bounds(query.spatial_bounds())
            .map(move |tile_info| {
                let tile_data = vec![0.; tile_shape.number_of_elements()];
                let tile_grid = Grid2D::new(tile_shape, tile_data)
                    .expect("Data vector length should match the number of pixels in the tile");

                Ok(RasterTile2D::new_with_tile_info(
                    time_interval,
                    tile_info,
                    0,
                    GridOrEmpty::Grid(tile_grid.into()),
                    CacheHint::no_cache(),
                ))
            }),
    )
    .boxed()
}

fn extended_bounding_box_from_spatial_partition(
    spatial_partition: SpatialPartition2D,
    extent: f64,
) -> BoundingBox2D {
    BoundingBox2D::new_unchecked(
        Coordinate2D::new(
            spatial_partition.lower_left().x - extent,
            spatial_partition.lower_left().y - extent,
        ),
        Coordinate2D::new(
            spatial_partition.upper_right().x + extent,
            spatial_partition.upper_right().y + extent,
        ),
    )
}

/// Calculates the gaussian density value for
/// `x`, the distance from the mean and
/// `stddev`, the standard deviation
fn gaussian(x: f64, stddev: f64) -> f64 {
    (1. / (f64::sqrt(2. * f64::PI()) * stddev)) * f64::exp(-(x * x) / (2. * stddev * stddev))
}

/// The inverse function of [gaussian](gaussian)
fn gaussian_inverse(x: f64, stddev: f64) -> f64 {
    f64::sqrt(2.)
        * f64::sqrt(stddev * stddev * f64::ln(1. / (f64::sqrt(2. * f64::PI()) * stddev * x)))
}

#[cfg(test)]
mod tests {
    use crate::engine::{
        InitializedRasterOperator, MockExecutionContext, MockQueryContext, QueryProcessor,
        RasterOperator, SingleVectorSource, VectorOperator, WorkflowOperatorPath,
    };
    use crate::mock::{MockPointSource, MockPointSourceParams};
    use crate::processing::rasterization::{
        DensityParams, Rasterization, RasterizationParams, gaussian,
    };
    use futures::StreamExt;
    use geoengine_datatypes::primitives::{
        BandSelection, BoundingBox2D, Coordinate2D, RasterQueryRectangle, SpatialResolution,
    };
    use geoengine_datatypes::raster::{GridBoundingBox2D, TilingSpecification};
    use geoengine_datatypes::util::test::TestDefault;

    async fn get_results(
        rasterization: Box<dyn InitializedRasterOperator>,
        query: RasterQueryRectangle,
        query_ctx: &MockQueryContext,
    ) -> Vec<Vec<f64>> {
        rasterization
            .query_processor()
            .unwrap()
            .get_f64()
            .unwrap()
            .query(query, query_ctx)
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
        let execution_context =
            MockExecutionContext::new_with_tiling_spec(TilingSpecification::new([2, 2].into()));
        let rasterization = Rasterization {
            params: RasterizationParams {
                spatial_resolution: SpatialResolution { x: 1.0, y: 1.0 },
                origin_coordinate: [0., 0.].into(),
                density_params: None,
            },
            sources: SingleVectorSource {
                vector: MockPointSource {
                    params: MockPointSourceParams::new_with_bounds(
                        vec![
                            (-1., 1.).into(),
                            (1., 1.).into(),
                            (-1., -1.).into(),
                            (1., -1.).into(),
                        ],
                        crate::mock::SpatialBoundsDerive::Bounds(
                            BoundingBox2D::new(
                                Coordinate2D::new(-2., -2.),
                                Coordinate2D::new(2., 2.),
                            )
                            .unwrap(),
                        ),
                    ),
                }
                .boxed(),
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
        .await
        .unwrap();

        let query = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-2, -2], [1, 1]).unwrap(),
            Default::default(),
            BandSelection::first(),
        );

        let res = get_results(
            rasterization,
            query,
            &execution_context.mock_query_context(TestDefault::test_default()),
        )
        .await;

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
        let execution_context =
            MockExecutionContext::new_with_tiling_spec(TilingSpecification::new([2, 2].into()));
        let rasterization = Rasterization {
            params: RasterizationParams {
                spatial_resolution: SpatialResolution { x: 1.0, y: 1.0 },
                origin_coordinate: [0.0, 0.0].into(),
                density_params: None,
            },
            sources: SingleVectorSource {
                vector: MockPointSource {
                    params: MockPointSourceParams::new_with_bounds(
                        vec![
                            (-1., 1.).into(),
                            (1., 1.).into(),
                            (-1., -1.).into(),
                            (1., -1.).into(),
                        ],
                        crate::mock::SpatialBoundsDerive::Bounds(
                            BoundingBox2D::new(
                                Coordinate2D::new(-2., -2.),
                                Coordinate2D::new(2., 2.),
                            )
                            .unwrap(),
                        ),
                    ),
                }
                .boxed(),
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
        .await
        .unwrap();

        let query = RasterQueryRectangle::new(
            GridBoundingBox2D::new_min_max(-2, 1, -2, 1).unwrap(),
            Default::default(),
            BandSelection::first(),
        );

        let res = get_results(
            rasterization,
            query,
            &execution_context.mock_query_context(TestDefault::test_default()),
        )
        .await;

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
    async fn density_basic() {
        let execution_context =
            MockExecutionContext::new_with_tiling_spec(TilingSpecification::new([2, 2].into()));
        let rasterization = Rasterization {
            params: RasterizationParams {
                spatial_resolution: SpatialResolution { x: 1.0, y: 1.0 },
                origin_coordinate: [0.0, 0.0].into(),
                density_params: Some(DensityParams {
                    cutoff: gaussian(0.99, 1.0) / gaussian(0., 1.0),
                    stddev: 1.0,
                }),
            },
            sources: SingleVectorSource {
                vector: MockPointSource {
                    params: MockPointSourceParams::new_with_bounds(
                        vec![(-1., 1.).into(), (1., 1.).into()],
                        crate::mock::SpatialBoundsDerive::Bounds(
                            BoundingBox2D::new(
                                Coordinate2D::new(-2., -2.),
                                Coordinate2D::new(2., 2.),
                            )
                            .unwrap(),
                        ),
                    ),
                }
                .boxed(),
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
        .await
        .unwrap();

        let query = RasterQueryRectangle::new(
            GridBoundingBox2D::new_min_max(-2, -1, -2, 1).unwrap(),
            Default::default(),
            BandSelection::first(),
        );

        let res = get_results(
            rasterization,
            query,
            &execution_context.mock_query_context_test_default(),
        )
        .await;

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
        let execution_context =
            MockExecutionContext::new_with_tiling_spec(TilingSpecification::new([2, 2].into()));
        let rasterization = Rasterization {
            params: RasterizationParams {
                spatial_resolution: SpatialResolution { x: 1.0, y: 1.0 },
                origin_coordinate: [0.0, 0.0].into(),
                density_params: Some(DensityParams {
                    cutoff: gaussian(1.99, 1.0) / gaussian(0., 1.0),
                    stddev: 1.0,
                }),
            },
            sources: SingleVectorSource {
                vector: MockPointSource {
                    params: MockPointSourceParams::new_with_bounds(
                        vec![(-1., 1.).into(), (1., 1.).into()],
                        crate::mock::SpatialBoundsDerive::Bounds(
                            BoundingBox2D::new(
                                Coordinate2D::new(-2., -2.),
                                Coordinate2D::new(2., 2.),
                            )
                            .unwrap(),
                        ),
                    ),
                }
                .boxed(),
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
        .await
        .unwrap();

        let query = RasterQueryRectangle::new(
            GridBoundingBox2D::new_min_max(-2, -1, -2, 1).unwrap(),
            Default::default(),
            BandSelection::first(),
        );

        let res = get_results(
            rasterization,
            query,
            &execution_context.mock_query_context_test_default(),
        )
        .await;

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
