use crate::{
    engine::{
        InitializedOperator, InitializedOperatorBase, InitializedRasterOperator, QueryProcessor,
        RasterOperator, RasterQueryProcessor, RasterResultDescriptor, SourceOperator,
        TypedRasterQueryProcessor,
    },
    util::Result,
};

use gdal::raster::{GdalType, RasterBand as GdalRasterBand};
use gdal::Dataset as GdalDataset;
use std::{
    io::{BufReader, BufWriter, Read},
    marker::PhantomData,
    path::Path,
    path::PathBuf,
};
//use gdal::metadata::Metadata; // TODO: handle metadata

use serde::{Deserialize, Serialize};

use futures::stream::{self, BoxStream, StreamExt};

use geoengine_datatypes::primitives::Measurement;
use geoengine_datatypes::raster::{
    GeoTransform, Grid2D, Pixel, RasterDataType, RasterTile2D, TileInformation, TilingStrategy,
};
use geoengine_datatypes::{
    primitives::{
        BoundingBox2D, SpatialBounded, SpatialResolution, TimeInstance, TimeInterval, TimeStep,
        TimeStepIter,
    },
    raster::{
        Grid, GridBlit, GridBoundingBox2D, GridBounds, GridIdx, GridShape2D, GridSize,
        GridSpaceToLinearSpace, TilingSpecification,
    },
    spatial_reference::SpatialReferenceOption,
};

/// Parameters for the GDAL Source Operator
///
/// # Examples
///
/// ```rust
/// use serde_json::{Result, Value};
/// use geoengine_operators::source::{GdalSource, GdalSourceParameters};
///
/// let json_string = r#"
///     {
///         "type": "GdalSource",
///         "params": {
///                     "dataset_id": "modis_ndvi",
///                     "channel": 1
///         }
///     }"#;
///
/// let operator: GdalSource = serde_json::from_str(json_string).unwrap();
///
/// assert_eq!(operator, GdalSource {
///     params: GdalSourceParameters {
///         dataset_id: "modis_ndvi".to_owned(),
///         channel: Some(1),
///     },
/// });
/// ```
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct GdalSourceParameters {
    pub dataset_id: String,
    pub channel: Option<u32>,
    // TODO: add some kind of tick interval
}

pub trait GdalDatasetInformationProvider {
    type CreatedType: Sized;
    fn with_dataset_id(id: &str, raster_data_root: &Path) -> Result<Self::CreatedType>;
    fn native_tiling_information(&self) -> Option<TilingInformation>;
    fn native_time_information(&self) -> &TimeIntervalInformation;
    fn geo_transform(&self) -> GeoTransform;
    fn bounding_box(&self) -> BoundingBox2D;
    fn grid_shape(&self) -> GridShape2D;
    fn file_name_with_time_placeholder(&self) -> &str;
    fn time_format(&self) -> &str;
    fn dataset_path(&self) -> PathBuf;
    fn data_type(&self) -> RasterDataType;
    fn spatial_ref(&self) -> SpatialReferenceOption;
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JsonDatasetInformationProvider {
    pub dataset_information: JsonDatasetInformation,
    pub raster_data_root: PathBuf,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JsonDatasetInformation {
    pub time: TimeIntervalInformation,
    pub tile: Option<TilingInformation>,
    pub file_name_with_time_placeholder: String,
    pub time_format: String,
    pub base_path: PathBuf,
    pub data_type: RasterDataType,
    pub geo_transform: GeoTransform,
    pub grid_shape: GridShape2D,
    pub spatial_ref: SpatialReferenceOption,
    pub measurement: Measurement,
}

impl JsonDatasetInformationProvider {
    const DEFINITION_SUBPATH: &'static str = "dataset_defs";

    // TODO: provide the base path from config?
    pub fn root_path(&self) -> &Path {
        &self.raster_data_root
    }

    pub fn write_to_file(&self, id: &str, raster_data_root: &Path) -> Result<()> {
        let mut dataset_information_path: PathBuf = PathBuf::from(raster_data_root)
            .join(Self::DEFINITION_SUBPATH)
            .join(id);
        dataset_information_path.set_extension("json");

        let file = std::fs::File::create(dataset_information_path)?;
        let buffered_writer = BufWriter::new(file);
        Ok(serde_json::to_writer(
            buffered_writer,
            &self.dataset_information,
        )?)
    }
}

impl GdalDatasetInformationProvider for JsonDatasetInformationProvider {
    type CreatedType = Self;
    fn with_dataset_id(id: &str, raster_data_root: &Path) -> Result<Self> {
        let raster_data_root_buf = PathBuf::from(raster_data_root);
        let mut dataset_information_path: PathBuf =
            raster_data_root_buf.join(Self::DEFINITION_SUBPATH).join(id);
        dataset_information_path.set_extension("json");
        let file = std::fs::File::open(dataset_information_path)?;
        let mut buffered_reader = BufReader::new(file);
        let mut contents = String::new();
        buffered_reader.read_to_string(&mut contents)?;
        let dataset_information = serde_json::from_str(&contents)?;
        Ok(JsonDatasetInformationProvider {
            dataset_information,
            raster_data_root: raster_data_root_buf,
        })
    }
    fn native_tiling_information(&self) -> Option<TilingInformation> {
        self.dataset_information.tile
    }
    fn native_time_information(&self) -> &TimeIntervalInformation {
        &self.dataset_information.time
    }
    fn file_name_with_time_placeholder(&self) -> &str {
        &self.dataset_information.file_name_with_time_placeholder
    }
    fn time_format(&self) -> &str {
        &self.dataset_information.time_format
    }

    fn dataset_path(&self) -> PathBuf {
        self.raster_data_root
            .clone()
            .join(Self::DEFINITION_SUBPATH)
            .join(&self.dataset_information.base_path)
    }
    fn data_type(&self) -> RasterDataType {
        self.dataset_information.data_type
    }

    fn geo_transform(&self) -> GeoTransform {
        self.dataset_information.geo_transform
    }

    fn bounding_box(&self) -> BoundingBox2D {
        let [size_y, size_x] = self.grid_shape().axis_size();
        let lower_right = self
            .geo_transform()
            .grid_idx_to_coordinate_2d([size_y as isize, size_x as isize].into());
        BoundingBox2D::new_upper_left_lower_right_unchecked(
            self.geo_transform().origin_coordinate,
            lower_right,
        )
    }

    fn grid_shape(&self) -> GridShape2D {
        self.dataset_information.grid_shape
    }

    fn spatial_ref(&self) -> SpatialReferenceOption {
        self.dataset_information.spatial_ref
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TimeIntervalInformation {
    pub start_time: TimeInstance,
    pub time_step: TimeStep,
    // TODO: add an end_time: TimeStep?
}

impl TimeIntervalInformation {}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct TilingInformation {
    pub x_axis_tiles: usize,
    pub y_axis_tiles: usize,
    pub x_axis_tile_size: usize,
    pub y_axis_tile_size: usize,
}

pub struct GdalSourceProcessor<P, T>
where
    T: Pixel,
{
    pub dataset_information: P,
    pub tiling_specification: TilingSpecification,
    pub gdal_params: GdalSourceParameters,
    pub phantom_data: PhantomData<T>,
}

impl<T> GdalSourceProcessor<JsonDatasetInformationProvider, T>
where
    T: gdal::raster::GdalType + Pixel,
{
    pub fn from_params_with_json_provider(
        params: GdalSourceParameters,
        raster_data_root: &Path,
        tiling_specification: TilingSpecification,
    ) -> Result<Self> {
        GdalSourceProcessor::from_params(params, raster_data_root, tiling_specification)
    }
}

impl<P, T> GdalSourceProcessor<P, T>
where
    P: GdalDatasetInformationProvider<CreatedType = P> + Sync + Send + Clone + 'static,
    T: gdal::raster::GdalType + Pixel,
{
    ///
    /// Generates a new `GdalSource` from the provided parameters
    /// TODO: move the time interval and grid tile information generation somewhere else...
    ///
    pub fn from_params(
        params: GdalSourceParameters,
        raster_data_root: &Path,
        tiling_specification: TilingSpecification,
    ) -> Result<Self> {
        let dataset_information = P::with_dataset_id(&params.dataset_id, raster_data_root)?;

        GdalSourceProcessor::from_params_with_provider(
            params,
            dataset_information,
            tiling_specification,
        )
    }

    ///
    /// Generates a new `GdalSource` from the provided parameters
    /// TODO: move the time interval and grid tile information generation somewhere else...
    ///
    #[allow(clippy::unnecessary_wraps)] // TODO: remove line
    fn from_params_with_provider(
        params: GdalSourceParameters,
        dataset_information: P,
        tiling_specification: TilingSpecification,
    ) -> Result<Self> {
        Ok(GdalSourceProcessor {
            dataset_information,
            gdal_params: params,
            tiling_specification,
            phantom_data: PhantomData,
        })
    }

    ///
    /// An iterator which will produce one element per time step and grid tile
    ///
    pub fn time_tile_iter(
        &self,
        tileing_strategy: TilingStrategy,
        time_interval: TimeInterval,
        bounding_box: BoundingBox2D,
    ) -> impl Iterator<Item = (TimeInterval, TileInformation)> + '_ {
        let time_information = self.dataset_information.native_time_information();
        let snapped_start = time_information
            .time_step
            .snap_relative(time_information.start_time, time_interval.start())
            .expect("is a valid time");

        let snapped_interval = TimeInterval::new_unchecked(snapped_start, time_interval.end());

        let time_iterator = TimeStepIter::new_with_interval_incl_start(
            snapped_interval,
            time_information.time_step,
        )
        .expect("is a valid interval");

        let time_interval_iterator = time_iterator
            .try_as_intervals(time_information.time_step)
            .map(|ti| ti.expect("is a valid time"));

        time_interval_iterator.flat_map(move |time| {
            tileing_strategy
                .tile_information_iterator(bounding_box)
                .map(move |tile| (time, tile))
        })
    }

    pub async fn load_tile_data_async(
        gdal_params: GdalSourceParameters,
        gdal_dataset_information: P,
        time_interval: TimeInterval,
        tile_information: TileInformation,
    ) -> Result<RasterTile2D<T>> {
        tokio::task::spawn_blocking(move || {
            Self::load_tile_data_impl(
                &gdal_params,
                &gdal_dataset_information,
                time_interval,
                tile_information,
            )
        })
        .await
        .unwrap() // TODO: handle TaskJoinError
    }

    pub fn load_tile_data(
        &self,
        time_interval: TimeInterval,
        tile_information: TileInformation,
    ) -> Result<RasterTile2D<T>> {
        GdalSourceProcessor::<P, T>::load_tile_data_impl(
            &self.gdal_params,
            &self.dataset_information,
            time_interval,
            tile_information,
        )
    }

    ///
    /// A method to load single tiles from a GDAL dataset.
    ///
    fn load_tile_data_impl(
        gdal_params: &GdalSourceParameters,
        gdal_dataset_information: &P,
        time_interval: TimeInterval,
        tile_information: TileInformation,
    ) -> Result<RasterTile2D<T>> {
        // format the time interval
        let time_string = time_interval.start().as_naive_date_time().map(|t| {
            t.format(&gdal_dataset_information.time_format())
                .to_string()
        });

        // TODO: replace -> parser?
        let file_name = gdal_dataset_information
            .file_name_with_time_placeholder()
            .replace("%%%_START_TIME_%%%", &time_string.unwrap_or_default());

        let path = gdal_dataset_information.dataset_path(); // TODO: add the path of the definition file for relative paths
        let data_file = path.join(file_name);

        let tile_grid = tile_information.tile_size_in_pixels();

        // open the dataset at path
        let dataset_result = GdalDataset::open(&data_file);
        // TODO: investigate if we need a dataset cache

        // shortcut if there is no raster file -> return a no-data file.
        if dataset_result.is_err() {
            return Ok(RasterTile2D::new_with_tile_info(
                time_interval,
                tile_information,
                Grid2D::new_filled(tile_grid, T::zero(), None),
            ));
        };

        // this was checked one line above...
        let dataset = dataset_result.expect("checked");

        // get the requested raster band of the dataset …
        let rasterband_index = gdal_params.channel.unwrap_or(1) as isize; // TODO: investigate if this should be isize in gdal
        let rasterband: GdalRasterBand = dataset.rasterband(rasterband_index)?;

        // dataset spatial relations
        let dataset_contains_tile = gdal_dataset_information
            .bounding_box()
            .contains_bbox(&tile_information.spatial_bounds());

        let dataset_intersects_tile = gdal_dataset_information
            .bounding_box()
            .intersects_bbox(&tile_information.spatial_bounds());

        let result_raster = match (dataset_contains_tile, dataset_intersects_tile) {
            (_, false) => {
                // TODO: refactor tile to hold an Option<GridData> and this will be empty in this case
                Grid2D::new_filled(tile_grid, T::zero(), None)
            }
            (true, true) => {
                let dataset_idx_ul = gdal_dataset_information
                    .geo_transform()
                    .coordinate_to_grid_idx_2d(tile_information.spatial_bounds().upper_left());

                let dataset_idx_lr = gdal_dataset_information
                    .geo_transform()
                    .coordinate_to_grid_idx_2d(tile_information.spatial_bounds().lower_right())
                    - 1; // the lr coordinate is the first pixel of the next tile so sub 1 from all axis.

                read_as_raster(
                    &rasterband,
                    &GridBoundingBox2D::new(dataset_idx_ul, dataset_idx_lr)?,
                    tile_information.tile_size_in_pixels,
                )?
            }
            (false, true) => {
                let intersecting_area = gdal_dataset_information
                    .bounding_box()
                    .intersection(&tile_information.spatial_bounds())
                    .expect("checked intersection earlier");

                let dataset_idx_ul = gdal_dataset_information
                    .geo_transform()
                    .coordinate_to_grid_idx_2d(intersecting_area.upper_left());

                let dataset_idx_lr = gdal_dataset_information
                    .geo_transform()
                    .coordinate_to_grid_idx_2d(intersecting_area.lower_right())
                    - 1;

                let tile_idx_ul = tile_information
                    .tile_geo_transform()
                    .coordinate_to_grid_idx_2d(intersecting_area.upper_left());

                let tile_idx_lr = tile_information
                    .tile_geo_transform()
                    .coordinate_to_grid_idx_2d(intersecting_area.lower_right())
                    - 1;

                let dataset_raster = read_as_raster(
                    &rasterband,
                    &GridBoundingBox2D::new(dataset_idx_ul, dataset_idx_lr)?,
                    GridBoundingBox2D::new(tile_idx_ul, tile_idx_lr)?,
                )?;

                let mut tile_raster = Grid2D::new_filled(tile_grid, T::zero(), None);
                tile_raster.grid_blit_from(dataset_raster)?;
                tile_raster
            }
        };

        Ok(RasterTile2D::new_with_tile_info(
            time_interval,
            tile_information,
            result_raster,
        ))
    }

    ///
    /// A stream of `RasterTile2D`
    ///
    pub fn tile_stream(
        &self,
        bbox: BoundingBox2D,
        time_interval: TimeInterval,
        spatial_resolution: SpatialResolution,
    ) -> BoxStream<Result<RasterTile2D<T>>> {
        // adjust the spatial resolution to the sign of the geotransform
        let x_signed = if self
            .dataset_information
            .geo_transform()
            .x_pixel_size
            .is_sign_positive()
            && spatial_resolution.x.is_sign_positive()
        {
            spatial_resolution.x
        } else {
            spatial_resolution.x * -1.0
        };

        let y_signed = if self
            .dataset_information
            .geo_transform()
            .y_pixel_size
            .is_sign_positive()
            && spatial_resolution.y.is_sign_positive()
        {
            spatial_resolution.y
        } else {
            spatial_resolution.y * -1.0
        };

        let tiling_strategy =
            TilingStrategy::new_with_tiling_spec(self.tiling_specification, x_signed, y_signed);

        stream::iter(self.time_tile_iter(tiling_strategy, time_interval, bbox))
            .map(move |(time, tile)| {
                (
                    self.gdal_params.clone(),
                    self.dataset_information.clone(),
                    time,
                    tile,
                )
            })
            .then(|(gdal_params, dataset_information, time, tile)| {
                Self::load_tile_data_async(gdal_params, dataset_information, time, tile)
            })
            .boxed()
    }
}

impl<T, P> QueryProcessor for GdalSourceProcessor<P, T>
where
    P: GdalDatasetInformationProvider<CreatedType = P> + Send + Sync + 'static + Clone,
    T: Pixel + gdal::raster::GdalType,
{
    type Output = RasterTile2D<T>;
    fn query<'a>(
        &'a self,
        query: crate::engine::QueryRectangle,
        _ctx: &'a dyn crate::engine::QueryContext,
    ) -> BoxStream<Result<RasterTile2D<T>>> {
        self.tile_stream(query.bbox, query.time_interval, query.spatial_resolution)
            .boxed() // TODO: handle query, ctx, remove one boxed
    }
}

pub type GdalSource = SourceOperator<GdalSourceParameters>;

#[typetag::serde]
impl RasterOperator for GdalSource {
    fn initialize(
        self: Box<Self>,
        context: &dyn crate::engine::ExecutionContext,
    ) -> Result<Box<InitializedRasterOperator>> {
        let provider = JsonDatasetInformationProvider::with_dataset_id(
            &self.params.dataset_id,
            &context.raster_data_root()?,
        )?;
        let spatial_ref = provider.spatial_ref();

        let data_type = provider.data_type();

        let result_descriptor = RasterResultDescriptor {
            data_type,
            spatial_reference: spatial_ref,
            measurement: provider.dataset_information.measurement.clone(),
        };

        Ok(InitializedGdalSourceOperator {
            provider,
            result_descriptor,
            tiling_specification: context.tiling_specification(),
            params: self.params,
        }
        .boxed())
    }
}

pub struct InitializedGdalSourceOperator<P> {
    pub provider: P,
    pub params: GdalSourceParameters,
    pub result_descriptor: RasterResultDescriptor,
    pub tiling_specification: TilingSpecification,
}

impl<P> InitializedOperatorBase for InitializedGdalSourceOperator<P> {
    type Descriptor = RasterResultDescriptor;

    fn result_descriptor(&self) -> &Self::Descriptor {
        &self.result_descriptor
    }

    fn raster_sources(&self) -> &[Box<InitializedRasterOperator>] {
        &[]
    }

    fn vector_sources(&self) -> &[Box<crate::engine::InitializedVectorOperator>] {
        &[]
    }

    fn raster_sources_mut(&mut self) -> &mut [Box<InitializedRasterOperator>] {
        &mut []
    }

    fn vector_sources_mut(&mut self) -> &mut [Box<crate::engine::InitializedVectorOperator>] {
        &mut []
    }
}

impl InitializedOperator<RasterResultDescriptor, TypedRasterQueryProcessor>
    for InitializedGdalSourceOperator<JsonDatasetInformationProvider>
{
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        Ok(match self.result_descriptor().data_type {
            RasterDataType::U8 => TypedRasterQueryProcessor::U8(
                GdalSourceProcessor::from_params_with_provider(
                    self.params.clone(),
                    self.provider.clone(),
                    self.tiling_specification,
                )?
                .boxed(),
            ),
            RasterDataType::U16 => TypedRasterQueryProcessor::U16(
                GdalSourceProcessor::from_params_with_provider(
                    self.params.clone(),
                    self.provider.clone(),
                    self.tiling_specification,
                )?
                .boxed(),
            ),
            RasterDataType::U32 => TypedRasterQueryProcessor::U32(
                GdalSourceProcessor::from_params_with_provider(
                    self.params.clone(),
                    self.provider.clone(),
                    self.tiling_specification,
                )?
                .boxed(),
            ),
            RasterDataType::U64 => unimplemented!("implement U64 type"), // TypedRasterQueryProcessor::U64(self.create_processor()),
            RasterDataType::I8 => unimplemented!("I8 type is not supported"),
            RasterDataType::I16 => TypedRasterQueryProcessor::I16(
                GdalSourceProcessor::from_params_with_provider(
                    self.params.clone(),
                    self.provider.clone(),
                    self.tiling_specification,
                )?
                .boxed(),
            ),
            RasterDataType::I32 => TypedRasterQueryProcessor::I32(
                GdalSourceProcessor::from_params_with_provider(
                    self.params.clone(),
                    self.provider.clone(),
                    self.tiling_specification,
                )?
                .boxed(),
            ),
            RasterDataType::I64 => unimplemented!("implement I64 type"), // TypedRasterQueryProcessor::I64(self.create_processor()),
            RasterDataType::F32 => TypedRasterQueryProcessor::F32(
                GdalSourceProcessor::from_params_with_provider(
                    self.params.clone(),
                    self.provider.clone(),
                    self.tiling_specification,
                )?
                .boxed(),
            ),
            RasterDataType::F64 => TypedRasterQueryProcessor::F64(
                GdalSourceProcessor::from_params_with_provider(
                    self.params.clone(),
                    self.provider.clone(),
                    self.tiling_specification,
                )?
                .boxed(),
            ),
        })
    }
}

fn read_as_raster<
    T,
    D: GridSize<ShapeArray = [usize; 2]> + GridSpaceToLinearSpace<IndexArray = [isize; 2]>,
>(
    rasterband: &GdalRasterBand,
    dataset_grid_box: &GridBoundingBox2D,
    tile_grid: D,
) -> Result<Grid<D, T>>
where
    T: Pixel + GdalType,
{
    let GridIdx([dataset_ul_y, dataset_ul_x]) = dataset_grid_box.min_index();
    let [dataset_y_size, dataset_x_size] = dataset_grid_box.axis_size();
    let [tile_y_size, tile_x_size] = tile_grid.axis_size();
    let buffer = rasterband.read_as::<T>(
        (dataset_ul_x, dataset_ul_y),     // pixelspace origin
        (dataset_x_size, dataset_y_size), // pixelspace size
        (tile_x_size, tile_y_size),       // requested raster size
    )?;
    Grid::new(tile_grid, buffer.data, None).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{MockExecutionContext, MockQueryContext, QueryRectangle};
    use crate::error::Error;
    use crate::util::Result;
    use chrono::NaiveDate;
    use futures::executor::block_on_stream;
    use geoengine_datatypes::{
        primitives::{Coordinate2D, TimeGranularity},
        raster::{GridIdx2D, GridIndexAccess},
        spatial_reference::SpatialReference,
    };

    #[test]
    fn tiling_strategy_origin() {
        let tile_size_in_pixels = [600, 600];
        let dataset_upper_right_coord = (-180.0, 90.0).into();
        let dataset_x_pixel_size = 0.1;
        let dataset_y_pixel_size = -0.1;
        let dataset_geo_transform = GeoTransform::new(
            dataset_upper_right_coord,
            dataset_x_pixel_size,
            dataset_y_pixel_size,
        );

        let bounding_box = BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap();

        let origin_split_tileing_strategy = TilingStrategy {
            tile_size_in_pixels: tile_size_in_pixels.into(),
            geo_transform: dataset_geo_transform,
        };

        assert_eq!(
            origin_split_tileing_strategy.upper_left_pixel_idx(bounding_box),
            [0, 0].into()
        );
        assert_eq!(
            origin_split_tileing_strategy.lower_right_pixel_idx(bounding_box),
            [1800 - 1, 3600 - 1].into()
        );

        let tile_grid = origin_split_tileing_strategy.tile_grid_box(bounding_box);
        assert_eq!(tile_grid.axis_size(), [3, 6]);
        assert_eq!(tile_grid.min_index(), [0, 0].into());
        assert_eq!(tile_grid.max_index(), [2, 5].into());
    }

    #[test]
    fn tiling_strategy_zero() {
        let tile_size_in_pixels = [600, 600];
        let dataset_x_pixel_size = 0.1;
        let dataset_y_pixel_size = -0.1;
        let central_geo_transform = GeoTransform::new_with_coordinate_x_y(
            0.0,
            dataset_x_pixel_size,
            0.0,
            dataset_y_pixel_size,
        );

        let bounding_box = BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap();

        let origin_split_tileing_strategy = TilingStrategy {
            tile_size_in_pixels: tile_size_in_pixels.into(),
            geo_transform: central_geo_transform,
        };

        assert_eq!(
            origin_split_tileing_strategy.upper_left_pixel_idx(bounding_box),
            [-900, -1800].into()
        );
        assert_eq!(
            origin_split_tileing_strategy.lower_right_pixel_idx(bounding_box),
            [1800 / 2 - 1, 3600 / 2 - 1].into()
        );

        let tile_grid = origin_split_tileing_strategy.tile_grid_box(bounding_box);
        assert_eq!(tile_grid.axis_size(), [4, 6]);
        assert_eq!(tile_grid.min_index(), [-2, -3].into());
        assert_eq!(tile_grid.max_index(), [1, 2].into());
    }

    #[test]
    fn tile_idx_iterator() {
        let tile_size_in_pixels = [600, 600];
        let dataset_x_pixel_size = 0.1;
        let dataset_y_pixel_size = -0.1;
        let central_geo_transform = GeoTransform::new_with_coordinate_x_y(
            0.0,
            dataset_x_pixel_size,
            0.0,
            dataset_y_pixel_size,
        );

        let bounding_box = BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap();

        let origin_split_tileing_strategy = TilingStrategy {
            tile_size_in_pixels: tile_size_in_pixels.into(),
            geo_transform: central_geo_transform,
        };

        let vres: Vec<GridIdx2D> = origin_split_tileing_strategy
            .tile_idx_iterator(bounding_box)
            .collect();
        assert_eq!(vres.len(), 4 * 6);
        assert_eq!(vres[0], [-2, -3].into());
        assert_eq!(vres[1], [-2, -2].into());
        assert_eq!(vres[2], [-2, -1].into());
        assert_eq!(vres[23], [1, 2].into());
    }

    #[test]
    fn tile_information_iterator() {
        let tile_size_in_pixels = [600, 600];
        let dataset_x_pixel_size = 0.1;
        let dataset_y_pixel_size = -0.1;

        let central_geo_transform = GeoTransform::new_with_coordinate_x_y(
            0.0,
            dataset_x_pixel_size,
            0.0,
            dataset_y_pixel_size,
        );

        let bounding_box = BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap();

        let origin_split_tileing_strategy = TilingStrategy {
            tile_size_in_pixels: tile_size_in_pixels.into(),
            geo_transform: central_geo_transform,
        };

        let vres: Vec<TileInformation> = origin_split_tileing_strategy
            .tile_information_iterator(bounding_box)
            .collect();
        assert_eq!(vres.len(), 4 * 6);
        assert_eq!(
            vres[0],
            TileInformation::new(
                [-2, -3].into(),
                tile_size_in_pixels.into(),
                central_geo_transform
            )
        );
        assert_eq!(
            vres[1],
            TileInformation::new(
                [-2, -2].into(),
                tile_size_in_pixels.into(),
                central_geo_transform
            )
        );
        assert_eq!(
            vres[12],
            TileInformation::new(
                [0, -3].into(),
                tile_size_in_pixels.into(),
                central_geo_transform
            )
        );
        assert_eq!(
            vres[23],
            TileInformation::new(
                [1, 2].into(),
                tile_size_in_pixels.into(),
                central_geo_transform
            )
        );
    }

    #[test]
    fn test_time_tile_iter() {
        let global_size_in_pixels = [1800, 3600];
        let tile_size_in_pixels = [600, 600];
        let dataset_upper_right_coord = (-180.0, 90.0).into();

        let dataset_geo_transform = GeoTransform::new(dataset_upper_right_coord, 0.1, -0.1);
        let central_geo_transform = GeoTransform::new_with_coordinate_x_y(0.0, 0.1, 0.0, -0.1);

        let bounding_box = BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap();

        let time_interval_provider = TimeIntervalInformation {
            start_time: TimeInstance::from_millis(0),
            time_step: TimeStep {
                granularity: TimeGranularity::Seconds,
                step: 1,
            },
        };

        let gdal_params = GdalSourceParameters {
            dataset_id: "modis_ndvi".to_owned(),
            channel: None,
        };

        let dataset_information = JsonDatasetInformation {
            time: time_interval_provider,
            tile: None,
            file_name_with_time_placeholder: "MOD13A2_M_NDVI_2014-01-01.TIFF".into(),
            time_format: "".into(),
            base_path: "../modis_ndvi".into(),
            data_type: RasterDataType::U8,
            geo_transform: dataset_geo_transform,
            grid_shape: global_size_in_pixels.into(),
            spatial_ref: SpatialReference::epsg_4326().into(),
            measurement: Measurement::Unitless,
        };

        let dataset_information_provider = JsonDatasetInformationProvider {
            dataset_information,
            raster_data_root: "../operators/test-data/raster".into(),
        };

        let gdal_source = GdalSourceProcessor::<_, u8> {
            dataset_information: dataset_information_provider,
            gdal_params,
            tiling_specification: TilingSpecification {
                origin_coordinate: central_geo_transform.origin_coordinate,
                tile_size_in_pixels: GridShape2D::from(tile_size_in_pixels),
            },
            phantom_data: PhantomData,
        };

        let origin_split_tileing_strategy = TilingStrategy {
            tile_size_in_pixels: tile_size_in_pixels.into(),
            geo_transform: central_geo_transform,
        };

        let vres: Vec<_> = gdal_source
            .time_tile_iter(
                origin_split_tileing_strategy,
                TimeInterval::new_unchecked(0, 1000),
                bounding_box,
            )
            .collect();

        assert_eq!(
            vres[0],
            (
                TimeInterval::new_unchecked(0, 1000),
                TileInformation::new(
                    [-2, -3].into(),
                    tile_size_in_pixels.into(),
                    central_geo_transform
                )
            )
        );
        assert_eq!(
            vres[1],
            (
                TimeInterval::new_unchecked(0, 1000),
                TileInformation::new(
                    [-2, -2].into(),
                    tile_size_in_pixels.into(),
                    central_geo_transform
                )
            )
        );
        assert_eq!(
            vres[12],
            (
                TimeInterval::new_unchecked(0, 1000),
                TileInformation::new(
                    [0, -3].into(),
                    tile_size_in_pixels.into(),
                    central_geo_transform
                )
            )
        );
        assert_eq!(
            vres[23],
            (
                TimeInterval::new_unchecked(0, 1000),
                TileInformation::new(
                    [1, 2].into(),
                    tile_size_in_pixels.into(),
                    central_geo_transform
                )
            )
        );
    }

    #[test]
    fn test_load_tile_data() {
        let global_size_in_pixels = [1800, 3600];
        let tile_size_in_pixels = [600, 600];
        let dataset_upper_right_coord = (-180.0, 90.0).into();
        let dataset_x_pixel_size = 0.1;
        let dataset_y_pixel_size = -0.1;
        let dataset_geo_transform = GeoTransform::new(
            dataset_upper_right_coord,
            dataset_x_pixel_size,
            dataset_y_pixel_size,
        );

        let time_interval_provider = TimeIntervalInformation {
            start_time: TimeInstance::from_millis(0),
            time_step: TimeStep {
                granularity: TimeGranularity::Seconds,
                step: 1,
            },
        };

        let gdal_params = GdalSourceParameters {
            dataset_id: "modis_ndvi".to_owned(),
            channel: None,
        };

        let dataset_information = JsonDatasetInformation {
            time: time_interval_provider,
            tile: None,
            file_name_with_time_placeholder: "MOD13A2_M_NDVI_2014-01-01.TIFF".into(),
            time_format: "".into(),
            base_path: "../modis_ndvi".into(),
            data_type: RasterDataType::U8,
            geo_transform: dataset_geo_transform,
            grid_shape: global_size_in_pixels.into(),
            spatial_ref: SpatialReference::epsg_4326().into(),
            measurement: Measurement::Unitless,
        };

        let dataset_information_provider = JsonDatasetInformationProvider {
            dataset_information,
            raster_data_root: "../operators/test-data/raster".into(),
        };

        let gdal_source = GdalSourceProcessor::<_, u8> {
            dataset_information: dataset_information_provider,
            gdal_params,
            tiling_specification: TilingSpecification {
                origin_coordinate: Coordinate2D::default(),
                tile_size_in_pixels: GridShape2D::from(tile_size_in_pixels),
            },
            phantom_data: PhantomData,
        };

        let tile_information = TileInformation::new(
            [0, 0].into(),
            tile_size_in_pixels.into(),
            dataset_geo_transform,
        );
        let time_interval = TimeInterval::new_unchecked(0, 1000);

        let x = gdal_source
            .load_tile_data(time_interval, tile_information)
            .unwrap();

        assert_eq!(x.tile_information(), tile_information);
        assert_eq!(x.time, time_interval);
    }

    #[test]
    fn test_load_tile_data_overlaps_dataset_bounds() {
        let global_size_in_pixels = [1800, 3600];
        let tile_size_in_pixels = [600, 600];
        let dataset_upper_right_coord = (-180.0, 90.0).into();
        let dataset_x_pixel_size = 0.1;
        let dataset_y_pixel_size = -0.1;
        let dataset_geo_transform = GeoTransform::new(
            dataset_upper_right_coord,
            dataset_x_pixel_size,
            dataset_y_pixel_size,
        );

        let time_interval_provider = TimeIntervalInformation {
            start_time: TimeInstance::from_millis(0),
            time_step: TimeStep {
                granularity: TimeGranularity::Seconds,
                step: 1,
            },
        };

        let gdal_params = GdalSourceParameters {
            dataset_id: "modis_ndvi".to_owned(),
            channel: None,
        };

        let dataset_information = JsonDatasetInformation {
            time: time_interval_provider,
            tile: None,
            file_name_with_time_placeholder: "MOD13A2_M_NDVI_2014-01-01.TIFF".into(),
            time_format: "".into(),
            base_path: "../modis_ndvi".into(),
            data_type: RasterDataType::U8,
            geo_transform: dataset_geo_transform,
            grid_shape: global_size_in_pixels.into(),
            spatial_ref: SpatialReference::epsg_4326().into(),
            measurement: Measurement::Unitless,
        };

        let dataset_information_provider = JsonDatasetInformationProvider {
            dataset_information,
            raster_data_root: "../operators/test-data/raster".into(),
        };

        let gdal_source = GdalSourceProcessor::<_, u8> {
            dataset_information: dataset_information_provider,
            gdal_params,
            tiling_specification: TilingSpecification {
                origin_coordinate: Coordinate2D::new(0., 0.),
                tile_size_in_pixels: GridShape2D::from(tile_size_in_pixels),
            },
            phantom_data: PhantomData,
        };

        let tile_geo_transform = GeoTransform::new(
            Coordinate2D::new(0., 0.),
            dataset_x_pixel_size,
            dataset_y_pixel_size,
        );

        // let x_tiles = 3600 / 600; // 6 => min tile position = -3
        // let y_tiles = 1800 / 600; // 3 -> a split at 0,0 forces 4 tiles on the y-axis. => min tile position = -2

        let tile_information = TileInformation::new(
            [-2, -3].into(),
            tile_size_in_pixels.into(),
            tile_geo_transform,
        );
        let time_interval = TimeInterval::new_unchecked(0, 1000);

        let x = gdal_source
            .load_tile_data(time_interval, tile_information)
            .unwrap();

        assert_eq!(x.tile_information(), tile_information);
        assert_eq!(x.time, time_interval);
    }

    #[test]
    fn test_iter_and_load_tile_data() {
        let global_size_in_pixels = [1800, 3600];
        let tile_size_in_pixels = [600, 600];
        let dataset_upper_right_coord = (-180.0, 90.0).into();
        let dataset_x_pixel_size = 0.1;
        let dataset_y_pixel_size = -0.1;
        let dataset_geo_transform = GeoTransform::new(
            dataset_upper_right_coord,
            dataset_x_pixel_size,
            dataset_y_pixel_size,
        );

        let bounding_box = BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap();

        let origin_split_tileing_strategy = TilingStrategy {
            tile_size_in_pixels: tile_size_in_pixels.into(),
            geo_transform: dataset_geo_transform,
        };

        let time_interval_provider = TimeIntervalInformation {
            start_time: TimeInstance::from_millis(0),
            time_step: TimeStep {
                granularity: TimeGranularity::Seconds,
                step: 1,
            },
        };

        let gdal_params = GdalSourceParameters {
            dataset_id: "modis_ndvi".to_owned(),
            channel: None,
        };

        let dataset_information = JsonDatasetInformation {
            time: time_interval_provider,
            tile: None,
            file_name_with_time_placeholder: "MOD13A2_M_NDVI_2014-01-01.TIFF".into(),
            time_format: "".into(),
            base_path: "../modis_ndvi".into(),
            data_type: RasterDataType::U8,
            geo_transform: dataset_geo_transform,
            grid_shape: global_size_in_pixels.into(),
            spatial_ref: SpatialReference::epsg_4326().into(),
            measurement: Measurement::Unitless,
        };

        let dataset_information_provider = JsonDatasetInformationProvider {
            dataset_information,
            raster_data_root: "../operators/test-data/raster".into(),
        };

        let gdal_source = GdalSourceProcessor {
            dataset_information: dataset_information_provider,
            gdal_params,
            tiling_specification: TilingSpecification {
                origin_coordinate: Coordinate2D::new(0., 0.),
                tile_size_in_pixels: GridShape2D::from(tile_size_in_pixels),
            },
            phantom_data: PhantomData,
        };

        let vres: Vec<Result<RasterTile2D<u8>, Error>> = gdal_source
            .time_tile_iter(
                origin_split_tileing_strategy,
                TimeInterval::new_unchecked(0, 1000),
                bounding_box,
            )
            .map(|(time_interval, tile_information)| {
                gdal_source.load_tile_data(time_interval, tile_information)
            })
            .collect();
        assert_eq!(vres.len(), 6 * 3);
        let upper_left_pixels: Vec<_> = vres
            .into_iter()
            .map(|t| {
                let raster_tile = t.unwrap();
                raster_tile
                    .grid_array
                    .get_at_grid_index([
                        (tile_size_in_pixels[1] / 2) as isize,
                        (tile_size_in_pixels[0] / 2) as isize,
                    ])
                    .unwrap() // pixel
            })
            .collect();

        let ndvi_center_pixel_values = vec![
            19, 255, 255, 43, 76, 17, 255, 255, 255, 145, 255, 255, 255, 255, 255, 255, 255, 255,
        ];

        assert_eq!(upper_left_pixels, ndvi_center_pixel_values);
    }

    #[test]
    fn test_iter_and_load_tile_data_center_split() {
        let global_size_in_pixels = [1800, 3600];
        let tile_size_in_pixels = [600, 600];
        let dataset_upper_right_coord = (-180.0, 90.0).into();
        let dataset_x_pixel_size = 0.1;
        let dataset_y_pixel_size = -0.1;
        let dataset_geo_transform = GeoTransform::new(
            dataset_upper_right_coord,
            dataset_x_pixel_size,
            dataset_y_pixel_size,
        );

        let center_geo_transform = GeoTransform::new(
            Coordinate2D::new(0.0, 0.0),
            dataset_x_pixel_size,
            dataset_y_pixel_size,
        );

        let bounding_box = BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap();

        let origin_split_tileing_strategy = TilingStrategy {
            tile_size_in_pixels: tile_size_in_pixels.into(),
            geo_transform: center_geo_transform,
        };

        let time_interval_provider = TimeIntervalInformation {
            start_time: TimeInstance::from_millis(0),
            time_step: TimeStep {
                granularity: TimeGranularity::Seconds,
                step: 1,
            },
        };

        let gdal_params = GdalSourceParameters {
            dataset_id: "modis_ndvi".to_owned(),
            channel: None,
        };

        let dataset_information = JsonDatasetInformation {
            time: time_interval_provider,
            tile: None,
            file_name_with_time_placeholder: "MOD13A2_M_NDVI_2014-01-01.TIFF".into(),
            time_format: "".into(),
            base_path: "../modis_ndvi".into(),
            data_type: RasterDataType::U8,
            geo_transform: dataset_geo_transform,
            grid_shape: global_size_in_pixels.into(),
            spatial_ref: SpatialReference::epsg_4326().into(),
            measurement: Measurement::Unitless,
        };

        let dataset_information_provider = JsonDatasetInformationProvider {
            dataset_information,
            raster_data_root: "../operators/test-data/raster".into(),
        };

        let gdal_source = GdalSourceProcessor {
            dataset_information: dataset_information_provider,
            gdal_params,
            tiling_specification: TilingSpecification {
                origin_coordinate: Coordinate2D::new(0., 0.),
                tile_size_in_pixels: GridShape2D::from(tile_size_in_pixels),
            },
            phantom_data: PhantomData,
        };

        let vres: Vec<Result<RasterTile2D<u8>, Error>> = gdal_source
            .time_tile_iter(
                origin_split_tileing_strategy,
                TimeInterval::new_unchecked(0, 1000),
                bounding_box,
            )
            .map(|(time_interval, tile_information)| {
                gdal_source.load_tile_data(time_interval, tile_information)
            })
            .collect();
        assert_eq!(vres.len(), 6 * 4);
        let upper_left_pixels: Vec<_> = vres
            .into_iter()
            .map(|t| {
                let raster_tile = t.unwrap();
                raster_tile
                    .grid_array
                    .get_at_grid_index([
                        (tile_size_in_pixels[1] / 2) as isize,
                        (tile_size_in_pixels[0] / 2) as isize,
                    ])
                    .unwrap() // pixel
            })
            .collect();

        let ndvi_center_pixel_values = vec![
            255, 255, 255, 255, 255, 255, 255, 116, 255, 54, 34, 255, 255, 255, 255, 212, 255, 145,
            0, 0, 0, 0, 0, 0,
        ];

        assert_eq!(upper_left_pixels, ndvi_center_pixel_values);
    }

    #[test]
    fn test_iter_and_load_tile_data_bbox() {
        let global_size_in_pixels = [1800, 3600];
        let tile_size_in_pixels = [600, 600];
        let dataset_upper_right_coord = (-180.0, 90.0).into();
        let dataset_x_pixel_size = 0.1;
        let dataset_y_pixel_size = -0.1;
        let dataset_geo_transform = GeoTransform::new(
            dataset_upper_right_coord,
            dataset_x_pixel_size,
            dataset_y_pixel_size,
        );

        let time_interval_provider = TimeIntervalInformation {
            start_time: TimeInstance::from_millis(0),
            time_step: TimeStep {
                granularity: TimeGranularity::Seconds,
                step: 1,
            },
        };

        let gdal_params = GdalSourceParameters {
            dataset_id: "modis_ndvi".to_owned(),
            channel: None,
        };

        let dataset_information = JsonDatasetInformation {
            time: time_interval_provider,
            tile: None,
            file_name_with_time_placeholder: "MOD13A2_M_NDVI_2014-01-01.TIFF".into(),
            time_format: "".into(),
            base_path: "../modis_ndvi".into(),
            data_type: RasterDataType::U8,
            geo_transform: dataset_geo_transform,
            grid_shape: global_size_in_pixels.into(),
            spatial_ref: SpatialReference::epsg_4326().into(),
            measurement: Measurement::Unitless,
        };

        let dataset_information_provider = JsonDatasetInformationProvider {
            dataset_information,
            raster_data_root: "../operators/test-data/raster".into(),
        };

        let gdal_source = GdalSourceProcessor {
            dataset_information: dataset_information_provider,
            gdal_params,
            tiling_specification: TilingSpecification {
                origin_coordinate: Coordinate2D::new(0., 0.),
                tile_size_in_pixels: GridShape2D::from(tile_size_in_pixels),
            },
            phantom_data: PhantomData,
        };

        let query_bbox = BoundingBox2D::new((-30., 0.).into(), (35., 65.).into()).unwrap();

        let origin_split_tileing_strategy = TilingStrategy {
            tile_size_in_pixels: tile_size_in_pixels.into(),
            geo_transform: dataset_geo_transform,
        };

        let vres: Vec<Result<RasterTile2D<u8>, Error>> = gdal_source
            .time_tile_iter(
                origin_split_tileing_strategy,
                TimeInterval::new_unchecked(0, 1000),
                query_bbox,
            )
            .map(|(time_interval, tile_information)| {
                gdal_source.load_tile_data(time_interval, tile_information)
            })
            .collect();
        assert_eq!(vres.len(), 2 * 2);
        let upper_left_pixels: Vec<_> = vres
            .into_iter()
            .map(|t| {
                let raster_tile = t.unwrap();

                raster_tile
                    .grid_array
                    .get_at_grid_index([
                        (tile_size_in_pixels[1] / 2) as isize,
                        (tile_size_in_pixels[0] / 2) as isize,
                    ])
                    .unwrap() // pixel
            })
            .collect();

        let ndvi_center_pixel_values = vec![255, 43, 255, 145];

        assert_eq!(upper_left_pixels, ndvi_center_pixel_values);
    }

    #[tokio::test]
    async fn test_tile_stream_len() {
        let global_size_in_pixels = [1800, 3600];
        let tile_size_in_pixels = [600, 600];
        let dataset_upper_right_coord = (-180.0, 90.0).into();
        let dataset_x_pixel_size = 0.1;
        let dataset_y_pixel_size = -0.1;
        let dataset_geo_transform = GeoTransform::new(
            dataset_upper_right_coord,
            dataset_x_pixel_size,
            dataset_y_pixel_size,
        );

        let t_1 = TimeInstance::from(NaiveDate::from_ymd(2014, 1, 1).and_hms(0, 0, 0));

        let time_interval_provider = TimeIntervalInformation {
            start_time: t_1,
            time_step: TimeStep {
                granularity: TimeGranularity::Months,
                step: 1,
            },
        };

        let gdal_params = GdalSourceParameters {
            dataset_id: "modis_ndvi".to_owned(),
            channel: None,
        };

        let dataset_information = JsonDatasetInformation {
            time: time_interval_provider,
            tile: None,
            file_name_with_time_placeholder: "MOD13A2_M_NDVI_%%%_START_TIME_%%%.TIFF".into(),
            time_format: "%Y-%m-%d".into(),
            base_path: "../modis_ndvi".into(),
            data_type: RasterDataType::U8,
            geo_transform: dataset_geo_transform,
            grid_shape: global_size_in_pixels.into(),
            spatial_ref: SpatialReference::epsg_4326().into(),
            measurement: Measurement::Unitless,
        };

        let dataset_information_provider = JsonDatasetInformationProvider {
            dataset_information,
            raster_data_root: "../operators/test-data/raster".into(),
        };

        let gdal_source: GdalSourceProcessor<JsonDatasetInformationProvider, u8> =
            GdalSourceProcessor {
                dataset_information: dataset_information_provider,
                gdal_params,
                tiling_specification: TilingSpecification {
                    origin_coordinate: Coordinate2D::new(0., 0.),
                    tile_size_in_pixels: GridShape2D::from(tile_size_in_pixels),
                },
                phantom_data: PhantomData,
            };

        let mut stream_data = block_on_stream(gdal_source.tile_stream(
            BoundingBox2D::new((-180.0, -90.0).into(), (180.0, 90.0).into()).unwrap(),
            TimeInterval::new_unchecked(t_1, t_1),
            SpatialResolution::zero_point_one(),
        ));

        let ndvi_center_pixel_values: Vec<u8> = vec![
            255, 255, 255, 255, 255, 255, 255, 116, 255, 54, 34, 255, 255, 255, 255, 212, 255, 145,
            0, 0, 0, 0, 0, 0,
        ];

        for p in ndvi_center_pixel_values {
            let tile = stream_data.next().unwrap().unwrap();

            let cp = tile
                .grid_array
                .get_at_grid_index([
                    (tile_size_in_pixels[1] / 2) as isize,
                    (tile_size_in_pixels[0] / 2) as isize,
                ])
                .unwrap();

            assert_eq!(p, cp);
        }

        assert!(stream_data.next().is_none());
    }

    #[tokio::test]
    async fn test_load_tile_data_async() {
        let global_size_in_pixels = [1800, 3600];
        let tile_size_in_pixels = [600, 600];
        let dataset_upper_right_coord = (-180.0, 90.0).into();
        let dataset_x_pixel_size = 0.1;
        let dataset_y_pixel_size = -0.1;
        let dataset_geo_transform = GeoTransform::new(
            dataset_upper_right_coord,
            dataset_x_pixel_size,
            dataset_y_pixel_size,
        );
        let time_interval = TimeInterval::new_unchecked(1, 2);

        let tile_information = TileInformation::new(
            [0, 0].into(),
            tile_size_in_pixels.into(),
            dataset_geo_transform,
        );

        let time_interval_provider = TimeIntervalInformation {
            start_time: TimeInstance::from_millis(0),
            time_step: TimeStep {
                granularity: TimeGranularity::Seconds,
                step: 1,
            },
        };

        let gdal_params = GdalSourceParameters {
            dataset_id: "modis_ndvi".to_owned(),
            channel: None,
        };

        let dataset_information = JsonDatasetInformation {
            time: time_interval_provider,
            tile: None,
            file_name_with_time_placeholder: "MOD13A2_M_NDVI_2014-01-01.TIFF".into(),
            time_format: "".into(),
            base_path: "../modis_ndvi".into(),
            data_type: RasterDataType::U8,
            geo_transform: dataset_geo_transform,
            grid_shape: global_size_in_pixels.into(),
            spatial_ref: SpatialReference::epsg_4326().into(),
            measurement: Measurement::Unitless,
        };
        let dataset_information_provider = JsonDatasetInformationProvider {
            dataset_information,
            raster_data_root: "../operators/test-data/raster".into(),
        };

        let x_r = GdalSourceProcessor::<_, u8>::load_tile_data_async(
            gdal_params,
            dataset_information_provider,
            time_interval,
            tile_information,
        )
        .await;
        let x = x_r.expect("GDAL Error");

        assert_eq!(x.tile_information(), tile_information);
        assert_eq!(x.time, time_interval);
        let center_pixel = x
            .grid_array
            .get_at_grid_index([
                (tile_size_in_pixels[1] / 2) as isize,
                (tile_size_in_pixels[0] / 2) as isize,
            ])
            .unwrap();
        assert_eq!(center_pixel, 19);
    }

    #[tokio::test]
    async fn no_data() {
        let op: Box<dyn RasterOperator> = serde_json::from_str(
            r#"
        {
            "type": "GdalSource",
            "params": {
            "dataset_id": "modis_ndvi",
            "channel": null
            }
        }"#,
        )
        .unwrap();
        let exe_ctx = MockExecutionContext::default();
        let o = op.initialize(&exe_ctx).unwrap();
        let p = o.query_processor().unwrap().get_u8().unwrap();
        let bbox = BoundingBox2D::new_unchecked((0., 0.).into(), (30., 30.).into());
        let x_query_resolution = bbox.size_x() / f64::from(256);
        let y_query_resolution = bbox.size_y() / f64::from(256);
        let query_ctx = MockQueryContext::default();
        let q = p.query(
            QueryRectangle {
                bbox,
                time_interval: TimeInterval::new_unchecked(0, 0),
                spatial_resolution: SpatialResolution::new_unchecked(
                    x_query_resolution,
                    y_query_resolution,
                ),
            },
            &query_ctx,
        );
        let c: Vec<Result<RasterTile2D<u8>>> = q.collect().await;

        assert_eq!(c.len(), 1);
        assert_eq!(
            c[0].as_ref().unwrap().time,
            TimeInterval::new_unchecked(0, 2_678_400_000) // (1970-01-01T00:00:00, 1970-02-01T00:00:00)
        );
    }
}
