use crate::adapters::SparseTilesFillAdapter;
use crate::engine::{MetaData, OperatorDatasets, QueryProcessor};
use crate::util::gdal::gdal_open_dataset_ex;
use crate::util::input::float_option_with_nan;
use crate::{
    engine::{
        InitializedRasterOperator, RasterOperator, RasterQueryProcessor, RasterResultDescriptor,
        SourceOperator, TypedRasterQueryProcessor,
    },
    error::{self, Error},
    util::Result,
};
use async_trait::async_trait;
use futures::{
    stream::{self, BoxStream, StreamExt},
    Stream,
};
use futures::{Future, TryStreamExt};
use gdal::raster::{GdalType, RasterBand as GdalRasterBand};
use gdal::{DatasetOptions, GdalOpenFlags, Metadata as GdalMetadata};
use geoengine_datatypes::primitives::{
    Coordinate2D, RasterQueryRectangle, SpatialPartition2D, SpatialPartitioned,
};
use geoengine_datatypes::raster::{
    EmptyGrid, GeoTransform, Grid2D, GridShape2D, GridShapeAccess, Pixel, RasterDataType,
    RasterProperties, RasterPropertiesEntry, RasterPropertiesEntryType, RasterPropertiesKey,
    RasterTile2D, TilingStrategy,
};
use geoengine_datatypes::util::test::TestDefault;
use geoengine_datatypes::{dataset::DatasetId, raster::TileInformation};
use geoengine_datatypes::{
    primitives::TimeInterval,
    raster::{
        Grid, GridBlit, GridBoundingBox2D, GridBounds, GridIdx, GridSize, GridSpaceToLinearSpace,
        TilingSpecification,
    },
};
use log::{debug, info};
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::path::PathBuf;
use std::time::Instant;

pub use loading_info::{
    GdalLoadingInfo, GdalLoadingInfoTemporalSlice, GdalLoadingInfoTemporalSliceIterator,
    GdalMetaDataRegular, GdalMetaDataStatic, GdalMetadataNetCdfCf,
};

mod loading_info;

/// Parameters for the GDAL Source Operator
///
/// # Examples
///
/// ```rust
/// use serde_json::{Result, Value};
/// use geoengine_operators::source::{GdalSource, GdalSourceParameters};
/// use geoengine_datatypes::dataset::InternalDatasetId;
/// use geoengine_datatypes::util::Identifier;
/// use std::str::FromStr;
///
/// let json_string = r#"
///     {
///         "type": "GdalSource",
///         "params": {
///             "dataset": {
///                 "type": "internal",
///                 "datasetId": "a626c880-1c41-489b-9e19-9596d129859c"
///             }
///         }
///     }"#;
///
/// let operator: GdalSource = serde_json::from_str(json_string).unwrap();
///
/// assert_eq!(operator, GdalSource {
///     params: GdalSourceParameters {
///         dataset: InternalDatasetId::from_str("a626c880-1c41-489b-9e19-9596d129859c").unwrap().into()
///     },
/// });
/// ```
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct GdalSourceParameters {
    pub dataset: DatasetId,
}

impl OperatorDatasets for GdalSourceParameters {
    fn datasets_collect(&self, datasets: &mut Vec<DatasetId>) {
        datasets.push(self.dataset.clone());
    }
}

type GdalMetaData =
    Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GdalSourceTimePlaceholder {
    pub format: String,
    pub reference: TimeReference,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum TimeReference {
    Start,
    End,
}

/// Parameters for loading data using Gdal
#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GdalDatasetParameters {
    pub file_path: PathBuf,
    pub rasterband_channel: usize,
    pub geo_transform: GdalDatasetGeoTransform,
    pub width: usize,
    pub height: usize,
    pub file_not_found_handling: FileNotFoundHandling,
    #[serde(with = "float_option_with_nan")]
    pub no_data_value: Option<f64>,
    pub properties_mapping: Option<Vec<GdalMetadataMapping>>,
    // Dataset open option as strings, e.g. `vec!["UserPwd=geoengine:pwd".to_owned(), "HttpAuth=BASIC".to_owned()]`
    pub gdal_open_options: Option<Vec<String>>,
    // Configs as key, value pairs that will be set as thread local config options, e.g.
    // `vec!["AWS_REGION".to_owned(), "eu-central-1".to_owned()]` and unset afterwards
    // TODO: validate the config options: only allow specific keys and specific values
    pub gdal_config_options: Option<Vec<(String, String)>>,
}

/// A user friendly representation of Gdal's geo transform. In contrast to [`GeoTransform`] this
/// geo transform allows arbitrary pixel sizes and can thus also represent rasters where the origin is not located
/// in the upper left corner. It should only be used for loading rasters with Gdal and not internally.
#[derive(Copy, Clone, PartialEq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GdalDatasetGeoTransform {
    pub origin_coordinate: Coordinate2D,
    pub x_pixel_size: f64,
    pub y_pixel_size: f64,
}

/// Default implementation for testing purposes where geo transform doesn't matter
impl TestDefault for GdalDatasetGeoTransform {
    fn test_default() -> Self {
        Self {
            origin_coordinate: (0.0, 0.0).into(),
            x_pixel_size: 1.0,
            y_pixel_size: -1.0,
        }
    }
}

/// Direct conversion from `GdalDatasetGeoTransform` to [`GeoTransform`] only works if origin is located in the upper left corner.
impl TryFrom<GdalDatasetGeoTransform> for GeoTransform {
    type Error = Error;

    fn try_from(dataset_geo_transform: GdalDatasetGeoTransform) -> Result<Self> {
        ensure!(
            dataset_geo_transform.x_pixel_size > 0.0 && dataset_geo_transform.y_pixel_size < 0.0,
            error::GeoTransformOrigin
        );

        Ok(GeoTransform::new(
            dataset_geo_transform.origin_coordinate,
            dataset_geo_transform.x_pixel_size,
            dataset_geo_transform.y_pixel_size,
        ))
    }
}

impl From<gdal::GeoTransform> for GdalDatasetGeoTransform {
    fn from(gdal_geo_transform: gdal::GeoTransform) -> Self {
        Self {
            origin_coordinate: (gdal_geo_transform[0], gdal_geo_transform[3]).into(),
            x_pixel_size: gdal_geo_transform[1],
            y_pixel_size: gdal_geo_transform[5],
        }
    }
}

/// Set thread local gdal options and revert them on drop
struct TemporaryGdalThreadLocalConfigOptions {
    original_configs: Vec<(String, Option<String>)>,
}

impl TemporaryGdalThreadLocalConfigOptions {
    /// Set thread local gdal options and revert them on drop
    fn new(configs: &[(String, String)]) -> Result<Self> {
        let mut original_configs = vec![];

        for (key, value) in configs {
            let old = gdal::config::get_thread_local_config_option(key, "").map(|value| {
                if value.is_empty() {
                    None
                } else {
                    Some(value)
                }
            })?;

            // TODO: check if overriding existing config (local & global) is ok for the given key
            gdal::config::set_thread_local_config_option(key, value)?;
            info!("set {}={}", key, value);

            original_configs.push((key.clone(), old));
        }

        Ok(Self { original_configs })
    }
}

impl Drop for TemporaryGdalThreadLocalConfigOptions {
    fn drop(&mut self) {
        for (key, value) in &self.original_configs {
            if let Some(value) = value {
                let _result = gdal::config::set_thread_local_config_option(key, value);
            } else {
                let _result = gdal::config::clear_thread_local_config_option(key);
            }
        }
    }
}

impl SpatialPartitioned for GdalDatasetParameters {
    fn spatial_partition(&self) -> SpatialPartition2D {
        let lower_right_coordinate = self.geo_transform.origin_coordinate
            + Coordinate2D::from((
                self.geo_transform.x_pixel_size * self.width as f64,
                self.geo_transform.y_pixel_size * self.height as f64,
            ));
        SpatialPartition2D::new_unchecked(
            self.geo_transform.origin_coordinate,
            lower_right_coordinate,
        )
    }
}

impl GridShapeAccess for GdalDatasetParameters {
    type ShapeArray = [usize; 2];

    fn grid_shape_array(&self) -> Self::ShapeArray {
        [self.height, self.width]
    }
}

/// How to handle file not found errors
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub enum FileNotFoundHandling {
    NoData, // output tiles filled with nodata
    Error,  // return error tile
}

impl GdalDatasetParameters {
    /// Placeholders are replaced by formatted time value.
    /// E.g. `%my_placeholder%` could be replaced by `2014-04-01` depending on the format and time input.
    pub fn replace_time_placeholders(
        &self,
        placeholders: &HashMap<String, GdalSourceTimePlaceholder>,
        time: TimeInterval,
    ) -> Result<Self> {
        let mut file_path: String = self.file_path.to_string_lossy().into();

        for (placeholder, time_placeholder) in placeholders {
            let time = match time_placeholder.reference {
                TimeReference::Start => time.start(),
                TimeReference::End => time.end(),
            };
            let time_string = time
                .as_naive_date_time()
                .ok_or(Error::TimeInstanceNotDisplayable)?
                .format(&time_placeholder.format)
                .to_string();

            // TODO: use more efficient algorithm for replacing multiple placeholders, e.g. aho-corasick
            file_path = file_path.replace(placeholder, &time_string);
        }

        Ok(Self {
            file_not_found_handling: self.file_not_found_handling,
            file_path: file_path.into(),
            properties_mapping: self.properties_mapping.clone(),
            gdal_open_options: self.gdal_open_options.clone(),
            gdal_config_options: self.gdal_config_options.clone(),
            ..*self
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct TilingInformation {
    pub x_axis_tiles: usize,
    pub y_axis_tiles: usize,
    pub x_axis_tile_size: usize,
    pub y_axis_tile_size: usize,
}

pub struct GdalSourceProcessor<T>
where
    T: Pixel,
{
    pub tiling_specification: TilingSpecification,
    pub meta_data: GdalMetaData,
    pub no_data_value: Option<T>,
}

struct GdalRasterLoader {}

impl GdalRasterLoader {
    ///
    /// A method to async load single tiles from a GDAL dataset.
    ///
    async fn load_tile_data_async<T: Pixel + GdalType>(
        dataset_params: GdalDatasetParameters,
        tile_information: TileInformation,
        tile_time: TimeInterval,
    ) -> Result<RasterTile2D<T>> {
        crate::util::spawn_blocking(move || {
            Self::load_tile_data(&dataset_params, tile_information, tile_time)
        })
        .await
        .context(error::TokioJoin)?
    }

    async fn load_tile_async<T: Pixel + GdalType>(
        dataset_params: Option<GdalDatasetParameters>,
        tile_information: TileInformation,
        tile_time: TimeInterval,
        no_data_value: Option<T>,
    ) -> Result<RasterTile2D<T>> {
        let result_tile = match dataset_params {
            Some(ds)
                if tile_information
                    .spatial_partition()
                    .intersects(&ds.spatial_partition()) =>
            {
                debug!("Loading tile {:?}", &tile_information);
                Self::load_tile_data_async(ds, tile_information, tile_time).await
            }
            Some(_) => {
                debug!("Skipping tile not in query rect {:?}", &tile_information);

                Ok(create_no_data_tile(
                    tile_information,
                    tile_time,
                    no_data_value,
                ))
            }
            _ => {
                debug!(
                    "Skipping tile without GdalDatasetParameters {:?}",
                    &tile_information
                );

                Ok(create_no_data_tile(
                    tile_information,
                    tile_time,
                    no_data_value,
                ))
            }
        };
        result_tile
    }

    ///
    /// A method to load single tiles from a GDAL dataset.
    ///
    fn load_tile_data<T: Pixel + GdalType>(
        dataset_params: &GdalDatasetParameters,
        tile_information: TileInformation,
        tile_time: TimeInterval,
    ) -> Result<RasterTile2D<T>> {
        let start = Instant::now();
        // TODO: handle datasets where origin is not in the upper left corner

        let output_bounds = tile_information.spatial_partition();

        debug!(
            "GridOrEmpty2D<{:?}> requested for {:?}.",
            T::TYPE,
            &output_bounds
        );

        let options = dataset_params
            .gdal_open_options
            .as_ref()
            .map(|o| o.iter().map(String::as_str).collect::<Vec<_>>());

        // reverts the thread local configs on drop
        let _thread_local_configs = dataset_params
            .gdal_config_options
            .as_ref()
            .map(|config_options| TemporaryGdalThreadLocalConfigOptions::new(config_options));

        let dataset_result = gdal_open_dataset_ex(
            &dataset_params.file_path,
            DatasetOptions {
                open_flags: GdalOpenFlags::GDAL_OF_RASTER,
                open_options: options.as_deref(),
                ..DatasetOptions::default()
            },
        );
        let no_data_value = dataset_params.no_data_value.map(T::from_);

        debug!("no_data_value is {:?} ", &no_data_value,);

        if dataset_result.is_err() {
            // TODO: check if Gdal error is actually file not found

            let err_result = match dataset_params.file_not_found_handling {
                FileNotFoundHandling::NoData => Ok(create_no_data_tile(
                    tile_information,
                    tile_time,
                    no_data_value,
                )),
                FileNotFoundHandling::Error => Err(crate::error::Error::CouldNotOpenGdalDataset {
                    file_path: dataset_params.file_path.to_string_lossy().to_string(),
                }),
            };
            let elapsed = start.elapsed();
            debug!(
                "file not found -> returning error = {}, took {:?}",
                err_result.is_err(),
                elapsed
            );
            return err_result;
        };

        let dataset = dataset_result.expect("checked");

        let result_tile = read_raster_tile_with_properties(
            &dataset,
            dataset_params,
            tile_information,
            tile_time,
        )?
        .unwrap_or_else(|| create_no_data_tile(tile_information, tile_time, no_data_value));

        let elapsed = start.elapsed();
        debug!("data loaded -> returning data grid, took {:?}", elapsed);

        Ok(result_tile)
    }

    ///
    /// A stream of futures producing `RasterTile2D` for a single slice in time
    ///
    fn temporal_slice_tile_future_stream<T: Pixel + GdalType>(
        query: RasterQueryRectangle,
        info: GdalLoadingInfoTemporalSlice,
        no_data_value: Option<T>,
        tiling_strategy: TilingStrategy,
    ) -> impl Stream<Item = impl Future<Output = Result<RasterTile2D<T>>>> {
        stream::iter(tiling_strategy.tile_information_iterator(query.spatial_bounds)).map(
            move |tile| {
                GdalRasterLoader::load_tile_async(
                    info.params.clone(),
                    tile,
                    info.time,
                    no_data_value,
                )
            },
        )
    }

    fn loading_info_to_tile_stream<
        T: Pixel + GdalType,
        S: Stream<Item = Result<GdalLoadingInfoTemporalSlice>>,
    >(
        loading_info_stream: S,
        query: RasterQueryRectangle,
        no_data_value: Option<T>,
        tiling_strategy: TilingStrategy,
    ) -> impl Stream<Item = Result<RasterTile2D<T>>> {
        loading_info_stream
            .map_ok(move |info| {
                GdalRasterLoader::temporal_slice_tile_future_stream(
                    query,
                    info,
                    no_data_value,
                    tiling_strategy,
                )
                .map(Result::Ok)
            })
            .try_flatten()
            .try_buffered(16) // TODO: make this configurable
    }
}

impl<T> GdalSourceProcessor<T> where T: gdal::raster::GdalType + Pixel {}

#[async_trait]
impl<P> QueryProcessor for GdalSourceProcessor<P>
where
    P: Pixel + gdal::raster::GdalType,
{
    type Output = RasterTile2D<P>;
    type SpatialBounds = SpatialPartition2D;

    async fn query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        _ctx: &'a dyn crate::engine::QueryContext,
    ) -> Result<BoxStream<Result<Self::Output>>> {
        let start = Instant::now();
        debug!(
            "Querying GdalSourceProcessor<{:?}> with: {:?}.",
            P::TYPE,
            &query
        );

        let meta_data = self.meta_data.loading_info(query).await?;

        debug!(
            "GdalSourceProcessor<{:?}> meta data loaded, took {:?}.",
            P::TYPE,
            start.elapsed()
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

        // TODO: what to do if loading info is empty?
        let source_stream = stream::iter(meta_data.info);

        let source_stream = GdalRasterLoader::loading_info_to_tile_stream(
            source_stream,
            query,
            self.no_data_value,
            tiling_strategy,
        );

        // use SparseTilesFillAdapter to fill all the gaps
        let filled_stream = SparseTilesFillAdapter::new(
            source_stream,
            tiling_strategy.tile_grid_box(query.spatial_partition()),
            tiling_strategy.geo_transform,
            tiling_strategy.tile_size_in_pixels,
            self.no_data_value.unwrap_or_else(P::zero),
        );

        Ok(filled_stream.boxed())
    }
}

pub type GdalSource = SourceOperator<GdalSourceParameters>;

#[typetag::serde]
#[async_trait]
impl RasterOperator for GdalSource {
    async fn initialize(
        self: Box<Self>,
        context: &dyn crate::engine::ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let meta_data: GdalMetaData = context.meta_data(&self.params.dataset).await?;

        debug!("Initializing GdalSource for {:?}.", &self.params.dataset);

        Ok(InitializedGdalSourceOperator {
            result_descriptor: meta_data.result_descriptor().await?,
            meta_data,
            tiling_specification: context.tiling_specification(),
        }
        .boxed())
    }
}

pub struct InitializedGdalSourceOperator {
    pub meta_data: GdalMetaData,
    pub result_descriptor: RasterResultDescriptor,
    pub tiling_specification: TilingSpecification,
}

impl InitializedRasterOperator for InitializedGdalSourceOperator {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        Ok(match self.result_descriptor().data_type {
            RasterDataType::U8 => TypedRasterQueryProcessor::U8(
                GdalSourceProcessor {
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    no_data_value: self.result_descriptor.no_data_value_as_(),
                }
                .boxed(),
            ),
            RasterDataType::U16 => TypedRasterQueryProcessor::U16(
                GdalSourceProcessor {
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    no_data_value: self.result_descriptor.no_data_value_as_(),
                }
                .boxed(),
            ),
            RasterDataType::U32 => TypedRasterQueryProcessor::U32(
                GdalSourceProcessor {
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    no_data_value: self.result_descriptor.no_data_value_as_(),
                }
                .boxed(),
            ),
            RasterDataType::U64 => unimplemented!("implement U64 type"), // TypedRasterQueryProcessor::U64(self.create_processor()),
            RasterDataType::I8 => unimplemented!("I8 type is not supported"),
            RasterDataType::I16 => TypedRasterQueryProcessor::I16(
                GdalSourceProcessor {
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    no_data_value: self.result_descriptor.no_data_value_as_(),
                }
                .boxed(),
            ),
            RasterDataType::I32 => TypedRasterQueryProcessor::I32(
                GdalSourceProcessor {
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    no_data_value: self.result_descriptor.no_data_value_as_(),
                }
                .boxed(),
            ),
            RasterDataType::I64 => unimplemented!("implement I64 type"), // TypedRasterQueryProcessor::I64(self.create_processor()),
            RasterDataType::F32 => TypedRasterQueryProcessor::F32(
                GdalSourceProcessor {
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    no_data_value: self.result_descriptor.no_data_value_as_(),
                }
                .boxed(),
            ),
            RasterDataType::F64 => TypedRasterQueryProcessor::F64(
                GdalSourceProcessor {
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    no_data_value: self.result_descriptor.no_data_value_as_(),
                }
                .boxed(),
            ),
        })
    }
}

/// This method reads the data for a single grid with a specified size from the GDAL dataset.
/// It fails if the tile is not within the dataset.
fn read_grid_from_raster<
    T,
    D: GridSize<ShapeArray = [usize; 2]> + GridSpaceToLinearSpace<IndexArray = [isize; 2]>,
>(
    rasterband: &GdalRasterBand,
    dataset_grid_box: &GridBoundingBox2D,
    tile_grid: D,
    no_data_value: Option<T>,
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
        None,                             // sampling mode
    )?;
    Grid::new(tile_grid, buffer.data, no_data_value).map_err(Into::into)
}

/// This method reads the data for a single grid with a specified size from the GDAL dataset.
/// If the tile overlaps the borders of the dataset only the data in the dataset bounds is read.
/// The data read from the dataset is clipped into a grid with the requested size filled  with the `no_data_value`.
fn read_partial_grid_from_raster<T>(
    rasterband: &GdalRasterBand,
    dataset_grid_box: &GridBoundingBox2D,
    tile_grid_bounds: GridBoundingBox2D,
    tile_grid: GridShape2D,
    no_data_value: Option<T>,
) -> Result<Grid2D<T>>
where
    T: Pixel + GdalType,
{
    let dataset_raster = read_grid_from_raster(
        rasterband,
        dataset_grid_box,
        tile_grid_bounds,
        no_data_value,
    )?;

    let mut tile_raster = Grid2D::new_filled(
        tile_grid,
        no_data_value.unwrap_or_else(T::zero),
        no_data_value,
    );
    tile_raster.grid_blit_from(dataset_raster);
    Ok(tile_raster)
}

/// This method reads the data for a single tile with a specified size from the GDAL dataset.
/// It handles conversion to grid coordinates.
/// If the tile is inside the dataset it uses the `read_grid_from_raster` method.
/// f the tile overlaps the borders of the dataset it uses the `read_partial_grid_from_raster` method.  
fn read_grid_and_handle_edges<T>(
    tile_info: TileInformation,
    rasterband: &GdalRasterBand,
    dataset_bounds: SpatialPartition2D,
    dataset_geo_transform: GeoTransform,

    no_data_value: Option<T>,
) -> Result<Option<Grid2D<T>>>
where
    T: Pixel + GdalType,
{
    let output_bounds = tile_info.spatial_partition();
    let dataset_intersects_tile = dataset_bounds.intersection(&output_bounds);

    let dataset_intersection_area = match dataset_intersects_tile {
        Some(i) => i,
        None => {
            return Ok(None);
        }
    };

    let output_shape = tile_info.tile_size_in_pixels();
    let output_geo_transform = tile_info.tile_geo_transform();
    let dataset_grid_bounds =
        dataset_geo_transform.spatial_to_grid_bounds(&dataset_intersection_area);

    let result_grid = if dataset_intersection_area == output_bounds {
        read_grid_from_raster(
            rasterband,
            &dataset_grid_bounds,
            output_shape,
            no_data_value,
        )?
    } else {
        let tile_grid_bounds =
            output_geo_transform.spatial_to_grid_bounds(&dataset_intersection_area);
        read_partial_grid_from_raster(
            rasterband,
            &dataset_grid_bounds,
            tile_grid_bounds,
            output_shape,
            no_data_value,
        )?
    };

    Ok(Some(result_grid))
}

/// This method reads the data for a single tile with a specified size from the GDAL dataset and adds the requested metadata as properties to the tile.
fn read_raster_tile_with_properties<T: Pixel + gdal::raster::GdalType>(
    dataset: &gdal::Dataset,
    dataset_params: &GdalDatasetParameters,
    tile_info: TileInformation,
    tile_time: TimeInterval,
) -> Result<Option<RasterTile2D<T>>> {
    let rasterband = dataset.rasterband(dataset_params.rasterband_channel as isize)?;

    let mut properties = RasterProperties::default();

    if let Some(properties_mapping) = dataset_params.properties_mapping.as_ref() {
        properties_from_gdal(&mut properties, dataset, properties_mapping);
        properties_from_gdal(&mut properties, &rasterband, properties_mapping);
        properties_from_band(&mut properties, &rasterband);
    }

    let no_data_value = dataset_params.no_data_value.map(T::from_);
    let dataset_geo_transform = dataset_params.geo_transform.try_into()?;
    let dataset_bounds = dataset_params.spatial_partition();

    let result_grid = read_grid_and_handle_edges(
        tile_info,
        &rasterband,
        dataset_bounds,
        dataset_geo_transform,
        no_data_value,
    )?;

    Ok(result_grid.map(|grid| {
        RasterTile2D::new_with_tile_info_and_properties(
            tile_time,
            tile_info,
            grid.into(),
            properties,
        )
    }))
}

fn create_no_data_tile<T: Pixel>(
    tile_info: TileInformation,
    tile_time: TimeInterval,
    no_data_value: Option<T>,
) -> RasterTile2D<T> {
    if let Some(no_data) = no_data_value {
        RasterTile2D::new_with_tile_info_and_properties(
            tile_time,
            tile_info,
            EmptyGrid::new(tile_info.tile_size_in_pixels, no_data).into(),
            RasterProperties::default(),
        )
    } else {
        RasterTile2D::new_with_tile_info_and_properties(
            tile_time,
            tile_info,
            Grid2D::new_filled(tile_info.tile_size_in_pixels, T::zero(), None).into(),
            RasterProperties::default(),
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GdalMetadataMapping {
    pub source_key: RasterPropertiesKey,
    pub target_key: RasterPropertiesKey,
    pub target_type: RasterPropertiesEntryType,
}

impl GdalMetadataMapping {
    pub fn identity(
        key: RasterPropertiesKey,
        target_type: RasterPropertiesEntryType,
    ) -> GdalMetadataMapping {
        GdalMetadataMapping {
            source_key: key.clone(),
            target_key: key,
            target_type,
        }
    }
}

fn properties_from_gdal<'a, I, M>(
    properties: &mut RasterProperties,
    gdal_dataset: &M,
    properties_mapping: I,
) where
    I: IntoIterator<Item = &'a GdalMetadataMapping>,
    M: GdalMetadata,
{
    let mapping_iter = properties_mapping.into_iter();

    for m in mapping_iter {
        let data = if let Some(domain) = &m.source_key.domain {
            gdal_dataset.metadata_item(&m.source_key.key, domain)
        } else {
            gdal_dataset.metadata_item(&m.source_key.key, "")
        };

        if let Some(d) = data {
            let entry = match m.target_type {
                RasterPropertiesEntryType::Number => d.parse::<f64>().map_or_else(
                    |_| RasterPropertiesEntry::String(d),
                    RasterPropertiesEntry::Number,
                ),
                RasterPropertiesEntryType::String => RasterPropertiesEntry::String(d),
            };

            debug!(
                "gdal properties key \"{:?}\" => target key \"{:?}\". Value: {:?} ",
                &m.source_key, &m.target_key, &entry
            );

            properties
                .properties_map
                .insert(m.target_key.clone(), entry);
        }
    }
}

fn properties_from_band(properties: &mut RasterProperties, gdal_dataset: &GdalRasterBand) {
    if let Some(scale) = gdal_dataset.metadata_item("scale", "") {
        properties.scale = scale.parse::<f64>().ok();
    };

    if let Some(offset) = gdal_dataset.metadata_item("offset", "") {
        properties.offset = offset.parse::<f64>().ok();
    };

    if let Some(band_name) = gdal_dataset.metadata_item("band_name", "") {
        properties.band_name = Some(band_name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{MockExecutionContext, MockQueryContext};
    use crate::test_data;
    use crate::util::gdal::add_ndvi_dataset;
    use crate::util::Result;

    use geoengine_datatypes::hashmap;
    use geoengine_datatypes::primitives::{AxisAlignedRectangle, SpatialPartition2D, TimeInstance};
    use geoengine_datatypes::raster::{EmptyGrid2D, GridIdx2D};
    use geoengine_datatypes::raster::{TileInformation, TilingStrategy};
    use geoengine_datatypes::{primitives::SpatialResolution, raster::GridShape2D};

    async fn query_gdal_source(
        exe_ctx: &mut MockExecutionContext,
        query_ctx: &MockQueryContext,
        id: DatasetId,
        output_shape: GridShape2D,
        output_bounds: SpatialPartition2D,
        time_interval: TimeInterval,
    ) -> Vec<Result<RasterTile2D<u8>>> {
        let op = GdalSource {
            params: GdalSourceParameters {
                dataset: id.clone(),
            },
        }
        .boxed();

        let x_query_resolution = output_bounds.size_x() / output_shape.axis_size_x() as f64;
        let y_query_resolution = output_bounds.size_y() / output_shape.axis_size_y() as f64;
        let spatial_resolution =
            SpatialResolution::new_unchecked(x_query_resolution, y_query_resolution);

        let o = op.initialize(exe_ctx).await.unwrap();

        o.query_processor()
            .unwrap()
            .get_u8()
            .unwrap()
            .raster_query(
                RasterQueryRectangle {
                    spatial_bounds: output_bounds,
                    time_interval,
                    spatial_resolution,
                },
                query_ctx,
            )
            .await
            .unwrap()
            .collect()
            .await
    }

    fn load_ndvi_jan_2014(
        output_shape: GridShape2D,
        output_bounds: SpatialPartition2D,
    ) -> Result<RasterTile2D<u8>> {
        GdalRasterLoader::load_tile_data::<u8>(
            &GdalDatasetParameters {
                file_path: test_data!("raster/modis_ndvi/MOD13A2_M_NDVI_2014-01-01.TIFF").into(),
                rasterband_channel: 1,
                geo_transform: GdalDatasetGeoTransform {
                    origin_coordinate: (-180., 90.).into(),
                    x_pixel_size: 0.1,
                    y_pixel_size: -0.1,
                },
                width: 3600,
                height: 1800,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value: Some(0.),
                properties_mapping: Some(vec![
                    GdalMetadataMapping {
                        source_key: RasterPropertiesKey {
                            domain: None,
                            key: "AREA_OR_POINT".to_string(),
                        },
                        target_type: RasterPropertiesEntryType::String,
                        target_key: RasterPropertiesKey {
                            domain: None,
                            key: "AREA_OR_POINT".to_string(),
                        },
                    },
                    GdalMetadataMapping {
                        source_key: RasterPropertiesKey {
                            domain: Some("IMAGE_STRUCTURE".to_string()),
                            key: "COMPRESSION".to_string(),
                        },
                        target_type: RasterPropertiesEntryType::String,
                        target_key: RasterPropertiesKey {
                            domain: Some("IMAGE_STRUCTURE_INFO".to_string()),
                            key: "COMPRESSION".to_string(),
                        },
                    },
                ]),
                gdal_open_options: None,
                gdal_config_options: None,
            },
            TileInformation::with_partition_and_shape(output_bounds, output_shape),
            TimeInterval::default(),
        )
    }

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

        let partition = SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap();

        let origin_split_tileing_strategy = TilingStrategy {
            tile_size_in_pixels: tile_size_in_pixels.into(),
            geo_transform: dataset_geo_transform,
        };

        assert_eq!(
            origin_split_tileing_strategy.upper_left_pixel_idx(partition),
            [0, 0].into()
        );
        assert_eq!(
            origin_split_tileing_strategy.lower_right_pixel_idx(partition),
            [1800 - 1, 3600 - 1].into()
        );

        let tile_grid = origin_split_tileing_strategy.tile_grid_box(partition);
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

        let partition = SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap();

        let origin_split_tileing_strategy = TilingStrategy {
            tile_size_in_pixels: tile_size_in_pixels.into(),
            geo_transform: central_geo_transform,
        };

        assert_eq!(
            origin_split_tileing_strategy.upper_left_pixel_idx(partition),
            [-900, -1800].into()
        );
        assert_eq!(
            origin_split_tileing_strategy.lower_right_pixel_idx(partition),
            [1800 / 2 - 1, 3600 / 2 - 1].into()
        );

        let tile_grid = origin_split_tileing_strategy.tile_grid_box(partition);
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

        let partition = SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap();

        let origin_split_tileing_strategy = TilingStrategy {
            tile_size_in_pixels: tile_size_in_pixels.into(),
            geo_transform: central_geo_transform,
        };

        let vres: Vec<GridIdx2D> = origin_split_tileing_strategy
            .tile_idx_iterator(partition)
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

        let partition = SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap();

        let origin_split_tileing_strategy = TilingStrategy {
            tile_size_in_pixels: tile_size_in_pixels.into(),
            geo_transform: central_geo_transform,
        };

        let vres: Vec<TileInformation> = origin_split_tileing_strategy
            .tile_information_iterator(partition)
            .collect();
        assert_eq!(vres.len(), 4 * 6);
        assert_eq!(
            vres[0],
            TileInformation::new(
                [-2, -3].into(),
                tile_size_in_pixels.into(),
                central_geo_transform,
            )
        );
        assert_eq!(
            vres[1],
            TileInformation::new(
                [-2, -2].into(),
                tile_size_in_pixels.into(),
                central_geo_transform,
            )
        );
        assert_eq!(
            vres[12],
            TileInformation::new(
                [0, -3].into(),
                tile_size_in_pixels.into(),
                central_geo_transform,
            )
        );
        assert_eq!(
            vres[23],
            TileInformation::new(
                [1, 2].into(),
                tile_size_in_pixels.into(),
                central_geo_transform,
            )
        );
    }

    #[test]
    fn replace_time_placeholder() {
        let params = GdalDatasetParameters {
            file_path: "/foo/bar_%TIME%.tiff".into(),
            rasterband_channel: 0,
            geo_transform: TestDefault::test_default(),
            width: 360,
            height: 180,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: Some(0.),
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: None,
        };
        let replaced = params
            .replace_time_placeholders(
                &hashmap! {
                    "%TIME%".to_string() => GdalSourceTimePlaceholder {
                        format: "%f".to_string(),
                        reference: TimeReference::Start,
                    },
                },
                TimeInterval::new_instant(TimeInstance::from_millis_unchecked(22)).unwrap(),
            )
            .unwrap();
        assert_eq!(
            replaced.file_path.to_string_lossy(),
            "/foo/bar_022000000.tiff".to_string()
        );
        assert_eq!(params.rasterband_channel, replaced.rasterband_channel);
        assert_eq!(params.geo_transform, replaced.geo_transform);
        assert_eq!(params.width, replaced.width);
        assert_eq!(params.height, replaced.height);
        assert_eq!(
            params.file_not_found_handling,
            replaced.file_not_found_handling
        );
        assert_eq!(params.no_data_value, replaced.no_data_value);
    }

    #[test]
    fn test_load_tile_data() {
        let output_shape: GridShape2D = [8, 8].into();
        let output_bounds =
            SpatialPartition2D::new_unchecked((-180., 90.).into(), (180., -90.).into());

        let RasterTile2D {
            global_geo_transform: _,
            grid_array: grid,
            tile_position: _,
            time: _,
            properties,
        } = load_ndvi_jan_2014(output_shape, output_bounds).unwrap();

        assert!(!grid.is_empty());

        let grid = grid.into_materialized_grid();

        assert_eq!(grid.data.len(), 64);
        assert_eq!(
            grid.data,
            &[
                255, 255, 255, 255, 255, 255, 255, 255, 255, 75, 37, 255, 44, 34, 39, 32, 255, 86,
                255, 255, 255, 30, 96, 255, 255, 255, 255, 255, 90, 255, 255, 255, 255, 255, 202,
                255, 193, 255, 255, 255, 255, 255, 89, 255, 111, 255, 255, 255, 255, 255, 255, 255,
                255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255
            ]
        );
        assert_eq!(grid.no_data_value, Some(0));

        assert!(properties.scale.is_none());
        assert!(properties.offset.is_none());
        assert_eq!(
            properties.properties_map.get(&RasterPropertiesKey {
                key: "AREA_OR_POINT".to_string(),
                domain: None,
            }),
            Some(&RasterPropertiesEntry::String("Area".to_string()))
        );
        assert_eq!(
            properties.properties_map.get(&RasterPropertiesKey {
                domain: Some("IMAGE_STRUCTURE_INFO".to_string()),
                key: "COMPRESSION".to_string(),
            }),
            Some(&RasterPropertiesEntry::String("LZW".to_string()))
        );
    }

    #[test]
    fn test_load_tile_data_overlaps_dataset_bounds() {
        let output_shape: GridShape2D = [8, 8].into();
        // shift world bbox one pixel up and to the left
        let (x_size, y_size) = (45., 22.5);
        let output_bounds = SpatialPartition2D::new_unchecked(
            (-180. - x_size, 90. + y_size).into(),
            (180. - x_size, -90. + y_size).into(),
        );

        let RasterTile2D {
            global_geo_transform: _,
            grid_array: grid,
            tile_position: _,
            time: _,
            properties: _,
        } = load_ndvi_jan_2014(output_shape, output_bounds).unwrap();

        assert!(!grid.is_empty());

        let x = grid.into_materialized_grid();

        assert_eq!(x.data.len(), 64);
        assert_eq!(
            x.data,
            &[
                0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 0, 255, 75, 37, 255,
                44, 34, 39, 0, 255, 86, 255, 255, 255, 30, 96, 0, 255, 255, 255, 255, 90, 255, 255,
                0, 255, 255, 202, 255, 193, 255, 255, 0, 255, 255, 89, 255, 111, 255, 255, 0, 255,
                255, 255, 255, 255, 255, 255
            ]
        );
    }

    #[test]
    fn test_load_tile_data_is_inside_single_pixel() {
        let output_shape: GridShape2D = [8, 8].into();
        // shift world bbox one pixel up and to the left
        let (x_size, y_size) = (0.000_000_000_01, 0.000_000_000_01);
        let output_bounds = SpatialPartition2D::new(
            (-116.22222, 66.66666).into(),
            (-116.22222 + x_size, 66.66666 - y_size).into(),
        )
        .unwrap();

        let RasterTile2D {
            global_geo_transform: _,
            grid_array: grid,
            tile_position: _,
            time: _,
            properties: _,
        } = load_ndvi_jan_2014(output_shape, output_bounds).unwrap();

        assert!(!grid.is_empty());

        let x = grid.into_materialized_grid();

        assert_eq!(x.data.len(), 64);
        assert_eq!(x.data, &[1; 64]);
    }

    #[tokio::test]
    async fn test_query_single_time_slice() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = MockQueryContext::test_default();
        let id = add_ndvi_dataset(&mut exe_ctx);

        let output_shape: GridShape2D = [256, 256].into();
        let output_bounds =
            SpatialPartition2D::new_unchecked((-180., 90.).into(), (180., -90.).into());
        let time_interval = TimeInterval::new_unchecked(1_388_534_400_000, 1_388_534_400_001); // 2014-01-01

        let c = query_gdal_source(
            &mut exe_ctx,
            &query_ctx,
            id,
            output_shape,
            output_bounds,
            time_interval,
        )
        .await;
        let c: Vec<RasterTile2D<u8>> = c.into_iter().map(Result::unwrap).collect();

        assert_eq!(c.len(), 4);

        assert_eq!(
            c[0].time,
            TimeInterval::new_unchecked(1_388_534_400_000, 1_391_212_800_000)
        );

        assert_eq!(
            c[0].tile_information().global_tile_position(),
            [-1, -1].into()
        );

        assert_eq!(
            c[1].tile_information().global_tile_position(),
            [-1, 0].into()
        );

        assert_eq!(
            c[2].tile_information().global_tile_position(),
            [0, -1].into()
        );

        assert_eq!(
            c[3].tile_information().global_tile_position(),
            [0, 0].into()
        );
    }

    #[tokio::test]
    async fn test_query_multi_time_slices() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = MockQueryContext::test_default();
        let id = add_ndvi_dataset(&mut exe_ctx);

        let output_shape: GridShape2D = [256, 256].into();
        let output_bounds =
            SpatialPartition2D::new_unchecked((-180., 90.).into(), (180., -90.).into());
        let time_interval = TimeInterval::new_unchecked(1_388_534_400_000, 1_393_632_000_000); // 2014-01-01 - 2014-03-01

        let c = query_gdal_source(
            &mut exe_ctx,
            &query_ctx,
            id,
            output_shape,
            output_bounds,
            time_interval,
        )
        .await;
        let c: Vec<RasterTile2D<u8>> = c.into_iter().map(Result::unwrap).collect();

        assert_eq!(c.len(), 8);

        assert_eq!(
            c[0].time,
            TimeInterval::new_unchecked(1_388_534_400_000, 1_391_212_800_000)
        );

        assert_eq!(
            c[5].time,
            TimeInterval::new_unchecked(1_391_212_800_000, 1_393_632_000_000)
        );
    }

    #[tokio::test]
    async fn test_nodata() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = MockQueryContext::test_default();
        let id = add_ndvi_dataset(&mut exe_ctx);

        let output_shape: GridShape2D = [256, 256].into();
        let output_bounds =
            SpatialPartition2D::new_unchecked((-180., 90.).into(), (180., -90.).into());
        let time_interval = TimeInterval::new_unchecked(1_385_856_000_000, 1_388_534_400_000); // 2013-12-01 - 2014-01-01

        let c = query_gdal_source(
            &mut exe_ctx,
            &query_ctx,
            id,
            output_shape,
            output_bounds,
            time_interval,
        )
        .await;
        let c: Vec<RasterTile2D<u8>> = c.into_iter().map(Result::unwrap).collect();

        assert_eq!(c.len(), 4);

        let tile_1 = &c[0];

        assert_eq!(
            tile_1.time,
            TimeInterval::new_unchecked(1_385_856_000_000, 1_388_534_400_000)
        );

        assert!(tile_1.is_empty());
    }

    #[tokio::test]
    async fn timestep_without_params() {
        let output_bounds =
            SpatialPartition2D::new_unchecked((-90., 90.).into(), (90., -90.).into());
        let output_shape: GridShape2D = [256, 256].into();

        let tile_info = TileInformation::with_partition_and_shape(output_bounds, output_shape);
        let time_interval = TimeInterval::new_unchecked(1_388_534_400_000, 1_391_212_800_000); // 2014-01-01 - 2014-01-15
        let params = None;

        let tile =
            GdalRasterLoader::load_tile_async::<f64>(params, tile_info, time_interval, Some(1.))
                .await;

        assert!(tile.is_ok());

        let expected = RasterTile2D::<f64>::new_with_tile_info(
            time_interval,
            tile_info,
            EmptyGrid2D::new(output_shape, 1.).into(),
        );

        assert_eq!(tile.unwrap(), expected);
    }

    #[test]
    fn it_reverts_config_options() {
        let config_options = vec![("foo".to_owned(), "bar".to_owned())];

        {
            let _config =
                TemporaryGdalThreadLocalConfigOptions::new(config_options.as_slice()).unwrap();

            assert_eq!(
                gdal::config::get_config_option("foo", "default").unwrap(),
                "bar".to_owned()
            );
        }

        assert_eq!(
            gdal::config::get_config_option("foo", "").unwrap(),
            "".to_owned()
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn deserialize_dataset_parameters() {
        let dataset_parameters = GdalDatasetParameters {
            file_path: "path-to-data.tiff".into(),
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: (-180., 90.).into(),
                x_pixel_size: 0.1,
                y_pixel_size: -0.1,
            },
            width: 3600,
            height: 1800,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: Some(f64::NAN),
            properties_mapping: Some(vec![
                GdalMetadataMapping {
                    source_key: RasterPropertiesKey {
                        domain: None,
                        key: "AREA_OR_POINT".to_string(),
                    },
                    target_type: RasterPropertiesEntryType::String,
                    target_key: RasterPropertiesKey {
                        domain: None,
                        key: "AREA_OR_POINT".to_string(),
                    },
                },
                GdalMetadataMapping {
                    source_key: RasterPropertiesKey {
                        domain: Some("IMAGE_STRUCTURE".to_string()),
                        key: "COMPRESSION".to_string(),
                    },
                    target_type: RasterPropertiesEntryType::String,
                    target_key: RasterPropertiesKey {
                        domain: Some("IMAGE_STRUCTURE_INFO".to_string()),
                        key: "COMPRESSION".to_string(),
                    },
                },
            ]),
            gdal_open_options: None,
            gdal_config_options: None,
        };

        let dataset_parameters_json = serde_json::to_value(&dataset_parameters).unwrap();

        assert_eq!(
            dataset_parameters_json,
            serde_json::json!({
                "filePath": "path-to-data.tiff",
                "rasterbandChannel": 1,
                "geoTransform": {
                    "originCoordinate": {
                        "x": -180.,
                        "y": 90.
                    },
                    "xPixelSize": 0.1,
                    "yPixelSize": -0.1
                },
                "width": 3600,
                "height": 1800,
                "fileNotFoundHandling": "NoData",
                "noDataValue": "nan",
                "propertiesMapping": [{
                        "source_key": {
                            "domain": null,
                            "key": "AREA_OR_POINT"
                        },
                        "target_key": {
                            "domain": null,
                            "key": "AREA_OR_POINT"
                        },
                        "target_type": "String"
                    },
                    {
                        "source_key": {
                            "domain": "IMAGE_STRUCTURE",
                            "key": "COMPRESSION"
                        },
                        "target_key": {
                            "domain": "IMAGE_STRUCTURE_INFO",
                            "key": "COMPRESSION"
                        },
                        "target_type": "String"
                    }
                ],
                "gdalOpenOptions": null,
                "gdalConfigOptions": null
            })
        );

        let deserialized_parameters =
            serde_json::from_value::<GdalDatasetParameters>(dataset_parameters_json).unwrap();

        // since there is NaN in the data, we can't check for equality on the whole object

        assert_eq!(
            deserialized_parameters.file_path,
            dataset_parameters.file_path,
        );
        assert_eq!(
            deserialized_parameters.rasterband_channel,
            dataset_parameters.rasterband_channel,
        );
        assert_eq!(
            deserialized_parameters.geo_transform,
            dataset_parameters.geo_transform,
        );
        assert_eq!(deserialized_parameters.width, dataset_parameters.width);
        assert_eq!(deserialized_parameters.height, dataset_parameters.height);
        assert_eq!(
            deserialized_parameters.file_not_found_handling,
            dataset_parameters.file_not_found_handling,
        );
        assert!(
            deserialized_parameters.no_data_value.unwrap().is_nan()
                && dataset_parameters.no_data_value.unwrap().is_nan()
        );
        assert_eq!(
            deserialized_parameters.properties_mapping,
            dataset_parameters.properties_mapping,
        );
        assert_eq!(
            deserialized_parameters.gdal_open_options,
            dataset_parameters.gdal_open_options,
        );
        assert_eq!(
            deserialized_parameters.gdal_config_options,
            dataset_parameters.gdal_config_options,
        );
    }
}
