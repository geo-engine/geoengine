use crate::engine::{MetaData, OperatorDatasets, QueryProcessor, RasterQueryRectangle};
use crate::{
    engine::{
        InitializedRasterOperator, RasterOperator, RasterQueryProcessor, RasterResultDescriptor,
        SourceOperator, TypedRasterQueryProcessor,
    },
    error::{self, Error},
    util::Result,
};
use futures::{
    stream::{self, BoxStream, StreamExt},
    Stream,
};

use async_trait::async_trait;
use gdal::raster::{GdalType, RasterBand as GdalRasterBand};
use gdal::{Dataset as GdalDataset, DatasetOptions, Metadata as GdalMetadata};
use geoengine_datatypes::primitives::{Coordinate2D, SpatialPartition2D, SpatialPartitioned};
use geoengine_datatypes::raster::{
    EmptyGrid, GeoTransform, Grid2D, GridOrEmpty2D, GridShapeAccess, Pixel, RasterDataType,
    RasterProperties, RasterPropertiesEntry, RasterPropertiesEntryType, RasterPropertiesKey,
    RasterTile2D,
};
use geoengine_datatypes::{dataset::DatasetId, raster::TileInformation};
use geoengine_datatypes::{
    primitives::{TimeInstance, TimeInterval, TimeStep, TimeStepIter},
    raster::{
        Grid, GridBlit, GridBoundingBox2D, GridBounds, GridIdx, GridSize, GridSpaceToLinearSpace,
        TilingSpecification,
    },
};
use log::debug;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::{marker::PhantomData, path::PathBuf};
//use gdal::metadata::Metadata; // TODO: handle metadata

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

#[derive(Debug, Clone)]
pub struct GdalLoadingInfo {
    /// partitions of dataset sorted by time
    pub info: GdalLoadingInfoPartIterator,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum GdalLoadingInfoPartIterator {
    Static {
        parts: std::vec::IntoIter<GdalLoadingInfoPart>,
    },
    Dynamic {
        time_step_iter: TimeStepIter,
        params: GdalDatasetParameters,
        placeholder: String,
        time_format: String,
        step: TimeStep,
        max_t2: TimeInstance,
    },
}

impl Iterator for GdalLoadingInfoPartIterator {
    type Item = Result<GdalLoadingInfoPart>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            GdalLoadingInfoPartIterator::Static { parts } => parts.next().map(Result::Ok),
            GdalLoadingInfoPartIterator::Dynamic {
                time_step_iter,
                params,
                placeholder,
                time_format,
                step,
                max_t2,
            } => {
                let t1 = time_step_iter.next()?;

                let t2 = t1 + *step;
                let t2 = t2.unwrap_or(*max_t2);

                let time_interval = TimeInterval::new_unchecked(t1, t2);

                let loading_info_part = params
                    .replace_time_placeholder(placeholder, time_format, time_interval.start())
                    .map(|loading_info_part_params| GdalLoadingInfoPart {
                        time: time_interval,
                        params: loading_info_part_params,
                    });

                Some(loading_info_part)
            }
        }
    }
}

/// one temporal slice of the dataset that requires reading from exactly one Gdal dataset
#[derive(Debug, Clone, PartialEq)]
pub struct GdalLoadingInfoPart {
    pub time: TimeInterval,
    pub params: GdalDatasetParameters,
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GdalDatasetParameters {
    pub file_path: PathBuf,
    pub rasterband_channel: usize,
    #[serde(deserialize_with = "GeoTransform::deserialize_with_check")]
    pub geo_transform: GeoTransform,
    pub width: usize,
    pub height: usize,
    pub file_not_found_handling: FileNotFoundHandling,
    pub no_data_value: Option<f64>,
    pub properties_mapping: Option<Vec<GdalMetadataMapping>>,
    pub gdal_open_options: Option<Vec<String>>,
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
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum FileNotFoundHandling {
    NoData, // output tiles filled with nodata
    Error,  // return error tile
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GdalMetaDataStatic {
    pub time: Option<TimeInterval>,
    pub params: GdalDatasetParameters,
    pub result_descriptor: RasterResultDescriptor,
}

#[async_trait]
impl MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for GdalMetaDataStatic
{
    async fn loading_info(&self, _query: RasterQueryRectangle) -> Result<GdalLoadingInfo> {
        Ok(GdalLoadingInfo {
            info: GdalLoadingInfoPartIterator::Static {
                parts: vec![GdalLoadingInfoPart {
                    time: self.time.unwrap_or_default(),
                    params: self.params.clone(),
                }]
                .into_iter(),
            },
        })
    }

    async fn result_descriptor(&self) -> Result<RasterResultDescriptor> {
        Ok(self.result_descriptor.clone())
    }

    fn box_clone(
        &self,
    ) -> Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>> {
        Box::new(self.clone())
    }
}

/// Meta data for a regular time series that begins (is anchored) at `start` with multiple gdal data
/// sets `step` time apart. The `placeholder` in the file path of the dataset is replaced with the
/// queried time in specified `time_format`.
// TODO: `start` is actually more a reference time, because the time series also goes in
//        negative direction. Maybe it would be better to have a real start and end time, then
//        everything before start and after end is just one big nodata raster instead of many
#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GdalMetaDataRegular {
    pub result_descriptor: RasterResultDescriptor,
    pub params: GdalDatasetParameters,
    pub placeholder: String,
    pub time_format: String,
    pub start: TimeInstance,
    pub step: TimeStep,
}

#[async_trait]
impl MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for GdalMetaDataRegular
{
    async fn loading_info(&self, query: RasterQueryRectangle) -> Result<GdalLoadingInfo> {
        let snapped_start = self
            .step
            .snap_relative(self.start, query.time_interval.start())?;

        let snapped_interval =
            TimeInterval::new_unchecked(snapped_start, query.time_interval.end()); // TODO: snap end?

        let time_iterator =
            TimeStepIter::new_with_interval_incl_start(snapped_interval, self.step)?;

        Ok(GdalLoadingInfo {
            info: GdalLoadingInfoPartIterator::Dynamic {
                time_step_iter: time_iterator,
                params: self.params.clone(),
                placeholder: self.placeholder.clone(),
                time_format: self.time_format.clone(),
                step: self.step,
                max_t2: query.time_interval.end(),
            },
        })
    }

    async fn result_descriptor(&self) -> Result<RasterResultDescriptor> {
        Ok(self.result_descriptor.clone())
    }

    fn box_clone(
        &self,
    ) -> Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>> {
        Box::new(self.clone())
    }
}

impl GdalDatasetParameters {
    pub fn replace_time_placeholder(
        &self,
        placeholder: &str,
        time_format: &str,
        time: TimeInstance,
    ) -> Result<Self> {
        let time_string = time
            .as_naive_date_time()
            .ok_or(Error::TimeInstanceNotDisplayable)?
            .format(time_format)
            .to_string();
        let file_path = self
            .file_path
            .to_str()
            .ok_or(Error::FilePathNotRepresentableAsString)?
            .replace(placeholder, &time_string);

        Ok(Self {
            file_not_found_handling: FileNotFoundHandling::NoData,
            file_path: file_path.into(),
            properties_mapping: self.properties_mapping.clone(),
            gdal_open_options: self.gdal_open_options.clone(),
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
    pub phantom_data: PhantomData<T>,
}

impl<T> GdalSourceProcessor<T>
where
    T: gdal::raster::GdalType + Pixel,
{
    ///
    /// A method to async load single tiles from a GDAL dataset.
    ///
    pub async fn load_tile_data_async(
        dataset_params: GdalDatasetParameters,
        tile_information: TileInformation,
    ) -> Result<GridWithProperties<T>> {
        tokio::task::spawn_blocking(move || {
            Self::load_tile_data(&dataset_params, &tile_information)
        })
        .await
        .context(error::TokioJoin)?
    }

    pub async fn load_tile_async(
        dataset_params: GdalDatasetParameters,
        tile_information: TileInformation,
        time: TimeInterval,
    ) -> Result<RasterTile2D<T>> {
        let f = if tile_information
            .spatial_partition()
            .intersects(&dataset_params.spatial_partition())
        {
            Self::load_tile_data_async(dataset_params, tile_information).await
        } else {
            let fill_value: T = dataset_params.no_data_value.map_or_else(T::zero, T::from_);

            let empty_grid = if let Some(no_data) = dataset_params.no_data_value {
                EmptyGrid::new(tile_information.tile_size_in_pixels, T::from_(no_data)).into()
            } else {
                Grid2D::new_filled(tile_information.tile_size_in_pixels, fill_value, None).into()
            };

            Ok(GridWithProperties {
                grid: empty_grid,
                properties: Default::default(),
            })
        };

        f.map(|grid_with_properties| {
            RasterTile2D::new_with_tile_info_and_properties(
                time,
                tile_information,
                grid_with_properties.grid,
                grid_with_properties.properties,
            )
        })
    }

    ///
    /// A method to load single tiles from a GDAL dataset.
    ///
    #[allow(clippy::too_many_lines)]
    pub fn load_tile_data(
        dataset_params: &GdalDatasetParameters,
        tile_information: &TileInformation,
    ) -> Result<GridWithProperties<T>> {
        let dataset_bounds = dataset_params.spatial_partition();
        let geo_transform = dataset_params.geo_transform;
        let output_bounds = tile_information.spatial_partition();
        let output_shape = tile_information.tile_size_in_pixels();
        let output_geo_transform = tile_information.tile_geo_transform();

        debug!(
            "GridOrEmpty2D<{:?}> requested for {:?}.",
            T::TYPE,
            &output_bounds
        );

        let options = dataset_params
            .gdal_open_options
            .as_ref()
            .map(|o| o.iter().map(String::as_str).collect::<Vec<_>>());

        let dataset_result = GdalDataset::open_ex(
            &dataset_params.file_path,
            DatasetOptions {
                open_options: options.as_deref(),
                ..DatasetOptions::default()
            },
        );
        let no_data_value = dataset_params.no_data_value.map(T::from_);
        let fill_value = no_data_value.unwrap_or_else(T::zero);
        let mut properties = RasterProperties::default();

        debug!(
            "no_data_value is {:?} and fill_value is {:?}.",
            &no_data_value, &fill_value
        );

        if dataset_result.is_err() {
            // TODO: check if Gdal error is actually file not found
            return match dataset_params.file_not_found_handling {
                FileNotFoundHandling::NoData => {
                    if let Some(no_data) = no_data_value {
                        debug!("file not found -> returning empty grid");
                        Ok(GridWithProperties {
                            grid: EmptyGrid::new(output_shape, no_data).into(),
                            properties,
                        })
                    } else {
                        debug!("file not found -> returning filled grid");
                        Ok(GridWithProperties {
                            grid: Grid2D::new_filled(output_shape, fill_value, None).into(),
                            properties,
                        })
                    }
                }
                FileNotFoundHandling::Error => Err(crate::error::Error::CouldNotOpenGdalDataset {
                    file_path: dataset_params.file_path.to_string_lossy().to_string(),
                }),
            };
        };

        let dataset = dataset_result.expect("checked");
        let rasterband: GdalRasterBand =
            dataset.rasterband(dataset_params.rasterband_channel as isize)?;

        if let Some(properties_mapping) = dataset_params.properties_mapping.as_ref() {
            properties_from_gdal(&mut properties, &dataset, properties_mapping);
            properties_from_gdal(&mut properties, &rasterband, properties_mapping);
            properties_from_band(&mut properties, &rasterband);
        }

        // check if query and dataset intersect
        let dataset_intersects_tile = dataset_bounds.intersection(&output_bounds);

        let dataset_intersection_area = match dataset_intersects_tile {
            Some(i) => i,
            None => {
                // there is no intersection, return empty tile
                let no_data_grid = if let Some(no_data) = no_data_value {
                    EmptyGrid::new(output_shape, no_data).into()
                } else {
                    Grid2D::new_filled(output_shape, fill_value, None).into()
                };

                return Ok(GridWithProperties {
                    grid: no_data_grid,
                    properties,
                });
            }
        };

        let dataset_grid_bounds = geo_transform.spatial_to_grid_bounds(&dataset_intersection_area);

        let result_grid = if dataset_intersection_area == output_bounds {
            read_as_raster(
                &rasterband,
                &dataset_grid_bounds,
                output_shape,
                no_data_value,
            )?
            .into()
        } else {
            let tile_grid_bounds =
                output_geo_transform.spatial_to_grid_bounds(&dataset_intersection_area);

            let dataset_raster = read_as_raster(
                &rasterband,
                &dataset_grid_bounds,
                tile_grid_bounds,
                no_data_value,
            )?;

            let mut tile_raster = Grid2D::new_filled(output_shape, fill_value, no_data_value);
            tile_raster.grid_blit_from(dataset_raster);
            tile_raster.into()
        };

        Ok(GridWithProperties {
            grid: result_grid,
            properties,
        })
    }

    ///
    /// A stream of `RasterTile2D`
    ///
    pub fn tile_stream(
        &self,
        query: RasterQueryRectangle,
        info: GdalLoadingInfoPart,
    ) -> impl Stream<Item = Result<RasterTile2D<T>>> {
        let spatial_resolution = query.spatial_resolution;
        let geo_transform = info.params.geo_transform;

        // adjust the spatial resolution to the sign of the geotransform
        let x_signed = if geo_transform.x_pixel_size.is_sign_positive()
            && spatial_resolution.x.is_sign_positive()
        {
            spatial_resolution.x
        } else {
            spatial_resolution.x * -1.0
        };

        let y_signed = if geo_transform.y_pixel_size.is_sign_positive()
            && spatial_resolution.y.is_sign_positive()
        {
            spatial_resolution.y
        } else {
            spatial_resolution.y * -1.0
        };

        let tiling_strategy = self.tiling_specification.strategy(x_signed, y_signed);

        stream::iter(tiling_strategy.tile_information_iterator(query.spatial_bounds))
            .map(move |tile| Self::load_tile_async(info.params.clone(), tile, info.time))
            .buffered(1) // TODO: find a good default and / or add to config.
    }
}

#[async_trait]
impl<P> QueryProcessor for GdalSourceProcessor<P>
where
    P: Pixel + gdal::raster::GdalType,
{
    type Output = RasterTile2D<P>;
    type SpatialBounds = SpatialPartition2D;

    async fn query<'a>(
        &'a self,
        query: crate::engine::RasterQueryRectangle,
        _ctx: &'a dyn crate::engine::QueryContext,
    ) -> Result<BoxStream<Result<Self::Output>>> {
        debug!(
            "Querying GdalSourceProcessor<{:?}> with: {:?}.",
            P::TYPE,
            &query
        );

        let meta_data = self.meta_data.loading_info(query).await?;

        debug!("GdalLoadingInfo: {:?}.", &meta_data);

        // TODO: what to do if loading info is empty?
        let stream = stream::iter(meta_data.info)
            .map(move |info| match info {
                Ok(info) => self.tile_stream(query, info).boxed(),
                Err(err) => stream::once(async { Result::Err(err) }).boxed(),
            })
            .flatten();

        Ok(stream.boxed())
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

pub struct GridWithProperties<T> {
    grid: GridOrEmpty2D<T>,
    properties: RasterProperties,
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
                    phantom_data: Default::default(),
                }
                .boxed(),
            ),
            RasterDataType::U16 => TypedRasterQueryProcessor::U16(
                GdalSourceProcessor {
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    phantom_data: Default::default(),
                }
                .boxed(),
            ),
            RasterDataType::U32 => TypedRasterQueryProcessor::U32(
                GdalSourceProcessor {
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    phantom_data: Default::default(),
                }
                .boxed(),
            ),
            RasterDataType::U64 => unimplemented!("implement U64 type"), // TypedRasterQueryProcessor::U64(self.create_processor()),
            RasterDataType::I8 => unimplemented!("I8 type is not supported"),
            RasterDataType::I16 => TypedRasterQueryProcessor::I16(
                GdalSourceProcessor {
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    phantom_data: Default::default(),
                }
                .boxed(),
            ),
            RasterDataType::I32 => TypedRasterQueryProcessor::I32(
                GdalSourceProcessor {
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    phantom_data: Default::default(),
                }
                .boxed(),
            ),
            RasterDataType::I64 => unimplemented!("implement I64 type"), // TypedRasterQueryProcessor::I64(self.create_processor()),
            RasterDataType::F32 => TypedRasterQueryProcessor::F32(
                GdalSourceProcessor {
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    phantom_data: Default::default(),
                }
                .boxed(),
            ),
            RasterDataType::F64 => TypedRasterQueryProcessor::F64(
                GdalSourceProcessor {
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    phantom_data: Default::default(),
                }
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GdalMetadataMapping {
    source_key: RasterPropertiesKey,
    target_key: RasterPropertiesKey,
    target_type: RasterPropertiesEntryType,
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
    use crate::util::gdal::{add_ndvi_dataset, raster_dir};
    use crate::util::Result;
    use geoengine_datatypes::primitives::{AxisAlignedRectangle, SpatialPartition2D};
    use geoengine_datatypes::raster::{TileInformation, TilingStrategy};
    use geoengine_datatypes::{
        primitives::{Measurement, SpatialResolution, TimeGranularity},
        raster::GridShape2D,
    };
    use geoengine_datatypes::{raster::GridIdx2D, spatial_reference::SpatialReference};

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
    ) -> Result<GridWithProperties<u8>> {
        GdalSourceProcessor::<u8>::load_tile_data(
            &GdalDatasetParameters {
                file_path: raster_dir().join("modis_ndvi/MOD13A2_M_NDVI_2014-01-01.TIFF"),
                rasterband_channel: 1,
                geo_transform: GeoTransform {
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
            },
            &TileInformation::with_partition_and_shape(output_bounds, output_shape),
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
            geo_transform: Default::default(),
            width: 360,
            height: 180,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: Some(0.),
            properties_mapping: None,
            gdal_open_options: None,
        };
        let replaced = params
            .replace_time_placeholder("%TIME%", "%f", TimeInstance::from_millis_unchecked(22))
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

    #[tokio::test]
    async fn test_regular_meta_data() {
        let no_data_value = Some(0.);

        let meta_data = GdalMetaDataRegular {
            result_descriptor: RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReference::epsg_4326().into(),
                measurement: Measurement::Unitless,
                no_data_value,
            },
            params: GdalDatasetParameters {
                file_path: "/foo/bar_%TIME%.tiff".into(),
                rasterband_channel: 0,
                geo_transform: Default::default(),
                width: 360,
                height: 180,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value,
                properties_mapping: None,
                gdal_open_options: None,
            },
            placeholder: "%TIME%".to_string(),
            time_format: "%f".to_string(),
            start: TimeInstance::from_millis_unchecked(11),
            step: TimeStep {
                granularity: TimeGranularity::Millis,
                step: 11,
            },
        };

        assert_eq!(
            meta_data.result_descriptor().await.unwrap(),
            RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReference::epsg_4326().into(),
                measurement: Measurement::Unitless,
                no_data_value: Some(0.)
            }
        );

        assert_eq!(
            meta_data
                .loading_info(RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (0., 1.).into(),
                        (1., 0.).into()
                    ),
                    time_interval: TimeInterval::new_unchecked(0, 30),
                    spatial_resolution: SpatialResolution::one(),
                })
                .await
                .unwrap()
                .info
                .map(|p| p.unwrap().params.file_path.to_str().unwrap().to_owned())
                .collect::<Vec<_>>(),
            &[
                "/foo/bar_000000000.tiff",
                "/foo/bar_011000000.tiff",
                "/foo/bar_022000000.tiff"
            ]
        );
    }

    #[test]
    fn test_load_tile_data() {
        let output_shape: GridShape2D = [8, 8].into();
        let output_bounds =
            SpatialPartition2D::new_unchecked((-180., 90.).into(), (180., -90.).into());

        let GridWithProperties { grid, properties } =
            load_ndvi_jan_2014(output_shape, output_bounds).unwrap();

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

        let x = load_ndvi_jan_2014(output_shape, output_bounds)
            .unwrap()
            .grid;

        assert!(!x.is_empty());

        let x = x.into_materialized_grid();

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

        let x = load_ndvi_jan_2014(output_shape, output_bounds)
            .unwrap()
            .grid;

        assert!(!x.is_empty());

        let x = x.into_materialized_grid();

        assert_eq!(x.data.len(), 64);
        assert_eq!(x.data, &[1; 64]);
    }

    #[tokio::test]
    async fn test_query_single_time_slice() {
        let mut exe_ctx = MockExecutionContext::default();
        let query_ctx = MockQueryContext::default();
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
        let mut exe_ctx = MockExecutionContext::default();
        let query_ctx = MockQueryContext::default();
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
        let mut exe_ctx = MockExecutionContext::default();
        let query_ctx = MockQueryContext::default();
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
}
