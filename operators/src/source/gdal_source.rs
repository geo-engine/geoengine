use crate::{
    engine::{
        InitializedOperator, InitializedOperatorBase, InitializedRasterOperator, QueryProcessor,
        RasterOperator, RasterQueryProcessor, RasterResultDescriptor, SourceOperator,
        TypedRasterQueryProcessor,
    },
    error::{self, Error},
    util::Result,
};

use gdal::raster::{GdalType, RasterBand as GdalRasterBand};
use gdal::Dataset as GdalDataset;
use snafu::ResultExt;
use std::{marker::PhantomData, path::PathBuf};
//use gdal::metadata::Metadata; // TODO: handle metadata

use serde::{Deserialize, Serialize};

use futures::stream::{self, BoxStream, StreamExt};

use crate::engine::{MetaData, QueryRectangle};
use geoengine_datatypes::raster::{GeoTransform, Grid2D, Pixel, RasterDataType, RasterTile2D};
use geoengine_datatypes::{dataset::DataSetId, raster::TileInformation};
use geoengine_datatypes::{
    primitives::{
        BoundingBox2D, SpatialBounded, TimeInstance, TimeInterval, TimeStep, TimeStepIter,
    },
    raster::{
        Grid, GridBlit, GridBoundingBox2D, GridBounds, GridIdx, GridSize, GridSpaceToLinearSpace,
        TilingSpecification,
    },
};

/// Parameters for the GDAL Source Operator
///
/// # Examples
///
/// ```rust
/// use serde_json::{Result, Value};
/// use geoengine_operators::source::{GdalSource, GdalSourceParameters};
/// use geoengine_datatypes::dataset::InternalDataSetId;
/// use geoengine_datatypes::util::Identifier;
/// use std::str::FromStr;
///
/// let json_string = r#"
///     {
///         "type": "GdalSource",
///         "params": {
///             "data_set": {
///                 "Internal": "a626c880-1c41-489b-9e19-9596d129859c"
///             }
///         }
///     }"#;
///
/// let operator: GdalSource = serde_json::from_str(json_string).unwrap();
///
/// assert_eq!(operator, GdalSource {
///     params: GdalSourceParameters {
///         data_set: InternalDataSetId::from_str("a626c880-1c41-489b-9e19-9596d129859c").unwrap().into()
///     },
/// });
/// ```
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct GdalSourceParameters {
    pub data_set: DataSetId,
}

type GdalMetaData = Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor>>;

#[derive(Debug, Clone)]
pub struct GdalLoadingInfo {
    /// partitions of data set sorted by time
    pub info: Vec<GdalLoadingInfoPart>, // TODO: iterator?
}

/// one temporal slice of the data set that requires reading from exactly one Gdal data set
#[derive(Debug, Clone)]
pub struct GdalLoadingInfoPart {
    pub time: TimeInterval,
    pub params: GdalDataSetParameters,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GdalDataSetParameters {
    pub file_path: PathBuf,
    pub rasterband_channel: usize,
    pub geo_transform: GeoTransform,
    pub bbox: BoundingBox2D, // the bounding box of the data set containing the raster data
    pub file_not_found_handling: FileNotFoundHandling,
}

/// How to handle file not found errors
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum FileNotFoundHandling {
    NoData, // output tiles filled with nodata
    Error,  // return error tile
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GdalMetaDataStatic {
    pub time: Option<TimeInterval>,
    pub params: GdalDataSetParameters,
    pub result_descriptor: RasterResultDescriptor,
}

impl MetaData<GdalLoadingInfo, RasterResultDescriptor> for GdalMetaDataStatic {
    fn loading_info(&self, _query: QueryRectangle) -> Result<GdalLoadingInfo> {
        Ok(GdalLoadingInfo {
            info: vec![GdalLoadingInfoPart {
                time: self.time.unwrap_or_else(TimeInterval::default),
                params: self.params.clone(),
            }],
        })
    }

    fn result_descriptor(&self) -> Result<RasterResultDescriptor> {
        Ok(self.result_descriptor.clone())
    }

    fn box_clone(&self) -> Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor>> {
        Box::new(self.clone())
    }
}

/// Meta data for a regular time series that begins (is anchored) at `start` with multiple gdal data
/// sets `step` time apart. The `placeholder` in the file path of the data set is replaced with the
/// queried time in specified `time_format`.
// TODO: `start` is actually more a reference time, because the time series also goes in
//        negative direction. Maybe it would be better to have a real start and end time, then
//        everything before start and after end is just one big nodata raster instead of many
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GdalMetaDataRegular {
    pub result_descriptor: RasterResultDescriptor,
    pub params: GdalDataSetParameters,
    pub placeholder: String,
    pub time_format: String,
    pub start: TimeInstance,
    pub step: TimeStep,
}

impl MetaData<GdalLoadingInfo, RasterResultDescriptor> for GdalMetaDataRegular {
    fn loading_info(&self, query: QueryRectangle) -> Result<GdalLoadingInfo> {
        let snapped_start = self
            .step
            .snap_relative(self.start, query.time_interval.start())?;

        let snapped_interval =
            TimeInterval::new_unchecked(snapped_start, query.time_interval.end()); // TODO: snap end?

        let time_iterator =
            TimeStepIter::new_with_interval_incl_start(snapped_interval, self.step)?;

        let info: Result<Vec<_>> = time_iterator
            .try_as_intervals(self.step)
            .map(|time| {
                let time = time?;
                Ok(GdalLoadingInfoPart {
                    time,
                    params: self.params.replace_time_placeholder(
                        &self.placeholder,
                        &self.time_format,
                        time.start(),
                    )?,
                })
            })
            .collect();

        Ok(GdalLoadingInfo { info: info? })
    }

    fn result_descriptor(&self) -> Result<RasterResultDescriptor> {
        Ok(self.result_descriptor.clone())
    }

    fn box_clone(&self) -> Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor>> {
        Box::new(self.clone())
    }
}

impl GdalDataSetParameters {
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
            file_path: file_path.into(),
            rasterband_channel: self.rasterband_channel,
            geo_transform: self.geo_transform,
            bbox: self.bbox,
            file_not_found_handling: FileNotFoundHandling::NoData,
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
        data_set_params: GdalDataSetParameters,
        tile_information: TileInformation,
    ) -> Result<Grid2D<T>> {
        tokio::task::spawn_blocking(move || {
            Self::load_tile_data(&data_set_params, tile_information)
        })
        .await
        .context(error::TokioJoin)?
    }

    ///
    /// A method to load single tiles from a GDAL dataset.
    ///
    pub fn load_tile_data(
        data_set_params: &GdalDataSetParameters,
        tile_information: TileInformation,
    ) -> Result<Grid2D<T>> {
        let dataset_bounds = data_set_params.bbox;
        let geo_transform = data_set_params.geo_transform;

        let output_bounds = tile_information.spatial_bounds();
        let output_shape = tile_information.tile_size_in_pixels();
        let output_geo_transform = tile_information.tile_geo_transform();

        let dataset_result = GdalDataset::open(&data_set_params.file_path);

        if dataset_result.is_err() {
            // TODO: check if Gdal error is actually file not found
            return match data_set_params.file_not_found_handling {
                FileNotFoundHandling::NoData => {
                    // TODO: fill with actual no data
                    Ok(Grid2D::new_filled(output_shape, T::zero(), None))
                }
                FileNotFoundHandling::Error => Err(crate::error::Error::CouldNotOpenGdalDataSet {
                    file_path: data_set_params.file_path.to_string_lossy().to_string(),
                }),
            };
        };

        let dataset = dataset_result.expect("checked");

        // TODO: investigate if we need a dataset cache

        // get the requested raster band of the dataset …
        let rasterband: GdalRasterBand =
            dataset.rasterband(data_set_params.rasterband_channel as isize)?;

        // dataset spatial relations
        let dataset_contains_tile = dataset_bounds.contains_bbox(&output_bounds);

        let dataset_intersects_tile = dataset_bounds.intersects_bbox(&output_bounds);

        let result_raster = match (dataset_contains_tile, dataset_intersects_tile) {
            (_, false) => {
                // TODO: refactor tile to hold an Option<GridData> and this will be empty in this case
                Grid2D::new_filled(output_shape, T::zero(), None)
            }
            (true, true) => {
                let dataset_idx_ul =
                    geo_transform.coordinate_to_grid_idx_2d(output_bounds.upper_left());

                let dataset_idx_lr =
                    geo_transform.coordinate_to_grid_idx_2d(output_bounds.lower_right()) - 1; // the lr coordinate is the first pixel of the next tile so sub 1 from all axis.

                read_as_raster(
                    &rasterband,
                    &GridBoundingBox2D::new(dataset_idx_ul, dataset_idx_lr)?,
                    output_shape,
                )?
            }
            (false, true) => {
                let intersecting_area = dataset_bounds
                    .intersection(&output_bounds)
                    .expect("checked intersection earlier");

                let dataset_idx_ul =
                    geo_transform.coordinate_to_grid_idx_2d(intersecting_area.upper_left());

                let dataset_idx_lr =
                    geo_transform.coordinate_to_grid_idx_2d(intersecting_area.lower_right()) - 1;

                let tile_idx_ul =
                    output_geo_transform.coordinate_to_grid_idx_2d(intersecting_area.upper_left());

                let tile_idx_lr = output_geo_transform
                    .coordinate_to_grid_idx_2d(intersecting_area.lower_right())
                    - 1;

                let dataset_raster = read_as_raster(
                    &rasterband,
                    &GridBoundingBox2D::new(dataset_idx_ul, dataset_idx_lr)?,
                    GridBoundingBox2D::new(tile_idx_ul, tile_idx_lr)?,
                )?;

                // TODO: fill with actual no data
                let mut tile_raster = Grid2D::new_filled(output_shape, T::zero(), None);
                tile_raster.grid_blit_from(dataset_raster)?;
                tile_raster
            }
        };

        Ok(result_raster)
    }

    ///
    /// A stream of `RasterTile2D`
    ///
    pub fn tile_stream(
        &self,
        query: QueryRectangle,
        info: GdalLoadingInfoPart,
    ) -> BoxStream<Result<RasterTile2D<T>>> {
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

        stream::iter(tiling_strategy.tile_information_iterator(query.bbox))
            .map(move |tile| (tile, info.clone()))
            .then(async move |(tile, info)| {
                Ok(RasterTile2D::new_with_tile_info(
                    info.time,
                    tile,
                    Self::load_tile_data_async(info.params.clone(), tile).await?,
                ))
            })
            .boxed()
    }
}

impl<T> QueryProcessor for GdalSourceProcessor<T>
where
    T: Pixel + gdal::raster::GdalType,
{
    type Output = RasterTile2D<T>;
    fn query<'a>(
        &'a self,
        query: crate::engine::QueryRectangle,
        _ctx: &'a dyn crate::engine::QueryContext,
    ) -> Result<BoxStream<Result<RasterTile2D<T>>>> {
        let meta_data = self.meta_data.loading_info(query)?;

        Ok(stream::iter(meta_data.info.into_iter())
            .map(move |info| self.tile_stream(query, info))
            .flatten()
            .boxed())
    }
}

pub type GdalSource = SourceOperator<GdalSourceParameters>;

#[typetag::serde]
impl RasterOperator for GdalSource {
    fn initialize(
        self: Box<Self>,
        context: &dyn crate::engine::ExecutionContext,
    ) -> Result<Box<InitializedRasterOperator>> {
        let meta_data: GdalMetaData = context.meta_data(&self.params.data_set)?;

        Ok(InitializedGdalSourceOperator {
            result_descriptor: meta_data.result_descriptor()?,
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

impl InitializedOperatorBase for InitializedGdalSourceOperator {
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
    for InitializedGdalSourceOperator
{
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
    use crate::util::gdal::{add_ndvi_data_set, raster_dir};
    use crate::util::Result;
    use geoengine_datatypes::raster::{TileInformation, TilingStrategy};
    use geoengine_datatypes::{
        primitives::{Measurement, SpatialResolution, TimeGranularity},
        raster::GridShape2D,
    };
    use geoengine_datatypes::{raster::GridIdx2D, spatial_reference::SpatialReference};

    async fn query_gdal_source(
        exe_ctx: &mut MockExecutionContext,
        query_ctx: &MockQueryContext,
        id: DataSetId,
        output_shape: GridShape2D,
        output_bounds: BoundingBox2D,
        time_interval: TimeInterval,
    ) -> Vec<Result<RasterTile2D<u8>>> {
        let op = GdalSource {
            params: GdalSourceParameters {
                data_set: id.clone(),
            },
        }
        .boxed();

        let x_query_resolution = output_bounds.size_x() / output_shape.axis_size_x() as f64;
        let y_query_resolution = output_bounds.size_y() / output_shape.axis_size_y() as f64;
        let spatial_resolution =
            SpatialResolution::new_unchecked(x_query_resolution, y_query_resolution);

        let o = op.initialize(exe_ctx).unwrap();

        o.query_processor()
            .unwrap()
            .get_u8()
            .unwrap()
            .query(
                QueryRectangle {
                    bbox: output_bounds,
                    time_interval,
                    spatial_resolution,
                },
                query_ctx,
            )
            .unwrap()
            .collect()
            .await
    }

    fn load_ndvi_jan_2014(
        output_shape: GridShape2D,
        output_bounds: BoundingBox2D,
    ) -> Result<Grid2D<u8>> {
        GdalSourceProcessor::<u8>::load_tile_data(
            &GdalDataSetParameters {
                file_path: raster_dir().join("modis_ndvi/MOD13A2_M_NDVI_2014-01-01.TIFF"),
                rasterband_channel: 1,
                geo_transform: GeoTransform {
                    origin_coordinate: (-180., 90.).into(),
                    x_pixel_size: 0.1,
                    y_pixel_size: -0.1,
                },
                bbox: BoundingBox2D::new_unchecked((-180., -90.).into(), (180., 90.).into()),
                file_not_found_handling: FileNotFoundHandling::NoData,
            },
            TileInformation::with_bbox_and_shape(output_bounds, output_shape),
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
    fn test_regular_meta_data() {
        let meta_data = GdalMetaDataRegular {
            result_descriptor: RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReference::epsg_4326().into(),
                measurement: Measurement::Unitless,
            },
            params: GdalDataSetParameters {
                file_path: "/foo/bar_%TIME%.tiff".into(),
                rasterband_channel: 0,
                geo_transform: Default::default(),
                bbox: BoundingBox2D::new_unchecked((0., 0.).into(), (1., 1.).into()),
                file_not_found_handling: FileNotFoundHandling::NoData,
            },
            placeholder: "%TIME%".to_string(),
            time_format: "%f".to_string(),
            start: 11.into(),
            step: TimeStep {
                granularity: TimeGranularity::Millis,
                step: 11,
            },
        };

        assert_eq!(
            meta_data
                .loading_info(QueryRectangle {
                    bbox: BoundingBox2D::new_unchecked((0., 0.).into(), (1., 1.).into()),
                    time_interval: TimeInterval::new_unchecked(0, 30),
                    spatial_resolution: SpatialResolution::one(),
                })
                .unwrap()
                .info
                .iter()
                .map(|p| p.params.file_path.to_str().unwrap())
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
        let output_bounds = BoundingBox2D::new_unchecked((-180., -90.).into(), (180., 90.).into());

        let x = load_ndvi_jan_2014(output_shape, output_bounds).unwrap();

        assert_eq!(x.data.len(), 64);
        assert_eq!(
            x.data,
            &[
                255, 255, 255, 255, 255, 255, 255, 255, 255, 75, 37, 255, 44, 34, 39, 32, 255, 86,
                255, 255, 255, 30, 96, 255, 255, 255, 255, 255, 90, 255, 255, 255, 255, 255, 202,
                255, 193, 255, 255, 255, 255, 255, 89, 255, 111, 255, 255, 255, 255, 255, 255, 255,
                255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255
            ]
        );
    }

    #[test]
    fn test_load_tile_data_overlaps_dataset_bounds() {
        let output_shape: GridShape2D = [8, 8].into();
        // shift world bbox one pixel up and to the left
        let (x_size, y_size) = (45., 22.5);
        let output_bounds = BoundingBox2D::new_unchecked(
            (-180. - x_size, -90. + y_size).into(),
            (180. - x_size, 90. + y_size).into(),
        );

        let x = load_ndvi_jan_2014(output_shape, output_bounds).unwrap();

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

    #[tokio::test]
    async fn test_query_single_time_slice() {
        let mut exe_ctx = MockExecutionContext::default();
        let query_ctx = MockQueryContext::default();
        let id = add_ndvi_data_set(&mut exe_ctx);

        let output_shape: GridShape2D = [256, 256].into();
        let output_bounds = BoundingBox2D::new_unchecked((-180., -90.).into(), (180., 90.).into());
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
        let id = add_ndvi_data_set(&mut exe_ctx);

        let output_shape: GridShape2D = [256, 256].into();
        let output_bounds = BoundingBox2D::new_unchecked((-180., -90.).into(), (180., 90.).into());
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
        let id = add_ndvi_data_set(&mut exe_ctx);

        let output_shape: GridShape2D = [256, 256].into();
        let output_bounds = BoundingBox2D::new_unchecked((-180., -90.).into(), (180., 90.).into());
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

        assert_eq!(
            c[0].time,
            TimeInterval::new_unchecked(1_385_856_000_000, 1_388_534_400_000)
        );

        assert!(!c[0].grid_array.data.iter().any(|p| *p != 0)); // TODO: use actual no data value
    }
}
