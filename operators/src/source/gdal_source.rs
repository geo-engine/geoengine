use crate::{engine::QueryProcessor, util::Result};

use gdal::raster::dataset::Dataset as GdalDataset;
use gdal::raster::rasterband::RasterBand as GdalRasterBand;
use std::path::PathBuf;
//use gdal::metadata::Metadata; // TODO: handle metadata

use serde::{Deserialize, Serialize};

use futures::stream::{self, BoxStream, StreamExt};

use geoengine_datatypes::primitives::{BoundingBox2D, Coordinate2D, SpatialBounded, TimeInterval};
use geoengine_datatypes::raster::{Dim, GeoTransform, GridDimension, Ix, Raster2D, TypedRaster2D};

/// Parameters for the GDAL Source Operator
///
/// # Examples
///
/// ```rust
/// use serde_json::{Result, Value};
/// use geoengine_operators::Operator;
/// use geoengine_operators::operators::{GdalSourceParameters, NoSources, RasterSources};
///
/// let json_string = r#"
///     {
///         "type": "gdal_source",
///         "params": {
///                     "base_path": "base_path",
///                     "file_name_with_time_placeholder": "file_name_with_time_placeholder",
///                     "time_format": "file_name",
///                     "channel": 3
///         }
///     }"#;
///
/// let operator: Operator = serde_json::from_str(json_string).unwrap();
///
/// assert_eq!(operator, Operator::GdalSource {
///     params: GdalSourceParameters {
///                     base_path: "base_path".into(),
///                     file_name_with_time_placeholder: "file_name_with_time_placeholder".into(),
///                     time_format: "file_name".into(),
///                     channel: Some(3),
///     },
///     sources: Default::default(),
/// });
/// ```
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct GdalSourceParameters {
    pub base_path: PathBuf,
    pub file_name_with_time_placeholder: String,
    pub time_format: String,
    pub channel: Option<u32>,
    // TODO: add some kind of tick interval
}

/// A provider of tile (size) information for a raster/grid
pub struct GdalSourceTileGridProvider {
    pub global_pixel_size: Dim<[Ix; 2]>,
    pub tile_pixel_size: Dim<[Ix; 2]>,
    // pub grid_tiles : Vec<Dim<[Ix; 2]>>,
    pub dataset_geo_transform: GeoTransform,
}

impl GdalSourceTileGridProvider {
    /// generates a vec with `TileInformation` for each tile
    fn tile_informations(&self) -> Vec<TileInformation> {
        let &[.., y_pixels_global, x_pixels_global] = self.global_pixel_size.dimension_size();
        let &[.., y_pixels_tile, x_pixels_tile] = self.tile_pixel_size.dimension_size();
        let x_tiles = x_pixels_global / x_pixels_tile;
        let y_tiles = y_pixels_global / y_pixels_tile;

        let mut tile_information = Vec::with_capacity(y_tiles * x_tiles);

        for (yi, y) in (0..y_pixels_global).step_by(y_pixels_tile).enumerate() {
            // TODO: discuss if the tile information should store global pixel or global tile size)
            for (xi, x) in (0..x_pixels_global).step_by(x_pixels_tile).enumerate() {
                // prev.
                let tile_x_coord = self.dataset_geo_transform.upper_left_coordinate.x
                    + (x as f64 * self.dataset_geo_transform.x_pixel_size);
                let tile_y_coord = self.dataset_geo_transform.upper_left_coordinate.y
                    + (y as f64 * self.dataset_geo_transform.y_pixel_size);
                let tile_geo_transform = GeoTransform::new(
                    Coordinate2D::new(tile_x_coord, tile_y_coord),
                    self.dataset_geo_transform.x_pixel_size,
                    self.dataset_geo_transform.y_pixel_size,
                );

                tile_information.push(TileInformation::new(
                    (y_tiles, x_tiles).into(),
                    (yi, xi).into(),
                    (y, x).into(),
                    (y_pixels_tile, x_pixels_tile).into(),
                    tile_geo_transform,
                ))
            }
        }
        tile_information
    }
}

/// A `RasterTile2D` is the main type used to iterate over tiles of 2D raster data
#[derive(Debug)]
pub struct RasterTile2D {
    pub time: TimeInterval,
    pub tile: TileInformation,
    pub data: TypedRaster2D,
}

impl RasterTile2D {
    /// create a new `RasterTile2D`
    pub fn new(time: TimeInterval, tile: TileInformation, data: TypedRaster2D) -> Self {
        Self { time, tile, data }
    }
}

/// The `TileInformation` is used to represent the spatial position of each tile
#[derive(PartialEq, Debug, Copy, Clone, Serialize, Deserialize)]
pub struct TileInformation {
    global_size_in_tiles: Dim<[Ix; 2]>,
    global_tile_position: Dim<[Ix; 2]>,
    global_pixel_position: Dim<[Ix; 2]>,
    tile_size_in_pixels: Dim<[Ix; 2]>,
    geo_transform: GeoTransform,
}

impl TileInformation {
    pub fn new(
        global_size_in_tiles: Dim<[Ix; 2]>,
        global_tile_position: Dim<[Ix; 2]>,
        global_pixel_position: Dim<[Ix; 2]>,
        tile_size_in_pixels: Dim<[Ix; 2]>,
        geo_transform: GeoTransform,
    ) -> Self {
        Self {
            global_size_in_tiles,
            global_tile_position,
            global_pixel_position,
            tile_size_in_pixels,
            geo_transform,
        }
    }
    pub fn global_size_in_tiles(&self) -> Dim<[Ix; 2]> {
        self.global_size_in_tiles
    }
    pub fn global_tile_position(&self) -> Dim<[Ix; 2]> {
        self.global_tile_position
    }
    pub fn global_pixel_position(&self) -> Dim<[Ix; 2]> {
        self.global_pixel_position
    }
    pub fn tile_size_in_pixels(&self) -> Dim<[Ix; 2]> {
        self.tile_size_in_pixels
    }
}

impl SpatialBounded for TileInformation {
    fn spatial_bounds(&self) -> BoundingBox2D {
        let top_left_coord = self.geo_transform.grid_2d_to_coordinate_2d((0, 0));
        let (.., tile_y_size, tile_x_size) = self.tile_size_in_pixels.as_pattern();
        let lower_right_coord = self
            .geo_transform
            .grid_2d_to_coordinate_2d((tile_y_size, tile_x_size));
        BoundingBox2D::new_upper_left_lower_right_unchecked(top_left_coord, lower_right_coord)
    }
}

pub struct GdalSource {
    pub time_interval_provider: Vec<TimeInterval>,
    pub grid_tile_provider: GdalSourceTileGridProvider,
    pub gdal_params: GdalSourceParameters,
}

impl GdalSource {
    ///
    /// Generates a new `GdalSource` from the provided parameters
    /// TODO: move the time interval and grid tile information generation somewhere else...
    ///
    pub fn from_params(params: GdalSourceParameters) -> Self {
        // TODO load time and tile info from somewhere
        let global_size_in_pixels = (1800, 3600);
        let tile_size_in_pixels = (600, 600);
        let dataset_upper_right_coord = (-180.0, 90.0).into();
        let dataset_x_pixel_size = 0.1;
        let dataset_y_pixel_size = -0.1;
        let dataset_geo_transform = GeoTransform::new(
            dataset_upper_right_coord,
            dataset_x_pixel_size,
            dataset_y_pixel_size,
        );

        let grid_tile_provider = GdalSourceTileGridProvider {
            global_pixel_size: global_size_in_pixels.into(),
            tile_pixel_size: tile_size_in_pixels.into(),
            dataset_geo_transform,
        };

        let time_interval_provider = vec![TimeInterval::default()];

        GdalSource {
            time_interval_provider,
            grid_tile_provider,
            gdal_params: params,
        }
    }

    ///
    /// An iterator which will produce one element per time step and grid tile
    ///
    pub fn time_tile_iter(
        &self,
        bbox: Option<BoundingBox2D>,
    ) -> impl Iterator<Item = (TimeInterval, TileInformation)> + '_ {
        let time_interval_iterator = self.time_interval_provider.clone().into_iter();
        time_interval_iterator.flat_map(move |time| {
            self.grid_tile_provider
                .tile_informations()
                .into_iter()
                .map(move |tile| (time, tile))
                .filter(move |(_, tile)| {
                    if let Some(filter_bbox) = bbox {
                        filter_bbox.intersects_bbox(&tile.spatial_bounds())
                    } else {
                        true
                    }
                })
        })
    }

    pub async fn load_tile_data_async(
        gdal_params: GdalSourceParameters,
        time_interval: TimeInterval,
        tile_information: TileInformation,
    ) -> Result<RasterTile2D> {
        tokio::task::spawn_blocking(move || {
            GdalSource::load_tile_data_impl(&gdal_params, time_interval, tile_information)
        })
        .await
        .unwrap() // TODO: handle TaskJoinError
    }

    pub fn load_tile_data(
        &self,
        time_interval: TimeInterval,
        tile_information: TileInformation,
    ) -> Result<RasterTile2D> {
        GdalSource::load_tile_data_impl(&self.gdal_params, time_interval, tile_information)
    }

    ///
    /// A method to load single tiles from a GDAL dataset.
    ///
    fn load_tile_data_impl(
        gdal_params: &GdalSourceParameters,
        time_interval: TimeInterval,
        tile_information: TileInformation,
    ) -> Result<RasterTile2D> {
        // format the time interval
        let time_string = time_interval
            .start()
            .as_naive_date_time()
            .map(|t| t.format(&gdal_params.time_format).to_string());

        // TODO: replace -> parser?
        let file_name = gdal_params.file_name_with_time_placeholder.replace(
            "%%%_START_TIME_%%%",
            &time_string.unwrap_or_else(|| "".into()),
        );

        let path = &gdal_params.base_path.join(&file_name);

        // open the dataset at path (or 'throw' an error)
        let dataset = GdalDataset::open(&path)?; // TODO: investigate if we need a dataset cache
                                                 // get the geo transform (pixel size ...) of the dataset (or 'throw' an error)
                                                 // let gdal_geo_transform = dataset.geo_transform()?;
                                                 // let geo_transform = GeoTransform::from(gdal_geo_transform); //TODO: clip the geotransform information / is this required at all?

        // get the requested raster band of the dataset …
        let rasterband_index = gdal_params.channel.unwrap_or(1) as isize; // TODO: investigate if this should be isize in gdal
        let rasterband: GdalRasterBand = dataset.rasterband(rasterband_index)?;

        let (.., y_pixel_position, x_pixel_position) =
            tile_information.global_pixel_position().as_pattern();
        let (.., y_tile_size, x_tile_size) = tile_information.tile_size_in_pixels.as_pattern();
        // read the data from the rasterband
        let pixel_origin = (x_pixel_position as isize, y_pixel_position as isize);
        let pixel_size = (x_tile_size, y_tile_size);

        // TODO: create buffer with type of the raster band
        let buffer = rasterband.read_as::<u8>(
            pixel_origin, // pixelspace origin
            pixel_size,   // pixelspace size
            pixel_size,   /* requested raster size */
        )?;

        // TODO: get the raster metadata!

        let raster_result = Raster2D::new(
            tile_information.tile_size_in_pixels,
            buffer.data,
            None,
            time_interval,
            tile_information.geo_transform,
        )?;
        Ok(RasterTile2D::new(
            time_interval,
            tile_information,
            TypedRaster2D::U8(raster_result), // TODO: create TypedRaster for type of band
        ))
    }

    ///
    /// A stream of `RasterTile2D`
    ///
    pub fn tile_stream(&self, bbox: Option<BoundingBox2D>) -> BoxStream<Result<RasterTile2D>> {
        stream::iter(self.time_tile_iter(bbox))
            .map(move |(time, tile)| (self.gdal_params.clone(), time, tile))
            .then(|(gdal_params, time, tile)| {
                GdalSource::load_tile_data_async(gdal_params, time, tile)
            })
            .boxed()
    }
}

impl QueryProcessor<RasterTile2D> for GdalSource {
    fn query(
        &self,
        query: crate::engine::QueryRectangle,
        _ctx: crate::engine::QueryContext,
    ) -> BoxStream<Result<RasterTile2D>> {
        self.tile_stream(Some(query.bbox)).boxed() // TODO: handle query, ctx, remove one boxed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::util::Result;
    use futures::executor::block_on_stream;
    use geoengine_datatypes::raster::GridPixelAccess;

    #[test]
    fn tile_informations() {
        let global_size_in_pixels = (1800, 3600);
        let tile_size_in_pixels = (600, 600);
        let global_size_in_tiles = (3, 6);
        let dataset_upper_right_coord = (-180.0, 90.0).into();
        let dataset_x_pixel_size = 0.1;
        let dataset_y_pixel_size = -0.1;
        let dataset_geo_transform = GeoTransform::new(
            dataset_upper_right_coord,
            dataset_x_pixel_size,
            dataset_y_pixel_size,
        );

        let grid_tile_provider = GdalSourceTileGridProvider {
            global_pixel_size: global_size_in_pixels.into(),
            tile_pixel_size: tile_size_in_pixels.into(),
            dataset_geo_transform,
        };

        let vres: Vec<TileInformation> = grid_tile_provider.tile_informations();
        assert_eq!(vres.len(), 6 * 3);
        assert_eq!(
            vres[0],
            TileInformation::new(
                global_size_in_tiles.into(),
                (0, 0).into(),
                (0, 0).into(),
                tile_size_in_pixels.into(),
                dataset_geo_transform
            )
        );
        assert_eq!(
            vres[1],
            TileInformation::new(
                global_size_in_tiles.into(),
                (0, 1).into(),
                (0, 600).into(),
                tile_size_in_pixels.into(),
                GeoTransform::new(
                    (-120.0, 90.0).into(),
                    dataset_x_pixel_size,
                    dataset_y_pixel_size
                )
            )
        );
        assert_eq!(
            vres[6],
            TileInformation::new(
                global_size_in_tiles.into(),
                (1, 0).into(),
                (600, 0).into(),
                tile_size_in_pixels.into(),
                GeoTransform::new(
                    (-180.0, 30.0).into(),
                    dataset_x_pixel_size,
                    dataset_y_pixel_size
                )
            )
        );
        assert_eq!(
            vres[7],
            TileInformation::new(
                global_size_in_tiles.into(),
                (1, 1).into(),
                (600, 600).into(),
                tile_size_in_pixels.into(),
                GeoTransform::new(
                    (-120.0, 30.0).into(),
                    dataset_x_pixel_size,
                    dataset_y_pixel_size
                )
            )
        );
        assert_eq!(
            vres[10],
            TileInformation::new(
                global_size_in_tiles.into(),
                (1, 4).into(),
                (600, 2400).into(),
                tile_size_in_pixels.into(),
                GeoTransform::new(
                    (60.0, 30.0).into(),
                    dataset_x_pixel_size,
                    dataset_y_pixel_size
                )
            )
        );
        assert_eq!(
            vres[16],
            TileInformation::new(
                global_size_in_tiles.into(),
                (2, 4).into(),
                (1200, 2400).into(),
                tile_size_in_pixels.into(),
                GeoTransform::new(
                    (60.0, -30.0).into(),
                    dataset_x_pixel_size,
                    dataset_y_pixel_size
                )
            )
        );
    }

    #[allow(clippy::too_many_lines)]
    #[test]
    fn test_time_tile_iter() {
        let global_size_in_pixels = (1800, 3600);
        let tile_size_in_pixels = (600, 600);
        let global_size_in_tiles = (3, 6);
        let dataset_upper_right_coord = (-180.0, 90.0).into();
        let dataset_x_pixel_size = 0.1;
        let dataset_y_pixel_size = -0.1;
        let dataset_geo_transform = GeoTransform::new(
            dataset_upper_right_coord,
            dataset_x_pixel_size,
            dataset_y_pixel_size,
        );

        let grid_tile_provider = GdalSourceTileGridProvider {
            global_pixel_size: global_size_in_pixels.into(),
            tile_pixel_size: tile_size_in_pixels.into(),
            dataset_geo_transform,
        };

        let time_interval_provider = vec![
            TimeInterval::new_unchecked(1, 2),
            TimeInterval::new_unchecked(2, 3),
        ];

        let gdal_params = GdalSourceParameters {
            base_path: "".into(),
            file_name_with_time_placeholder: "".into(),
            time_format: "".into(),
            channel: None,
        };

        let gdal_source = GdalSource {
            time_interval_provider,
            grid_tile_provider,
            gdal_params,
        };

        let vres: Vec<_> = gdal_source.time_tile_iter(None).collect();

        assert_eq!(
            vres[0],
            (
                TimeInterval::new_unchecked(1, 2),
                TileInformation::new(
                    global_size_in_tiles.into(),
                    (0, 0).into(),
                    (0, 0).into(),
                    tile_size_in_pixels.into(),
                    dataset_geo_transform
                )
            )
        );
        assert_eq!(
            vres[1],
            (
                TimeInterval::new_unchecked(1, 2),
                TileInformation::new(
                    global_size_in_tiles.into(),
                    (0, 1).into(),
                    (0, 600).into(),
                    tile_size_in_pixels.into(),
                    GeoTransform::new(
                        (-120.0, 90.0).into(),
                        dataset_x_pixel_size,
                        dataset_y_pixel_size
                    )
                )
            )
        );
        assert_eq!(
            vres[6],
            (
                TimeInterval::new_unchecked(1, 2),
                TileInformation::new(
                    global_size_in_tiles.into(),
                    (1, 0).into(),
                    (600, 0).into(),
                    tile_size_in_pixels.into(),
                    GeoTransform::new(
                        (-180.0, 30.0).into(),
                        dataset_x_pixel_size,
                        dataset_y_pixel_size
                    )
                )
            )
        );
        assert_eq!(
            vres[7],
            (
                TimeInterval::new_unchecked(1, 2),
                TileInformation::new(
                    global_size_in_tiles.into(),
                    (1, 1).into(),
                    (600, 600).into(),
                    tile_size_in_pixels.into(),
                    GeoTransform::new(
                        (-120.0, 30.0).into(),
                        dataset_x_pixel_size,
                        dataset_y_pixel_size
                    )
                )
            )
        );
        assert_eq!(
            vres[10],
            (
                TimeInterval::new_unchecked(1, 2),
                TileInformation::new(
                    global_size_in_tiles.into(),
                    (1, 4).into(),
                    (600, 2400).into(),
                    tile_size_in_pixels.into(),
                    GeoTransform::new(
                        (60.0, 30.0).into(),
                        dataset_x_pixel_size,
                        dataset_y_pixel_size
                    )
                )
            )
        );
        assert_eq!(
            vres[16],
            (
                TimeInterval::new_unchecked(1, 2),
                TileInformation::new(
                    global_size_in_tiles.into(),
                    (2, 4).into(),
                    (1200, 2400).into(),
                    tile_size_in_pixels.into(),
                    GeoTransform::new(
                        (60.0, -30.0).into(),
                        dataset_x_pixel_size,
                        dataset_y_pixel_size
                    )
                )
            )
        );
    }
    #[test]
    fn test_load_tile_data() {
        let global_size_in_pixels = (1800, 3600);
        let tile_size_in_pixels = (600, 600);
        let global_size_in_tiles = (3, 6);
        let dataset_upper_right_coord = (-180.0, 90.0).into();
        let dataset_x_pixel_size = 0.1;
        let dataset_y_pixel_size = -0.1;
        let dataset_geo_transform = GeoTransform::new(
            dataset_upper_right_coord,
            dataset_x_pixel_size,
            dataset_y_pixel_size,
        );

        let grid_tile_provider = GdalSourceTileGridProvider {
            global_pixel_size: global_size_in_pixels.into(),
            tile_pixel_size: tile_size_in_pixels.into(),
            dataset_geo_transform,
        };

        let time_interval_provider = vec![TimeInterval::new_unchecked(0, 1)];

        let gdal_params = GdalSourceParameters {
            base_path: "../operators/test-data/raster/modis_ndvi".into(),
            file_name_with_time_placeholder: "MOD13A2_M_NDVI_2014-01-01.TIFF".into(),
            time_format: "".into(),
            channel: None,
        };

        let gdal_source = GdalSource {
            time_interval_provider,
            grid_tile_provider,
            gdal_params,
        };

        let tile_information = TileInformation::new(
            global_size_in_tiles.into(),
            (0, 0).into(),
            (0, 0).into(),
            tile_size_in_pixels.into(),
            dataset_geo_transform,
        );
        let time_interval = TimeInterval::new_unchecked(0, 1);

        let x = gdal_source
            .load_tile_data(time_interval, tile_information)
            .unwrap();

        assert_eq!(x.tile, tile_information);
        assert_eq!(x.time, time_interval);
    }

    #[test]
    fn test_iter_and_load_tile_data() {
        let global_size_in_pixels = (1800, 3600);
        let tile_size_in_pixels = (600, 600);
        let dataset_upper_right_coord = (-180.0, 90.0).into();
        let dataset_x_pixel_size = 0.1;
        let dataset_y_pixel_size = -0.1;
        let dataset_geo_transform = GeoTransform::new(
            dataset_upper_right_coord,
            dataset_x_pixel_size,
            dataset_y_pixel_size,
        );

        let grid_tile_provider = GdalSourceTileGridProvider {
            global_pixel_size: global_size_in_pixels.into(),
            tile_pixel_size: tile_size_in_pixels.into(),
            dataset_geo_transform,
        };

        let time_interval_provider = vec![TimeInterval::new_unchecked(1, 2)];

        let gdal_params = GdalSourceParameters {
            base_path: "../operators/test-data/raster/modis_ndvi".into(),
            file_name_with_time_placeholder: "MOD13A2_M_NDVI_2014-01-01.TIFF".into(),
            time_format: "".into(),
            channel: None,
        };

        let gdal_source = GdalSource {
            time_interval_provider,
            grid_tile_provider,
            gdal_params,
        };

        let vres: Vec<Result<RasterTile2D, Error>> = gdal_source
            .time_tile_iter(None)
            .map(|(time_interval, tile_information)| {
                gdal_source.load_tile_data(time_interval, tile_information)
            })
            .collect();
        assert_eq!(vres.len(), 3 * 6);
        let upper_left_pixels: Vec<_> = vres
            .into_iter()
            .map(|t| {
                let raster_tile = t.unwrap();
                if let TypedRaster2D::U8(tile_data) = raster_tile.data {
                    tile_data
                        .pixel_value_at_grid_index(&(
                            tile_size_in_pixels.1 / 2,
                            tile_size_in_pixels.0 / 2,
                        ))
                        .unwrap() // pixel
                } else {
                    unreachable!();
                }
            })
            .collect();

        let ndvi_center_pixel_values = vec![
            19, 255, 255, 43, 76, 17, 255, 255, 255, 145, 255, 255, 255, 255, 255, 255, 255, 255,
        ];

        assert_eq!(upper_left_pixels, ndvi_center_pixel_values);
    }

    #[test]
    fn test_iter_and_load_tile_data_bbox() {
        let global_size_in_pixels = (1800, 3600);
        let tile_size_in_pixels = (600, 600);
        let dataset_upper_right_coord = (-180.0, 90.0).into();
        let dataset_x_pixel_size = 0.1;
        let dataset_y_pixel_size = -0.1;
        let dataset_geo_transform = GeoTransform::new(
            dataset_upper_right_coord,
            dataset_x_pixel_size,
            dataset_y_pixel_size,
        );

        let grid_tile_provider = GdalSourceTileGridProvider {
            global_pixel_size: global_size_in_pixels.into(),
            tile_pixel_size: tile_size_in_pixels.into(),
            dataset_geo_transform,
        };

        let time_interval_provider = vec![TimeInterval::new_unchecked(1, 2)];

        let gdal_params = GdalSourceParameters {
            base_path: "../operators/test-data/raster/modis_ndvi".into(),
            file_name_with_time_placeholder: "MOD13A2_M_NDVI_2014-01-01.TIFF".into(),
            time_format: "".into(),
            channel: None,
        };

        let gdal_source = GdalSource {
            time_interval_provider,
            grid_tile_provider,
            gdal_params,
        };

        let query_bbox = BoundingBox2D::new((-30., 0.).into(), (35., 65.).into()).unwrap();

        let vres: Vec<Result<RasterTile2D, Error>> = gdal_source
            .time_tile_iter(Some(query_bbox))
            .map(|(time_interval, tile_information)| {
                gdal_source.load_tile_data(time_interval, tile_information)
            })
            .collect();
        assert_eq!(vres.len(), 2 * 2);
        let upper_left_pixels: Vec<_> = vres
            .into_iter()
            .map(|t| {
                let raster_tile = t.unwrap();
                if let TypedRaster2D::U8(tile_data) = raster_tile.data {
                    tile_data
                        .pixel_value_at_grid_index(&(
                            tile_size_in_pixels.1 / 2,
                            tile_size_in_pixels.0 / 2,
                        ))
                        .unwrap() // pixel
                } else {
                    unreachable!();
                }
            })
            .collect();

        let ndvi_center_pixel_values = vec![255, 43, 255, 145];

        assert_eq!(upper_left_pixels, ndvi_center_pixel_values);
    }

    #[tokio::test]
    async fn test_tile_stream_len() {
        let global_size_in_pixels = (1800, 3600);
        let tile_size_in_pixels = (600, 600);
        let dataset_upper_right_coord = (-180.0, 90.0).into();
        let dataset_x_pixel_size = 0.1;
        let dataset_y_pixel_size = -0.1;
        let dataset_geo_transform = GeoTransform::new(
            dataset_upper_right_coord,
            dataset_x_pixel_size,
            dataset_y_pixel_size,
        );

        let grid_tile_provider = GdalSourceTileGridProvider {
            global_pixel_size: global_size_in_pixels.into(),
            tile_pixel_size: tile_size_in_pixels.into(),
            dataset_geo_transform,
        };

        let time_interval_provider = vec![TimeInterval::new_unchecked(1, 2)];

        let gdal_params = GdalSourceParameters {
            base_path: "../operators/test-data/raster/modis_ndvi".into(),
            file_name_with_time_placeholder: "MOD13A2_M_NDVI_2014-01-01.TIFF".into(),
            time_format: "".into(),
            channel: None,
        };

        let gdal_source = GdalSource {
            time_interval_provider,
            grid_tile_provider,
            gdal_params,
        };

        let mut stream_data = block_on_stream(gdal_source.tile_stream(None));

        let ndvi_center_pixel_values = vec![
            19, 255, 255, 43, 76, 17, 255, 255, 255, 145, 255, 255, 255, 255, 255, 255, 255, 255,
        ];

        for p in ndvi_center_pixel_values {
            let tile = stream_data.next().unwrap().unwrap();
            if let TypedRaster2D::U8(tile_data) = tile.data {
                let cp = tile_data
                    .pixel_value_at_grid_index(&(
                        tile_size_in_pixels.1 / 2,
                        tile_size_in_pixels.0 / 2,
                    ))
                    .unwrap();

                assert_eq!(p, cp);
            } else {
                unreachable!();
            }
        }

        assert!(stream_data.next().is_none());
    }

    #[tokio::test]
    async fn test_load_tile_data_async() {
        let tile_size_in_pixels = (600, 600);
        let global_size_in_tiles = (3, 6);
        let dataset_upper_right_coord = (-180.0, 90.0).into();
        let dataset_x_pixel_size = 0.1;
        let dataset_y_pixel_size = -0.1;
        let dataset_geo_transform = GeoTransform::new(
            dataset_upper_right_coord,
            dataset_x_pixel_size,
            dataset_y_pixel_size,
        );

        let gdal_params = GdalSourceParameters {
            base_path: "../operators/test-data/raster/modis_ndvi".into(),
            file_name_with_time_placeholder: "MOD13A2_M_NDVI_2014-01-01.TIFF".into(),
            time_format: "".into(),
            channel: None,
        };

        let tile_information = TileInformation::new(
            global_size_in_tiles.into(),
            (0, 0).into(),
            (0, 0).into(),
            tile_size_in_pixels.into(),
            dataset_geo_transform,
        );
        let time_interval = TimeInterval::new_unchecked(0, 1);

        let x_r =
            GdalSource::load_tile_data_async(gdal_params, time_interval, tile_information).await;
        let x = x_r.expect("GDAL Error");

        assert_eq!(x.tile, tile_information);
        assert_eq!(x.time, time_interval);
        if let TypedRaster2D::U8(tile_data) = x.data {
            let center_pixel = tile_data
                .pixel_value_at_grid_index(&(tile_size_in_pixels.1 / 2, tile_size_in_pixels.0 / 2))
                .unwrap();
            assert_eq!(center_pixel, 19);
        } else {
            unreachable!();
        }
    }
}
