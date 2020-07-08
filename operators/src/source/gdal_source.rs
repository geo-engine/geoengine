use crate::util::Result;

use gdal::raster::dataset::Dataset as GdalDataset;
use gdal::raster::rasterband::RasterBand as GdalRasterBand;
use std::path::PathBuf;
//use gdal::metadata::Metadata;

use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use serde::{Deserialize, Serialize};

use num::Integer;

use futures::stream::{self, BoxStream, StreamExt};

use geoengine_datatypes::primitives::TimeInterval;
use geoengine_datatypes::raster::{Dim, GeoTransform, Ix, Raster2D};

#[derive(Eq, PartialEq, Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Tick {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
}

impl Tick {
    pub fn new(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> Self {
        Self {
            year,
            month,
            day,
            hour,
            minute,
            second,
        }
    }

    pub fn snap_date<T: Datelike>(&self, date: &T) -> NaiveDate {
        let y = date.year().div_floor(&self.year) * self.year;
        let m = date.month().div_floor(&self.month) * self.month;
        let d = date.day().div_floor(&self.day) * self.day;
        NaiveDate::from_ymd(y, m, d)
    }

    pub fn snap_time<T: Timelike>(&self, time: &T) -> NaiveTime {
        let h = time.hour().div_floor(&self.hour) * self.hour;
        let m = time.minute().div_floor(&self.minute) * self.minute;
        let s = time.second().div_floor(&self.second) * self.second;
        NaiveTime::from_hms(h, m, s)
    }

    pub fn snap_datetime<T: Datelike + Timelike>(&self, datetime: &T) -> NaiveDateTime {
        NaiveDateTime::new(self.snap_date(datetime), self.snap_time(datetime))
    }
}

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
///                     tick: None,
///                     channel: Some(3),
///     },
///     sources: Default::default(),
/// });
/// ```
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GdalSourceParameters {
    pub base_path: PathBuf,
    pub file_name_with_time_placeholder: String,
    pub time_format: String,
    pub tick: Option<Tick>,
    pub channel: Option<u32>,
}

/// A provider of tile (size) information for a raster/grid
pub struct GdalSourceTileGridProvider {
    pub global_pixel_size: Dim<[Ix; 2]>,
    pub tile_pixel_size: Dim<[Ix; 2]>,
    // pub grid_tiles : Vec<Dim<[Ix; 2]>>,
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
                tile_information.push(TileInformation::new(
                    (y_tiles, x_tiles).into(),
                    (yi, xi).into(),
                    (y, x).into(),
                    (y_pixels_tile, x_pixels_tile).into(),
                ))
            }
        }
        tile_information
    }
}

/// A `RasterTile2D` is the main type used to iterate over tiles of 2D raster data
#[derive(Debug)]
pub struct RasterTile2D<T> {
    time: TimeInterval,
    tile: TileInformation,
    data: Raster2D<T>,
}

impl<T> RasterTile2D<T> {
    /// create a new `RasterTile2D`
    pub fn new(time: TimeInterval, tile: TileInformation, data: Raster2D<T>) -> Self {
        Self { time, tile, data }
    }
}

/// The `TileInformation` is used to represent the spatial position of each tile
#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct TileInformation {
    global_size_in_tiles: Dim<[Ix; 2]>,
    global_tile_position: Dim<[Ix; 2]>,
    global_pixel_position: Dim<[Ix; 2]>,
    tile_size_in_pixels: Dim<[Ix; 2]>,
}

impl TileInformation {
    pub fn new(
        global_size_in_tiles: Dim<[Ix; 2]>,
        global_tile_position: Dim<[Ix; 2]>,
        global_pixel_position: Dim<[Ix; 2]>,
        tile_size_in_pixels: Dim<[Ix; 2]>,
    ) -> Self {
        Self {
            global_size_in_tiles,
            global_tile_position,
            global_pixel_position,
            tile_size_in_pixels,
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

pub struct GdalSource {
    time_interval_provider: Vec<TimeInterval>,
    grid_tile_provider: GdalSourceTileGridProvider,
    gdal_params: GdalSourceParameters,
}

impl GdalSource {
    ///
    /// Generates a new `GdalSource` from the provided parameters
    /// TODO: move the time interval and grid tile information generation somewhere else...
    ///
    pub fn from_params(
        params: GdalSourceParameters,
        time_interval_provider: Vec<TimeInterval>,
        grid_tile_provider: GdalSourceTileGridProvider,
    ) -> Self {
        GdalSource {
            time_interval_provider,
            grid_tile_provider,
            gdal_params: params,
        }
    }

    ///
    /// An iterator which will produce one element per time step and grid tile
    ///
    pub fn time_tile_iter(&self) -> impl Iterator<Item = (TimeInterval, TileInformation)> + '_ {
        // )>
        let time_interval_iterator = self.time_interval_provider.clone().into_iter();
        time_interval_iterator.flat_map(move |time| {
            self.grid_tile_provider
                .tile_informations()
                .into_iter()
                .map(move |tile| (time, tile))
        })
    }

    ///
    /// A method to load single tiles from a GDAL dataset.
    ///
    pub fn load_tile_data<T: gdal::raster::types::GdalType + Copy>(
        &self,
        time_interval: &TimeInterval,
        tile_information: &TileInformation,
    ) -> Result<RasterTile2D<T>> {
        // snap the time interval start instance to the dataset tick configuration
        let time_string = self.gdal_params.tick.map(|t| {
            t.snap_datetime(&time_interval.start().as_naive_date_time())
                .format(&self.gdal_params.time_format)
                .to_string()
        });

        let file_name = self.gdal_params.file_name_with_time_placeholder.replace(
            "%%%_START_TIME_%%%",
            &time_string.unwrap_or_else(|| "".into()),
        );

        let path = self.gdal_params.base_path.join(&file_name);

        // open the dataset at path (or 'throw' an error)
        let dataset = GdalDataset::open(&path).expect("cant open dataset");
        // get the geo transform (pixel size ...) of the dataset (or 'throw' an error)
        let gdal_geo_transform = dataset.geo_transform()?;
        let geo_transform = GeoTransform::from(gdal_geo_transform);
        // TODO: clip the geotransform information?

        // get the requested raster band of the dataset …
        let rasterband_index = self.gdal_params.channel.unwrap_or(1) as isize; // TODO: investigate if this should be isize in gdal
        let rasterband: GdalRasterBand = dataset.rasterband(rasterband_index)?;

        let &[.., y_pixel_position, x_pixel_position] =
            tile_information.global_pixel_position().dimension_size();
        let &[.., y_tile_size, x_tile_size] = tile_information.tile_size_in_pixels.dimension_size();
        // read the data from the rasterband
        let pixel_origin = (x_pixel_position as isize, y_pixel_position as isize);
        let pixel_size = (x_tile_size, y_tile_size);
        let buffer = rasterband.read_as::<T>(
            pixel_origin, // pixelspace origin
            pixel_size,   // pixelspace size
            pixel_size,   /* requested raster size */
        )?;
        // println!("pixel_origin: {:?}, pixel_size: {:?}, size: {:?}", pixel_origin, pixel_size, pixel_size);

        // TODO: get the raster metadata!

        let raster_result = Raster2D::new(
            tile_information.tile_size_in_pixels,
            buffer.data,
            None,
            *time_interval,
            geo_transform,
        )?;
        Ok(RasterTile2D::new(
            *time_interval,
            tile_information.clone(),
            raster_result,
        ))
    }

    ///
    /// A stream of `RasterTile2D`
    ///
    pub fn tile_stream<T: gdal::raster::types::GdalType + Copy>(
        &self,
    ) -> BoxStream<Result<RasterTile2D<T>>> {
        stream::iter(self.time_tile_iter())
            .map(move |(time_interval, tile_information)| {
                self.load_tile_data::<T>(&time_interval, &tile_information)
            })
            .boxed()
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
        let global_size_in_pixels = (6000, 6000);
        let tile_size_in_pixels = (1000, 1000);
        let global_size_in_tiles = (6, 6);

        let grid_tile_provider = GdalSourceTileGridProvider {
            global_pixel_size: global_size_in_pixels.into(),
            tile_pixel_size: tile_size_in_pixels.into(),
        };

        let vres: Vec<TileInformation> = grid_tile_provider.tile_informations();
        assert_eq!(vres.len(), 6 * 6);
        assert_eq!(
            vres[0],
            TileInformation::new(
                global_size_in_tiles.into(),
                (0, 0).into(),
                (0, 0).into(),
                tile_size_in_pixels.into()
            )
        );
        assert_eq!(
            vres[1],
            TileInformation::new(
                global_size_in_tiles.into(),
                (0, 1).into(),
                (0, 1000).into(),
                tile_size_in_pixels.into()
            )
        );
        assert_eq!(
            vres[6],
            TileInformation::new(
                global_size_in_tiles.into(),
                (1, 0).into(),
                (1000, 0).into(),
                tile_size_in_pixels.into()
            )
        );
        assert_eq!(
            vres[7],
            TileInformation::new(
                global_size_in_tiles.into(),
                (1, 1).into(),
                (1000, 1000).into(),
                tile_size_in_pixels.into()
            )
        );
        assert_eq!(
            vres[34],
            TileInformation::new(
                global_size_in_tiles.into(),
                (5, 4).into(),
                (5000, 4000).into(),
                tile_size_in_pixels.into()
            )
        );
        assert_eq!(
            vres[35],
            TileInformation::new(
                global_size_in_tiles.into(),
                (5, 5).into(),
                (5000, 5000).into(),
                tile_size_in_pixels.into()
            )
        );
    }

    #[test]
    fn test_time_tile_iter() {
        let global_size_in_pixels = (6000, 6000);
        let tile_size_in_pixels = (1000, 1000);
        let global_size_in_tiles = (6, 6);

        let grid_tile_provider = GdalSourceTileGridProvider {
            global_pixel_size: global_size_in_pixels.into(),
            tile_pixel_size: tile_size_in_pixels.into(),
        };

        let time_interval_provider = vec![
            TimeInterval::new_unchecked(1, 2),
            TimeInterval::new_unchecked(2, 3),
        ];

        let gdal_params = GdalSourceParameters {
            base_path: "".into(),
            file_name_with_time_placeholder: "".into(),
            time_format: "".into(),
            tick: None,
            channel: None,
        };

        let gdal_source = GdalSource {
            time_interval_provider,
            grid_tile_provider,
            gdal_params,
        };

        let vres: Vec<_> = gdal_source.time_tile_iter().collect();

        assert_eq!(
            vres[0],
            (
                TimeInterval::new_unchecked(1, 2),
                TileInformation::new(
                    global_size_in_tiles.into(),
                    (0, 0).into(),
                    (0, 0).into(),
                    tile_size_in_pixels.into()
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
                    (0, 1000).into(),
                    tile_size_in_pixels.into()
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
                    (1000, 0).into(),
                    tile_size_in_pixels.into()
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
                    (1000, 1000).into(),
                    tile_size_in_pixels.into()
                )
            )
        );
        assert_eq!(
            vres[34],
            (
                TimeInterval::new_unchecked(1, 2),
                TileInformation::new(
                    global_size_in_tiles.into(),
                    (5, 4).into(),
                    (5000, 4000).into(),
                    tile_size_in_pixels.into()
                )
            )
        );
        assert_eq!(
            vres[35],
            (
                TimeInterval::new_unchecked(1, 2),
                TileInformation::new(
                    global_size_in_tiles.into(),
                    (5, 5).into(),
                    (5000, 5000).into(),
                    tile_size_in_pixels.into()
                )
            )
        );

        assert_eq!(
            vres[36],
            (
                TimeInterval::new_unchecked(2, 3),
                TileInformation::new(
                    global_size_in_tiles.into(),
                    (0, 0).into(),
                    (0, 0).into(),
                    tile_size_in_pixels.into()
                )
            )
        );
        assert_eq!(
            vres[36 + 1],
            (
                TimeInterval::new_unchecked(2, 3),
                TileInformation::new(
                    global_size_in_tiles.into(),
                    (0, 1).into(),
                    (0, 1000).into(),
                    tile_size_in_pixels.into()
                )
            )
        );
        assert_eq!(
            vres[36 + 6],
            (
                TimeInterval::new_unchecked(2, 3),
                TileInformation::new(
                    global_size_in_tiles.into(),
                    (1, 0).into(),
                    (1000, 0).into(),
                    tile_size_in_pixels.into()
                )
            )
        );
        assert_eq!(
            vres[36 + 7],
            (
                TimeInterval::new_unchecked(2, 3),
                TileInformation::new(
                    global_size_in_tiles.into(),
                    (1, 1).into(),
                    (1000, 1000).into(),
                    tile_size_in_pixels.into()
                )
            )
        );
        assert_eq!(
            vres[36 + 34],
            (
                TimeInterval::new_unchecked(2, 3),
                TileInformation::new(
                    global_size_in_tiles.into(),
                    (5, 4).into(),
                    (5000, 4000).into(),
                    tile_size_in_pixels.into()
                )
            )
        );
        assert_eq!(
            vres[36 + 35],
            (
                TimeInterval::new_unchecked(2, 3),
                TileInformation::new(
                    global_size_in_tiles.into(),
                    (5, 5).into(),
                    (5000, 5000).into(),
                    tile_size_in_pixels.into()
                )
            )
        );
    }
    #[test]
    fn test_load_tile_data() {
        let global_size_in_pixels = (1800, 3600);
        let tile_size_in_pixels = (600, 600);
        let global_size_in_tiles = (3, 6);

        let grid_tile_provider = GdalSourceTileGridProvider {
            global_pixel_size: global_size_in_pixels.into(),
            tile_pixel_size: tile_size_in_pixels.into(),
        };

        let time_interval_provider = vec![TimeInterval::new_unchecked(0, 1)];

        let gdal_params = GdalSourceParameters {
            base_path: "../operators/test-data/raster/modis_ndvi".into(),
            file_name_with_time_placeholder: "MOD13A2_M_NDVI_2014-01-01.TIFF".into(),
            time_format: "".into(),
            tick: None,
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
        );
        let time_interval = TimeInterval::new_unchecked(0, 1);

        let x = gdal_source
            .load_tile_data::<f32>(&time_interval, &tile_information)
            .unwrap();

        assert_eq!(x.tile, tile_information);
        assert_eq!(x.time, time_interval);
    }

    #[test]
    fn test_iter_and_load_tile_data() {
        let global_size_in_pixels = (1800, 3600);
        let tile_size_in_pixels = (600, 600);
        let ndvi_center_pixel_values = vec![
            19, 255, 255, 43, 76, 17, 255, 255, 255, 145, 255, 255, 255, 255, 255, 255, 255, 255,
        ];

        let grid_tile_provider = GdalSourceTileGridProvider {
            global_pixel_size: global_size_in_pixels.into(),
            tile_pixel_size: tile_size_in_pixels.into(),
        };

        let time_interval_provider = vec![TimeInterval::new_unchecked(1, 2)];

        let gdal_params = GdalSourceParameters {
            base_path: "../operators/test-data/raster/modis_ndvi".into(),
            file_name_with_time_placeholder: "MOD13A2_M_NDVI_2014-01-01.TIFF".into(),
            time_format: "".into(),
            tick: None,
            channel: None,
        };

        let gdal_source = GdalSource {
            time_interval_provider,
            grid_tile_provider,
            gdal_params,
        };

        let vres: Vec<Result<RasterTile2D<u8>, Error>> = gdal_source
            .time_tile_iter()
            .map(|(time_interval, tile_information)| {
                gdal_source.load_tile_data(&time_interval, &tile_information)
            })
            .collect();
        assert_eq!(vres.len(), 3 * 6);
        let upper_left_pixels: Vec<_> = vres
            .into_iter()
            .map(|t| {
                let raster_tile = t.unwrap();
                let tile_data = raster_tile.data;
                tile_data
                    .pixel_value_at_grid_index(&(
                        tile_size_in_pixels.1 / 2,
                        tile_size_in_pixels.0 / 2,
                    ))
                    .unwrap() // pixel
            })
            .collect();
        assert_eq!(upper_left_pixels, ndvi_center_pixel_values);
    }

    #[test]
    fn test_tile_stream_len() {
        let global_size_in_pixels = (1800, 3600);
        let tile_size_in_pixels = (600, 600);
        let ndvi_center_pixel_values = vec![
            19, 255, 255, 43, 76, 17, 255, 255, 255, 145, 255, 255, 255, 255, 255, 255, 255, 255,
        ];

        let grid_tile_provider = GdalSourceTileGridProvider {
            global_pixel_size: global_size_in_pixels.into(),
            tile_pixel_size: tile_size_in_pixels.into(),
        };

        let time_interval_provider = vec![TimeInterval::new_unchecked(1, 2)];

        let gdal_params = GdalSourceParameters {
            base_path: "../operators/test-data/raster/modis_ndvi".into(),
            file_name_with_time_placeholder: "MOD13A2_M_NDVI_2014-01-01.TIFF".into(),
            time_format: "".into(),
            tick: None,
            channel: None,
        };

        let gdal_source = GdalSource {
            time_interval_provider,
            grid_tile_provider,
            gdal_params,
        };

        let mut stream_data = block_on_stream(gdal_source.tile_stream::<u8>());

        for p in ndvi_center_pixel_values {
            let tile = stream_data.next().unwrap().unwrap();
            let cp = tile
                .data
                .pixel_value_at_grid_index(&(tile_size_in_pixels.1 / 2, tile_size_in_pixels.0 / 2))
                .unwrap();

            assert_eq!(p, cp);
        }
        assert!(stream_data.next().is_none());
    }
}
