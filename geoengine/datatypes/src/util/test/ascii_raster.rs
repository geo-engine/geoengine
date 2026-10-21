//! Describe lists of raster tiles as ASCII art in tests.
//!
//! A text consists of blocks, one per time step and band.
//! Each block starts with a header `[start, end)` and an optional `band N` (default `0`),
//! followed by the pixels of all tiles of that time step and band.
//! A time instance is either milliseconds (`-10`, `0`), an RFC 3339 date time (`2010-01-01T12:00:00Z`),
//! a date (`2010-01-01`, midnight UTC), or `-inf` and `inf` for the start and end of time
//! ([`TimeInstance::MIN`] and [`TimeInstance::MAX`], e.g., `[-inf, inf)` for [`TimeInterval::default`]):
//!
//! ```text
//! [0, 10)
//! +----------+----------+
//! |  1  2  3 |  4  5  6 |
//! |  7  8  _ | 10 11 12 |
//! +----------+----------+
//! | 13 14 15 |  _  _  _ |
//! |  _ 20 21 |  _  _  _ |
//! +----------+----------+
//!
//! [10, 20) band 1
//! ...
//! ```
//!
//! - `|` separates tile columns and `+---+` lines separate tile rows. The outer frame is optional for border lines.
//! - Values are separated by whitespace. `_` is no-data.
//! - A tile that only consists of `_` becomes an empty tile ([`GridOrEmpty::Empty`]).
//! - Empty lines and indentation are ignored.
//! - All tiles must have the same size, since the tile size is fixed for a whole stream.
//!
//! The tiles are returned in raster stream order: time, then tile position (row-major), then band.
//! The upper left tile of each block is at [`AsciiRasterTiles::tile_origin`].

use crate::{
    primitives::{CacheHint, DateTime, TimeGranularity, TimeInstance, TimeInterval, TimeStep},
    raster::{
        EmptyGrid2D, GeoTransform, Grid2D, GridBoundingBox2D, GridIdx, GridIdx2D, GridIndexAccess,
        GridOrEmpty, GridShape2D, GridSize, MaskedGrid2D, Pixel, RasterTile2D, TileIdx,
        TileInformation, TileSize, TilingSpecification,
    },
    util::test::TestDefault,
};
use chrono::{Datelike, NaiveDate, Timelike};
use float_cmp::approx_eq;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{Display, Write},
    str::FromStr,
};

const NO_DATA: &str = "_";
const MISSING_TILE: &str = "?";

/// How [`raster_tiles_to_ascii_with_time_format`] writes time instances.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AsciiTimeFormat {
    /// Milliseconds, e.g., `[0, 10)`.
    #[default]
    Millis,
    /// Dates for midnight UTC and RFC 3339 date times otherwise, e.g., `[2010-01-01, 2010-01-01T12:00:00.000Z)`.
    DateTime,
}

/// Builder for a list of raster tiles described as ASCII art.
///
/// See the [module documentation](self) for the format.
///
/// ```
/// use geoengine_datatypes::util::test::AsciiRasterTiles;
///
/// let tiles = AsciiRasterTiles::new(
///     "
///     [0, 10)
///     +-----+-----+
///     | 1 2 | 3 _ |
///     | 4 5 | 6 _ |
///     +-----+-----+
///     ",
/// )
/// .tile_origin([-1, 0])
/// .tiles::<u8>();
///
/// assert_eq!(tiles.len(), 2);
/// ```
#[derive(Debug, Clone, Copy)]
pub struct AsciiRasterTiles<'a> {
    text: &'a str,
    tile_origin: GridIdx2D,
    geo_transform: GeoTransform,
    cache_hint: CacheHint,
}

/// Properties of the raster that an [`AsciiRasterTiles`] text describes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AsciiRasterInfo {
    /// From the earliest start to the latest end of all time steps.
    pub time_bounds: TimeInterval,
    /// `Some` if all time steps have the same length and follow each other without gaps.
    /// This is the step with the largest granularity, e.g., one year for `[2010-01-01, 2011-01-01)` or ten
    /// milliseconds for `[0, 10)`.
    pub regular_time_step: Option<TimeStep>,
    /// The pixel bounds covered by all tiles.
    pub pixel_bounds: GridBoundingBox2D,
    pub geo_transform: GeoTransform,
    pub tile_size: GridShape2D,
    /// The highest band index plus one.
    pub num_bands: u32,
}

impl<'a> AsciiRasterTiles<'a> {
    /// Creates a builder with the upper left tile at `[0, 0]`, [`GeoTransform::test_default`] and no cache hint.
    pub fn new(text: &'a str) -> Self {
        Self {
            text,
            tile_origin: [0, 0].into(),
            geo_transform: GeoTransform::test_default(),
            cache_hint: CacheHint::no_cache(),
        }
    }

    /// Sets the tile position (`[y, x]`) of the upper left tile of each block.
    #[must_use]
    pub fn tile_origin(mut self, tile_position: impl Into<GridIdx2D>) -> Self {
        self.tile_origin = tile_position.into();
        self
    }

    #[must_use]
    pub fn geo_transform(mut self, geo_transform: GeoTransform) -> Self {
        self.geo_transform = geo_transform;
        self
    }

    #[must_use]
    pub fn cache_hint(mut self, cache_hint: CacheHint) -> Self {
        self.cache_hint = cache_hint;
        self
    }

    /// Builds the tiles in raster stream order.
    ///
    /// # Panics
    ///
    /// If the text is malformed or a value cannot be parsed as `T`.
    pub fn tiles<T>(&self) -> Vec<RasterTile2D<T>>
    where
        T: Pixel + FromStr,
    {
        let raster = parse(self.text);
        let tile_size = raster.tile_size;

        let mut tiles = Vec::new();
        for block in &raster.blocks {
            for (tile_row, raw_tile_row) in block.tile_rows.iter().enumerate() {
                for tile_column in 0..raw_tile_row.num_tile_columns() {
                    let tile_position = self.tile_position(tile_row, tile_column);
                    let grid = raw_tile_row.grid::<T>(tile_column, tile_size);
                    tiles.push(RasterTile2D::new_with_tile_info(
                        block.time,
                        TileInformation::new(
                            TileIdx::from(tile_position.0),
                            tile_size_of(tile_size),
                            self.geo_transform,
                        ),
                        block.band,
                        grid,
                        self.cache_hint,
                    ));
                }
            }
        }

        tiles.sort_by_key(tile_order_key);
        tiles
    }

    /// [`AsciiTimeFormat::DateTime`] if any header uses a date or date time, [`AsciiTimeFormat::Millis`] otherwise.
    ///
    /// # Panics
    ///
    /// If the text is malformed.
    pub fn time_format(&self) -> AsciiTimeFormat {
        if parse(self.text)
            .blocks
            .iter()
            .any(|block| block.uses_date_time)
        {
            AsciiTimeFormat::DateTime
        } else {
            AsciiTimeFormat::Millis
        }
    }

    /// A tiling specification with the tile size of the text.
    ///
    /// # Panics
    ///
    /// If the text is malformed.
    pub fn tiling_specification(&self) -> TilingSpecification {
        TilingSpecification::with_zero_origin(tile_size_of(parse(self.text).tile_size))
    }

    /// Returns the properties of the described raster, e.g., to build a result descriptor.
    ///
    /// # Panics
    ///
    /// If the text is malformed.
    pub fn info(&self) -> AsciiRasterInfo {
        let raster = parse(self.text);

        let times: BTreeSet<(i64, i64)> = raster
            .blocks
            .iter()
            .map(|block| (block.time.start().inner(), block.time.end().inner()))
            .collect();
        let (start, _) = *times.first().expect("there is at least one block");
        let end = times.iter().map(|&(_, end)| end).max().expect("non-empty");

        let num_tile_rows = raster
            .blocks
            .iter()
            .map(|block| block.tile_rows.len())
            .max()
            .expect("non-empty");
        let num_tile_columns = raster
            .blocks
            .iter()
            .flat_map(|block| &block.tile_rows)
            .map(RawTileRow::num_tile_columns)
            .max()
            .expect("non-empty");

        let upper_left = self.pixel_position(0, 0, raster.tile_size);
        let GridIdx([end_y, end_x]) =
            self.pixel_position(num_tile_rows, num_tile_columns, raster.tile_size);

        AsciiRasterInfo {
            time_bounds: TimeInterval::new_unchecked(start, end),
            regular_time_step: regular_time_step(&times),
            pixel_bounds: GridBoundingBox2D::new(upper_left, [end_y - 1, end_x - 1].into())
                .expect("bounds are valid since there is at least one tile"),
            geo_transform: self.geo_transform,
            tile_size: raster.tile_size,
            num_bands: raster
                .blocks
                .iter()
                .map(|block| block.band + 1)
                .max()
                .expect("non-empty"),
        }
    }

    fn tile_position(&self, tile_row: usize, tile_column: usize) -> GridIdx2D {
        let GridIdx([y, x]) = self.tile_origin;
        [y + to_isize(tile_row), x + to_isize(tile_column)].into()
    }

    fn pixel_position(
        &self,
        tile_row: usize,
        tile_column: usize,
        tile_size: GridShape2D,
    ) -> GridIdx2D {
        let GridIdx([y, x]) = self.tile_position(tile_row, tile_column);
        [
            y * to_isize(tile_size.axis_size_y()),
            x * to_isize(tile_size.axis_size_x()),
        ]
        .into()
    }
}

/// Builds the tiles of an ASCII art text with the defaults of [`AsciiRasterTiles::new`].
///
/// # Panics
///
/// If the text is malformed or a value cannot be parsed as `T`.
pub fn raster_tiles_from_ascii<T>(text: &str) -> Vec<RasterTile2D<T>>
where
    T: Pixel + FromStr,
{
    AsciiRasterTiles::new(text).tiles()
}

/// Renders tiles in the format of [`AsciiRasterTiles`].
///
/// Tiles are grouped by time step and band. Within a block, the upper left tile is the one with the smallest
/// tile position, so absolute tile positions and the geo transform are not part of the output.
/// Tile positions that are missing in a block are filled with `?`.
pub fn raster_tiles_to_ascii<T>(tiles: &[RasterTile2D<T>]) -> String
where
    T: Pixel + Display,
{
    raster_tiles_to_ascii_with_time_format(tiles, AsciiTimeFormat::Millis)
}

/// Renders tiles like [`raster_tiles_to_ascii`], with time instances in the given format.
pub fn raster_tiles_to_ascii_with_time_format<T>(
    tiles: &[RasterTile2D<T>],
    time_format: AsciiTimeFormat,
) -> String
where
    T: Pixel + Display,
{
    let show_band = tiles.iter().any(|tile| tile.band != 0);

    // (time start, time end, band) -> tiles
    let mut blocks: BTreeMap<(i64, i64, u32), Vec<&RasterTile2D<T>>> = BTreeMap::new();
    for tile in tiles {
        blocks
            .entry((
                tile.time.start().inner(),
                tile.time.end().inner(),
                tile.band,
            ))
            .or_default()
            .push(tile);
    }

    let blocks: Vec<(String, Cells)> = blocks
        .into_iter()
        .map(|((start, end, band), tiles)| {
            let mut header = format!(
                "[{}, {})",
                format_time_instance(start, time_format),
                format_time_instance(end, time_format)
            );
            if show_band {
                let _ = write!(header, " band {band}");
            }
            (header, block_cells(&tiles))
        })
        .collect();

    let cell_width = blocks
        .iter()
        .flat_map(|(_, cells)| cells.iter().flatten().flatten().flatten())
        .map(String::len)
        .max()
        .unwrap_or(1);

    blocks
        .iter()
        .map(|(header, cells)| format!("{header}\n{}", render_cells(cells, cell_width)))
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// Compares tiles with an ASCII art description and shows both as ASCII art if they differ.
///
/// Besides the pixel values, this checks the order of the tiles, their time, band, tile position and geo
/// transform. It does not distinguish an empty tile from a tile where all pixels are no-data, and it ignores
/// the cache hint.
///
/// # Panics
///
/// If the tiles differ or the text is malformed.
#[track_caller]
pub fn assert_raster_tiles_eq_ascii<T>(actual: &[RasterTile2D<T>], expected: &AsciiRasterTiles)
where
    T: Pixel + FromStr + Display,
{
    let time_format = expected.time_format();
    let expected = expected.tiles::<T>();

    let actual_ascii = raster_tiles_to_ascii_with_time_format(actual, time_format);
    let expected_ascii = raster_tiles_to_ascii_with_time_format(&expected, time_format);
    assert!(
        actual_ascii == expected_ascii,
        "tiles differ\n\nactual:\n{actual_ascii}\n\nexpected:\n{expected_ascii}\n"
    );

    let actual_keys: Vec<_> = actual.iter().map(tile_key).collect();
    let expected_keys: Vec<_> = expected.iter().map(tile_key).collect();
    assert_eq!(
        actual_keys, expected_keys,
        "order or tile positions of tiles differ, as (time, tile position, band)"
    );

    for (i, (a, e)) in actual.iter().zip(&expected).enumerate() {
        assert!(
            approx_eq!(GeoTransform, a.global_geo_transform, e.global_geo_transform),
            "geo transform of tile {i} differs, actual: {:?}, expected: {:?}",
            a.global_geo_transform,
            e.global_geo_transform
        );
    }
}

fn format_time_instance(millis: i64, time_format: AsciiTimeFormat) -> String {
    if millis == TimeInstance::MIN.inner() {
        return "-inf".to_string();
    }
    if millis == TimeInstance::MAX.inner() {
        return "inf".to_string();
    }

    let date_time = match time_format {
        AsciiTimeFormat::Millis => None,
        AsciiTimeFormat::DateTime => TimeInstance::from_millis(millis)
            .ok()
            .and_then(TimeInstance::as_date_time),
    };
    let Some(date_time) = date_time else {
        return millis.to_string();
    };

    let chrono_date_time: chrono::DateTime<chrono::Utc> = date_time.into();
    if chrono_date_time.num_seconds_from_midnight() == 0 && chrono_date_time.nanosecond() == 0 {
        chrono_date_time.format("%Y-%m-%d").to_string()
    } else {
        date_time.to_datetime_string_with_millis()
    }
}

fn tile_size_of(shape: GridShape2D) -> TileSize {
    TileSize::try_from(shape).expect("parsed tiles are non-empty")
}

fn tile_key<T>(tile: &RasterTile2D<T>) -> (TimeInterval, GridIdx2D, u32) {
    (tile.time, tile.tile_position.grid_idx(), tile.band)
}

fn tile_order_key<T>(tile: &RasterTile2D<T>) -> (i64, i64, isize, isize, u32) {
    let GridIdx([y, x]) = tile.tile_position.grid_idx();
    (
        tile.time.start().inner(),
        tile.time.end().inner(),
        y,
        x,
        tile.band,
    )
}

/// Returns the step if all time steps follow each other without gaps and have the same length.
/// The length can also be a calendar step (months, years), e.g., `[2010-01-01, 2011-01-01)`, `[2011-01-01, 2012-01-01)`.
fn regular_time_step(times: &BTreeSet<(i64, i64)>) -> Option<TimeStep> {
    let &(first_start, first_end) = times.first()?;
    let step = time_step_between(
        TimeInstance::from_millis(first_start).ok()?,
        TimeInstance::from_millis(first_end).ok()?,
    )?;

    let mut expected_start = first_start;
    for &(start, end) in times {
        // a step from or to the start or end of time is not regular
        if start == TimeInstance::MIN.inner() || end == TimeInstance::MAX.inner() {
            return None;
        }
        let start_instance = TimeInstance::from_millis(start).ok()?;
        if start != expected_start || (start_instance + step).ok()?.inner() != end {
            return None;
        }
        expected_start = end;
    }

    Some(step)
}

/// The step with the largest granularity that leads from `start` to `end`.
fn time_step_between(start: TimeInstance, end: TimeInstance) -> Option<TimeStep> {
    const FIXED_GRANULARITIES: [(TimeGranularity, i64); 5] = [
        (TimeGranularity::Days, 86_400_000),
        (TimeGranularity::Hours, 3_600_000),
        (TimeGranularity::Minutes, 60_000),
        (TimeGranularity::Seconds, 1_000),
        (TimeGranularity::Millis, 1),
    ];

    let start_date_time: chrono::DateTime<chrono::Utc> = start.as_date_time()?.into();
    let end_date_time: chrono::DateTime<chrono::Utc> = end.as_date_time()?.into();
    let years = i64::from(end_date_time.year()) - i64::from(start_date_time.year());
    let months = years * 12 + i64::from(end_date_time.month()) - i64::from(start_date_time.month());

    let calendar_candidates = [
        (TimeGranularity::Years, years),
        (TimeGranularity::Months, months),
    ];
    for (granularity, count) in calendar_candidates {
        let Some(step) = u32::try_from(count)
            .ok()
            .and_then(|count| TimeStep::new(granularity, count).ok())
        else {
            continue;
        };
        if (start + step).ok() == Some(end) {
            return Some(step);
        }
    }

    let duration = end.inner().checked_sub(start.inner())?;
    let (granularity, unit) = FIXED_GRANULARITIES
        .into_iter()
        .find(|(_, unit)| duration % unit == 0)?;
    TimeStep::new(granularity, u32::try_from(duration / unit).ok()?).ok()
}

/// Rendered values of a block, indexed by tile row, tile column, pixel row and pixel column.
type Cells = Vec<Vec<Vec<Vec<String>>>>;

fn block_cells<T>(tiles: &[&RasterTile2D<T>]) -> Cells
where
    T: Pixel + Display,
{
    let positions: Vec<[isize; 2]> = tiles
        .iter()
        .map(|tile| tile.tile_position.grid_idx().0)
        .collect();
    let min_y = positions
        .iter()
        .map(|p| p[0])
        .min()
        .expect("block is non-empty");
    let max_y = positions
        .iter()
        .map(|p| p[0])
        .max()
        .expect("block is non-empty");
    let min_x = positions
        .iter()
        .map(|p| p[1])
        .min()
        .expect("block is non-empty");
    let max_x = positions
        .iter()
        .map(|p| p[1])
        .max()
        .expect("block is non-empty");

    let shape = *tiles[0].grid_array.shape_ref();
    let (height, width) = (shape.axis_size_y(), shape.axis_size_x());

    let by_position: BTreeMap<[isize; 2], &RasterTile2D<T>> = tiles
        .iter()
        .map(|tile| (tile.tile_position.grid_idx().0, *tile))
        .collect();

    (min_y..=max_y)
        .map(|y| {
            (min_x..=max_x)
                .map(|x| match by_position.get(&[y, x]) {
                    Some(tile) => tile_cells(tile),
                    None => vec![vec![MISSING_TILE.to_string(); width]; height],
                })
                .collect()
        })
        .collect()
}

fn render_cells(cells: &Cells, cell_width: usize) -> String {
    let num_tile_columns = cells[0].len();
    let width = cells[0][0][0].len();
    let segment_width = width * cell_width + width.saturating_sub(1) + 2;
    let border = format!(
        "+{}",
        format!("{}+", "-".repeat(segment_width)).repeat(num_tile_columns)
    );

    let mut lines = vec![border.clone()];
    for tile_row in cells {
        for pixel_row in 0..tile_row[0].len() {
            let mut line = String::from("|");
            for tile in tile_row {
                let values: Vec<String> = tile[pixel_row]
                    .iter()
                    .map(|value| format!("{value:>cell_width$}"))
                    .collect();
                let _ = write!(line, " {} |", values.join(" "));
            }
            lines.push(line);
        }
        lines.push(border.clone());
    }

    lines.join("\n")
}

fn tile_cells<T>(tile: &RasterTile2D<T>) -> Vec<Vec<String>>
where
    T: Pixel + Display,
{
    let shape = *tile.grid_array.shape_ref();
    (0..shape.axis_size_y())
        .map(|y| {
            (0..shape.axis_size_x())
                .map(|x| {
                    let idx: GridIdx2D = [to_isize(y), to_isize(x)].into();
                    match tile
                        .grid_array
                        .get_at_grid_index(idx)
                        .expect("index is inside the tile")
                    {
                        Some(value) => value.to_string(),
                        None => NO_DATA.to_string(),
                    }
                })
                .collect()
        })
        .collect()
}

fn to_isize(value: usize) -> isize {
    isize::try_from(value).expect("tile sizes and counts in tests fit into isize")
}

/// The parsed text, with values still as strings.
struct RawRaster<'a> {
    blocks: Vec<RawBlock<'a>>,
    tile_size: GridShape2D,
}

struct RawBlock<'a> {
    time: TimeInterval,
    band: u32,
    /// Whether the header uses dates or date times instead of milliseconds.
    uses_date_time: bool,
    tile_rows: Vec<RawTileRow<'a>>,
}

#[derive(Default)]
struct RawTileRow<'a> {
    pixel_rows: Vec<RawPixelRow<'a>>,
}

struct RawPixelRow<'a> {
    line_number: usize,
    /// The values of this pixel row, per tile column.
    tiles: Vec<Vec<&'a str>>,
}

impl RawTileRow<'_> {
    fn num_tile_columns(&self) -> usize {
        self.pixel_rows.first().map_or(0, |row| row.tiles.len())
    }

    fn grid<T>(&self, tile_column: usize, tile_size: GridShape2D) -> GridOrEmpty<GridShape2D, T>
    where
        T: Pixel + FromStr,
    {
        let mut data = Vec::with_capacity(tile_size.number_of_elements());
        let mut mask = Vec::with_capacity(tile_size.number_of_elements());

        for row in &self.pixel_rows {
            for &token in &row.tiles[tile_column] {
                if token == NO_DATA {
                    data.push(T::zero());
                    mask.push(false);
                } else {
                    let value = token.parse::<T>().unwrap_or_else(|_| {
                        panic!(
                            "line {}: cannot parse `{token}` as {}",
                            row.line_number,
                            std::any::type_name::<T>()
                        )
                    });
                    data.push(value);
                    mask.push(true);
                }
            }
        }

        if mask.iter().all(|valid| !valid) {
            return EmptyGrid2D::new(tile_size).into();
        }

        let data = Grid2D::new(tile_size, data).expect("tile size was checked while parsing");
        if mask.iter().all(|valid| *valid) {
            return MaskedGrid2D::new_with_data(data).into();
        }

        let mask = Grid2D::new(tile_size, mask).expect("tile size was checked while parsing");
        MaskedGrid2D::new(data, mask)
            .expect("data and mask have the same size")
            .into()
    }
}

fn parse(text: &str) -> RawRaster<'_> {
    let mut blocks: Vec<RawBlock> = Vec::new();

    for (index, line) in text.lines().enumerate() {
        let line_number = index + 1;
        let line = line.trim();

        if line.is_empty() {
            continue;
        }

        if line.starts_with('[') {
            let (time, band, uses_date_time) = parse_header(line, line_number);
            blocks.push(RawBlock {
                time,
                band,
                uses_date_time,
                tile_rows: vec![RawTileRow::default()],
            });
            continue;
        }

        let Some(block) = blocks.last_mut() else {
            panic!("line {line_number}: expected a header `[start, end)` before `{line}`");
        };
        let tile_row = block
            .tile_rows
            .last_mut()
            .expect("a block has at least one tile row");

        if is_border(line) {
            if !tile_row.pixel_rows.is_empty() {
                block.tile_rows.push(RawTileRow::default());
            }
            continue;
        }

        let Some(inner) = line
            .strip_prefix('|')
            .and_then(|line| line.strip_suffix('|'))
        else {
            panic!(
                "line {line_number}: expected a header, a border `+---+` or a pixel row `| … |`, found `{line}`"
            );
        };

        tile_row.pixel_rows.push(RawPixelRow {
            line_number,
            tiles: inner
                .split('|')
                .map(|tile| tile.split_whitespace().collect())
                .collect(),
        });
    }

    for block in &mut blocks {
        if block
            .tile_rows
            .last()
            .is_some_and(|row| row.pixel_rows.is_empty())
        {
            block.tile_rows.pop();
        }
    }

    let tile_size = check(&blocks);

    RawRaster { blocks, tile_size }
}

/// Checks that all tiles have the same size and returns it.
fn check(blocks: &[RawBlock]) -> GridShape2D {
    assert!(!blocks.is_empty(), "the text contains no tiles");

    let mut seen = BTreeSet::new();
    let mut tile_size: Option<(usize, usize)> = None;

    for block in blocks {
        let key = (
            block.time.start().inner(),
            block.time.end().inner(),
            block.band,
        );
        assert!(
            seen.insert(key),
            "block {} band {} is defined twice",
            block.time,
            block.band
        );
        assert!(
            !block.tile_rows.is_empty(),
            "block {} band {} contains no tiles",
            block.time,
            block.band
        );

        for tile_row in &block.tile_rows {
            let first = &tile_row.pixel_rows[0];
            let height = tile_row.pixel_rows.len();
            let width = first.tiles[0].len();
            let (expected_height, expected_width) = *tile_size.get_or_insert((height, width));

            assert!(
                width > 0,
                "line {}: a tile must contain at least one value",
                first.line_number
            );
            assert_eq!(
                height, expected_height,
                "line {}: tile row has {height} pixel rows, but tiles are {expected_height} pixels high",
                first.line_number
            );

            for row in &tile_row.pixel_rows {
                assert_eq!(
                    row.tiles.len(),
                    first.tiles.len(),
                    "line {}: expected {} tile columns like line {}, found {}",
                    row.line_number,
                    first.tiles.len(),
                    first.line_number,
                    row.tiles.len()
                );
                for (tile_column, tile) in row.tiles.iter().enumerate() {
                    assert_eq!(
                        tile.len(),
                        expected_width,
                        "line {}: tile column {tile_column} has {} values, but tiles are {expected_width} pixels wide",
                        row.line_number,
                        tile.len()
                    );
                }
            }
        }
    }

    let (height, width) = tile_size.expect("there is at least one tile");
    [height, width].into()
}

fn is_border(line: &str) -> bool {
    line.contains('-')
        && line
            .chars()
            .all(|c| matches!(c, '+' | '-' | '|') || c.is_whitespace())
}

fn parse_header(line: &str, line_number: usize) -> (TimeInterval, u32, bool) {
    let invalid = || invalid_header(line, line_number);

    let Some((interval, rest)) = line.strip_prefix('[').and_then(|line| line.split_once(')'))
    else {
        invalid()
    };
    let Some((start, end)) = interval.split_once(',') else {
        invalid()
    };
    let (Some((start, start_is_date_time)), Some((end, end_is_date_time))) = (
        parse_time_instance(start.trim()),
        parse_time_instance(end.trim()),
    ) else {
        invalid()
    };

    let band = match rest.split_whitespace().collect::<Vec<_>>().as_slice() {
        [] => 0,
        ["band", band] => band.parse().unwrap_or_else(|_| invalid()),
        _ => invalid(),
    };

    let time = TimeInterval::new(start, end)
        .unwrap_or_else(|error| panic!("line {line_number}: invalid time interval: {error}"));

    (time, band, start_is_date_time || end_is_date_time)
}

/// Parses milliseconds, an RFC 3339 date time, a date (midnight UTC), `-inf` or `inf`.
/// Returns whether the instance was given as a date or date time.
fn parse_time_instance(text: &str) -> Option<(TimeInstance, bool)> {
    match text {
        "-inf" => return Some((TimeInstance::MIN, false)),
        "inf" => return Some((TimeInstance::MAX, false)),
        _ => {}
    }

    if let Ok(millis) = text.parse::<i64>() {
        return TimeInstance::from_millis(millis)
            .ok()
            .map(|instance| (instance, false));
    }

    if let Ok(date_time) = DateTime::from_str(text) {
        return Some((date_time.into(), true));
    }

    let millis = NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .ok()?
        .and_hms_opt(0, 0, 0)?
        .and_utc()
        .timestamp_millis();
    TimeInstance::from_millis(millis)
        .ok()
        .map(|instance| (instance, true))
}

fn invalid_header(line: &str, line_number: usize) -> ! {
    panic!(
        "line {line_number}: expected `[start, end)` or `[start, end) band N` with milliseconds, \
        RFC 3339 date times, dates, `-inf` or `inf`, found `{line}`"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;

    const TWO_BY_TWO_TILES: &str = "
        [0, 10)
        +----------+----------+
        |  1  2  3 |  4  5  6 |
        |  7  8  _ | 10 11 12 |
        +----------+----------+
        | 13 14 15 |  _  _  _ |
        |  _ 20 21 |  _  _  _ |
        +----------+----------+
    ";

    #[test]
    fn it_builds_tiles_with_positions_and_no_data() {
        let tiles = AsciiRasterTiles::new(TWO_BY_TWO_TILES).tiles::<u8>();

        assert_eq!(tiles.len(), 4);
        assert_eq!(
            tiles.iter().map(|t| t.tile_position).collect::<Vec<_>>(),
            vec![[0, 0].into(), [0, 1].into(), [1, 0].into(), [1, 1].into()]
        );
        assert!(
            tiles
                .iter()
                .all(|t| t.time == TimeInterval::new_unchecked(0, 10))
        );
        assert!(tiles.iter().all(|t| t.band == 0));
        assert_eq!(*tiles[0].grid_array.shape_ref(), [2, 3].into());
        assert_eq!(
            AsciiRasterTiles::new(TWO_BY_TWO_TILES).tiling_specification(),
            TilingSpecification::with_zero_origin(TileSize::new_unchecked(2, 3))
        );

        let GridOrEmpty::Grid(upper_left) = &tiles[0].grid_array else {
            panic!("upper left tile must not be empty");
        };
        assert_eq!(upper_left.inner_grid.data, vec![1, 2, 3, 7, 8, 0]);
        assert_eq!(
            upper_left.validity_mask.data,
            vec![true, true, true, true, true, false]
        );

        let GridOrEmpty::Grid(upper_right) = &tiles[1].grid_array else {
            panic!("upper right tile must not be empty");
        };
        assert!(upper_right.validity_mask.data.iter().all(|v| *v));

        let lower_left: Option<u8> = tiles[2].grid_array.get_at_grid_index([1, 0]).unwrap();
        assert_eq!(lower_left, None);
        assert!(tiles[3].grid_array.is_empty());
    }

    #[test]
    fn it_sorts_bands_and_time_steps_into_stream_order() {
        let tiles = raster_tiles_from_ascii::<i16>(
            "
            [10, 20) band 1
            +---+---+
            | 4 | 8 |
            +---+---+
            [10, 20)
            +---+---+
            | 3 | 7 |
            +---+---+
            [0, 10) band 1
            +---+---+
            | 2 | 6 |
            +---+---+
            [0, 10)
            +---+---+
            | 1 | 5 |
            +---+---+
            ",
        );

        let keys: Vec<_> = tiles
            .iter()
            .map(|t| {
                (
                    t.time.start().inner(),
                    t.tile_position.grid_idx().0[1],
                    t.band,
                )
            })
            .collect();
        assert_eq!(
            keys,
            vec![
                (0, 0, 0),
                (0, 0, 1),
                (0, 1, 0),
                (0, 1, 1),
                (10, 0, 0),
                (10, 0, 1),
                (10, 1, 0),
                (10, 1, 1),
            ]
        );
        let values: Vec<i16> = tiles
            .iter()
            .map(|t| {
                let value: Option<i16> = t.grid_array.get_at_grid_index([0, 0]).unwrap();
                value.unwrap()
            })
            .collect();
        assert_eq!(values, vec![1, 2, 5, 6, 3, 4, 7, 8]);
    }

    #[test]
    fn it_applies_tile_origin_geo_transform_and_cache_hint() {
        let geo_transform = GeoTransform::new_with_coordinate_x_y_unchecked(10.0, 0.5, 20.0, -0.5);
        let cache_hint = CacheHint::seconds(42);
        let tiles = AsciiRasterTiles::new(TWO_BY_TWO_TILES)
            .tile_origin([-1, -2])
            .geo_transform(geo_transform)
            .cache_hint(cache_hint)
            .tiles::<u8>();

        assert_eq!(tiles[0].tile_position, [-1, -2].into());
        assert_eq!(tiles[3].tile_position, [0, -1].into());
        assert!(
            tiles
                .iter()
                .all(|t| t.global_geo_transform == geo_transform)
        );
        assert!(tiles.iter().all(|t| t.cache_hint == cache_hint));
    }

    #[test]
    fn it_parses_floats_and_negative_values() {
        let tiles = raster_tiles_from_ascii::<f32>(
            "
            [-10, 0)
            +-------------+
            | -1.5    2.25 |
            +-------------+
            ",
        );

        assert_eq!(tiles[0].time, TimeInterval::new_unchecked(-10, 0));
        let GridOrEmpty::Grid(grid) = &tiles[0].grid_array else {
            panic!("tile must not be empty");
        };
        assert_eq!(grid.inner_grid.data, vec![-1.5, 2.25]);
    }

    #[test]
    fn it_renders_what_it_parses() {
        let text = indoc! {"
            [0, 10) band 0
            +----------+----------+
            |  1  2  3 |  4  5  6 |
            |  7  8  _ | 10 11 12 |
            +----------+----------+
            | 13 14 15 |  _  _  _ |
            |  _ 20 21 |  _  _  _ |
            +----------+----------+

            [0, 10) band 1
            +----------+----------+
            | -1  2  3 |  4  5  6 |
            |  7  8  9 | 10 11 12 |
            +----------+----------+
            | 13 14 15 | 16 17 18 |
            | 19 20 21 | 22 23 24 |
            +----------+----------+

            [10, 20) band 0
            +----------+----------+
            |  _  _  _ |  _  _  _ |
            |  _  _  _ |  _  _  _ |
            +----------+----------+
            |  _  _  _ |  _  _  _ |
            |  _  _  _ |  _  _  _ |
            +----------+----------+

            [10, 20) band 1
            +----------+----------+
            |  1  1  1 |  1  1  1 |
            |  1  1  1 |  1  1  1 |
            +----------+----------+
            |  1  1  1 |  1  1  1 |
            |  1  1  1 |  1  1  1 |
            +----------+----------+"};

        let tiles = AsciiRasterTiles::new(text)
            .tile_origin([5, 5])
            .tiles::<i32>();

        assert_eq!(raster_tiles_to_ascii(&tiles), text);
    }

    #[test]
    fn it_parses_dates_and_date_times() {
        let tiles = AsciiRasterTiles::new(indoc! {"
            [2010-01-01, 2010-01-01T12:00:00Z)
            | 1 |
            [2010-01-01T12:00:00+02:00, 1262390400000)
            | 2 |
        "});

        assert_eq!(tiles.time_format(), AsciiTimeFormat::DateTime);
        let times: Vec<TimeInterval> = tiles.tiles::<u8>().iter().map(|t| t.time).collect();
        assert_eq!(
            times,
            vec![
                TimeInterval::new_unchecked(
                    DateTime::new_utc(2010, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2010, 1, 1, 12, 0, 0),
                ),
                TimeInterval::new_unchecked(
                    DateTime::new_utc(2010, 1, 1, 10, 0, 0),
                    DateTime::new_utc(2010, 1, 2, 0, 0, 0),
                ),
            ]
        );
    }

    #[test]
    fn it_renders_dates_and_date_times() {
        let text = indoc! {"
            [2010-01-01, 2010-01-01T12:00:00.000Z)
            +---+
            | 1 |
            +---+

            [2010-01-01T12:00:00.000Z, 2011-01-01)
            +---+
            | 2 |
            +---+"};

        let tiles = AsciiRasterTiles::new(text);

        assert_eq!(
            raster_tiles_to_ascii_with_time_format(&tiles.tiles::<u8>(), tiles.time_format()),
            text
        );
        assert_eq!(
            AsciiRasterTiles::new(TWO_BY_TWO_TILES).time_format(),
            AsciiTimeFormat::Millis
        );
    }

    #[test]
    fn it_accepts_the_start_and_end_of_time() {
        let text = indoc! {"
            [-inf, 0)
            +---+
            | 1 |
            +---+

            [0, inf)
            +---+
            | 2 |
            +---+"};

        let tiles = AsciiRasterTiles::new(text);

        assert_eq!(
            tiles
                .tiles::<u8>()
                .iter()
                .map(|tile| tile.time)
                .collect::<Vec<_>>(),
            vec![
                TimeInterval::new_unchecked(TimeInstance::MIN, 0),
                TimeInterval::new_unchecked(0, TimeInstance::MAX),
            ]
        );
        assert_eq!(raster_tiles_to_ascii(&tiles.tiles::<u8>()), text);
        assert_eq!(tiles.info().regular_time_step, None);
        assert_eq!(
            AsciiRasterTiles::new("[-inf, inf)\n| 1 |")
                .info()
                .time_bounds,
            TimeInterval::default()
        );
    }

    #[test]
    #[should_panic(expected = "tiles differ\n\nactual:\n[2010-01-01, 2011-01-01)")]
    fn it_shows_differences_with_dates() {
        let expected = AsciiRasterTiles::new("[2010-01-01, 2011-01-01)\n| 2 |");
        let mut actual = expected.tiles::<u8>();
        actual[0].grid_array = Grid2D::new([1, 1].into(), vec![1]).unwrap().into();

        assert_raster_tiles_eq_ascii(&actual, &expected);
    }

    #[test]
    #[should_panic(expected = "line 1: expected `[start, end)`")]
    fn it_rejects_invalid_time_instances() {
        raster_tiles_from_ascii::<u8>("[2010-13-01, 2011-01-01)\n| 1 |");
    }

    #[test]
    fn it_renders_missing_tiles() {
        let mut tiles = raster_tiles_from_ascii::<u8>(TWO_BY_TWO_TILES);
        tiles.remove(1);

        assert_eq!(
            raster_tiles_to_ascii(&tiles),
            indoc! {"
                [0, 10)
                +----------+----------+
                |  1  2  3 |  ?  ?  ? |
                |  7  8  _ |  ?  ?  ? |
                +----------+----------+
                | 13 14 15 |  _  _  _ |
                |  _ 20 21 |  _  _  _ |
                +----------+----------+"}
        );
    }

    #[test]
    fn it_describes_the_raster() {
        let info = AsciiRasterTiles::new(
            "
            [0, 10)
            +-----+-----+-----+
            | 1 2 | 3 4 | 5 6 |
            | 1 2 | 3 4 | 5 6 |
            +-----+-----+-----+
            | 1 2 | 3 4 | 5 6 |
            | 1 2 | 3 4 | 5 6 |
            +-----+-----+-----+
            [10, 20) band 2
            +-----+-----+-----+
            | 1 2 | 3 4 | 5 6 |
            | 1 2 | 3 4 | 5 6 |
            +-----+-----+-----+
            | 1 2 | 3 4 | 5 6 |
            | 1 2 | 3 4 | 5 6 |
            +-----+-----+-----+
            ",
        )
        .tile_origin([-1, 1])
        .info();

        assert_eq!(
            info,
            AsciiRasterInfo {
                time_bounds: TimeInterval::new_unchecked(0, 20),
                regular_time_step: Some(TimeStep::millis(10).unwrap()),
                pixel_bounds: GridBoundingBox2D::new([-2, 2], [1, 7]).unwrap(),
                geo_transform: GeoTransform::test_default(),
                tile_size: [2, 2].into(),
                num_bands: 3,
            }
        );
    }

    #[test]
    fn it_detects_calendar_and_fixed_time_steps() {
        let step = |text: &str| AsciiRasterTiles::new(text).info().regular_time_step;
        let tile = "| 1 |\n";

        assert_eq!(
            step(&format!(
                "[2010-01-01, 2011-01-01)\n{tile}[2011-01-01, 2012-01-01)\n{tile}[2012-01-01, 2013-01-01)\n{tile}"
            )),
            Some(TimeStep::years(1).unwrap())
        );
        assert_eq!(
            step(&format!(
                "[2014-01-31, 2014-03-31)\n{tile}[2014-03-31, 2014-05-31)\n{tile}"
            )),
            Some(TimeStep::months(2).unwrap())
        );
        assert_eq!(
            step(&format!(
                "[2014-01-01, 2014-01-08)\n{tile}[2014-01-08, 2014-01-15)\n{tile}"
            )),
            Some(TimeStep::days(7).unwrap())
        );
        assert_eq!(
            step(&format!("[0, 3600000)\n{tile}[3600000, 7200000)\n{tile}")),
            Some(TimeStep::new(TimeGranularity::Hours, 1).unwrap())
        );
        assert_eq!(
            step(&format!(
                "[2010-01-01, 2011-01-01)\n{tile}[2011-01-01, 2013-01-01)\n{tile}"
            )),
            None
        );
    }

    #[test]
    fn it_detects_irregular_time_steps() {
        let tile = "+---+\n| 1 |\n+---+\n";
        let irregular = format!("[0, 10)\n{tile}[20, 30)\n{tile}");
        let different_lengths = format!("[0, 10)\n{tile}[10, 30)\n{tile}");

        assert_eq!(
            AsciiRasterTiles::new(&irregular).info().regular_time_step,
            None
        );
        assert_eq!(
            AsciiRasterTiles::new(&different_lengths)
                .info()
                .regular_time_step,
            None
        );
    }

    #[test]
    fn it_accepts_equal_tiles() {
        let tiles = AsciiRasterTiles::new(TWO_BY_TWO_TILES).tile_origin([-1, 0]);
        assert_raster_tiles_eq_ascii(&tiles.tiles::<u8>(), &tiles);
    }

    #[test]
    #[should_panic(expected = "tiles differ")]
    fn it_rejects_different_values() {
        let actual = raster_tiles_from_ascii::<u8>(TWO_BY_TWO_TILES);
        let expected = TWO_BY_TWO_TILES.replace("20", "42");
        assert_raster_tiles_eq_ascii(&actual, &AsciiRasterTiles::new(&expected));
    }

    #[test]
    #[should_panic(expected = "order or tile positions")]
    fn it_rejects_different_positions() {
        let actual = raster_tiles_from_ascii::<u8>(TWO_BY_TWO_TILES);
        assert_raster_tiles_eq_ascii(
            &actual,
            &AsciiRasterTiles::new(TWO_BY_TWO_TILES).tile_origin([1, 1]),
        );
    }

    #[test]
    #[should_panic(expected = "line 7: tile row has 1 pixel rows, but tiles are 2 pixels high")]
    fn it_rejects_different_tile_heights() {
        raster_tiles_from_ascii::<u8>(
            "
            [0, 10)
            +-----+
            | 1 2 |
            | 1 2 |
            +-----+
            | 1 2 |
            +-----+
            ",
        );
    }

    #[test]
    #[should_panic(expected = "line 4: tile column 1 has 1 values, but tiles are 2 pixels wide")]
    fn it_rejects_different_tile_widths() {
        raster_tiles_from_ascii::<u8>(
            "
            [0, 10)
            +-----+---+
            | 1 2 | 3 |
            +-----+---+
            ",
        );
    }

    #[test]
    #[should_panic(expected = "line 4: cannot parse `300` as u8")]
    fn it_rejects_unparsable_values() {
        raster_tiles_from_ascii::<u8>(
            "
            [0, 10)
            +-------+
            | 1 300 |
            +-------+
            ",
        );
    }

    #[test]
    #[should_panic(expected = "line 2: expected a header")]
    fn it_rejects_a_missing_header() {
        raster_tiles_from_ascii::<u8>(
            "
            +---+
            | 1 |
            +---+
            ",
        );
    }

    #[test]
    #[should_panic(expected = "is defined twice")]
    fn it_rejects_duplicate_blocks() {
        raster_tiles_from_ascii::<u8>("[0, 10)\n| 1 |\n[0, 10)\n| 2 |\n");
    }
}
