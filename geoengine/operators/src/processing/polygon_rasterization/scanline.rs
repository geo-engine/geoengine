//! Scanline rasterization of polygons by pixel centers.
//!
//! A pixel is inside a polygon if its center is inside the polygon (even-odd rule).
//! Pixel centers that lie exactly on an edge are resolved by half-open rules, so that two
//! polygons sharing an edge never both contain a pixel.

use geoengine_datatypes::primitives::{AxisAlignedRectangle, BoundingBox2D, Coordinate2D};
use std::ops::Range;

/// The pixel grid of one tile in world coordinates.
///
/// The pixel centers are at `origin + (idx + 0.5) * pixel_size` per axis.
/// `pixel_size_y` is usually negative, i.e., rows go from north to south.
#[derive(Debug, Clone, Copy)]
pub struct PixelGrid {
    pub origin: Coordinate2D,
    pub pixel_size_x: f64,
    pub pixel_size_y: f64,
    pub rows: usize,
    pub cols: usize,
}

impl PixelGrid {
    /// The y coordinate of the pixel centers of `row`.
    pub fn row_center(&self, row: usize) -> f64 {
        self.origin.y + (row as f64 + 0.5) * self.pixel_size_y
    }

    /// The rows whose pixel centers lie within the y extent of `bbox`.
    pub fn rows_within(&self, bbox: &BoundingBox2D) -> Range<usize> {
        let a = (bbox.upper_right().y - self.origin.y) / self.pixel_size_y - 0.5;
        let b = (bbox.lower_left().y - self.origin.y) / self.pixel_size_y - 0.5;
        index_range_within(a.min(b), a.max(b), self.rows)
    }

    /// The columns whose pixel centers lie within the x extent of `bbox`.
    pub fn cols_within(&self, bbox: &BoundingBox2D) -> Range<usize> {
        let a = (bbox.lower_left().x - self.origin.x) / self.pixel_size_x - 0.5;
        let b = (bbox.upper_right().x - self.origin.x) / self.pixel_size_x - 0.5;
        index_range_within(a.min(b), a.max(b), self.cols)
    }

    /// The columns whose pixel centers `x_c` satisfy `x_start <= x_c < x_end`.
    fn cols_of_span(&self, x_start: f64, x_end: f64) -> Range<usize> {
        let a = (x_start - self.origin.x) / self.pixel_size_x - 0.5;
        let b = (x_end - self.origin.x) / self.pixel_size_x - 0.5;
        // for a negative pixel size, the order of the coordinates flips
        let (from, to) = if self.pixel_size_x > 0. {
            (a, b)
        } else {
            (b, a)
        };
        clamp_index_range(from.ceil(), to.ceil(), self.cols)
    }
}

/// The integer indices `i` with `from <= i <= to`, clamped to `0..len`.
fn index_range_within(from: f64, to: f64, len: usize) -> Range<usize> {
    clamp_index_range(from.ceil(), to.floor() + 1., len)
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn clamp_index_range(start: f64, end: f64, len: usize) -> Range<usize> {
    let len_f = len as f64;
    let start = start.clamp(0., len_f) as usize;
    let end = end.clamp(0., len_f) as usize;
    start..end.max(start)
}

/// Appends the column ranges of `row` that are covered by `polygons` to `spans` and merges them,
/// so that each column is covered at most once.
///
/// `crossings` is a scratch buffer to avoid allocations.
pub fn row_spans<P, R>(
    grid: &PixelGrid,
    row: usize,
    polygons: &[P],
    crossings: &mut Vec<f64>,
    spans: &mut Vec<Range<usize>>,
) where
    P: AsRef<[R]>,
    R: AsRef<[Coordinate2D]>,
{
    spans.clear();

    let y = grid.row_center(row);

    for polygon in polygons {
        crossings.clear();

        for ring in polygon.as_ref() {
            for edge in ring.as_ref().windows(2) {
                let (p1, p2) = (edge[0], edge[1]);
                // half-open rule: a vertex on the scanline is counted for exactly one of its edges
                if (p1.y <= y) != (p2.y <= y) {
                    crossings.push(p1.x + (y - p1.y) * (p2.x - p1.x) / (p2.y - p1.y));
                }
            }
        }

        crossings.sort_unstable_by(f64::total_cmp);

        for pair in crossings.as_chunks::<2>().0 {
            let cols = grid.cols_of_span(pair[0], pair[1]);
            if !cols.is_empty() {
                spans.push(cols);
            }
        }
    }

    merge_spans(spans);
}

/// Sorts and merges overlapping or adjacent spans in place.
fn merge_spans(spans: &mut Vec<Range<usize>>) {
    if spans.len() < 2 {
        return;
    }

    spans.sort_unstable_by_key(|span| span.start);

    let mut merged = 0;
    for i in 1..spans.len() {
        if spans[i].start <= spans[merged].end {
            spans[merged].end = spans[merged].end.max(spans[i].end);
        } else {
            merged += 1;
            spans[merged] = spans[i].clone();
        }
    }
    spans.truncate(merged + 1);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 4×4 grid of unit pixels with its upper left corner at (0, 4).
    fn grid() -> PixelGrid {
        PixelGrid {
            origin: Coordinate2D::new(0., 4.),
            pixel_size_x: 1.,
            pixel_size_y: -1.,
            rows: 4,
            cols: 4,
        }
    }

    fn ring(coords: &[(f64, f64)]) -> Vec<Coordinate2D> {
        coords
            .iter()
            .map(|&(x, y)| Coordinate2D::new(x, y))
            .collect()
    }

    /// Renders the covered pixels as rows of `#` and `.`.
    fn render(grid: &PixelGrid, polygons: &[Vec<Vec<Coordinate2D>>]) -> Vec<String> {
        let mut crossings = Vec::new();
        let mut spans = Vec::new();
        (0..grid.rows)
            .map(|row| {
                row_spans(grid, row, polygons, &mut crossings, &mut spans);
                let mut line = vec!['.'; grid.cols];
                for span in &spans {
                    for col in span.clone() {
                        assert_eq!(line[col], '.', "pixel covered twice");
                        line[col] = '#';
                    }
                }
                line.into_iter().collect()
            })
            .collect()
    }

    #[test]
    fn it_covers_pixel_centers_inside() {
        let square = vec![ring(&[(1., 1.), (3., 1.), (3., 3.), (1., 3.), (1., 1.)])];

        assert_eq!(render(&grid(), &[square]), ["....", ".##.", ".##.", "...."]);
    }

    #[test]
    fn it_excludes_holes() {
        let with_hole = vec![
            ring(&[(0., 0.), (4., 0.), (4., 4.), (0., 4.), (0., 0.)]),
            ring(&[(1., 1.), (1., 3.), (3., 3.), (3., 1.), (1., 1.)]),
        ];

        assert_eq!(
            render(&grid(), &[with_hole]),
            ["####", "#..#", "#..#", "####"]
        );
    }

    #[test]
    fn it_assigns_centers_on_shared_edges_to_one_polygon() {
        // two triangles sharing the diagonal through the pixel centers (0.5, 3.5), (1.5, 2.5), …
        let lower = vec![ring(&[(0., 4.), (4., 0.), (0., 0.), (0., 4.)])];
        let upper = vec![ring(&[(0., 4.), (4., 4.), (4., 0.), (0., 4.)])];

        let lower_pixels = render(&grid(), &[lower]);
        let upper_pixels = render(&grid(), &[upper]);

        assert_eq!(lower_pixels, ["....", "#...", "##..", "###."]);
        assert_eq!(upper_pixels, ["####", ".###", "..##", "...#"]);
    }

    #[test]
    fn it_covers_a_vertex_on_a_pixel_center_once() {
        // a diamond whose vertices lie exactly on pixel centers
        let diamond = vec![ring(&[
            (1.5, 0.5),
            (2.5, 1.5),
            (1.5, 2.5),
            (0.5, 1.5),
            (1.5, 0.5),
        ])];

        // the diamond has an area of two pixels, and the half-open rules assign exactly two
        // pixel centers to it: the top and right vertices are excluded, left and center included
        assert_eq!(
            render(&grid(), &[diamond]),
            ["....", "....", "##..", "...."]
        );
    }

    #[test]
    fn it_covers_overlapping_parts_of_a_multi_polygon_once() {
        let left = vec![ring(&[(0., 0.), (3., 0.), (3., 4.), (0., 4.), (0., 0.)])];
        let right = vec![ring(&[(1., 0.), (4., 0.), (4., 4.), (1., 4.), (1., 0.)])];

        assert_eq!(
            render(&grid(), &[left, right]),
            ["####", "####", "####", "####"]
        );
    }

    #[test]
    fn it_clips_to_the_grid() {
        let large = vec![ring(&[
            (-10., -10.),
            (10., -10.),
            (10., 1.5),
            (-10., 1.5),
            (-10., -10.),
        ])];

        assert_eq!(render(&grid(), &[large]), ["....", "....", "....", "####"]);
    }

    #[test]
    fn it_computes_rows_and_cols_within_a_bbox() {
        let grid = grid();
        let bbox = BoundingBox2D::new_unchecked((0.5, 0.6).into(), (2.5, 3.5).into());

        // row centers 3.5, 2.5, 1.5, 0.5 → 3.5..=0.6 contains rows 0, 1, 2
        assert_eq!(grid.rows_within(&bbox), 0..3);
        // col centers 0.5, 1.5, 2.5, 3.5 → 0.5..=2.5 contains cols 0, 1, 2
        assert_eq!(grid.cols_within(&bbox), 0..3);

        let outside = BoundingBox2D::new_unchecked((5., 5.).into(), (6., 6.).into());
        assert!(grid.rows_within(&outside).is_empty());
        assert!(grid.cols_within(&outside).is_empty());
    }
}
