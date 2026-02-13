use crate::{
    primitives::{BandSelection, BandSelectionIter},
    raster::{GridBoundingBox2D, GridIdx2D, GridIdx2DIter, TileInformation, TileInformationIter},
};

#[derive(Clone, Debug)]
pub struct TileIdxBandCrossProductIter {
    tile_iter: GridIdx2DIter,
    band_iter: BandSelectionIter, // TODO: maybe change this to actual attributes from ResultDescriptor not the Selection?
    current_tile: Option<GridIdx2D>,
}

impl TileIdxBandCrossProductIter {
    pub fn new(tile_iter: GridIdx2DIter, band_iter: BandSelectionIter) -> Self {
        let mut tile_iter = tile_iter;
        let current_tile = tile_iter.next();
        Self {
            tile_iter,
            band_iter,
            current_tile,
        }
    }

    pub fn grid_bounds(&self) -> GridBoundingBox2D {
        self.tile_iter.grid_bounds
    }

    pub fn band_selection(&self) -> &BandSelection {
        &self.band_iter.band_selection
    }

    pub fn with_grid_bounds_and_selection(
        bounds: GridBoundingBox2D,
        band_selection: BandSelection,
    ) -> Self {
        let tile_iter = GridIdx2DIter::new(&bounds);
        let band_iter = BandSelectionIter::new(band_selection);
        Self::new(tile_iter, band_iter)
    }

    pub fn reset(&mut self) {
        self.band_iter.reset();
        self.tile_iter.reset();
        self.current_tile = self.tile_iter.next();
    }
}

impl Iterator for TileIdxBandCrossProductIter {
    type Item = (GridIdx2D, u32);

    fn next(&mut self) -> Option<Self::Item> {
        let current_t = self.current_tile;

        match (current_t, self.band_iter.next()) {
            (None, _) => None,
            (Some(t), Some(b)) => Some((t, b)),
            (Some(_t), None) => {
                self.band_iter.reset();
                self.current_tile = self.tile_iter.next();
                self.current_tile.map(|t| {
                    (
                        t,
                        self.band_iter
                            .next()
                            .expect("There must be at least one band"),
                    )
                })
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct TileInformationBandCrossProductIter {
    tile_iter: TileInformationIter,
    band_iter: BandSelectionIter,
    current_tile: Option<TileInformation>,
}

impl TileInformationBandCrossProductIter {
    pub fn new(tile_iter: TileInformationIter, band_iter: BandSelectionIter) -> Self {
        let mut tile_iter = tile_iter;
        let current_tile = tile_iter.next();
        Self {
            tile_iter,
            band_iter,
            current_tile,
        }
    }

    pub fn reset(&mut self) {
        self.band_iter.reset();
        self.tile_iter.reset();
        self.current_tile = self.tile_iter.next();
    }
}

impl Iterator for TileInformationBandCrossProductIter {
    type Item = (TileInformation, u32);

    fn next(&mut self) -> Option<Self::Item> {
        let current_t = self.current_tile;

        match (current_t, self.band_iter.next()) {
            (None, _) => None,
            (Some(t), Some(b)) => Some((t, b)),
            (Some(_t), None) => {
                self.band_iter.reset();
                self.current_tile = self.tile_iter.next();
                self.current_tile.map(|t| {
                    (
                        t,
                        self.band_iter
                            .next()
                            .expect("There must be at least one band"),
                    )
                })
            }
        }
    }
}
