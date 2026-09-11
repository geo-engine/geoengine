#[cfg(feature = "new-subquery-adapter")]
mod raster_subquery_adapter;
#[cfg(not(feature = "new-subquery-adapter"))]
mod raster_subquery_adapter_old;
mod raster_subquery_reprojection;

#[cfg(feature = "new-subquery-adapter")]
pub use raster_subquery_adapter::{
    FoldTileAccu, FoldTileAccuMut, RasterSubQueryAdapter, SubQueryTileAggregator,
};
#[cfg(not(feature = "new-subquery-adapter"))]
pub use raster_subquery_adapter_old::{
    FoldTileAccu, FoldTileAccuMut, RasterSubQueryAdapter, SubQueryTileAggregator,
};

pub use raster_subquery_reprojection::{
    TileReprojectionSubQuery, TileReprojectionSubqueryGridInfo, fold_by_coordinate_lookup_future,
};
