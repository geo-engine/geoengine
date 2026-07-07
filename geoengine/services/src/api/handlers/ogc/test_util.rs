use crate::{
    api::model::datatypes::{DataProviderId, LayerId},
    contexts::{ApplicationContext, PostgresContext, Session, SessionContext, SessionId},
    util::tests::{
        add_file_definition_to_datasets_and_return_layer, add_ndvi_3857_to_layers,
        add_ndvi_to_layers, admin_login, ndvi_255_symbology,
    },
};
use geoengine_datatypes::{
    raster::{GridBoundingBox2D, GridIdx2D, GridShape2D, GridSize, TilingSpatialGridDefinition},
    test_data,
};
use tokio_postgres::NoTls;

/// Returns a session id, data connector id and layer id for the Modis NDVI dataset in EPSG:4326 projection.
pub async fn session_and_4326_layer_id(
    app_ctx: &PostgresContext<NoTls>,
) -> (SessionId, DataProviderId, LayerId) {
    let session = admin_login(app_ctx).await;
    let ctx = app_ctx.session_context(session.clone());

    let session_id = ctx.session().id();
    let (data_connector_id, layer_id) = add_ndvi_to_layers(app_ctx).await;

    (session_id, data_connector_id.into(), layer_id.into())
}

/// Returns a session id, data connector id and layer id for the Modis NDVI dataset in reprojected EPSG:3857 projection.
pub async fn session_and_3857_layer_id(
    app_ctx: &PostgresContext<NoTls>,
) -> (SessionId, DataProviderId, LayerId) {
    let session = admin_login(app_ctx).await;
    let ctx = app_ctx.session_context(session.clone());

    let session_id = ctx.session().id();
    let (data_connector_id, layer_id) = add_ndvi_3857_to_layers(app_ctx).await;

    (session_id, data_connector_id.into(), layer_id.into())
}

/// Returns a session id, data connector id and layer id for the Modis NDVI dataset in native EPSG:3857 projection.
pub async fn session_and_native_3857_layer_id(
    app_ctx: &PostgresContext<NoTls>,
) -> (SessionId, DataProviderId, LayerId) {
    let session = admin_login(app_ctx).await;
    let ctx = app_ctx.session_context(session.clone());

    let session_id = ctx.session().id();
    let (data_connector_id, layer_id) = add_file_definition_to_datasets_and_return_layer(
        &ctx.db(),
        test_data!("dataset_defs/ndvi (3587).json"),
        Some(ndvi_255_symbology()),
    )
    .await;

    (session_id, data_connector_id.into(), layer_id.into())
}

/// Merges multiple `GridBoundingBox2D` into one.
///
/// # Panics
/// Panics if the iterator is empty.
pub fn merge_bounds(bounds: impl IntoIterator<Item = GridBoundingBox2D>) -> GridBoundingBox2D {
    let mut bounds = bounds.into_iter();
    let mut merged_bounds = bounds.next().unwrap();
    for bound in bounds {
        merged_bounds = GridBoundingBox2D::new_min_max(
            merged_bounds.y_min().min(bound.y_min()),
            merged_bounds.y_max().max(bound.y_max()),
            merged_bounds.x_min().min(bound.x_min()),
            merged_bounds.x_max().max(bound.x_max()),
        )
        .unwrap();
    }
    merged_bounds
}

/// Shorthand for creating a `GridBoundingBox2D` from min and max coordinates.
pub fn grid_bbox(min: [isize; 2], max: [isize; 2]) -> GridBoundingBox2D {
    GridBoundingBox2D::new(GridIdx2D::new(min), GridIdx2D::new(max)).unwrap()
}

/// Calculates the number of tiles in x and y direction for a given tiling spatial grid definition.
pub fn calculate_number_of_tiles(
    tiling_spatial_grid_definition: &TilingSpatialGridDefinition,
) -> GridShape2D {
    let grid_bounds = tiling_spatial_grid_definition.tiling_grid_bounds();

    let tiling_strategy = tiling_spatial_grid_definition.generate_data_tiling_strategy();
    let tile_bounds = tiling_strategy.global_pixel_grid_bounds_to_tile_grid_bounds(grid_bounds);

    GridShape2D::new(tile_bounds.axis_size())
}
