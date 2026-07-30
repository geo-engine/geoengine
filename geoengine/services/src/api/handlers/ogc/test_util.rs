use crate::{
    api::handlers::datasets::AddDatasetTile,
    api::model::datatypes::{DataProviderId, LayerId},
    contexts::{ApplicationContext, PostgresContext, Session, SessionContext, SessionId},
    datasets::storage::DatasetStore,
    layers::{
        listing::LayerCollectionProvider,
        storage::{INTERNAL_PROVIDER_ID, LayerDb},
    },
    permissions::PermissionDb,
    util::tests::{
        add_file_definition_to_datasets, add_file_definition_to_datasets_and_return_layer,
        add_file_definition_to_layers, add_ndvi_3857_to_layers, add_ndvi_to_layers, admin_login,
        ndvi_255_symbology,
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

/// Returns a session id, data connector id and layer id for the Modis NDVI dataset in native EPSG:3857 projection.
pub async fn session_and_4326_rgb_layer_id(
    app_ctx: &PostgresContext<NoTls>,
) -> (SessionId, DataProviderId, LayerId) {
    let session = admin_login(app_ctx).await;
    let ctx = app_ctx.session_context(session.clone());

    let session_id = ctx.session().id();

    for color in ["red", "green", "blue"] {
        let _ = add_file_definition_to_datasets(
            &ctx.db(),
            test_data!(&format!("dataset_defs/natural_earth_2_{color}.json")),
        )
        .await;
    }

    let (data_connector_id, layer_id) =
        add_file_definition_to_layers(&ctx.db(), test_data!("layer_defs/natural_earth_rgb.json"))
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

/// Returns a session id, data connector id and layer id for an NDVI dataset loaded via MultiBandGdalSource.
pub async fn session_and_ndvi_multi_band_layer_id(
    app_ctx: &PostgresContext<NoTls>,
) -> (SessionId, DataProviderId, LayerId) {
    let session = admin_login(app_ctx).await;
    let ctx = app_ctx.session_context(session.clone());

    let session_id = ctx.session().id();
    let db = ctx.db();

    // Create GdalMultiBand metadata for NDVI data
    use geoengine_datatypes::primitives::TimeInterval as DtTimeInterval;

    let result_descriptor = geoengine_operators::engine::RasterResultDescriptor {
        data_type: geoengine_datatypes::raster::RasterDataType::U8,
        spatial_reference: geoengine_datatypes::spatial_reference::SpatialReferenceOption::from(
            geoengine_datatypes::spatial_reference::SpatialReference::epsg_4326(),
        ),
        time: geoengine_operators::engine::TimeDescriptor::new_irregular(Some(
            DtTimeInterval::new_unchecked(1_396_310_400_000, 1_398_902_400_000),
        )),
        spatial_grid: geoengine_operators::engine::SpatialGridDescriptor::new_source(
            geoengine_datatypes::raster::SpatialGridDefinition::new(
                geoengine_datatypes::raster::GeoTransform::new((-180.0, 90.0).into(), 0.1, -0.1),
                geoengine_datatypes::raster::GridBoundingBox2D::new([0, 0], [1799, 3599]).unwrap(),
            ),
        ),
        bands: vec![geoengine_operators::engine::RasterBandDescriptor::new(
            "ndvi".to_string(),
            Default::default(),
        )]
        .try_into()
        .unwrap(),
    };

    let dataset = db
        .add_dataset(
            crate::datasets::AddDataset {
                name: None,
                display_name: "NDVI MultiBand".to_string(),
                description: "NDVI data loaded via MultiBandGdalSource".to_string(),
                source_operator: "MultiBandGdalSource".to_string(),
                symbology: None,
                provenance: None,
                tags: Some(vec!["raster".to_string(), "test".to_string()]),
            },
            crate::datasets::storage::MetaDataDefinition::GdalMultiBand(
                geoengine_operators::source::GdalMultiBand { result_descriptor },
            ),
            Some(crate::api::model::services::DataPath::Volume(
                crate::datasets::upload::VolumeName("test_data".to_string()),
            )),
        )
        .await
        .unwrap();

    // Add permissions for the dataset
    for role in [
        crate::permissions::Role::registered_user_role_id(),
        crate::permissions::Role::anonymous_role_id(),
    ] {
        db.add_permission(role, dataset.id, crate::permissions::Permission::Read)
            .await
            .unwrap();
    }

    // Create a single tile for the NDVI data
    use crate::api::model::datatypes::{Coordinate2D, GeoTransform};
    let tile = AddDatasetTile {
        time: crate::api::model::datatypes::TimeInterval {
            start: geoengine_datatypes::primitives::TimeInstance::from_millis_unchecked(
                1_396_310_400_000,
            )
            .into(),
            end: geoengine_datatypes::primitives::TimeInstance::from_millis_unchecked(
                1_398_902_400_000,
            )
            .into(),
        },
        spatial_partition: crate::api::model::datatypes::SpatialPartition2D {
            upper_left_coordinate: Coordinate2D { x: -180., y: 90. },
            lower_right_coordinate: Coordinate2D { x: 180., y: -90. },
        },
        band: 0,
        z_index: 0,
        params: crate::api::model::operators::GdalDatasetParameters {
            file_path: test_data!("raster/modis_ndvi/MOD13A2_M_NDVI_2014-04-01.TIFF").into(),
            rasterband_channel: 1,
            geo_transform: GeoTransform {
                origin_coordinate: Coordinate2D { x: -180., y: 90. },
                x_pixel_size: 0.1,
                y_pixel_size: -0.1,
            },
            width: 3600,
            height: 1800,
            file_not_found_handling: crate::api::model::operators::FileNotFoundHandling::NoData,
            no_data_value: Some(0.),
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
        },
    };

    db.add_dataset_tiles(dataset.id, vec![tile]).await.unwrap();

    // Create a layer with MultiBandGdalSource workflow
    let db_layer_id = geoengine_datatypes::dataset::LayerId(dataset.id.to_string());
    let layer_id = LayerId(dataset.id.to_string());

    let root_collection_id = db.get_root_layer_collection_id().await.unwrap();

    let operator = crate::api::model::processing_graphs::TypedOperator::Raster(
        crate::api::model::processing_graphs::RasterOperator::MultiBandGdalSource(
            crate::api::model::processing_graphs::MultiBandGdalSource {
                r#type: Default::default(),
                params: crate::api::model::processing_graphs::GdalSourceParameters {
                    data: dataset.name.to_string(),
                    overview_level: None,
                },
            },
        ),
    );

    db.add_layer_with_id(
        &db_layer_id,
        crate::layers::layer::AddLayer {
            name: "NDVI MultiBand".to_string(),
            description: "NDVI data loaded via MultiBandGdalSource".to_string(),
            workflow: crate::workflows::workflow::Workflow::Typed { operator },
            symbology: Some(ndvi_255_symbology()),
            properties: vec![],
            metadata: Default::default(),
        },
        &root_collection_id,
    )
    .await
    .unwrap();

    for role in [
        crate::permissions::Role::registered_user_role_id(),
        crate::permissions::Role::anonymous_role_id(),
    ] {
        db.add_permission(
            role,
            db_layer_id.clone(),
            crate::permissions::Permission::Read,
        )
        .await
        .unwrap();
    }

    (session_id, INTERNAL_PROVIDER_ID.into(), layer_id)
}
