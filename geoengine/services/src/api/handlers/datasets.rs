use crate::{
    api::model::{
        datatypes::SpatialPartition2D,
        operators::{GdalDatasetParameters, GdalLoadingInfoTemporalSlice, GdalMetaDataList},
        responses::{
            ErrorResponse,
            datasets::{DatasetNameResponse, errors::*},
        },
        services::{
            AddDataset, CreateDataset, DataPath, Dataset, DatasetDefinition, MetaDataDefinition,
            MetaDataSuggestion, Provenances, UpdateDataset, Volume,
        },
    },
    config::{DatasetService, get_config_element},
    contexts::{ApplicationContext, SessionContext},
    datasets::{
        DatasetName,
        listing::{DatasetListOptions, DatasetListing, DatasetProvider},
        storage::{AutoCreateDataset, DatasetStore, SuggestMetaData},
        upload::{AdjustFilePath, Upload, UploadDb, UploadId, UploadRootPath, VolumeName, Volumes},
    },
    error::{self, Error, Result},
    permissions::{Permission, PermissionDb, Role},
    projects::Symbology,
    util::{
        extractors::{ValidatedJson, ValidatedQuery},
        path_with_base_path,
    },
};
use actix_web::{
    FromRequest, HttpResponse, HttpResponseBuilder, Responder,
    web::{self, Json},
};
use gdal::GdalOpenFlags;
use gdal::{
    DatasetOptions,
    spatial_ref::SpatialRef,
    vector::{Layer, LayerAccess, OGRFieldType},
};
use geoengine_datatypes::{
    collections::VectorDataType,
    error::BoxedResultExt,
    primitives::{FeatureDataType, Measurement, TimeInterval, VectorQueryRectangle},
    spatial_reference::{SpatialReference, SpatialReferenceOption},
};
use geoengine_operators::util::GdalConfigOptions;
use geoengine_operators::{
    engine::{
        OperatorName, RasterResultDescriptor, StaticMetaData, TypedResultDescriptor,
        VectorColumnInfo, VectorResultDescriptor,
    },
    source::{
        MultiBandGdalSource as OperatorsMultiBandGdalSource, OgrSourceColumnSpec, OgrSourceDataset,
        OgrSourceDatasetTimeType, OgrSourceDurationSpec, OgrSourceErrorSpec, OgrSourceTimeFormat,
    },
    util::gdal::{
        gdal_open_dataset, gdal_open_dataset_ex, gdal_parameters_from_dataset,
        raster_descriptor_from_dataset,
    },
};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, ensure};
use std::{
    collections::{HashMap, HashSet},
    convert::{TryFrom, TryInto},
    path::{Path, PathBuf},
};
use utoipa::{ToResponse, ToSchema};

pub(crate) fn init_dataset_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ApplicationContext,
    C::Session: FromRequest,
{
    let config_dataset_json_limit = get_config_element::<DatasetService>()
        .expect("DatasetService config element not found")
        .json_size_limit;
    let json_config = web::JsonConfig::default().limit(config_dataset_json_limit);

    cfg.service(
        web::scope("/dataset")
            .app_data(json_config)
            .service(
                web::resource("/suggest").route(web::post().to(suggest_meta_data_handler::<C>)),
            )
            .service(web::resource("/auto").route(web::post().to(auto_create_dataset_handler::<C>)))
            .service(
                web::resource("/volumes/{volume_name}/files/{file_name}/layers")
                    .route(web::get().to(list_volume_file_layers_handler::<C>)),
            )
            .service(web::resource("/volumes").route(web::get().to(list_volumes_handler::<C>)))
            .service(
                web::resource("/{dataset}/loadingInfo")
                    .route(web::get().to(get_loading_info_handler::<C>))
                    .route(web::put().to(update_loading_info_handler::<C>)),
            )
            .service(
                web::resource("/{dataset}/symbology")
                    .route(web::put().to(update_dataset_symbology_handler::<C>)),
            )
            .service(
                web::resource("/{dataset}/provenance")
                    .route(web::put().to(update_dataset_provenance_handler::<C>)),
            )
            .service(
                web::resource("/{dataset}/tiles")
                    .route(web::post().to(add_dataset_tiles_handler::<C>)),
            )
            .service(
                web::resource("/{dataset}/md-tiles")
                    .route(web::post().to(add_md_dataset_tiles_handler::<C>)),
            )
            .service(
                web::resource("/{dataset}")
                    .route(web::get().to(get_dataset_handler::<C>))
                    .route(web::post().to(update_dataset_handler::<C>))
                    .route(web::delete().to(delete_dataset_handler::<C>)),
            )
            .service(web::resource("").route(web::post().to(create_dataset_handler::<C>))), // must come last to not match other routes
    )
    .service(web::resource("/datasets").route(web::get().to(list_datasets_handler::<C>)));
}

/// Lists available volumes.
#[utoipa::path(
    tag = "Datasets",
    get,
    path = "/dataset/volumes",
    responses(
        (status = 200, description = "OK", body = [Volume],
            example = json!([
                {
                    "name": "test_data",
                    "path": "./test_data/"
                }
            ])
        ),
        (status = 401, response = crate::api::model::responses::UnauthorizedAdminResponse)
    ),
    security(
        ("session_token" = [])
    )
)]
#[allow(clippy::unused_async)]
pub async fn list_volumes_handler<C: ApplicationContext>(
    app_ctx: web::Data<C>,
    session: C::Session,
) -> Result<impl Responder> {
    let volumes = app_ctx.session_context(session).volumes()?;
    Ok(web::Json(volumes))
}

/// Lists available datasets.
#[utoipa::path(
    tag = "Datasets",
    get,
    path = "/datasets",
    responses(
        (status = 200, description = "OK", body = [DatasetListing],
            example = json!([
                {
                    "id": {
                        "internal": "9c874b9e-cea0-4553-b727-a13cb26ae4bb"
                    },
                    "name": "Germany",
                    "description": "Boundaries of Germany",
                    "tags": [],
                    "sourceOperator": "OgrSource",
                    "resultDescriptor": {
                        "vector": {
                            "dataType": "MultiPolygon",
                            "spatialReference": "EPSG:4326",
                            "columns": {}
                        }
                    }
                }
            ])
        ),
        (status = 400, response = crate::api::model::responses::BadRequestQueryResponse),
        (status = 401, response = crate::api::model::responses::UnauthorizedUserResponse)
    ),
    params(
        DatasetListOptions
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn list_datasets_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    options: ValidatedQuery<DatasetListOptions>,
) -> Result<impl Responder> {
    let options = options.into_inner();
    let list = app_ctx
        .session_context(session)
        .db()
        .list_datasets(options)
        .await?;
    Ok(web::Json(list))
}

/// Add a tile to a gdal dataset.
#[utoipa::path(
    tag = "Datasets",
    post,
    path = "/dataset/{dataset}/tiles",
    request_body = Vec<AddDatasetTile>,
    responses(
        (status = 200),
    ),
    params(
        ("dataset" = DatasetName, description = "Dataset Name"),
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn add_dataset_tiles_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    dataset: web::Path<DatasetName>,
    tiles: Json<Vec<AddDatasetTile>>,
) -> Result<HttpResponse, AddDatasetTilesError> {
    let session_context = app_ctx.session_context(session);
    let db = session_context.db();

    let dataset = dataset.into_inner();
    let dataset_id = db
        .resolve_dataset_name_to_id(&dataset)
        .await
        .context(CannotLoadDatasetForAddingTiles)?;

    // handle the case where the dataset name is not known
    let dataset_id = dataset_id
        .ok_or(error::Error::UnknownDatasetName {
            dataset_name: dataset.to_string(),
        })
        .context(CannotLoadDatasetForAddingTiles)?;

    let dataset = db
        .load_dataset(&dataset_id)
        .await
        .context(CannotLoadDatasetForAddingTiles)?;

    ensure!(
        dataset.source_operator == OperatorsMultiBandGdalSource::TYPE_NAME,
        DatasetIsNotGdalMultiBand
    );

    let TypedResultDescriptor::Raster(dataset_descriptor) = dataset.result_descriptor else {
        return Err(AddDatasetTilesError::DatasetIsNotGdalMultiBand);
    };

    let tiles = tiles.into_inner();

    let data_path = dataset
        .data_path
        .ok_or(AddDatasetTilesError::DatasetIsMissingDataPath)?;

    let data_path_file_path =
        file_path_from_data_path(&data_path, &session_context).context(CannotAddTilesToDataset)?;

    for tile in &tiles {
        validate_tile(tile, &data_path, &data_path_file_path, &dataset_descriptor)?;
    }

    db.add_dataset_tiles(dataset_id, tiles)
        .await
        .context(CannotAddTilesToDataset)?;

    Ok(HttpResponse::Ok().finish())
}

/// Validates a tile file path against the dataset's data path and returns the absolute path
/// to open. External data uses remote URLs (http://, https://, s3://) and must not refer to
/// local filesystem paths; the GDAL virtual file system prefix is added only when the dataset
/// is opened, so external paths resolve to an empty base.
fn validate_tile_file_path(
    file_path: &Path,
    data_path: &DataPath,
    data_path_file_path: &Path,
) -> Result<PathBuf, AddDatasetTilesError> {
    if matches!(data_path, DataPath::External) {
        ensure!(
            data_path.validate_file_path(file_path).is_ok(),
            TileFileMustBeExternalPath {
                file_path: file_path.to_string_lossy().to_string()
            }
        );
        return Ok(PathBuf::new());
    }

    // Volume and upload paths must be relative; they are resolved against the data path
    ensure!(
        file_path.is_relative(),
        TileFilePathNotRelative {
            file_path: file_path.to_string_lossy().to_string()
        }
    );

    let absolute_path = data_path_file_path.join(file_path);

    ensure!(
        absolute_path.exists(),
        TileFilePathDoesNotExist {
            file_path: file_path.to_string_lossy().to_string(),
            absolute_path: absolute_path.to_string_lossy().to_string(),
        }
    );

    Ok(absolute_path)
}

fn validate_tile(
    tile: &AddDatasetTile,
    data_path: &DataPath,
    data_path_file_path: &Path,
    dataset_descriptor: &RasterResultDescriptor,
) -> Result<(), AddDatasetTilesError> {
    // external paths are opened through GDAL's virtual file system, which the caller
    // already prefixed, so there is no local file to inspect
    if matches!(data_path, DataPath::External) {
        validate_tile_file_path(&tile.params.file_path, data_path, data_path_file_path)?;
        return Ok(());
    }

    let absolute_path =
        validate_tile_file_path(&tile.params.file_path, data_path, data_path_file_path)?;

    let ds = gdal_open_dataset_ex(&absolute_path, DatasetOptions::default()).context(
        CannotOpenTileFile {
            file_path: tile.params.file_path.to_string_lossy().to_string(),
        },
    )?;

    let rd = raster_descriptor_from_dataset(&ds, tile.params.rasterband_channel).context(
        CannotGetRasterDescriptorFromTileFile {
            file_path: tile.params.file_path.to_string_lossy().to_string(),
        },
    )?;

    // TODO: move this inside the db? we do not want to open datasets while keeping a database transaction, though
    ensure!(
        rd.data_type == dataset_descriptor.data_type,
        TileFileDataTypeMismatch {
            expected: dataset_descriptor.data_type,
            found: rd.data_type,
            file_path: tile.params.file_path.to_string_lossy().to_string(),
        }
    );

    ensure!(
        rd.spatial_reference == dataset_descriptor.spatial_reference,
        TileFileSpatialReferenceMismatch {
            expected: dataset_descriptor.spatial_reference,
            found: rd.spatial_reference,
            file_path: tile.params.file_path.to_string_lossy().to_string(),
        }
    );

    ensure!(
        tile.band < dataset_descriptor.bands.count(),
        TileFileBandDoesNotExist {
            band_count: dataset_descriptor.bands.count(),
            found: tile.band,
            file_path: tile.params.file_path.to_string_lossy().to_string(),
        }
    );

    // TODO: also check that the tiles bbox (from the tile definition, and not the actual gdal dataset of the tile's file) fits into the dataset's spatial grid?
    let tile_geotransform = geoengine_datatypes::raster::GeoTransform::try_from(
        geoengine_operators::source::GdalDatasetGeoTransform::from(tile.params.geo_transform),
    )
    .map_err(|_| AddDatasetTilesError::InvalidTileFileGeoTransform {
        file_path: tile.params.file_path.to_string_lossy().to_string(),
    })?;
    ensure!(
        dataset_descriptor
            .spatial_grid
            .geo_transform()
            .is_compatible_grid(tile_geotransform),
        TileFileGeoTransformMismatch {
            expected: dataset_descriptor.spatial_grid.geo_transform(),
            found: tile_geotransform,
            file_path: tile.params.file_path.to_string_lossy().to_string(),
        }
    );

    Ok(())
}

fn file_path_from_data_path<T: SessionContext>(
    data_path: &DataPath,
    session_context: &T,
) -> Result<std::path::PathBuf> {
    Ok(match data_path {
        DataPath::Volume(volume_name) => session_context
            .volumes()?
            .iter()
            .find(|v| v.name == volume_name.0)
            .ok_or(Error::UnknownVolumeName {
                volume_name: volume_name.0.clone(),
            })?
            .path
            .clone()
            .ok_or(Error::CannotAccessVolumePath {
                volume_name: volume_name.0.clone(),
            })?
            .into(),
        DataPath::Upload(upload_id) => upload_id.root_path()?,
        DataPath::External => PathBuf::new(),
    })
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, ToSchema)]
pub struct AddDatasetTile {
    pub time: crate::api::model::datatypes::TimeInterval,
    pub spatial_partition: SpatialPartition2D,
    pub band: u32,
    pub z_index: i64,
    pub params: GdalDatasetParameters,
}

/// One MD array file of an `MdGdalSource` dataset, covering all of that file's z slices.
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, ToSchema)]
pub struct AddDatasetMdTile {
    /// the presented footprint (wrap-around aware)
    pub spatial_partition: SpatialPartition2D,
    pub band: u32,
    /// position of this file in the concatenated z axis of `band`
    pub z_index: i64,
    pub array_name: String,
    /// "/"-separated path to the MD group below the root group; `None` = root group
    #[serde(default)]
    pub array_group: Option<String>,
    pub time_descriptor: crate::api::model::operators::TimeDescriptor,
    /// One interval per z slice, always required.
    ///
    /// Both forms are stored on purpose: `time_descriptor` is the compact form the API
    /// advertises, `time_steps` says which slices exist and is what the read path derives
    /// the file's slice count from. Omitting it would make the file contribute zero slices
    /// and every later file come back at the wrong time.
    pub time_steps: Vec<crate::api::model::datatypes::TimeInterval>,
    pub params: GdalDatasetParameters,
    /// Fixed index into each dimension between z and (y, x), so one row is one slice of a
    /// 4D array - `[depth]` for `(time, depth, y, x)`. Empty for 3D.
    ///
    /// Per row rather than per dataset, so the bands of one dataset can each select a
    /// different slice: a `(time, depth, y, x)` file with one row per depth becomes one
    /// dataset whose band `b` is depth `b`.
    #[serde(default)]
    pub leading_prefix: Vec<i64>,
}

impl AddDatasetMdTile {
    /// The file's overall time bounds, derived from the declared steps.
    ///
    /// Row filtering and gap filling read the stored `time` column, so it has to be the
    /// extent the steps actually cover. Deriving it keeps a row from being described as
    /// covering a window it does not.
    pub(crate) fn file_time_bounds(&self) -> crate::api::model::datatypes::TimeInterval {
        self.md_times().bounds().into()
    }

    fn md_times(&self) -> geoengine_operators::source::MdFileTimes {
        geoengine_operators::source::MdFileTimes {
            descriptor: self.time_descriptor.clone().into(),
            steps: self
                .time_steps
                .iter()
                .map(|t| geoengine_datatypes::primitives::TimeInterval::from(*t))
                .collect(),
        }
    }
}

/// Adds MD array files to an `MdGdalSource` dataset.
///
/// One row per file, covering all of that file's z slices. The per-slice times live in
/// `timeDescriptor` plus `timeSteps`, and the file's overall bounds are derived from them
/// when the row is stored, so a request cannot describe a row as covering a window it does
/// not.
#[utoipa::path(
    tag = "Datasets",
    post,
    path = "/dataset/{dataset}/md-tiles",
    request_body = Vec<AddDatasetMdTile>,
    responses(
        (status = 200),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 401, response = crate::api::model::responses::UnauthorizedUserResponse)
    ),
    params(
        ("dataset" = DatasetName, description = "Dataset Name"),
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn add_md_dataset_tiles_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    dataset: web::Path<DatasetName>,
    tiles: Json<Vec<AddDatasetMdTile>>,
) -> Result<HttpResponse, AddDatasetMdTilesError> {
    let session_context = app_ctx.session_context(session);
    let db = session_context.db();

    let dataset = dataset.into_inner();
    let dataset_id = db
        .resolve_dataset_name_to_id(&dataset)
        .await
        .context(CannotLoadDatasetForAddingMdTiles)?;

    let dataset_id = dataset_id
        .ok_or(error::Error::UnknownDatasetName {
            dataset_name: dataset.to_string(),
        })
        .context(CannotLoadDatasetForAddingMdTiles)?;

    let dataset = db
        .load_dataset(&dataset_id)
        .await
        .context(CannotLoadDatasetForAddingMdTiles)?;

    ensure!(
        dataset.source_operator == geoengine_operators::source::MdGdalSource::TYPE_NAME,
        DatasetIsNotMdGdal
    );

    let meta_data = db
        .load_loading_info(&dataset_id)
        .await
        .context(CannotLoadDatasetForAddingMdTiles)?;

    let crate::datasets::storage::MetaDataDefinition::GdalMdMetaData(md_meta_data) = &meta_data
    else {
        return Err(AddDatasetMdTilesError::DatasetIsNotMdGdal);
    };
    let wrap = md_meta_data.wrap;

    let TypedResultDescriptor::Raster(dataset_descriptor) = dataset.result_descriptor else {
        return Err(AddDatasetMdTilesError::DatasetIsNotMdGdal);
    };

    let tiles = tiles.into_inner();

    let data_path = dataset
        .data_path
        .ok_or(AddDatasetMdTilesError::MdDatasetIsMissingDataPath)?;

    let data_path_file_path = file_path_from_data_path(&data_path, &session_context)
        .context(CannotAddMdTilesToDataset)?;

    // Opening a tile's array is a header read, but for external data it is a /vsicurl
    // request, and a 65-file yearly series would pay 65 of them to learn the same thing 65
    // times over. So every file is checked for local data, and for external data only the
    // first row of each distinct array is - which is enough to catch the errors that are
    // shared by all rows of a dataset: a wrong array name, group, grid, slice count, prefix
    // length or CRS. A per-file divergence still fails loudly at read time, because the read
    // window then exceeds the array.
    let external = matches!(data_path, DataPath::External);
    let mut checked_arrays: HashSet<(String, Option<String>)> = HashSet::new();
    for tile in &tiles {
        if external && !checked_arrays.insert((tile.array_name.clone(), tile.array_group.clone())) {
            validate_md_tile(
                tile,
                wrap,
                &data_path,
                &data_path_file_path,
                &dataset_descriptor,
                false,
            )?;
            continue;
        }
        validate_md_tile(
            tile,
            wrap,
            &data_path,
            &data_path_file_path,
            &dataset_descriptor,
            true,
        )?;
    }

    db.add_md_dataset_tiles(dataset_id, tiles)
        .await
        .context(CannotAddMdTilesToDataset)?;

    Ok(HttpResponse::Ok().finish())
}

/// The z size of the tile's MD array, or `None` if the file, group or array cannot be opened.
///
/// MD arrays are not exposed as classic raster bands, so the array has to be reached through
/// GDAL's multidim API instead of `raster_descriptor_from_dataset`.
///
/// ponytail: GDAL on the caller's thread, not in the worker pool - see the note on
/// `ProbedGdalMdMetaData`. Fold this into the probe endpoint once probing is pooled.
/// The shape and declared CRS of the tile's MD array.
///
/// Returns `None` if the file, group or array cannot be opened, and otherwise the z size,
/// the `(y, x)` sizes and the CF-declared CRS (if any). Reading these is a header-only
/// operation.
struct MdArrayShape {
    z: usize,
    y: usize,
    x: usize,
    /// one index per dimension between z and `(y, x)`; `[depth]` for `(time, depth, y, x)`
    prefix_len: usize,
    crs: Option<SpatialReference>,
}

fn md_array_shape(path: &Path, tile: &AddDatasetMdTile) -> Option<MdArrayShape> {
    // The tile's config options are what lets GDAL read `.nc` over `/vsicurl` at all, so a
    // check that opens the file has to run in the same GDAL environment as the read does.
    // Without this guard every remote row was rejected as unopenable while reading it worked.
    let params: geoengine_operators::source::GdalDatasetParameters = tile.params.clone().into();
    let _configs = params
        .gdal_config_options_for_request()
        .map(|configs| GdalConfigOptions::new(&configs).expect("GDAL config options are valid"));

    let dataset = gdal_open_dataset_ex(
        path,
        DatasetOptions {
            open_flags: GdalOpenFlags::GDAL_OF_RASTER | GdalOpenFlags::GDAL_OF_MULTIDIM_RASTER,
            ..Default::default()
        },
    )
    .ok()?;

    let mut group = dataset.root_group().ok()?;
    for segment in tile
        .array_group
        .as_deref()
        .unwrap_or_default()
        .split('/')
        .filter(|segment| !segment.is_empty())
    {
        group = group.open_group(segment, Default::default()).ok()?;
    }

    let md_array = group
        .open_md_array(&tile.array_name, Default::default())
        .ok()?;
    let dimensions = md_array.dimensions().ok()?;
    if dimensions.len() < 3 {
        return None;
    }

    // z is dimension 0; any dimensions between it and (y, x) are the leading prefix and do
    // not contribute z slices
    let crs = ["crs", "spatial_ref", "grid_mapping"]
        .into_iter()
        .find_map(|name| md_array.attribute(name).ok().map(|a| a.read_as_string()))
        .and_then(|definition| SpatialRef::from_definition(definition.trim()).ok())
        .and_then(|srs| SpatialReference::try_from(srs).ok());

    Some(MdArrayShape {
        z: dimensions[0].size(),
        y: dimensions[dimensions.len() - 2].size(),
        x: dimensions[dimensions.len() - 1].size(),
        prefix_len: dimensions.len() - 3,
        crs,
    })
}

/// The path an MD row's array is opened with.
///
/// [`validate_tile_file_path`] resolves volume/upload rows against the data path root and
/// returns an **empty** path for external data, whose rows carry their own absolute
/// `/vsicurl` URL in `params.file_path` - which is what the read path opens. Using the
/// resolved path unconditionally opened `""` and rejected every remote row as unopenable
/// while reading it worked.
fn md_tile_open_path(
    tile: &AddDatasetMdTile,
    data_path: &DataPath,
    absolute_path: &Path,
) -> PathBuf {
    if matches!(data_path, DataPath::External) {
        tile.params.file_path.clone()
    } else {
        absolute_path.to_path_buf()
    }
}

/// Checks a tile's declared metadata against the array it names.
///
/// This is the only place a wrong `arrayName`, `leadingPrefix`, grid, slice count or CRS can
/// be caught, so it runs even for external data - where the file is not opened elsewhere.
fn check_md_tile_against_array(
    tile: &AddDatasetMdTile,
    open_path: &Path,
    dataset_descriptor: &RasterResultDescriptor,
) -> Result<(), AddDatasetMdTilesError> {
    let file_path = tile.params.file_path.to_string_lossy().to_string();

    let shape =
        md_array_shape(open_path, tile).ok_or(AddDatasetMdTilesError::CannotOpenMdTileFile {
            source: geoengine_operators::error::Error::InvalidOperatorSpec {
                reason: format!(
                    "MD array '{}{}' could not be opened",
                    tile.array_name,
                    tile.array_group
                        .as_deref()
                        .map(|g| format!(" in group '{g}'"))
                        .unwrap_or_default()
                ),
            },
            file_path: file_path.clone(),
        })?;

    let slices = tile.time_steps.len();
    if shape.z != slices {
        return Err(AddDatasetMdTilesError::MdTileSliceCountMismatch {
            expected: slices,
            found: shape.z,
            file_path,
        });
    }

    // a 4D array needs one prefix index per dimension between z and (y, x)
    if shape.prefix_len != tile.leading_prefix.len() {
        return Err(AddDatasetMdTilesError::MdTileLeadingPrefixMismatch {
            expected: shape.prefix_len,
            found: tile.leading_prefix.len(),
            file_path,
        });
    }

    if (shape.x, shape.y) != (tile.params.width, tile.params.height) {
        return Err(AddDatasetMdTilesError::MdTileArraySizeMismatch {
            declared: (tile.params.width, tile.params.height),
            found: (shape.x, shape.y),
            file_path,
        });
    }

    // a declared CRS the file contradicts is a mislabelled dataset; a file that declares
    // none is not an error, because degrees-based x units justify EPSG:4326 on their own
    if let (Some(declared), Some(found)) = (
        Option::<SpatialReference>::from(dataset_descriptor.spatial_reference),
        shape.crs,
    ) && declared != found
    {
        return Err(AddDatasetMdTilesError::MdTileCrsMismatch {
            declared: declared.to_string(),
            found: found.to_string(),
            file_path,
        });
    }

    Ok(())
}

/// Validates one MD tile at the trust boundary: the file must exist, the named array must
/// open with a z dimension matching the declared time steps, the band must exist, and the
/// file's grid must match the dataset's (with the wrap-around shift applied).
fn validate_md_tile(
    tile: &AddDatasetMdTile,
    wrap: bool,
    data_path: &DataPath,
    data_path_file_path: &Path,
    dataset_descriptor: &RasterResultDescriptor,
    check_against_array: bool,
) -> Result<(), AddDatasetMdTilesError> {
    let file_path = tile.params.file_path.to_string_lossy().to_string();

    let absolute_path =
        validate_tile_file_path(&tile.params.file_path, data_path, data_path_file_path).map_err(
            |e| AddDatasetMdTilesError::InvalidMdTileFile {
                source: e,
                file_path: file_path.clone(),
            },
        )?;

    ensure!(
        tile.band < dataset_descriptor.bands.count(),
        MdTileBandDoesNotExist {
            band_count: dataset_descriptor.bands.count(),
            found: tile.band,
            file_path: file_path.clone(),
        }
    );

    // required for every file, on every data path: the slice count below is derived from it,
    // and a file with no declared slices would contribute nothing to the time axis
    ensure!(
        !tile.time_steps.is_empty(),
        MdTileMissingTimeSteps {
            file_path: file_path.clone(),
        }
    );

    // the file's raw transform (possibly south-up and/or 0..360) has to land on the
    // dataset's presented transform
    let expected = dataset_descriptor.spatial_grid.geo_transform();
    let file_transform = geoengine_operators::source::presented_geo_transform(
        geoengine_operators::source::GdalDatasetGeoTransform::from(tile.params.geo_transform),
        tile.params.height,
        wrap,
    );
    ensure!(
        expected.is_compatible_grid(file_transform),
        MdTileGeoTransformMismatch {
            expected,
            found: file_transform,
            file_path: file_path.clone(),
        }
    );

    // both columns come from outside, and a row whose descriptor disagrees with its steps
    // would be read with `steps` while advertised with `descriptor`. Checked before anything
    // path-specific: it needs no file, and external rows are stored exactly the same way.
    let times = tile.md_times();
    ensure!(
        times.is_consistent(),
        MdTileTimeAxisInconsistent {
            file_path: file_path.clone(),
        }
    );
    ensure!(
        times.is_ordered(),
        MdTileTimeAxisUnordered {
            file_path: file_path.clone(),
        }
    );

    if check_against_array {
        check_md_tile_against_array(
            tile,
            &md_tile_open_path(tile, data_path, &absolute_path),
            dataset_descriptor,
        )?;
    }

    Ok(())
}

/// Retrieves details about a dataset using the internal name.
#[utoipa::path(
    tag = "Datasets",
    get,
    path = "/dataset/{dataset}",
    responses(
        (status = 200, description = "OK", body = Dataset,
            example = json!({
                "id": {
                    "internal": "9c874b9e-cea0-4553-b727-a13cb26ae4bb"
                },
                "name": "Germany",
                "description": "Boundaries of Germany",
                "resultDescriptor": {
                    "vector": {
                        "dataType": "MultiPolygon",
                        "spatialReference": "EPSG:4326",
                        "columns": {}
                    }
                },
                "sourceOperator": "OgrSource"
            })
        ),
        (status = 400, description = "Bad request", body = ErrorResponse, examples(
            ("Referenced an unknown dataset" = (value = json!({
                "error": "CannotLoadDataset",
                "message": "CannotLoadDataset: UnknownDatasetName"
            })))
        )),
        (status = 401, response = crate::api::model::responses::UnauthorizedUserResponse)
    ),
    params(
        ("dataset" = DatasetName, description = "Dataset Name")
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn get_dataset_handler<C: ApplicationContext>(
    dataset: web::Path<DatasetName>,
    session: C::Session,
    app_ctx: web::Data<C>,
) -> Result<impl Responder, GetDatasetError> {
    let session_ctx = app_ctx.session_context(session).db();

    let real_dataset = dataset.into_inner();

    let dataset_id = session_ctx
        .resolve_dataset_name_to_id(&real_dataset)
        .await
        .context(CannotLoadDataset)?;

    // handle the case where the dataset name is not known
    let dataset_id = dataset_id
        .ok_or(error::Error::UnknownDatasetName {
            dataset_name: real_dataset.to_string(),
        })
        .context(CannotLoadDataset)?;

    let dataset = session_ctx
        .load_dataset(&dataset_id)
        .await
        .context(CannotLoadDataset)?;

    let dataset: Dataset = dataset.into();

    Ok(web::Json(dataset))
}

/// Update details about a dataset using the internal name.
#[utoipa::path(
    tag = "Datasets",
    post,
    path = "/dataset/{dataset}",
    request_body = UpdateDataset,
    responses(
        (status = 200, description = "OK" ),
        (status = 400, description = "Bad request", body = ErrorResponse, examples(
            ("Referenced an unknown dataset" = (value = json!({
                "error": "CannotLoadDataset",
                "message": "CannotLoadDataset: UnknownDatasetName"
            })))
        )),
        (status = 401, response = crate::api::model::responses::UnauthorizedUserResponse)
    ),
    params(
        ("dataset" = DatasetName, description = "Dataset Name"),
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn update_dataset_handler<C: ApplicationContext>(
    dataset: web::Path<DatasetName>,
    session: C::Session,
    app_ctx: web::Data<C>,
    update: ValidatedJson<UpdateDataset>,
) -> Result<impl Responder, UpdateDatasetError> {
    let session_ctx = app_ctx.session_context(session).db();

    let real_dataset = dataset.into_inner();

    let dataset_id = session_ctx
        .resolve_dataset_name_to_id(&real_dataset)
        .await
        .context(CannotLoadDatasetForUpdate)?;

    // handle the case where the dataset name is not known
    let dataset_id = dataset_id
        .ok_or(error::Error::UnknownDatasetName {
            dataset_name: real_dataset.to_string(),
        })
        .context(CannotLoadDatasetForUpdate)?;

    session_ctx
        .update_dataset(dataset_id, update.into_inner())
        .await
        .context(CannotUpdateDataset)?;

    Ok(HttpResponse::Ok())
}

/// Retrieves the loading information of a dataset
#[utoipa::path(
    tag = "Datasets",
    get,
    path = "/dataset/{dataset}/loadingInfo",
    responses(
        (status = 200, description = "OK", body = MetaDataDefinition)
    ),
    params(
        ("dataset" = DatasetName, description = "Dataset Name")
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn get_loading_info_handler<C: ApplicationContext>(
    dataset: web::Path<DatasetName>,
    session: C::Session,
    app_ctx: web::Data<C>,
) -> Result<web::Json<MetaDataDefinition>> {
    let session_ctx = app_ctx.session_context(session).db();

    let real_dataset = dataset.into_inner();

    let dataset_id = session_ctx
        .resolve_dataset_name_to_id(&real_dataset)
        .await?;

    // handle the case where the dataset name is not known
    let dataset_id = dataset_id.ok_or(error::Error::UnknownDatasetName {
        dataset_name: real_dataset.to_string(),
    })?;

    let dataset = session_ctx.load_loading_info(&dataset_id).await?;

    Ok(web::Json(dataset.into()))
}

/// Updates the dataset's loading info
#[utoipa::path(
    tag = "Datasets",
    put,
    path = "/dataset/{dataset}/loadingInfo",
    request_body = MetaDataDefinition,
    responses(
        (status = 200, description = "OK"),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 401, response = crate::api::model::responses::UnauthorizedUserResponse)
    ),
    params(
        ("dataset" = DatasetName, description = "Dataset Name"),
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn update_loading_info_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    dataset: web::Path<DatasetName>,
    meta_data: web::Json<MetaDataDefinition>,
) -> Result<HttpResponse> {
    let session_ctx = app_ctx.session_context(session).db();

    let real_dataset = dataset.into_inner();

    let dataset_id = session_ctx
        .resolve_dataset_name_to_id(&real_dataset)
        .await?;

    // handle the case where the dataset name is not known
    let dataset_id = dataset_id.ok_or(error::Error::UnknownDatasetName {
        dataset_name: real_dataset.to_string(),
    })?;

    session_ctx
        .update_dataset_loading_info(dataset_id, &meta_data.into_inner().into())
        .await?;

    Ok(HttpResponse::Ok().finish())
}

/// Updates the dataset's symbology
#[utoipa::path(
    tag = "Datasets",
    put,
    path = "/dataset/{dataset}/symbology",
    request_body = Symbology,
    responses(
        (status = 200, description = "OK"),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 401, response = crate::api::model::responses::UnauthorizedUserResponse)
    ),
    params(
        ("dataset" = DatasetName, description = "Dataset Name"),
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn update_dataset_symbology_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    dataset: web::Path<DatasetName>,
    symbology: web::Json<Symbology>,
) -> Result<impl Responder> {
    let session_ctx = app_ctx.session_context(session).db();

    let real_dataset = dataset.into_inner();

    let dataset_id = session_ctx
        .resolve_dataset_name_to_id(&real_dataset)
        .await?;

    // handle the case where the dataset name is not known
    let dataset_id = dataset_id.ok_or(error::Error::UnknownDatasetName {
        dataset_name: real_dataset.to_string(),
    })?;

    session_ctx
        .update_dataset_symbology(dataset_id, &symbology.into_inner())
        .await?;

    Ok(HttpResponse::Ok())
}

// Updates the dataset's provenance
#[utoipa::path(
    tag = "Datasets",
    put,
    path = "/dataset/{dataset}/provenance",
    request_body = Provenances,
    responses(
        (status = 200, description = "OK"),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 401, response = crate::api::model::responses::UnauthorizedUserResponse)
    ),
    params(
        ("dataset" = DatasetName, description = "Dataset Name"),
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn update_dataset_provenance_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    dataset: web::Path<DatasetName>,
    provenance: ValidatedJson<Provenances>,
) -> Result<HttpResponseBuilder> {
    let session_ctx = app_ctx.session_context(session).db();

    let real_dataset = dataset.into_inner();

    let dataset_id = session_ctx
        .resolve_dataset_name_to_id(&real_dataset)
        .await?;

    // handle the case where the dataset name is not known
    let dataset_id = dataset_id.ok_or(error::Error::UnknownDatasetName {
        dataset_name: real_dataset.to_string(),
    })?;

    let provenance = provenance
        .into_inner()
        .provenances
        .into_iter()
        .map(Into::into)
        .collect::<Vec<_>>();

    session_ctx
        .update_dataset_provenance(dataset_id, &provenance)
        .await?;

    Ok(HttpResponse::Ok())
}

pub async fn create_upload_dataset<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    upload_id: UploadId,
    mut definition: DatasetDefinition,
) -> Result<web::Json<DatasetNameResponse>, CreateDatasetError> {
    let db = app_ctx.session_context(session).db();
    let upload = db.load_upload(upload_id).await.context(UploadNotFound)?;

    add_tag(&mut definition.properties, "upload".to_owned());

    adjust_meta_data_path(&mut definition.meta_data, &upload)
        .context(CannotResolveUploadFilePath)?;

    let result = db
        .add_dataset(
            definition.properties.into(),
            definition.meta_data.into(),
            Some(DataPath::Upload(upload_id)),
        )
        .await
        .context(CannotCreateDataset)?;

    Ok(web::Json(result.name.into()))
}

pub fn adjust_meta_data_path<A: AdjustFilePath>(
    meta: &mut MetaDataDefinition,
    adjust: &A,
) -> Result<()> {
    match meta {
        MetaDataDefinition::MockMetaData(_) => {}
        MetaDataDefinition::OgrMetaData(m) => {
            m.loading_info.file_name = adjust.adjust_file_path(&m.loading_info.file_name)?;
        }
        MetaDataDefinition::GdalMetaDataRegular(m) => {
            m.params.file_path = adjust.adjust_file_path(&m.params.file_path)?;
        }
        MetaDataDefinition::GdalStatic(m) => {
            m.params.file_path = adjust.adjust_file_path(&m.params.file_path)?;
        }
        MetaDataDefinition::GdalMetadataNetCdfCf(m) => {
            m.params.file_path = adjust.adjust_file_path(&m.params.file_path)?;
        }
        MetaDataDefinition::GdalMetaDataList(m) => {
            for p in &mut m.params {
                if let Some(ref mut params) = p.params {
                    params.file_path = adjust.adjust_file_path(&params.file_path)?;
                }
            }
        }
        MetaDataDefinition::GdalMultiBand(_gdal_multi_band) => {
            // do nothing, the file paths are not inside the meta data defintion but inside the dataset's tiles
        }
        MetaDataDefinition::GdalMdMetaData(_gdal_md_meta_data) => {
            // do nothing, the file paths are not inside the meta data definition but inside the dataset's MD tiles
        }
    }
    Ok(())
}

/// Add the upload tag to the dataset properties.
/// If the tag already exists, it will not be added again.
pub fn add_tag(properties: &mut AddDataset, tag: String) {
    if let Some(ref mut tags) = properties.tags {
        if !tags.contains(&tag) {
            tags.push(tag);
        }
    } else {
        properties.tags = Some(vec![tag]);
    }
}

/// Creates a new dataset using previously uploaded files.
/// The format of the files will be automatically detected when possible.
#[utoipa::path(
    tag = "Datasets",
    post,
    path = "/dataset/auto",
    request_body = AutoCreateDataset,
    responses(
        (status = 200, body = DatasetNameResponse),
        (status = 400, description = "Bad request", body = ErrorResponse, examples(
            ("Body is invalid json" = (value = json!({
                "error": "BodyDeserializeError",
                "message": "expected `,` or `}` at line 13 column 7"
            }))),
            ("Failed to read body" = (value = json!({
                "error": "Payload",
                "message": "Error that occur during reading payload: Can not decode content-encoding."
            }))),
            ("Referenced an unknown upload" = (value = json!({
                "error": "UnknownUploadId",
                "message": "Unknown upload id"
            }))),
            ("Dataset name is empty" = (value = json!({
                "error": "InvalidDatasetName",
                "message": "Invalid dataset name"
            }))),
            ("Upload filename is invalid" = (value = json!({
                "error": "InvalidUploadFileName",
                "message": "Invalid upload file name"
            }))),
            ("File does not exist" = (value = json!({
                "error": "GdalError",
                "message": "GdalError: GDAL method 'GDALOpenEx' returned a NULL pointer. Error msg: 'upload/0bdd1062-7796-4d44-a655-e548144281a6/asdf: No such file or directory'"
            }))),
            ("Dataset has no auto-importable layer" = (value = json!({
                "error": "DatasetHasNoAutoImportableLayer",
                "message": "Dataset has no auto importable layer"
            })))
        )),
        (status = 401, response = crate::api::model::responses::UnauthorizedUserResponse),
        (status = 413, response = crate::api::model::responses::PayloadTooLargeResponse),
        (status = 415, response = crate::api::model::responses::UnsupportedMediaTypeForJsonResponse)
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn auto_create_dataset_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    create: ValidatedJson<AutoCreateDataset>,
) -> Result<web::Json<DatasetNameResponse>> {
    let db = app_ctx.session_context(session).db();
    let upload = db.load_upload(create.upload).await?;

    let create = create.into_inner();

    let main_file_path = upload.id.root_path()?.join(&create.main_file);
    let meta_data = auto_detect_vector_meta_data_definition(&main_file_path, &create.layer_name)?;
    let meta_data = crate::datasets::storage::MetaDataDefinition::OgrMetaData(meta_data);

    let properties = AddDataset {
        name: None,
        display_name: create.dataset_name,
        description: create.dataset_description,
        source_operator: meta_data.source_operator_type().to_owned(),
        symbology: None,
        provenance: None,
        tags: Some(vec!["upload".to_owned(), "auto".to_owned()]),
    };

    let result = db
        .add_dataset(
            properties.into(),
            meta_data,
            Some(DataPath::Upload(upload.id)),
        )
        .await?;

    Ok(web::Json(result.name.into()))
}

/// Inspects an upload and suggests metadata that can be used when creating a new dataset based on it.
/// Tries to automatically detect the main file and layer name if not specified.
#[utoipa::path(
    tag = "Datasets",
    post,
    path = "/dataset/suggest",
    request_body = SuggestMetaData,
    responses(
        (status = 200, description = "OK", body = MetaDataSuggestion,
            example = json!({
                "mainFile": "germany_polygon.gpkg",
                "metaData": {
                    "type": "OgrMetaData",
                    "loadingInfo": {
                        "fileName": "upload/23c9ea9e-15d6-453b-a243-1390967a5669/germany_polygon.gpkg",
                        "layerName": "test_germany",
                        "dataType": "MultiPolygon",
                        "time": {
                            "type": "none"
                        },
                        "defaultGeometry": null,
                        "columns": {
                            "formatSpecifics": null,
                            "x": "",
                            "y": null,
                            "int": [],
                            "float": [],
                            "text": [],
                            "bool": [],
                            "datetime": [],
                            "rename": null
                        },
                        "forceOgrTimeFilter": false,
                        "forceOgrSpatialFilter": false,
                        "onError": "ignore",
                        "sqlQuery": null,
                        "attributeQuery": null
                    },
                    "resultDescriptor": {
                        "dataType": "MultiPolygon",
                        "spatialReference": "EPSG:4326",
                        "columns": {},
                        "time": null,
                        "bbox": null
                    }
                }
            })
        ),
        (status = 400, description = "Bad request", body = ErrorResponse, examples(
            ("Missing field in query string" = (value = json!({
                "error": "UnableToParseQueryString",
                "message": "Unable to parse query string: missing field `offset`"
            }))),
            ("Number in query string contains letters" = (value = json!({
                "error": "UnableToParseQueryString",
                "message": "Unable to parse query string: invalid digit found in string"
            }))),
            ("Referenced an unknown upload" = (value = json!({
                "error": "UnknownUploadId",
                "message": "Unknown upload id"
            }))),
            ("No suitable mainfile found" = (value = json!({
                "error": "NoMainFileCandidateFound",
                "message": "No main file candidate found"
            }))),
            ("File does not exist" = (value = json!({
                "error": "GdalError",
                "message": "GdalError: GDAL method 'GDALOpenEx' returned a NULL pointer. Error msg: 'upload/0bdd1062-7796-4d44-a655-e548144281a6/asdf: No such file or directory'"
            }))),
            ("Dataset has no auto-importable layer" = (value = json!({
                "error": "DatasetHasNoAutoImportableLayer",
                "message": "Dataset has no auto importable layer"
            })))
        )),
        (status = 401, response = crate::api::model::responses::UnauthorizedUserResponse)
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn suggest_meta_data_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    suggest: web::Json<SuggestMetaData>,
) -> Result<impl Responder> {
    let suggest = suggest.into_inner();

    let (root_path, main_file) = match suggest.data_path {
        DataPath::Upload(upload) => {
            let upload = app_ctx
                .session_context(session)
                .db()
                .load_upload(upload)
                .await?;

            let main_file = suggest
                .main_file
                .or_else(|| suggest_main_file(&upload))
                .ok_or(error::Error::NoMainFileCandidateFound)?;

            let root_path = upload.id.root_path()?;

            (Some(root_path), main_file)
        }
        DataPath::Volume(volume) => {
            let main_file = suggest
                .main_file
                .ok_or(error::Error::NoMainFileCandidateFound)?;

            let volumes = Volumes::default();

            let root_path = volumes.volumes.iter().find(|v| v.name == volume).ok_or(
                crate::error::Error::UnknownVolumeName {
                    volume_name: volume.0,
                },
            )?;

            (Some(root_path.path.clone()), main_file)
        }
        DataPath::External => {
            let main_file = suggest
                .main_file
                .ok_or(error::Error::NoMainFileCandidateFound)?;

            // Validate that the file path is a valid external path (GDAL VSI path)
            DataPath::External
                .validate_file_path(Path::new(&main_file))
                .map_err(|_| error::Error::InvalidPath)?;

            // For external data, the file path is already absolute; no root path to resolve against
            (None, main_file)
        }
    };

    let layer_name = suggest.layer_name;

    let main_file_path = if let Some(ref root_path) = root_path {
        path_with_base_path(root_path, Path::new(&main_file))?
    } else {
        // For external data, the file path is already absolute
        Path::new(&main_file).to_path_buf()
    };

    let dataset = gdal_open_dataset(&main_file_path)?;

    if dataset.layer_count() > 0 {
        let meta_data = auto_detect_vector_meta_data_definition(&main_file_path, &layer_name)?;

        let layer_name = meta_data.loading_info.layer_name.clone();

        let meta_data = crate::datasets::storage::MetaDataDefinition::OgrMetaData(meta_data);

        Ok(web::Json(MetaDataSuggestion {
            main_file,
            layer_name,
            meta_data: meta_data.into(),
        }))
    } else {
        let mut gdal_params =
            gdal_parameters_from_dataset(&dataset, 1, &main_file_path, None, None)?;
        if let Some(ref root_path) = root_path
            && let Ok(relative_path) = gdal_params.file_path.strip_prefix(root_path)
        {
            gdal_params.file_path = relative_path.to_path_buf();
        }
        let result_descriptor = raster_descriptor_from_dataset(&dataset, 1)?;

        Ok(web::Json(MetaDataSuggestion {
            main_file,
            layer_name: String::new(),
            meta_data: MetaDataDefinition::GdalMetaDataList(GdalMetaDataList {
                r#type: Default::default(),
                result_descriptor: result_descriptor.into(),
                params: vec![GdalLoadingInfoTemporalSlice {
                    time: TimeInterval::default().into(),
                    params: Some(gdal_params.into()),
                    cache_ttl: None,
                }],
            }),
        }))
    }
}

fn suggest_main_file(upload: &Upload) -> Option<String> {
    let known_extensions = ["csv", "shp", "json", "geojson", "gpkg", "sqlite"]; // TODO: rasters

    if upload.files.len() == 1 {
        return Some(upload.files[0].name.clone());
    }

    let mut sorted_files = upload.files.clone();
    sorted_files.sort_by_key(|b| std::cmp::Reverse(b.byte_size));

    for file in sorted_files {
        if known_extensions.iter().any(|ext| file.name.ends_with(ext)) {
            return Some(file.name);
        }
    }
    None
}

#[allow(clippy::ref_option)]
fn select_layer_from_dataset<'a>(
    dataset: &'a gdal::Dataset,
    layer_name: &Option<String>,
) -> Result<Layer<'a>> {
    if let Some(layer_name) = layer_name {
        dataset.layer_by_name(layer_name).map_err(|_| {
            crate::error::Error::DatasetInvalidLayerName {
                layer_name: layer_name.clone(),
            }
        })
    } else {
        dataset
            .layer(0)
            .map_err(|_| crate::error::Error::DatasetHasNoAutoImportableLayer)
    }
}

#[allow(clippy::ref_option)]
fn auto_detect_vector_meta_data_definition(
    main_file_path: &Path,
    layer_name: &Option<String>,
) -> Result<StaticMetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>> {
    let dataset = gdal_open_dataset(main_file_path)?;

    auto_detect_vector_meta_data_definition_from_dataset(&dataset, main_file_path, layer_name)
}

#[allow(clippy::ref_option)]
fn auto_detect_vector_meta_data_definition_from_dataset(
    dataset: &gdal::Dataset,
    main_file_path: &Path,
    layer_name: &Option<String>,
) -> Result<StaticMetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>> {
    let layer = select_layer_from_dataset(dataset, layer_name)?;

    let columns_map = detect_columns(&layer);
    let columns_vecs = column_map_to_column_vecs(&columns_map);

    let mut geometry = detect_vector_geometry(&layer);
    let mut x = String::new();
    let mut y: Option<String> = None;

    if geometry.data_type == VectorDataType::Data {
        // help Gdal detecting geometry
        if let Some(auto_detect) = gdal_autodetect(main_file_path, &columns_vecs.text) {
            let layer = select_layer_from_dataset(&auto_detect.dataset, layer_name)?;
            geometry = detect_vector_geometry(&layer);
            if geometry.data_type != VectorDataType::Data {
                x = auto_detect.x;
                y = auto_detect.y;
            }
        }
    }

    let time = detect_time_type(&columns_vecs);

    Ok(StaticMetaData::<_, _, VectorQueryRectangle> {
        loading_info: OgrSourceDataset {
            file_name: main_file_path.into(),
            layer_name: geometry.layer_name.unwrap_or_else(|| layer.name()),
            data_type: Some(geometry.data_type),
            time,
            default_geometry: None,
            columns: Some(OgrSourceColumnSpec {
                format_specifics: None,
                x,
                y,
                int: columns_vecs.int,
                float: columns_vecs.float,
                text: columns_vecs.text,
                bool: vec![],
                datetime: columns_vecs.date,
                rename: None,
            }),
            force_ogr_time_filter: false,
            force_ogr_spatial_filter: false,
            on_error: OgrSourceErrorSpec::Ignore,
            sql_query: None,
            attribute_query: None,
            cache_ttl: None,
        },
        result_descriptor: VectorResultDescriptor {
            data_type: geometry.data_type,
            spatial_reference: geometry.spatial_reference,
            columns: columns_map
                .into_iter()
                .filter_map(|(k, v)| {
                    v.try_into()
                        .map(|v| {
                            (
                                k,
                                VectorColumnInfo {
                                    data_type: v,
                                    measurement: Measurement::Unitless,
                                },
                            )
                        })
                        .ok()
                }) // ignore all columns here that don't have a corresponding type in our collections
                .collect(),
            time: None,
            bbox: None,
        },
        phantom: Default::default(),
    })
}

/// create Gdal dataset with autodetect parameters based on available columns
fn gdal_autodetect(path: &Path, columns: &[String]) -> Option<GdalAutoDetect> {
    let columns_lower = columns.iter().map(|s| s.to_lowercase()).collect::<Vec<_>>();

    // TODO: load candidates from config
    let xy = [("x", "y"), ("lon", "lat"), ("longitude", "latitude")];

    for (x, y) in xy {
        let mut found_x = None;
        let mut found_y = None;

        for (column_lower, column) in columns_lower.iter().zip(columns) {
            if x == column_lower {
                found_x = Some(column);
            }

            if y == column_lower {
                found_y = Some(column);
            }

            if let (Some(x), Some(y)) = (found_x, found_y) {
                let mut dataset_options = DatasetOptions::default();

                let open_opts = &[
                    &format!("X_POSSIBLE_NAMES={x}"),
                    &format!("Y_POSSIBLE_NAMES={y}"),
                    "AUTODETECT_TYPE=YES",
                ];

                dataset_options.open_options = Some(open_opts);

                return gdal_open_dataset_ex(path, dataset_options)
                    .ok()
                    .map(|dataset| GdalAutoDetect {
                        dataset,
                        x: x.clone(),
                        y: Some(y.clone()),
                    });
            }
        }
    }

    // TODO: load candidates from config
    let geoms = ["geom", "wkt"];
    for geom in geoms {
        for (column_lower, column) in columns_lower.iter().zip(columns) {
            if geom == column_lower {
                let mut dataset_options = DatasetOptions::default();

                let open_opts = &[
                    &format!("GEOM_POSSIBLE_NAMES={column}"),
                    "AUTODETECT_TYPE=YES",
                ];

                dataset_options.open_options = Some(open_opts);

                return gdal_open_dataset_ex(path, dataset_options)
                    .ok()
                    .map(|dataset| GdalAutoDetect {
                        dataset,
                        x: geom.to_owned(),
                        y: None,
                    });
            }
        }
    }

    None
}

fn detect_time_type(columns: &Columns) -> OgrSourceDatasetTimeType {
    // TODO: load candidate names from config
    let known_start = [
        "start",
        "time",
        "begin",
        "date",
        "time_start",
        "start time",
        "date_start",
        "start date",
        "datetime",
        "date_time",
        "date time",
        "event",
        "timestamp",
        "time_from",
        "t1",
        "t",
    ];
    let known_end = [
        "end",
        "stop",
        "time2",
        "date2",
        "time_end",
        "time_stop",
        "time end",
        "time stop",
        "end time",
        "stop time",
        "date_end",
        "date_stop",
        "date end",
        "date stop",
        "end date",
        "stop date",
        "time_to",
        "t2",
    ];
    let known_duration = ["duration", "length", "valid for", "valid_for"];

    let mut start = None;
    let mut end = None;
    for column in &columns.date {
        if known_start.contains(&column.as_ref()) && start.is_none() {
            start = Some(column);
        } else if known_end.contains(&column.as_ref()) && end.is_none() {
            end = Some(column);
        }

        if start.is_some() && end.is_some() {
            break;
        }
    }

    let duration = columns
        .int
        .iter()
        .find(|c| known_duration.contains(&c.as_ref()));

    match (start, end, duration) {
        (Some(start), Some(end), _) => OgrSourceDatasetTimeType::StartEnd {
            start_field: start.clone(),
            start_format: OgrSourceTimeFormat::Auto,
            end_field: end.clone(),
            end_format: OgrSourceTimeFormat::Auto,
        },
        (Some(start), None, Some(duration)) => OgrSourceDatasetTimeType::StartDuration {
            start_field: start.clone(),
            start_format: OgrSourceTimeFormat::Auto,
            duration_field: duration.clone(),
        },
        (Some(start), None, None) => OgrSourceDatasetTimeType::Start {
            start_field: start.clone(),
            start_format: OgrSourceTimeFormat::Auto,
            duration: OgrSourceDurationSpec::Zero,
        },
        _ => OgrSourceDatasetTimeType::None,
    }
}

fn detect_vector_geometry(layer: &Layer) -> DetectedGeometry {
    for g in layer.defn().geom_fields() {
        if let Ok(data_type) = VectorDataType::try_from_ogr_type_code(g.field_type()) {
            return DetectedGeometry {
                layer_name: Some(layer.name()),
                data_type,
                spatial_reference: g
                    .spatial_ref()
                    .context(error::Gdal)
                    .and_then(|s| {
                        let s: Result<SpatialReference> = s.try_into().map_err(Into::into);
                        s
                    })
                    .map_or(SpatialReferenceOption::Unreferenced, Into::into),
            };
        }
    }

    // fallback type if no geometry was found
    DetectedGeometry {
        layer_name: Some(layer.name()),
        data_type: VectorDataType::Data,
        spatial_reference: SpatialReferenceOption::Unreferenced,
    }
}

struct GdalAutoDetect {
    dataset: gdal::Dataset,
    x: String,
    y: Option<String>,
}

struct DetectedGeometry {
    layer_name: Option<String>,
    data_type: VectorDataType,
    spatial_reference: SpatialReferenceOption,
}

struct Columns {
    int: Vec<String>,
    float: Vec<String>,
    text: Vec<String>,
    date: Vec<String>,
}

enum ColumnDataType {
    Int,
    Float,
    Text,
    Date,
    Unknown,
}

impl TryFrom<ColumnDataType> for FeatureDataType {
    type Error = error::Error;

    fn try_from(value: ColumnDataType) -> Result<Self, Self::Error> {
        match value {
            ColumnDataType::Int => Ok(Self::Int),
            ColumnDataType::Float => Ok(Self::Float),
            ColumnDataType::Text => Ok(Self::Text),
            ColumnDataType::Date => Ok(Self::DateTime),
            ColumnDataType::Unknown => Err(error::Error::NoFeatureDataTypeForColumnDataType),
        }
    }
}

impl TryFrom<ColumnDataType> for crate::api::model::datatypes::FeatureDataType {
    type Error = error::Error;

    fn try_from(value: ColumnDataType) -> Result<Self, Self::Error> {
        match value {
            ColumnDataType::Int => Ok(Self::Int),
            ColumnDataType::Float => Ok(Self::Float),
            ColumnDataType::Text => Ok(Self::Text),
            ColumnDataType::Date => Ok(Self::DateTime),
            ColumnDataType::Unknown => Err(error::Error::NoFeatureDataTypeForColumnDataType),
        }
    }
}

fn detect_columns(layer: &Layer) -> HashMap<String, ColumnDataType> {
    let mut columns = HashMap::default();

    for field in layer.defn().fields() {
        let field_type = field.field_type();

        let data_type = match field_type {
            OGRFieldType::OFTInteger | OGRFieldType::OFTInteger64 => ColumnDataType::Int,
            OGRFieldType::OFTReal => ColumnDataType::Float,
            OGRFieldType::OFTString => ColumnDataType::Text,
            OGRFieldType::OFTDate | OGRFieldType::OFTDateTime => ColumnDataType::Date,
            _ => ColumnDataType::Unknown,
        };

        columns.insert(field.name(), data_type);
    }

    columns
}

fn column_map_to_column_vecs(columns: &HashMap<String, ColumnDataType>) -> Columns {
    let mut int = Vec::new();
    let mut float = Vec::new();
    let mut text = Vec::new();
    let mut date = Vec::new();

    for (k, v) in columns {
        match v {
            ColumnDataType::Int => int.push(k.clone()),
            ColumnDataType::Float => float.push(k.clone()),
            ColumnDataType::Text => text.push(k.clone()),
            ColumnDataType::Date => date.push(k.clone()),
            ColumnDataType::Unknown => {}
        }
    }

    Columns {
        int,
        float,
        text,
        date,
    }
}

/// Delete a dataset
#[utoipa::path(
    tag = "Datasets",
    delete,
    path = "/dataset/{dataset}",
    responses(
        (status = 200, description = "OK"),
        (status = 400, description = "Bad request", body = ErrorResponse, examples(
            ("Referenced an unknown dataset" = (value = json!({
                "error": "UnknownDatasetName",
                "message": "Unknown dataset name"
            }))),
            ("Given dataset can only be deleted by owner" = (value = json!({
                "error": "OperationRequiresOwnerPermission",
                "message": "Operation requires owner permission"
            })))
        )),
        (status = 401, response = crate::api::model::responses::UnauthorizedUserResponse)
    ),
    params(
        ("dataset" = DatasetName, description = "Dataset id")
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn delete_dataset_handler<C: ApplicationContext>(
    dataset: web::Path<DatasetName>,
    session: C::Session,
    app_ctx: web::Data<C>,
) -> Result<HttpResponse> {
    let session_ctx = app_ctx.session_context(session).db();

    let real_dataset = dataset.into_inner();

    let dataset_id = session_ctx
        .resolve_dataset_name_to_id(&real_dataset)
        .await?;

    // handle the case where the dataset name is not known
    let dataset_id = dataset_id.ok_or(error::Error::UnknownDatasetName {
        dataset_name: real_dataset.to_string(),
    })?;

    session_ctx.delete_dataset(dataset_id).await?;

    Ok(actix_web::HttpResponse::Ok().finish())
}

#[derive(Deserialize, Serialize, ToSchema, ToResponse)]
pub struct VolumeFileLayersResponse {
    layers: Vec<String>,
}

/// List the layers of a file in a volume.
#[utoipa::path(
    tag = "Datasets",
    get,
    path = "/dataset/volumes/{volume_name}/files/{file_name}/layers",
    responses(
        (status = 200, body = VolumeFileLayersResponse,
             example = json!({"layers": ["layer1", "layer2"]}))
    ),
    params(
        ("volume_name" = VolumeName, description = "Volume name"),
        ("file_name" = String, description = "File name")
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn list_volume_file_layers_handler<C: ApplicationContext>(
    path: web::Path<(VolumeName, String)>,
    session: C::Session,
    app_ctx: web::Data<C>,
) -> Result<impl Responder> {
    let (volume_name, file_name) = path.into_inner();

    let session_ctx = app_ctx.session_context(session);
    let volumes = session_ctx.volumes()?;

    let volume = volumes.iter().find(|v| v.name == volume_name.0).ok_or(
        crate::error::Error::UnknownVolumeName {
            volume_name: volume_name.0.clone(),
        },
    )?;

    let Some(volume_path) = volume.path.as_ref() else {
        return Err(crate::error::Error::CannotAccessVolumePath {
            volume_name: volume_name.0.clone(),
        });
    };

    let file_path = path_with_base_path(Path::new(volume_path), Path::new(&file_name))?;

    let layers = crate::util::spawn_blocking(move || {
        let dataset = gdal_open_dataset(&file_path)?;

        // TODO: hide system/internal layer like "layer_styles"
        Result::<_, Error>::Ok(dataset.layers().map(|l| l.name()).collect::<Vec<_>>())
    })
    .await??;

    Ok(web::Json(VolumeFileLayersResponse { layers }))
}

/// Creates a new dataset referencing files.
/// Users can reference previously uploaded files.
/// Admins can reference files from a volume.
#[utoipa::path(
    tag = "Datasets",
    post,
    path = "/dataset", 
    request_body = CreateDataset,
    responses(
        (status = 200, body = DatasetNameResponse),
    ),
    security(
        ("session_token" = [])
    )
)]
async fn create_dataset_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    create: web::Json<CreateDataset>,
) -> Result<web::Json<DatasetNameResponse>, CreateDatasetError> {
    let create = create.into_inner();
    match create {
        CreateDataset {
            data_path: DataPath::Volume(upload),
            definition,
        } => create_system_dataset(session, app_ctx, upload, definition).await,
        CreateDataset {
            data_path: DataPath::Upload(volume),
            definition,
        } => create_upload_dataset(session, app_ctx, volume, definition).await,
        CreateDataset {
            data_path: DataPath::External,
            definition,
        } => create_external_dataset(session, app_ctx, definition).await,
    }
}

async fn create_system_dataset<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    volume_name: VolumeName,
    mut definition: DatasetDefinition,
) -> Result<web::Json<DatasetNameResponse>, CreateDatasetError> {
    let volumes = Volumes::default().volumes;
    let volume = volumes
        .iter()
        .find(|v| v.name == volume_name)
        .ok_or(CreateDatasetError::UnknownVolume)?;

    adjust_meta_data_path(&mut definition.meta_data, volume)
        .context(CannotResolveUploadFilePath)?;

    let db = app_ctx.session_context(session).db();

    let dataset = db
        .add_dataset(
            definition.properties.into(),
            definition.meta_data.into(),
            Some(DataPath::Volume(volume_name)),
        )
        .await
        .context(CannotCreateDataset)?;

    db.add_permission(
        Role::registered_user_role_id(),
        dataset.id,
        Permission::Read,
    )
    .await
    .boxed_context(crate::error::PermissionDb)
    .context(DatabaseAccess)?;

    db.add_permission(Role::anonymous_role_id(), dataset.id, Permission::Read)
        .await
        .boxed_context(crate::error::PermissionDb)
        .context(DatabaseAccess)?;

    Ok(web::Json(dataset.name.into()))
}

async fn create_external_dataset<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    definition: DatasetDefinition,
) -> Result<web::Json<DatasetNameResponse>, CreateDatasetError> {
    let db = app_ctx.session_context(session).db();

    let dataset = db
        .add_dataset(
            definition.properties.into(),
            definition.meta_data.into(),
            Some(DataPath::External),
        )
        .await
        .context(CannotCreateDataset)?;

    db.add_permission(
        Role::registered_user_role_id(),
        dataset.id,
        Permission::Read,
    )
    .await
    .boxed_context(crate::error::PermissionDb)
    .context(DatabaseAccess)?;

    db.add_permission(Role::anonymous_role_id(), dataset.id, Permission::Read)
        .await
        .boxed_context(crate::error::PermissionDb)
        .context(DatabaseAccess)?;

    Ok(web::Json(dataset.name.into()))
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        api::model::{
            datatypes::{
                Coordinate2D, DataId, GeoTransform, GridBoundingBox2D, GridIdx2D, InternalDataId,
                NamedData, RasterDataType, SingleBandRasterColorizer, SpatialGridDefinition,
                TimeInstance,
            },
            operators::{
                FileNotFoundHandling, GdalMultiBand, RasterBandDescriptor, RasterBandDescriptors,
                RasterResultDescriptor, SpatialGridDescriptor, SpatialGridDescriptorState,
                TimeDescriptor, TimeDimension,
            },
            processing_graphs::{
                GdalSourceParameters, MultiBandGdalSource, RasterOperator, TypedOperator,
            },
            responses::{IdResponse, datasets::DatasetNameResponse},
            services::{DatasetDefinition, Provenance},
        },
        contexts::{PostgresContext, PostgresSessionContext, Session, SessionId},
        datasets::{
            DatasetIdAndName,
            storage::DatasetStore,
            upload::{UploadId, VolumeName},
        },
        error::Result,
        ge_context,
        projects::{PointSymbology, RasterSymbology, Symbology},
        quota::ComputationId,
        test_data,
        users::{RoleDb, UserAuth},
        util::tests::{
            MockQueryContext, SetMultipartBody, TestDataUploads, add_file_definition_to_datasets,
            admin_login, read_body_json, read_body_string, send_test_request,
        },
        workflows::{
            registry::WorkflowRegistry,
            workflow::{Workflow, WorkflowId},
        },
    };
    use actix_web;
    use actix_web::http::header;
    use actix_web_httpauth::headers::authorization::Bearer;
    use futures::TryStreamExt;
    use geoengine_datatypes::{
        collections::{GeometryCollection, MultiPointCollection, VectorDataType},
        operations::image::{RasterColorizer, RgbaColor},
        primitives::{
            BandSelection, BoundingBox2D, ColumnSelection, DateTimeParseFormat,
            RasterQueryRectangle, SpatialPartition2D,
        },
        raster::{GridShape2D, TilingSpecification},
        spatial_reference::SpatialReferenceOption,
        util::{Identifier, assert_image_equals, test::assert_eq_two_list_of_tiles},
    };
    use geoengine_operators::{
        engine::{
            ExecutionContext, InitializedVectorOperator, MetaData, MetaDataProvider,
            QueryProcessor, RasterOperator as _, StaticMetaData, VectorOperator,
            VectorResultDescriptor, WorkflowOperatorPath,
        },
        source::{
            MultiBandGdalLoadingInfo, MultiBandGdalLoadingInfoQueryRectangle, OgrSource,
            OgrSourceDataset, OgrSourceErrorSpec, OgrSourceParameters,
        },
        util::{
            gdal::{create_ndvi_meta_data, create_ndvi_result_descriptor},
            test::raster_tile_from_file,
        },
    };
    use httptest::{Expectation, Server, all_of, matchers::request, responders::status_code};
    use serde_json::{Value, json};
    use std::str::FromStr;
    use tokio_postgres::NoTls;

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn test_list_datasets(app_ctx: PostgresContext<NoTls>) {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::MultiPoint,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
            time: None,
            bbox: None,
        };

        let ds = AddDataset {
            name: Some(DatasetName::new(None, "My_Dataset")),
            display_name: "OgrDataset".to_string(),
            description: "My Ogr dataset".to_string(),
            source_operator: "OgrSource".to_string(),
            symbology: None,
            provenance: None,
            tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
        };

        let meta = crate::datasets::storage::MetaDataDefinition::OgrMetaData(StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: Default::default(),
                layer_name: String::new(),
                data_type: None,
                time: Default::default(),
                default_geometry: None,
                columns: None,
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: None,
                attribute_query: None,
                cache_ttl: None,
            },
            result_descriptor: descriptor.clone(),
            phantom: Default::default(),
        });

        let db = ctx.db();
        let DatasetIdAndName { id: id1, name: _ } =
            db.add_dataset(ds.into(), meta, None).await.unwrap();

        let ds = AddDataset {
            name: Some(DatasetName::new(None, "My_Dataset2")),
            display_name: "OgrDataset2".to_string(),
            description: "My Ogr dataset2".to_string(),
            source_operator: "OgrSource".to_string(),
            symbology: Some(Symbology::Point(PointSymbology::default())),
            provenance: None,
            tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
        };

        let meta = crate::datasets::storage::MetaDataDefinition::OgrMetaData(StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: Default::default(),
                layer_name: String::new(),
                data_type: None,
                time: Default::default(),
                default_geometry: None,
                columns: None,
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: None,
                attribute_query: None,
                cache_ttl: None,
            },
            result_descriptor: descriptor,
            phantom: Default::default(),
        });

        let DatasetIdAndName { id: id2, name: _ } =
            db.add_dataset(ds.into(), meta, None).await.unwrap();

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/datasets?{}",
                serde_urlencoded::to_string([
                    ("order", "NameAsc"),
                    ("offset", "0"),
                    ("limit", "2"),
                ])
                .unwrap()
            ))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 200);

        assert_eq!(
            read_body_json(res).await,
            json!([ {
                "id": id1,
                "name": "My_Dataset",
                "displayName": "OgrDataset",
                "description": "My Ogr dataset",
                "tags": ["upload", "test"],
                "sourceOperator": "OgrSource",
                "resultDescriptor": {
                    "type": "vector",
                    "dataType": "MultiPoint",
                    "spatialReference": "",
                    "columns": {},
                    "time": null,
                    "bbox": null
                },
                "symbology": null
            },{
                "id": id2,
                "name": "My_Dataset2",
                "displayName": "OgrDataset2",
                "description": "My Ogr dataset2",
                "tags": ["upload", "test"],
                "sourceOperator": "OgrSource",
                "resultDescriptor": {
                    "type": "vector",
                    "dataType": "MultiPoint",
                    "spatialReference": "",
                    "columns": {},
                    "time": null,
                    "bbox": null
                },
                "symbology": {
                    "type": "point",
                    "radius": {
                        "type": "static",
                        "value": 10
                    },
                    "fillColor": {
                        "type": "static",
                        "color": [255, 255, 255, 255]
                    },
                    "stroke": {
                        "width": {
                            "type": "static",
                            "value": 1
                        },
                        "color": {
                            "type": "static",
                            "color": [0, 0, 0, 255]
                        }
                    },
                    "text": null
                }
            }])
        );
    }

    async fn upload_ne_10m_ports_files(
        app_ctx: PostgresContext<NoTls>,
        session_id: SessionId,
    ) -> Result<UploadId> {
        let files = vec![
            test_data!("vector/data/ne_10m_ports/ne_10m_ports.shp").to_path_buf(),
            test_data!("vector/data/ne_10m_ports/ne_10m_ports.shx").to_path_buf(),
            test_data!("vector/data/ne_10m_ports/ne_10m_ports.prj").to_path_buf(),
            test_data!("vector/data/ne_10m_ports/ne_10m_ports.dbf").to_path_buf(),
            test_data!("vector/data/ne_10m_ports/ne_10m_ports.cpg").to_path_buf(),
        ];

        let req = actix_web::test::TestRequest::post()
            .uri("/upload")
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_multipart_files(&files);
        let res = send_test_request(req, app_ctx).await;
        assert_eq!(res.status(), 200);

        let upload: IdResponse<UploadId> = actix_web::test::read_body_json(res).await;
        let root = upload.id.root_path()?;

        for file in files {
            let file_name = file.file_name().unwrap();
            assert!(root.join(file_name).exists());
        }

        Ok(upload.id)
    }

    pub async fn construct_dataset_from_upload(
        app_ctx: PostgresContext<NoTls>,
        upload_id: UploadId,
        session_id: SessionId,
    ) -> DatasetName {
        let s = json!({
            "dataPath": {
                "upload": upload_id
            },
            "definition": {
                "properties": {
                    "name": null,
                    "displayName": "Uploaded Natural Earth 10m Ports",
                    "description": "Ports from Natural Earth",
                    "sourceOperator": "OgrSource"
                },
                "metaData": {
                    "type": "OgrMetaData",
                    "loadingInfo": {
                        "fileName": "ne_10m_ports.shp",
                        "layerName": "ne_10m_ports",
                        "dataType": "MultiPoint",
                        "time": {
                            "type": "none"
                        },
                        "columns": {
                            "x": "",
                            "y": null,
                            "float": ["natlscale"],
                            "int": ["scalerank"],
                            "text": ["featurecla", "name", "website"],
                            "bool": [],
                            "datetime": []
                        },
                        "forceOgrTimeGilter": false,
                        "onError": "ignore",
                        "provenance": null
                    },
                    "resultDescriptor": {
                        "dataType": "MultiPoint",
                        "spatialReference": "EPSG:4326",
                        "columns": {
                            "website": {
                                "dataType": "text",
                                "measurement": {
                                    "type": "unitless"
                                }
                            },
                            "name": {
                                "dataType": "text",
                                "measurement": {
                                    "type": "unitless"
                                }
                            },
                            "natlscale": {
                                "dataType": "float",
                                "measurement": {
                                    "type": "unitless"
                                }
                            },
                            "scalerank": {
                                "dataType": "int",
                                "measurement": {
                                    "type": "unitless"
                                }
                            },
                            "featurecla": {
                                "dataType": "text",
                                "measurement": {
                                    "type": "unitless"
                                }
                            }
                        }
                    }
                }
            }
        });

        let req = actix_web::test::TestRequest::post()
            .uri("/dataset")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_json(s);
        let res = send_test_request(req, app_ctx).await;
        assert_eq!(res.status(), 200, "response: {res:?}");

        let DatasetNameResponse { dataset_name } = actix_web::test::read_body_json(res).await;
        dataset_name
    }

    async fn make_ogr_source<C: ExecutionContext>(
        exe_ctx: &C,
        named_data: NamedData,
    ) -> Result<Box<dyn InitializedVectorOperator>> {
        OgrSource {
            params: OgrSourceParameters {
                data: named_data.into(),
                attribute_projection: None,
                attribute_filters: None,
            },
        }
        .boxed()
        .initialize(WorkflowOperatorPath::initialize_root(), exe_ctx)
        .await
        .map_err(Into::into)
    }

    fn ctx_tiling_spec_600x600() -> TilingSpecification {
        TilingSpecification {
            tile_size_in_pixels: GridShape2D::new([600, 600]),
        }
    }

    #[ge_context::test(tiling_spec = "ctx_tiling_spec_600x600")]
    async fn create_dataset(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let mut test_data = TestDataUploads::default(); // remember created folder and remove them on drop

        let session = app_ctx.create_anonymous_session().await.unwrap();
        let session_id = session.id();
        let session_context = app_ctx.session_context(session);

        let upload_id = upload_ne_10m_ports_files(app_ctx.clone(), session_id).await?;
        test_data.uploads.push(upload_id);

        let dataset_name =
            construct_dataset_from_upload(app_ctx.clone(), upload_id, session_id).await;
        let exe_ctx = session_context.execution_context()?;

        let source = make_ogr_source(
            &exe_ctx,
            NamedData {
                namespace: dataset_name.namespace,
                provider: None,
                name: dataset_name.name,
            },
        )
        .await?;

        let query_processor = source.query_processor()?.multi_point().unwrap();
        let query_ctx = session_context.query_context(WorkflowId::new(), ComputationId::new())?;

        let query = query_processor
            .query(
                VectorQueryRectangle::new(
                    BoundingBox2D::new((1.85, 50.88).into(), (4.82, 52.95).into())?,
                    Default::default(),
                    ColumnSelection::all(),
                ),
                &query_ctx,
            )
            .await?;

        let result: Vec<MultiPointCollection> = query.try_collect().await?;

        let coords = result[0].coordinates();
        assert_eq!(coords.len(), 10);
        assert_eq!(
            coords,
            &[
                [2.933_686_69, 51.23].into(),
                [3.204_593_64_f64, 51.336_388_89].into(),
                [4.651_413_428, 51.805_833_33].into(),
                [4.11, 51.95].into(),
                [4.386_160_188, 50.886_111_11].into(),
                [3.767_373_38, 51.114_444_44].into(),
                [4.293_757_362, 51.297_777_78].into(),
                [1.850_176_678, 50.965_833_33].into(),
                [2.170_906_949, 51.021_666_67].into(),
                [4.292_873_969, 51.927_222_22].into(),
            ]
        );

        Ok(())
    }

    #[ge_context::test]
    async fn it_creates_system_dataset(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let volume = VolumeName("test_data".to_string());

        let mut meta_data = create_ndvi_meta_data();

        // make path relative to volume
        meta_data.params.file_path = "raster/modis_ndvi/MOD13A2_M_NDVI_%_START_TIME_%.TIFF".into();

        let create = CreateDataset {
            data_path: DataPath::Volume(volume.clone()),
            definition: DatasetDefinition {
                properties: AddDataset {
                    name: None,
                    display_name: "ndvi".to_string(),
                    description: "ndvi".to_string(),
                    source_operator: "GdalSource".to_string(),
                    symbology: None,
                    provenance: None,
                    tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
                },
                meta_data: MetaDataDefinition::GdalMetaDataRegular(meta_data.into()),
            },
        };

        // create via admin session
        let req = actix_web::test::TestRequest::post()
            .uri("/dataset")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_payload(serde_json::to_string(&create)?);
        let res = send_test_request(req, app_ctx.clone()).await;
        assert_eq!(res.status(), 200);

        let DatasetNameResponse { dataset_name } = actix_web::test::read_body_json(res).await;

        // assert dataset is accessible via regular session
        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/dataset/{dataset_name}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_payload(serde_json::to_string(&create)?);

        let res = send_test_request(req, app_ctx.clone()).await;
        assert_eq!(res.status(), 200);

        Ok(())
    }

    #[ge_context::test]
    async fn it_creates_external_gdal_dataset(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let mut meta_data = create_ndvi_meta_data();

        // For external data, the file path is already absolute; no resolution against a volume
        meta_data.params.file_path =
            test_data!("raster/modis_ndvi/MOD13A2_M_NDVI_%_START_TIME_%.TIFF").to_path_buf();

        let create = CreateDataset {
            data_path: DataPath::External,
            definition: DatasetDefinition {
                properties: AddDataset {
                    name: None,
                    display_name: "ndvi external".to_string(),
                    description: "ndvi".to_string(),
                    source_operator: "GdalSource".to_string(),
                    symbology: None,
                    provenance: None,
                    tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
                },
                meta_data: MetaDataDefinition::GdalMetaDataRegular(meta_data.into()),
            },
        };

        // create via admin session
        let admin_session = admin_login(&app_ctx).await;
        let req = actix_web::test::TestRequest::post()
            .uri("/dataset")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((
                header::AUTHORIZATION,
                Bearer::new(admin_session.id().to_string()),
            ))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_json(create);
        let res = send_test_request(req, app_ctx.clone()).await;
        assert_eq!(res.status(), 200);

        let DatasetNameResponse { dataset_name } = actix_web::test::read_body_json(res).await;

        // assert dataset is accessible via regular session
        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/dataset/{dataset_name}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));

        let res = send_test_request(req, app_ctx.clone()).await;
        assert_eq!(res.status(), 200);

        Ok(())
    }

    #[test]
    fn it_auto_detects() {
        let meta_data = auto_detect_vector_meta_data_definition(
            test_data!("vector/data/ne_10m_ports/ne_10m_ports.shp"),
            &None,
        )
        .unwrap();
        let mut meta_data = crate::datasets::storage::MetaDataDefinition::OgrMetaData(meta_data);

        if let crate::datasets::storage::MetaDataDefinition::OgrMetaData(meta_data) = &mut meta_data
            && let Some(columns) = &mut meta_data.loading_info.columns
        {
            columns.text.sort();
        }

        assert_eq!(
            meta_data,
            crate::datasets::storage::MetaDataDefinition::OgrMetaData(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/ne_10m_ports/ne_10m_ports.shp").into(),
                    layer_name: "ne_10m_ports".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::None,
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: None,
                        x: String::new(),
                        y: None,
                        int: vec!["scalerank".to_string()],
                        float: vec!["natlscale".to_string()],
                        text: vec![
                            "featurecla".to_string(),
                            "name".to_string(),
                            "website".to_string(),
                        ],
                        bool: vec![],
                        datetime: vec![],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                    cache_ttl: None
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [
                        (
                            "name".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Text,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "scalerank".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Int,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "website".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Text,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "natlscale".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Float,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "featurecla".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Text,
                                measurement: Measurement::Unitless
                            }
                        ),
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                    time: None,
                    bbox: None,
                },
                phantom: Default::default(),
            })
        );
    }

    #[test]
    fn it_detects_time_json() {
        let meta_data = auto_detect_vector_meta_data_definition(
            test_data!("vector/data/points_with_iso_time.json"),
            &None,
        )
        .unwrap();

        let mut meta_data = crate::datasets::storage::MetaDataDefinition::OgrMetaData(meta_data);

        if let crate::datasets::storage::MetaDataDefinition::OgrMetaData(meta_data) = &mut meta_data
            && let Some(columns) = &mut meta_data.loading_info.columns
        {
            columns.datetime.sort();
        }

        assert_eq!(
            meta_data,
            crate::datasets::storage::MetaDataDefinition::OgrMetaData(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/points_with_iso_time.json").into(),
                    layer_name: "points_with_iso_time".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::StartEnd {
                        start_field: "time_start".to_owned(),
                        start_format: OgrSourceTimeFormat::Auto,
                        end_field: "time_end".to_owned(),
                        end_format: OgrSourceTimeFormat::Auto,
                    },
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: None,
                        x: String::new(),
                        y: None,
                        float: vec![],
                        int: vec![],
                        text: vec![],
                        bool: vec![],
                        datetime: vec!["time_end".to_owned(), "time_start".to_owned()],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                    cache_ttl: None
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [
                        (
                            "time_start".to_owned(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::DateTime,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "time_end".to_owned(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::DateTime,
                                measurement: Measurement::Unitless
                            }
                        )
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                    time: None,
                    bbox: None,
                },
                phantom: Default::default()
            })
        );
    }

    #[test]
    fn it_detects_time_gpkg() {
        let meta_data = auto_detect_vector_meta_data_definition(
            test_data!("vector/data/points_with_time.gpkg"),
            &None,
        )
        .unwrap();

        let mut meta_data = crate::datasets::storage::MetaDataDefinition::OgrMetaData(meta_data);

        if let crate::datasets::storage::MetaDataDefinition::OgrMetaData(meta_data) = &mut meta_data
            && let Some(columns) = &mut meta_data.loading_info.columns
        {
            columns.datetime.sort();
        }

        assert_eq!(
            meta_data,
            crate::datasets::storage::MetaDataDefinition::OgrMetaData(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/points_with_time.gpkg").into(),
                    layer_name: "points_with_time".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::StartEnd {
                        start_field: "time_start".to_owned(),
                        start_format: OgrSourceTimeFormat::Auto,
                        end_field: "time_end".to_owned(),
                        end_format: OgrSourceTimeFormat::Auto,
                    },
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: None,
                        x: String::new(),
                        y: None,
                        float: vec![],
                        int: vec![],
                        text: vec![],
                        bool: vec![],
                        datetime: vec!["time_end".to_owned(), "time_start".to_owned()],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                    cache_ttl: None
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [
                        (
                            "time_start".to_owned(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::DateTime,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "time_end".to_owned(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::DateTime,
                                measurement: Measurement::Unitless
                            }
                        )
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                    time: None,
                    bbox: None,
                },
                phantom: Default::default(),
            })
        );
    }

    #[test]
    fn it_detects_time_shp() {
        let meta_data = auto_detect_vector_meta_data_definition(
            test_data!("vector/data/points_with_date.shp"),
            &None,
        )
        .unwrap();

        let mut meta_data = crate::datasets::storage::MetaDataDefinition::OgrMetaData(meta_data);

        if let crate::datasets::storage::MetaDataDefinition::OgrMetaData(meta_data) = &mut meta_data
            && let Some(columns) = &mut meta_data.loading_info.columns
        {
            columns.datetime.sort();
        }

        assert_eq!(
            meta_data,
            crate::datasets::storage::MetaDataDefinition::OgrMetaData(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/points_with_date.shp").into(),
                    layer_name: "points_with_date".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::StartEnd {
                        start_field: "time_start".to_owned(),
                        start_format: OgrSourceTimeFormat::Auto,
                        end_field: "time_end".to_owned(),
                        end_format: OgrSourceTimeFormat::Auto,
                    },
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: None,
                        x: String::new(),
                        y: None,
                        float: vec![],
                        int: vec![],
                        text: vec![],
                        bool: vec![],
                        datetime: vec!["time_end".to_owned(), "time_start".to_owned()],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                    cache_ttl: None
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [
                        (
                            "time_end".to_owned(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::DateTime,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "time_start".to_owned(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::DateTime,
                                measurement: Measurement::Unitless
                            }
                        )
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                    time: None,
                    bbox: None,
                },
                phantom: Default::default(),
            })
        );
    }

    #[test]
    fn it_detects_time_start_duration() {
        let meta_data = auto_detect_vector_meta_data_definition(
            test_data!("vector/data/points_with_iso_start_duration.json"),
            &None,
        )
        .unwrap();

        let meta_data = crate::datasets::storage::MetaDataDefinition::OgrMetaData(meta_data);

        assert_eq!(
            meta_data,
            crate::datasets::storage::MetaDataDefinition::OgrMetaData(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/points_with_iso_start_duration.json").into(),
                    layer_name: "points_with_iso_start_duration".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::StartDuration {
                        start_field: "time_start".to_owned(),
                        start_format: OgrSourceTimeFormat::Auto,
                        duration_field: "duration".to_owned(),
                    },
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: None,
                        x: String::new(),
                        y: None,
                        float: vec![],
                        int: vec!["duration".to_owned()],
                        text: vec![],
                        bool: vec![],
                        datetime: vec!["time_start".to_owned()],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                    cache_ttl: None
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [
                        (
                            "time_start".to_owned(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::DateTime,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "duration".to_owned(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Int,
                                measurement: Measurement::Unitless
                            }
                        )
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                    time: None,
                    bbox: None,
                },
                phantom: Default::default()
            })
        );
    }

    #[test]
    fn it_detects_csv() {
        let meta_data =
            auto_detect_vector_meta_data_definition(test_data!("vector/data/lonlat.csv"), &None)
                .unwrap();

        let mut meta_data = crate::datasets::storage::MetaDataDefinition::OgrMetaData(meta_data);

        if let crate::datasets::storage::MetaDataDefinition::OgrMetaData(meta_data) = &mut meta_data
            && let Some(columns) = &mut meta_data.loading_info.columns
        {
            columns.text.sort();
        }

        assert_eq!(
            meta_data,
            crate::datasets::storage::MetaDataDefinition::OgrMetaData(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/lonlat.csv").into(),
                    layer_name: "lonlat".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::None,
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: None,
                        x: "Longitude".to_string(),
                        y: Some("Latitude".to_string()),
                        float: vec![],
                        int: vec![],
                        text: vec![
                            "Latitude".to_string(),
                            "Longitude".to_string(),
                            "Name".to_string()
                        ],
                        bool: vec![],
                        datetime: vec![],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                    cache_ttl: None
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReferenceOption::Unreferenced,
                    columns: [
                        (
                            "Latitude".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Text,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "Longitude".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Text,
                                measurement: Measurement::Unitless
                            }
                        ),
                        (
                            "Name".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Text,
                                measurement: Measurement::Unitless
                            }
                        )
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                    time: None,
                    bbox: None,
                },
                phantom: Default::default()
            })
        );
    }

    #[ge_context::test]
    async fn get_dataset(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
            time: None,
            bbox: None,
        };

        let ds = AddDataset {
            name: None,
            display_name: "OgrDataset".to_string(),
            description: "My Ogr dataset".to_string(),
            source_operator: "OgrSource".to_string(),
            symbology: None,
            provenance: None,
            tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
        };

        let meta = crate::datasets::storage::MetaDataDefinition::OgrMetaData(StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: Default::default(),
                layer_name: String::new(),
                data_type: None,
                time: Default::default(),
                default_geometry: None,
                columns: None,
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: None,
                attribute_query: None,
                cache_ttl: None,
            },
            result_descriptor: descriptor,
            phantom: Default::default(),
        });

        let db = ctx.db();
        let DatasetIdAndName {
            id,
            name: dataset_name,
        } = db.add_dataset(ds.into(), meta, None).await?;

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/dataset/{dataset_name}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_test_request(req, app_ctx).await;

        let res_status = res.status();
        let res_body = serde_json::from_str::<Value>(&read_body_string(res).await).unwrap();
        assert_eq!(res_status, 200, "{res_body}");

        assert_eq!(
            res_body,
            json!({
                "name": dataset_name,
                "id": id,
                "displayName": "OgrDataset",
                "description": "My Ogr dataset",
                "resultDescriptor": {
                    "type": "vector",
                    "dataType": "Data",
                    "spatialReference": "",
                    "columns": {},
                    "time": null,
                    "bbox": null
                },
                "sourceOperator": "OgrSource",
                "symbology": null,
                "provenance": null,
                "tags": ["upload", "test"],
                "dataPath": null,
            })
        );

        Ok(())
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_suggests_metadata(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let mut test_data = TestDataUploads::default(); // remember created folder and remove them on drop

        let session = app_ctx.create_anonymous_session().await.unwrap();

        let body = vec![(
            "test.json",
            r#"{
                "type": "FeatureCollection",
                "features": [
                  {
                    "type": "Feature",
                    "geometry": {
                      "type": "Point",
                      "coordinates": [
                        1,
                        1
                      ]
                    },
                    "properties": {
                      "name": "foo",
                      "id": 1
                    }
                  },
                  {
                    "type": "Feature",
                    "geometry": {
                      "type": "Point",
                      "coordinates": [
                        2,
                        2
                      ]
                    },
                    "properties": {
                      "name": "bar",
                      "id": 2
                    }
                  }
                ]
              }"#,
        )];

        let req = actix_web::test::TestRequest::post()
            .uri("/upload")
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .set_multipart(body.clone());

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);

        let upload: IdResponse<UploadId> = actix_web::test::read_body_json(res).await;
        test_data.uploads.push(upload.id);

        let upload_content =
            std::fs::read_to_string(upload.id.root_path().unwrap().join("test.json")).unwrap();

        assert_eq!(&upload_content, body[0].1);

        let req = actix_web::test::TestRequest::post()
            .uri("/dataset/suggest")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .set_json(SuggestMetaData {
                data_path: DataPath::Upload(upload.id),
                layer_name: None,
                main_file: None,
            });
        let res = send_test_request(req, app_ctx.clone()).await;

        let res_status = res.status();
        let res_body = read_body_string(res).await;
        assert_eq!(res_status, 200, "{res_body}");

        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&res_body).unwrap(),
            json!({
              "mainFile": "test.json",
              "layerName": "test",
              "metaData": {
                "type": "OgrMetaData",
                "loadingInfo": {
                  "fileName": format!("test_upload/{}/test.json", upload.id),
                  "layerName": "test",
                  "dataType": "MultiPoint",
                  "time": {
                    "type": "none"
                  },
                  "defaultGeometry": null,
                  "columns": {
                    "formatSpecifics": null,
                    "x": "",
                    "y": null,
                    "int": [
                      "id"
                    ],
                    "float": [],
                    "text": [
                      "name"
                    ],
                    "bool": [],
                    "datetime": [],
                    "rename": null
                  },
                  "forceOgrTimeFilter": false,
                  "forceOgrSpatialFilter": false,
                  "onError": "ignore",
                  "sqlQuery": null,
                  "attributeQuery": null,
                },
                "resultDescriptor": {
                  "dataType": "MultiPoint",
                  "spatialReference": "EPSG:4326",
                  "columns": {
                    "id": {
                      "dataType": "int",
                      "measurement": {
                        "type": "unitless"
                      }
                    },
                    "name": {
                      "dataType": "text",
                      "measurement": {
                        "type": "unitless"
                      }
                    }
                  },
                  "time": null,
                  "bbox": null
                }
              }
            })
        );

        let req = actix_web::test::TestRequest::post()
            .uri("/dataset/suggest")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .set_json(SuggestMetaData {
                data_path: DataPath::Volume(VolumeName("test_data".to_string())),
                layer_name: None,
                main_file: Some(
                    "raster/modis_ndvi/tiled/MOD13A2_M_NDVI_2014-01-01_1_1.tif".to_string(),
                ),
            });
        let res = send_test_request(req, app_ctx).await;
        let res_status = res.status();
        let suggestion: Value = read_body_json(res).await;

        assert_eq!(res_status, 200, "{suggestion}");
        assert_eq!(
            suggestion["metaData"]["type"], "GdalMetaDataList",
            "{suggestion}"
        );
        assert!(
            suggestion["metaData"]["params"][0]
                .get("cacheTtl")
                .is_none()
        );

        Ok(())
    }

    #[ge_context::test]
    async fn it_deletes_system_dataset(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let volume = VolumeName("test_data".to_string());

        let mut meta_data = create_ndvi_meta_data();

        // make path relative to volume
        meta_data.params.file_path = "raster/modis_ndvi/MOD13A2_M_NDVI_%_START_TIME_%.TIFF".into();

        let create = CreateDataset {
            data_path: DataPath::Volume(volume.clone()),
            definition: DatasetDefinition {
                properties: AddDataset {
                    name: None,
                    display_name: "ndvi".to_string(),
                    description: "ndvi".to_string(),
                    source_operator: "GdalSource".to_string(),
                    symbology: None,
                    provenance: None,
                    tags: None,
                },
                meta_data: MetaDataDefinition::GdalMetaDataRegular(meta_data.into()),
            },
        };

        let req = actix_web::test::TestRequest::post()
            .uri("/dataset")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_payload(serde_json::to_string(&create)?);
        let res = send_test_request(req, app_ctx.clone()).await;

        let DatasetNameResponse { dataset_name } = actix_web::test::read_body_json(res).await;

        let db = ctx.db();
        let dataset_id = db
            .resolve_dataset_name_to_id(&dataset_name)
            .await
            .unwrap()
            .unwrap();
        assert!(db.load_dataset(&dataset_id).await.is_ok());

        let req = actix_web::test::TestRequest::delete()
            .uri(&format!("/dataset/{dataset_name}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"));

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);

        assert!(db.load_dataset(&dataset_id).await.is_err());

        Ok(())
    }

    #[ge_context::test]
    async fn it_gets_loading_info(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
            time: None,
            bbox: None,
        };

        let ds = AddDataset {
            name: None,
            display_name: "OgrDataset".to_string(),
            description: "My Ogr dataset".to_string(),
            source_operator: "OgrSource".to_string(),
            symbology: None,
            provenance: None,
            tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
        };

        let meta = crate::datasets::storage::MetaDataDefinition::OgrMetaData(StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: Default::default(),
                layer_name: String::new(),
                data_type: None,
                time: Default::default(),
                default_geometry: None,
                columns: None,
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: None,
                attribute_query: None,
                cache_ttl: None,
            },
            result_descriptor: descriptor,
            phantom: Default::default(),
        });

        let db = ctx.db();
        let DatasetIdAndName {
            id: _,
            name: dataset_name,
        } = db.add_dataset(ds.into(), meta, None).await?;

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/dataset/{dataset_name}/loadingInfo"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_test_request(req, app_ctx).await;

        let res_status = res.status();
        let res_body = serde_json::from_str::<Value>(&read_body_string(res).await).unwrap();
        assert_eq!(res_status, 200, "{res_body}");

        assert_eq!(
            res_body,
            json!({
                "loadingInfo":  {
                    "attributeQuery": null,
                    "columns": null,
                    "dataType": null,
                    "defaultGeometry": null,
                    "fileName": "",
                    "forceOgrSpatialFilter": false,
                    "forceOgrTimeFilter": false,
                    "layerName": "",
                    "onError": "ignore",
                    "sqlQuery": null,
                    "time":  {
                        "type": "none"
                    }
                },
                 "resultDescriptor":  {
                    "bbox": null,
                    "columns":  {},
                    "dataType": "Data",
                    "spatialReference": "",
                    "time": null
                },
                "type": "OgrMetaData"
            })
        );

        Ok(())
    }

    #[ge_context::test]
    async fn it_updates_loading_info(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
            time: None,
            bbox: None,
        };

        let ds = AddDataset {
            name: None,
            display_name: "OgrDataset".to_string(),
            description: "My Ogr dataset".to_string(),
            source_operator: "OgrSource".to_string(),
            symbology: None,
            provenance: None,
            tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
        };

        let meta = crate::datasets::storage::MetaDataDefinition::OgrMetaData(StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: Default::default(),
                layer_name: String::new(),
                data_type: None,
                time: Default::default(),
                default_geometry: None,
                columns: None,
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: None,
                attribute_query: None,
                cache_ttl: None,
            },
            result_descriptor: descriptor.clone(),
            phantom: Default::default(),
        });

        let db = ctx.db();
        let DatasetIdAndName {
            id,
            name: dataset_name,
        } = db.add_dataset(ds.into(), meta, None).await?;

        let update: MetaDataDefinition =
            crate::datasets::storage::MetaDataDefinition::OgrMetaData(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: "foo.bar".into(),
                    layer_name: "baz".to_string(),
                    data_type: None,
                    time: Default::default(),
                    default_geometry: None,
                    columns: None,
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                    cache_ttl: None,
                },
                result_descriptor: descriptor,
                phantom: Default::default(),
            })
            .into();

        let req = actix_web::test::TestRequest::put()
            .uri(&format!("/dataset/{dataset_name}/loadingInfo"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .set_json(update.clone());

        let res = send_test_request(req, app_ctx).await;
        assert_eq!(res.status(), 200);

        let loading_info: MetaDataDefinition = db.load_loading_info(&id).await.unwrap().into();

        assert_eq!(loading_info, update);

        Ok(())
    }

    #[ge_context::test]
    async fn it_gets_updates_symbology(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let DatasetIdAndName {
            id: dataset_id,
            name: dataset_name,
        } = add_file_definition_to_datasets(&ctx.db(), test_data!("dataset_defs/ndvi.json")).await;

        let symbology = Symbology::Raster(RasterSymbology {
            r#type: Default::default(),
            opacity: 1.0,
            raster_colorizer: RasterColorizer::SingleBand {
                band: 0,
                band_colorizer: geoengine_datatypes::operations::image::Colorizer::linear_gradient(
                    vec![
                        (0.0, RgbaColor::white())
                            .try_into()
                            .expect("valid breakpoint"),
                        (10_000.0, RgbaColor::black())
                            .try_into()
                            .expect("valid breakpoint"),
                    ],
                    RgbaColor::transparent(),
                    RgbaColor::white(),
                    RgbaColor::black(),
                )
                .expect("valid colorizer"),
            },
        });

        let req = actix_web::test::TestRequest::put()
            .uri(&format!("/dataset/{dataset_name}/symbology"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .set_json(symbology.clone());
        let res = send_test_request(req, app_ctx).await;

        let res_status = res.status();
        assert_eq!(res_status, 200);

        let dataset = ctx.db().load_dataset(&dataset_id).await?;

        assert_eq!(dataset.symbology, Some(symbology));

        Ok(())
    }

    #[ge_context::test()]
    async fn it_updates_dataset(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let DatasetIdAndName {
            id: dataset_id,
            name: dataset_name,
        } = add_file_definition_to_datasets(&ctx.db(), test_data!("dataset_defs/ndvi.json")).await;

        let update: UpdateDataset = UpdateDataset {
            name: DatasetName::new(None, "new_name"),
            display_name: "new display name".to_string(),
            description: "new description".to_string(),
            tags: vec!["foo".to_string(), "bar".to_string()],
        };

        let req = actix_web::test::TestRequest::post()
            .uri(&format!("/dataset/{dataset_name}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .set_json(update.clone());
        let res = send_test_request(req, app_ctx).await;

        let res_status = res.status();
        assert_eq!(res_status, 200);

        let dataset = ctx.db().load_dataset(&dataset_id).await?;

        assert_eq!(dataset.name, update.name);
        assert_eq!(dataset.display_name, update.display_name);
        assert_eq!(dataset.description, update.description);
        assert_eq!(dataset.tags, Some(update.tags));

        Ok(())
    }

    #[ge_context::test()]
    async fn it_updates_provenance(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let DatasetIdAndName {
            id: dataset_id,
            name: dataset_name,
        } = add_file_definition_to_datasets(&ctx.db(), test_data!("dataset_defs/ndvi.json")).await;

        let provenances: Provenances = Provenances {
            provenances: vec![Provenance {
                citation: "foo".to_string(),
                license: "bar".to_string(),
                uri: "http://example.com".to_string(),
            }],
        };

        let req = actix_web::test::TestRequest::put()
            .uri(&format!("/dataset/{dataset_name}/provenance"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .set_json(provenances.clone());
        let res = send_test_request(req, app_ctx).await;

        let res_status = res.status();
        assert_eq!(res_status, 200);

        let dataset = ctx.db().load_dataset(&dataset_id).await?;

        assert_eq!(
            dataset.provenance,
            Some(
                provenances
                    .provenances
                    .into_iter()
                    .map(Into::into)
                    .collect()
            )
        );

        Ok(())
    }

    // TODO: better way to get to the root of the project
    struct TestWorkdirChanger {
        package_dir: &'static str,
        modified: bool,
    }

    impl TestWorkdirChanger {
        fn go_to_workspace(package_dir: &'static str) -> Self {
            let mut working_dir = std::env::current_dir().unwrap();

            if !working_dir.ends_with(package_dir) {
                return Self {
                    package_dir,
                    modified: false,
                };
            }

            working_dir.pop();

            std::env::set_current_dir(working_dir).unwrap();

            Self {
                package_dir,
                modified: true,
            }
        }
    }

    impl Drop for TestWorkdirChanger {
        fn drop(&mut self) {
            if !self.modified {
                return;
            }

            let mut working_dir = std::env::current_dir().unwrap();
            working_dir.push(self.package_dir);
            std::env::set_current_dir(working_dir).unwrap();
        }
    }

    #[ge_context::test(test_execution = "serial")]
    async fn it_lists_layers(app_ctx: PostgresContext<NoTls>) {
        let changed_workdir = TestWorkdirChanger::go_to_workspace("services");

        let session = admin_login(&app_ctx).await;

        let volume_name = "test_data";
        let file_name = "vector%2Fdata%2Ftwo_layers.gpkg";

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/dataset/volumes/{volume_name}/files/{file_name}/layers"
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));

        let res = send_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 200, "{res:?}");

        let layers: VolumeFileLayersResponse = actix_web::test::read_body_json(res).await;

        assert_eq!(
            layers.layers,
            vec![
                "points_with_time".to_string(),
                "points_with_time_and_more".to_string(),
                "layer_styles".to_string() // TOOO: remove once internal/system layers are hidden
            ]
        );

        drop(changed_workdir);
    }

    /// override the pixel size since this test was designed for 600 x 600 pixel tiles
    fn create_dataset_tiling_specification() -> TilingSpecification {
        TilingSpecification {
            tile_size_in_pixels: GridShape2D::new([600, 600]),
        }
    }

    #[ge_context::test(tiling_spec = "create_dataset_tiling_specification")]
    async fn create_datasets(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let mut test_data = TestDataUploads::default(); // remember created folder and remove them on drop

        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let upload_id = upload_ne_10m_ports_files(app_ctx.clone(), session.id()).await?;
        test_data.uploads.push(upload_id);

        let dataset_name =
            construct_dataset_from_upload(app_ctx.clone(), upload_id, session.id()).await;
        let exe_ctx = ctx.execution_context()?;

        let source = make_ogr_source(
            &exe_ctx,
            geoengine_datatypes::dataset::NamedData::from(dataset_name).into(),
        )
        .await?;

        let query_processor = source.query_processor()?.multi_point().unwrap();
        let query_ctx = ctx.mock_query_context()?;

        let query = query_processor
            .query(
                VectorQueryRectangle::new(
                    BoundingBox2D::new((1.85, 50.88).into(), (4.82, 52.95).into())?,
                    Default::default(),
                    ColumnSelection::all(),
                ),
                &query_ctx,
            )
            .await
            .unwrap();

        let result: Vec<MultiPointCollection> = query.try_collect().await?;

        let coords = result[0].coordinates();
        assert_eq!(coords.len(), 10);
        assert_eq!(
            coords,
            &[
                [2.933_686_69, 51.23].into(),
                [3.204_593_64_f64, 51.336_388_89].into(),
                [4.651_413_428, 51.805_833_33].into(),
                [4.11, 51.95].into(),
                [4.386_160_188, 50.886_111_11].into(),
                [3.767_373_38, 51.114_444_44].into(),
                [4.293_757_362, 51.297_777_78].into(),
                [1.850_176_678, 50.965_833_33].into(),
                [2.170_906_949, 51.021_666_67].into(),
                [4.292_873_969, 51.927_222_22].into(),
            ]
        );

        Ok(())
    }

    #[ge_context::test]
    async fn it_creates_volume_dataset(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let volume = VolumeName("test_data".to_string());

        let mut meta_data = create_ndvi_meta_data();

        // make path relative to volume
        meta_data.params.file_path = "raster/modis_ndvi/MOD13A2_M_NDVI_%_START_TIME_%.TIFF".into();

        let create = CreateDataset {
            data_path: DataPath::Volume(volume.clone()),
            definition: DatasetDefinition {
                properties: AddDataset {
                    name: None,
                    display_name: "ndvi".to_string(),
                    description: "ndvi".to_string(),
                    source_operator: "GdalSource".to_string(),
                    symbology: None,
                    provenance: None,
                    tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
                },
                meta_data: MetaDataDefinition::GdalMetaDataRegular(meta_data.into()),
            },
        };

        // create via admin session
        let admin_session = admin_login(&app_ctx).await;
        let req = actix_web::test::TestRequest::post()
            .uri("/dataset")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((
                header::AUTHORIZATION,
                Bearer::new(admin_session.id().to_string()),
            ))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_json(create);
        let res = send_test_request(req, app_ctx.clone()).await;
        assert_eq!(res.status(), 200);

        let DatasetNameResponse { dataset_name } = actix_web::test::read_body_json(res).await;

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/dataset/{dataset_name}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));

        let res = send_test_request(req, app_ctx.clone()).await;
        assert_eq!(res.status(), 200);

        Ok(())
    }

    #[ge_context::test]
    async fn it_deletes_dataset(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let mut test_data = TestDataUploads::default(); // remember created folder and remove them on drop

        let session = app_ctx.create_anonymous_session().await.unwrap();
        let session_id = session.id();
        let ctx = app_ctx.session_context(session);

        let upload_id = upload_ne_10m_ports_files(app_ctx.clone(), session_id).await?;
        test_data.uploads.push(upload_id);

        let dataset_name =
            construct_dataset_from_upload(app_ctx.clone(), upload_id, session_id).await;

        let db = ctx.db();
        let dataset_id = db
            .resolve_dataset_name_to_id(&dataset_name)
            .await
            .unwrap()
            .unwrap();

        assert!(db.load_dataset(&dataset_id).await.is_ok());

        let req = actix_web::test::TestRequest::delete()
            .uri(&format!("/dataset/{dataset_name}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"));

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200, "response: {res:?}");

        assert!(db.load_dataset(&dataset_id).await.is_err());

        Ok(())
    }

    #[ge_context::test]
    async fn it_deletes_dataset_with_additional_read_permission(
        app_ctx: PostgresContext<NoTls>,
    ) -> Result<()> {
        let mut test_data = TestDataUploads::default(); // remember created folder and remove them on drop

        let session = app_ctx.create_anonymous_session().await.unwrap();
        let session_id = session.id();
        let ctx = app_ctx.session_context(session);

        let upload_id = upload_ne_10m_ports_files(app_ctx.clone(), session_id).await?;
        test_data.uploads.push(upload_id);

        let dataset_name =
            construct_dataset_from_upload(app_ctx.clone(), upload_id, session_id).await;

        let db = ctx.db();
        let dataset_id = db
            .resolve_dataset_name_to_id(&dataset_name)
            .await
            .unwrap()
            .unwrap();

        assert!(db.load_dataset(&dataset_id).await.is_ok());

        let admin_session = admin_login(&app_ctx).await;
        let admin_ctx = app_ctx.session_context(admin_session);
        let admin_db = admin_ctx.db();

        let role_id = admin_db.add_role("test_role").await.unwrap();
        admin_db
            .assign_role(&role_id, &ctx.session().user.id)
            .await
            .unwrap();

        db.add_permission(role_id, dataset_id, Permission::Read)
            .await
            .unwrap();

        let req = actix_web::test::TestRequest::delete()
            .uri(&format!("/dataset/{dataset_name}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"));

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200, "response: {res:?}");

        assert!(db.load_dataset(&dataset_id).await.is_err());

        Ok(())
    }

    #[ge_context::test]
    async fn it_deletes_volume_dataset(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let volume = VolumeName("test_data".to_string());

        let mut meta_data = create_ndvi_meta_data();

        // make path relative to volume
        meta_data.params.file_path = "raster/modis_ndvi/MOD13A2_M_NDVI_%_START_TIME_%.TIFF".into();

        let create = CreateDataset {
            data_path: DataPath::Volume(volume.clone()),
            definition: DatasetDefinition {
                properties: AddDataset {
                    name: None,
                    display_name: "ndvi".to_string(),
                    description: "ndvi".to_string(),
                    source_operator: "GdalSource".to_string(),
                    symbology: None,
                    provenance: None,
                    tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
                },
                meta_data: MetaDataDefinition::GdalMetaDataRegular(meta_data.into()),
            },
        };

        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let db = ctx.db();

        let req = actix_web::test::TestRequest::post()
            .uri("/dataset")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_payload(serde_json::to_string(&create)?);
        let res = send_test_request(req, app_ctx.clone()).await;

        let DatasetNameResponse { dataset_name } = actix_web::test::read_body_json(res).await;
        let dataset_id = db
            .resolve_dataset_name_to_id(&dataset_name)
            .await
            .unwrap()
            .unwrap();

        assert!(db.load_dataset(&dataset_id).await.is_ok());

        let req = actix_web::test::TestRequest::delete()
            .uri(&format!("/dataset/{dataset_name}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"));

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);

        assert!(db.load_dataset(&dataset_id).await.is_err());

        Ok(())
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_adds_tiles_to_dataset(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let volume = VolumeName("test_data".to_string());

        // add data
        let create = CreateDataset {
            data_path: DataPath::Volume(volume.clone()),
            definition: DatasetDefinition {
                properties: AddDataset {
                    name: None,
                    display_name: "ndvi (tiled)".to_string(),
                    description: "ndvi".to_string(),
                    source_operator: "MultiBandGdalSource".to_string(),
                    symbology: None,
                    provenance: None,
                    tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
                },
                meta_data: MetaDataDefinition::GdalMultiBand(GdalMultiBand {
                    r#type: Default::default(),
                    result_descriptor: create_ndvi_result_descriptor(true).into(),
                    cache_ttl: None,
                }),
            },
        };

        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let db = ctx.db();

        let req = actix_web::test::TestRequest::post()
            .uri("/dataset")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_payload(serde_json::to_string(&create)?);
        let res = send_test_request(req, app_ctx.clone()).await;

        let DatasetNameResponse { dataset_name } = actix_web::test::read_body_json(res).await;
        let dataset_id = db
            .resolve_dataset_name_to_id(&dataset_name)
            .await
            .unwrap()
            .unwrap();

        assert!(db.load_dataset(&dataset_id).await.is_ok());

        // add tiles
        let tiles = create_ndvi_tiles();

        let req = actix_web::test::TestRequest::post()
            .uri(&format!("/dataset/{dataset_name}/tiles"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_payload(serde_json::to_string(&tiles)?);

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200, "response: {res:?}");

        // create workflow
        let workflow = Workflow {
            operator: TypedOperator::Raster(RasterOperator::MultiBandGdalSource(
                MultiBandGdalSource {
                    r#type: Default::default(),
                    params: GdalSourceParameters {
                        data: dataset_name.into(),
                        overview_level: None,
                    },
                },
            ))
            .try_into()
            .unwrap(),
        };

        let id = ctx.db().register_workflow(workflow.clone()).await.unwrap();

        let colorizer = geoengine_datatypes::operations::image::Colorizer::linear_gradient(
            vec![
                (0.0, RgbaColor::white()).try_into().unwrap(),
                (255.0, RgbaColor::black()).try_into().unwrap(),
            ],
            RgbaColor::transparent(),
            RgbaColor::white(),
            RgbaColor::black(),
        )
        .unwrap();

        let raster_colorizer =
            crate::api::model::datatypes::RasterColorizer::SingleBand(SingleBandRasterColorizer {
                r#type: Default::default(),
                band: 0,
                band_colorizer: colorizer.into(),
            });

        let params = &[
            ("request", "GetMap"),
            ("service", "WMS"),
            ("version", "1.3.0"),
            ("layers", &id.to_string()),
            ("bbox", "-90,-180,90,180"),
            ("width", "3600"),
            ("height", "1800"),
            ("crs", "EPSG:4326"),
            (
                "styles",
                &format!(
                    "custom:{}",
                    serde_json::to_string(&raster_colorizer).unwrap()
                ),
            ),
            ("format", "image/png"),
            ("time", "2014-01-01T00:00:00.0Z"),
        ];

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/wms/{}?{}",
                id,
                serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 200);

        let image_bytes = actix_web::test::read_body(res).await;

        // geoengine_datatypes::util::test::save_test_bytes(&image_bytes, "wms.png");

        assert_image_equals(test_data!("raster/multi_tile/wms.png"), &image_bytes);

        Ok(())
    }

    /// Registers tile files on a mock HTTP server and creates tiles with plain `http://` URLs.
    /// The `/vsicurl/` prefix is only added when GDAL opens the dataset.
    /// The mock server serves the actual test data files so GDAL can read them via `/vsicurl/`.
    fn create_ndvi_tiles_vsicurl(server: &Server) -> Vec<AddDatasetTile> {
        let mut tiles = create_ndvi_tiles();

        for tile in &mut tiles {
            let file_path_str = tile.params.file_path.to_string_lossy().to_string();

            // Read the actual tile file from the test data directory
            let local_path: PathBuf = test_data!(&file_path_str).into();
            let data = std::fs::read(&local_path).unwrap();
            let content_length = data.len();

            let server_path = format!("/{file_path_str}");

            // HEAD request (GDAL uses this to discover file size)
            server.expect(
                Expectation::matching(all_of![
                    request::method("HEAD"),
                    request::path(server_path.clone())
                ])
                .times(0..)
                .respond_with(
                    status_code(200)
                        .append_header("Content-Length", content_length.to_string())
                        .append_header("Accept-Ranges", "bytes"),
                ),
            );

            // GET request (GDAL fetches tile data via /vsicurl/)
            server.expect(
                Expectation::matching(all_of![request::method("GET"), request::path(server_path)])
                    .times(0..)
                    .respond_with(
                        status_code(200)
                            .append_header("Content-Type", "image/tiff")
                            .append_header("Content-Length", content_length.to_string())
                            .body(data),
                    ),
            );

            // Store the plain http URL — the /vsicurl/ prefix is added when opening the dataset
            tile.params.file_path = server.url_str(&format!("/{file_path_str}")).into();
        }

        tiles
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_adds_tiles_to_external_dataset(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        // Start a mock HTTP server that serves the tile files (GDAL reads them via /vsicurl/)
        let mock_server = httptest::Server::run();

        // Create tiles with plain http URLs backed by the mock server.
        // The mock server is kept alive for the full test duration (including the WMS query).
        let tiles = create_ndvi_tiles_vsicurl(&mock_server);

        // add data
        let create = CreateDataset {
            data_path: DataPath::External,
            definition: DatasetDefinition {
                properties: AddDataset {
                    name: None,
                    display_name: "ndvi external (tiled)".to_string(),
                    description: "ndvi".to_string(),
                    source_operator: "MultiBandGdalSource".to_string(),
                    symbology: None,
                    provenance: None,
                    tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
                },
                meta_data: MetaDataDefinition::GdalMultiBand(GdalMultiBand {
                    r#type: Default::default(),
                    result_descriptor: create_ndvi_result_descriptor(true).into(),
                    cache_ttl: None,
                }),
            },
        };

        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let db = ctx.db();

        let req = actix_web::test::TestRequest::post()
            .uri("/dataset")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_payload(serde_json::to_string(&create)?);
        let res = send_test_request(req, app_ctx.clone()).await;

        let DatasetNameResponse { dataset_name } = actix_web::test::read_body_json(res).await;
        let dataset_id = db
            .resolve_dataset_name_to_id(&dataset_name)
            .await
            .unwrap()
            .unwrap();

        assert!(db.load_dataset(&dataset_id).await.is_ok());

        // Add tiles through the API handler — validate_tile now checks that the paths
        // are valid http URLs (which they are), so this succeeds.
        let req = actix_web::test::TestRequest::post()
            .uri(&format!("/dataset/{dataset_name}/tiles"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_payload(serde_json::to_string(&tiles)?);

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200, "response: {res:?}");

        // create workflow
        let workflow = Workflow {
            operator: TypedOperator::Raster(RasterOperator::MultiBandGdalSource(
                MultiBandGdalSource {
                    r#type: Default::default(),
                    params: GdalSourceParameters {
                        data: dataset_name.into(),
                        overview_level: None,
                    },
                },
            ))
            .try_into()
            .unwrap(),
        };

        let id = ctx.db().register_workflow(workflow.clone()).await.unwrap();

        let colorizer = geoengine_datatypes::operations::image::Colorizer::linear_gradient(
            vec![
                (0.0, RgbaColor::white()).try_into().unwrap(),
                (255.0, RgbaColor::black()).try_into().unwrap(),
            ],
            RgbaColor::transparent(),
            RgbaColor::white(),
            RgbaColor::black(),
        )
        .unwrap();

        let raster_colorizer =
            crate::api::model::datatypes::RasterColorizer::SingleBand(SingleBandRasterColorizer {
                r#type: Default::default(),
                band: 0,
                band_colorizer: colorizer.into(),
            });

        let params = &[
            ("request", "GetMap"),
            ("service", "WMS"),
            ("version", "1.3.0"),
            ("layers", &id.to_string()),
            ("bbox", "-90,-180,90,180"),
            ("width", "3600"),
            ("height", "1800"),
            ("crs", "EPSG:4326"),
            (
                "styles",
                &format!(
                    "custom:{}",
                    serde_json::to_string(&raster_colorizer).unwrap()
                ),
            ),
            ("format", "image/png"),
            ("time", "2014-01-01T00:00:00.0Z"),
        ];

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/wms/{}?{}",
                id,
                serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 200);

        let image_bytes = actix_web::test::read_body(res).await;

        assert_image_equals(test_data!("raster/multi_tile/wms.png"), &image_bytes);

        Ok(())
    }

    pub fn create_ndvi_tiles() -> Vec<AddDatasetTile> {
        let no_data_value = Some(0.); // TODO: is it really 0?

        let starts: Vec<TimeInstance> = vec![
            TimeInstance::from_str("2014-01-01T00:00:00Z").unwrap(),
            TimeInstance::from_str("2014-02-01T00:00:00Z").unwrap(),
            TimeInstance::from_str("2014-03-01T00:00:00Z").unwrap(),
            TimeInstance::from_str("2014-04-01T00:00:00Z").unwrap(),
            TimeInstance::from_str("2014-05-01T00:00:00Z").unwrap(),
            TimeInstance::from_str("2014-06-01T00:00:00Z").unwrap(),
        ];

        let ends: Vec<TimeInstance> = vec![
            TimeInstance::from_str("2014-02-01T00:00:00Z").unwrap(),
            TimeInstance::from_str("2014-03-01T00:00:00Z").unwrap(),
            TimeInstance::from_str("2014-04-01T00:00:00Z").unwrap(),
            TimeInstance::from_str("2014-05-01T00:00:00Z").unwrap(),
            TimeInstance::from_str("2014-06-01T00:00:00Z").unwrap(),
            TimeInstance::from_str("2014-07-01T00:00:00Z").unwrap(),
        ];

        let mut tiles = vec![];

        for (start, end) in starts.iter().zip(ends.iter()) {
            let start_time: geoengine_datatypes::primitives::TimeInstance = (*start).into();
            let time_str = start_time
                .as_date_time()
                .unwrap()
                .format(&DateTimeParseFormat::custom("%Y-%m-%d".to_string()));

            // left
            tiles.push(AddDatasetTile {
                time: TimeInterval::new_unchecked(*start, *end).into(),
                spatial_partition:
                    geoengine_datatypes::primitives::SpatialPartition2D::new_unchecked(
                        (-180., 90.).into(),
                        (0.0, -90.).into(),
                    )
                    .into(),
                band: 0,
                z_index: 0,
                params: GdalDatasetParameters {
                    file_path: format!("raster/modis_ndvi/tiled/MOD13A2_M_NDVI_{time_str}_1_1.tif")
                        .into(),
                    rasterband_channel: 1,
                    geo_transform: geoengine_operators::source::GdalDatasetGeoTransform {
                        origin_coordinate: (-180., 90.).into(),
                        x_pixel_size: 0.1,
                        y_pixel_size: -0.1,
                    }
                    .into(),
                    width: 1800,
                    height: 1800,
                    file_not_found_handling: FileNotFoundHandling::Error,
                    no_data_value,
                    properties_mapping: None,
                    gdal_open_options: None,
                    gdal_config_options: None,
                    allow_alphaband_as_mask: true,
                },
            });

            // right
            tiles.push(AddDatasetTile {
                time: TimeInterval::new_unchecked(*start, *end).into(),
                spatial_partition:
                    geoengine_datatypes::primitives::SpatialPartition2D::new_unchecked(
                        (0., 90.).into(),
                        (180.0, -90.).into(),
                    )
                    .into(),
                band: 0,
                z_index: 0,
                params: GdalDatasetParameters {
                    file_path: format!("raster/modis_ndvi/tiled/MOD13A2_M_NDVI_{time_str}_1_2.tif")
                        .into(),
                    rasterband_channel: 1,
                    geo_transform: geoengine_operators::source::GdalDatasetGeoTransform {
                        origin_coordinate: (0., 90.).into(),
                        x_pixel_size: 0.1,
                        y_pixel_size: -0.1,
                    }
                    .into(),
                    width: 1800,
                    height: 1800,
                    file_not_found_handling: FileNotFoundHandling::Error,
                    no_data_value,
                    properties_mapping: None,
                    gdal_open_options: None,
                    gdal_config_options: None,
                    allow_alphaband_as_mask: true,
                },
            });
        }

        tiles
    }

    async fn add_multi_tile_dataset(
        app_ctx: &PostgresContext<NoTls>,
        reverse_z_order: bool,
        as_regular_dataset: bool,
    ) -> Result<(PostgresSessionContext<NoTls>, DatasetName)> {
        // add data
        let create: CreateDataset = if as_regular_dataset {
            serde_json::from_str(&std::fs::read_to_string(test_data!(
                "raster/multi_tile/metadata/dataset_regular.json"
            ))?)?
        } else {
            serde_json::from_str(&std::fs::read_to_string(test_data!(
                "raster/multi_tile/metadata/dataset_irregular.json"
            ))?)?
        };

        let session = admin_login(app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let db = ctx.db();

        let req = actix_web::test::TestRequest::post()
            .uri("/dataset")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_payload(serde_json::to_string(&create)?);
        let res = send_test_request(req, app_ctx.clone()).await;

        let DatasetNameResponse { dataset_name } = actix_web::test::read_body_json(res).await;
        let dataset_id = db
            .resolve_dataset_name_to_id(&dataset_name)
            .await
            .unwrap()
            .unwrap();

        assert!(db.load_dataset(&dataset_id).await.is_ok());

        // add tiles
        let tiles: Vec<AddDatasetTile> = if reverse_z_order {
            serde_json::from_str(&std::fs::read_to_string(test_data!(
                "raster/multi_tile/metadata/loading_info_rev.json"
            ))?)?
        } else {
            serde_json::from_str(&std::fs::read_to_string(test_data!(
                "raster/multi_tile/metadata/loading_info.json"
            ))?)?
        };

        let req = actix_web::test::TestRequest::post()
            .uri(&format!("/dataset/{dataset_name}/tiles"))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_json(tiles);

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200, "response: {res:?}");

        Ok((ctx, dataset_name))
    }

    #[ge_context::test]
    async fn it_loads_multi_band_multi_file_mosaics(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let (ctx, dataset_name) = add_multi_tile_dataset(&app_ctx, false, true).await?;

        let operator = MultiBandGdalSource {
            r#type: Default::default(),
            params: GdalSourceParameters {
                data: dataset_name.into(),
                overview_level: None,
            },
        };

        let execution_context = ctx.execution_context()?;

        let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

        let initialized = OperatorsMultiBandGdalSource::try_from(operator.clone())
            .unwrap()
            .boxed()
            .initialize(workflow_operator_path_root, &execution_context)
            .await?;

        let processor = initialized.query_processor()?;

        let query_ctx = ctx.query_context(WorkflowId::new(), ComputationId::new())?;

        let tiling_spec = execution_context.tiling_specification();

        let tiling_spatial_grid_definition = processor
            .result_descriptor()
            .spatial_grid_descriptor()
            .tiling_grid_definition(tiling_spec);

        let query_tiling_pixel_grid = tiling_spatial_grid_definition
            .tiling_spatial_grid_definition()
            .spatial_bounds_to_compatible_spatial_grid(SpatialPartition2D::new_unchecked(
                (-180., 90.).into(),
                (180.0, -90.).into(),
            ));

        let query_rect = RasterQueryRectangle::new(
            query_tiling_pixel_grid.grid_bounds(),
            TimeInterval::new_instant(
                geoengine_datatypes::primitives::TimeInstance::from_str("2025-01-01T00:00:00Z")
                    .unwrap(),
            )
            .unwrap(),
            BandSelection::first(),
        );

        let tiles = processor
            .get_u16()
            .unwrap()
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await?;

        let expected_tiles = [
            "2025-01-01_global_b0_tile_0.tif",
            "2025-01-01_global_b0_tile_1.tif",
            "2025-01-01_global_b0_tile_2.tif",
            "2025-01-01_global_b0_tile_3.tif",
            "2025-01-01_global_b0_tile_4.tif",
            "2025-01-01_global_b0_tile_5.tif",
            "2025-01-01_global_b0_tile_6.tif",
            "2025-01-01_global_b0_tile_7.tif",
        ];

        let expected_time = TimeInterval::new(
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-01-01T00:00:00Z")
                .unwrap(),
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-02-01T00:00:00Z")
                .unwrap(),
        )
        .unwrap();

        let expected_tiles: Vec<_> = expected_tiles
            .iter()
            .map(|f| {
                raster_tile_from_file::<u16>(
                    test_data!(format!("raster/multi_tile/results/z_index/tiles/{f}")),
                    tiling_spatial_grid_definition,
                    expected_time,
                    0,
                )
                .unwrap()
            })
            .collect();

        assert_eq_two_list_of_tiles(&tiles, &expected_tiles, false);

        Ok(())
    }

    #[ge_context::test]
    async fn it_loads_multi_band_multi_file_mosaics_2_bands(
        app_ctx: PostgresContext<NoTls>,
    ) -> Result<()> {
        let (ctx, dataset_name) = add_multi_tile_dataset(&app_ctx, false, false).await?;

        let operator = MultiBandGdalSource {
            r#type: Default::default(),
            params: GdalSourceParameters {
                data: dataset_name.into(),
                overview_level: None,
            },
        };

        let execution_context = ctx.execution_context()?;

        let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

        let initialized = OperatorsMultiBandGdalSource::try_from(operator.clone())
            .unwrap()
            .boxed()
            .initialize(workflow_operator_path_root, &execution_context)
            .await?;

        let processor = initialized.query_processor()?;

        let query_ctx = ctx.query_context(WorkflowId::new(), ComputationId::new())?;

        let tiling_spec = execution_context.tiling_specification();

        let tiling_spatial_grid_definition = processor
            .result_descriptor()
            .spatial_grid_descriptor()
            .tiling_grid_definition(tiling_spec);

        let query_tiling_pixel_grid = tiling_spatial_grid_definition
            .tiling_spatial_grid_definition()
            .spatial_bounds_to_compatible_spatial_grid(SpatialPartition2D::new_unchecked(
                (-180., 90.).into(),
                (180.0, -90.).into(),
            ));

        let query_rect = RasterQueryRectangle::new(
            query_tiling_pixel_grid.grid_bounds(),
            TimeInterval::new_instant(
                geoengine_datatypes::primitives::TimeInstance::from_str("2025-01-01T00:00:00Z")
                    .unwrap(),
            )
            .unwrap(),
            BandSelection::first_n(2),
        );

        let tiles = processor
            .get_u16()
            .unwrap()
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await?;

        let expected_tiles = [
            ("2025-01-01_global_b0_tile_0.tif", 0u32),
            ("2025-01-01_global_b1_tile_0.tif", 1),
            ("2025-01-01_global_b0_tile_1.tif", 0),
            ("2025-01-01_global_b1_tile_1.tif", 1),
            ("2025-01-01_global_b0_tile_2.tif", 0),
            ("2025-01-01_global_b1_tile_2.tif", 1),
            ("2025-01-01_global_b0_tile_3.tif", 0),
            ("2025-01-01_global_b1_tile_3.tif", 1),
            ("2025-01-01_global_b0_tile_4.tif", 0),
            ("2025-01-01_global_b1_tile_4.tif", 1),
            ("2025-01-01_global_b0_tile_5.tif", 0),
            ("2025-01-01_global_b1_tile_5.tif", 1),
            ("2025-01-01_global_b0_tile_6.tif", 0),
            ("2025-01-01_global_b1_tile_6.tif", 1),
            ("2025-01-01_global_b0_tile_7.tif", 0),
            ("2025-01-01_global_b1_tile_7.tif", 1),
        ];

        let expected_time = TimeInterval::new(
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-01-01T00:00:00Z")
                .unwrap(),
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-02-01T00:00:00Z")
                .unwrap(),
        )
        .unwrap();

        let expected_tiles: Vec<_> = expected_tiles
            .iter()
            .map(|(f, b)| {
                raster_tile_from_file::<u16>(
                    test_data!(format!("raster/multi_tile/results/z_index/tiles/{f}")),
                    tiling_spatial_grid_definition,
                    expected_time,
                    *b,
                )
                .unwrap()
            })
            .collect();

        assert_eq_two_list_of_tiles(&tiles, &expected_tiles, false);

        Ok(())
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_loads_multi_band_multi_file_mosaics_2_bands_2_timesteps(
        app_ctx: PostgresContext<NoTls>,
    ) -> Result<()> {
        let (ctx, dataset_name) = add_multi_tile_dataset(&app_ctx, false, false).await?;

        let operator = MultiBandGdalSource {
            r#type: Default::default(),
            params: GdalSourceParameters {
                data: dataset_name.into(),
                overview_level: None,
            },
        };

        let execution_context = ctx.execution_context()?;

        let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

        let initialized = OperatorsMultiBandGdalSource::try_from(operator.clone())
            .unwrap()
            .boxed()
            .initialize(workflow_operator_path_root, &execution_context)
            .await?;

        let processor = initialized.query_processor()?;

        let query_ctx = ctx.query_context(WorkflowId::new(), ComputationId::new())?;

        let tiling_spec = execution_context.tiling_specification();

        let tiling_spatial_grid_definition = processor
            .result_descriptor()
            .spatial_grid_descriptor()
            .tiling_grid_definition(tiling_spec);

        let query_tiling_pixel_grid = tiling_spatial_grid_definition
            .tiling_spatial_grid_definition()
            .spatial_bounds_to_compatible_spatial_grid(SpatialPartition2D::new_unchecked(
                (-180., 90.).into(),
                (180.0, -90.).into(),
            ));

        let query_rect = RasterQueryRectangle::new(
            query_tiling_pixel_grid.grid_bounds(),
            TimeInterval::new(
                geoengine_datatypes::primitives::TimeInstance::from_str("2025-01-01T00:00:00Z")
                    .unwrap(),
                geoengine_datatypes::primitives::TimeInstance::from_str("2025-03-01T00:00:00Z")
                    .unwrap(),
            )
            .unwrap(),
            BandSelection::first_n(2),
        );

        let tiles = processor
            .get_u16()
            .unwrap()
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await?;

        let expected_time1 = TimeInterval::new(
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-01-01T00:00:00Z")
                .unwrap(),
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-02-01T00:00:00Z")
                .unwrap(),
        )
        .unwrap();

        let expected_time2 = TimeInterval::new(
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-02-01T00:00:00Z")
                .unwrap(),
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-03-01T00:00:00Z")
                .unwrap(),
        )
        .unwrap();

        let expected_tiles = [
            ("2025-01-01_global_b0_tile_0.tif", 0u32, expected_time1),
            ("2025-01-01_global_b1_tile_0.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_1.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_1.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_2.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_2.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_3.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_3.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_4.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_4.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_5.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_5.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_6.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_6.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_7.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_7.tif", 1, expected_time1),
            ("2025-02-01_global_b0_tile_0.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_0.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_1.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_1.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_2.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_2.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_3.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_3.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_4.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_4.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_5.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_5.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_6.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_6.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_7.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_7.tif", 1, expected_time2),
        ];

        let expected_tiles: Vec<_> = expected_tiles
            .iter()
            .map(|(f, b, t)| {
                raster_tile_from_file::<u16>(
                    test_data!(format!("raster/multi_tile/results/z_index/tiles/{f}")),
                    tiling_spatial_grid_definition,
                    *t,
                    *b,
                )
                .unwrap()
            })
            .collect();

        assert_eq_two_list_of_tiles(&tiles, &expected_tiles, false);

        Ok(())
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_loads_multi_band_multi_file_mosaics_with_time_gaps_regular(
        app_ctx: PostgresContext<NoTls>,
    ) -> Result<()> {
        let (ctx, dataset_name) = add_multi_tile_dataset(&app_ctx, false, true).await?;

        let operator = MultiBandGdalSource {
            r#type: Default::default(),
            params: GdalSourceParameters {
                data: dataset_name.into(),
                overview_level: None,
            },
        };

        let execution_context = ctx.execution_context()?;

        let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

        let initialized = OperatorsMultiBandGdalSource::try_from(operator.clone())
            .unwrap()
            .boxed()
            .initialize(workflow_operator_path_root, &execution_context)
            .await?;

        let processor = initialized.query_processor()?;

        let query_ctx = ctx.query_context(WorkflowId::new(), ComputationId::new())?;

        let tiling_spec = execution_context.tiling_specification();

        let tiling_spatial_grid_definition = processor
            .result_descriptor()
            .spatial_grid_descriptor()
            .tiling_grid_definition(tiling_spec);

        let query_tiling_pixel_grid = tiling_spatial_grid_definition
            .tiling_spatial_grid_definition()
            .spatial_bounds_to_compatible_spatial_grid(SpatialPartition2D::new_unchecked(
                (-180., 90.).into(),
                (180.0, -90.).into(),
            ));

        // query a time interval that is greater than the time interval of the tiles and covers a region with a temporal gap
        let query_rect = RasterQueryRectangle::new(
            query_tiling_pixel_grid.grid_bounds(),
            TimeInterval::new(
                geoengine_datatypes::primitives::TimeInstance::from_str("2024-12-01T00:00:00Z")
                    .unwrap(),
                geoengine_datatypes::primitives::TimeInstance::from_str("2025-05-15T00:00:00Z")
                    .unwrap(),
            )
            .unwrap(),
            BandSelection::first_n(2),
        );

        let mut tiles = processor
            .get_u16()
            .unwrap()
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await?;

        // first 8 (spatial) x 2 (bands) tiles must be no data
        for tile in tiles.drain(..16) {
            assert_eq!(
                tile.time,
                TimeInterval::new(
                    geoengine_datatypes::primitives::TimeInstance::from_str("2024-12-01T00:00:00Z")
                        .unwrap(),
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-01-01T00:00:00Z")
                        .unwrap(),
                )
                .unwrap()
            );
            assert!(tile.is_empty());
        }

        // next comes data
        let expected_time1 = TimeInterval::new(
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-01-01T00:00:00Z")
                .unwrap(),
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-02-01T00:00:00Z")
                .unwrap(),
        )
        .unwrap();

        let expected_time2 = TimeInterval::new(
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-02-01T00:00:00Z")
                .unwrap(),
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-03-01T00:00:00Z")
                .unwrap(),
        )
        .unwrap();

        let expected_tiles = [
            ("2025-01-01_global_b0_tile_0.tif", 0u32, expected_time1),
            ("2025-01-01_global_b1_tile_0.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_1.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_1.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_2.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_2.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_3.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_3.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_4.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_4.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_5.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_5.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_6.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_6.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_7.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_7.tif", 1, expected_time1),
            ("2025-02-01_global_b0_tile_0.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_0.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_1.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_1.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_2.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_2.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_3.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_3.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_4.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_4.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_5.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_5.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_6.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_6.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_7.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_7.tif", 1, expected_time2),
        ];

        let expected_tiles: Vec<_> = expected_tiles
            .iter()
            .map(|(f, b, t)| {
                raster_tile_from_file::<u16>(
                    test_data!(format!("raster/multi_tile/results/z_index/tiles/{f}")),
                    tiling_spatial_grid_definition,
                    *t,
                    *b,
                )
                .unwrap()
            })
            .collect();

        assert_eq_two_list_of_tiles(
            &tiles.drain(..expected_tiles.len()).collect::<Vec<_>>(),
            &expected_tiles,
            false,
        );

        // next comes a gap of no data
        for tile in tiles.drain(..16) {
            assert_eq!(
                tile.time,
                TimeInterval::new(
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-03-01T00:00:00Z")
                        .unwrap(),
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-04-01T00:00:00Z")
                        .unwrap(),
                )
                .unwrap()
            );
            assert!(tile.is_empty());
        }

        // next comes data again
        let expected_time = TimeInterval::new(
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-04-01T00:00:00Z")
                .unwrap(),
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-05-01T00:00:00Z")
                .unwrap(),
        )
        .unwrap();

        let expected_tiles = [
            ("2025-04-01_global_b0_tile_0.tif", 0u32, expected_time),
            ("2025-04-01_global_b1_tile_0.tif", 1, expected_time),
            ("2025-04-01_global_b0_tile_1.tif", 0, expected_time),
            ("2025-04-01_global_b1_tile_1.tif", 1, expected_time),
            ("2025-04-01_global_b0_tile_2.tif", 0, expected_time),
            ("2025-04-01_global_b1_tile_2.tif", 1, expected_time),
            ("2025-04-01_global_b0_tile_3.tif", 0, expected_time),
            ("2025-04-01_global_b1_tile_3.tif", 1, expected_time),
            ("2025-04-01_global_b0_tile_4.tif", 0, expected_time),
            ("2025-04-01_global_b1_tile_4.tif", 1, expected_time),
            ("2025-04-01_global_b0_tile_5.tif", 0, expected_time),
            ("2025-04-01_global_b1_tile_5.tif", 1, expected_time),
            ("2025-04-01_global_b0_tile_6.tif", 0, expected_time),
            ("2025-04-01_global_b1_tile_6.tif", 1, expected_time),
            ("2025-04-01_global_b0_tile_7.tif", 0, expected_time),
            ("2025-04-01_global_b1_tile_7.tif", 1, expected_time),
        ];

        let expected_tiles: Vec<_> = expected_tiles
            .iter()
            .map(|(f, b, t)| {
                raster_tile_from_file::<u16>(
                    test_data!(format!("raster/multi_tile/results/z_index/tiles/{f}")),
                    tiling_spatial_grid_definition,
                    *t,
                    *b,
                )
                .unwrap()
            })
            .collect();

        assert_eq_two_list_of_tiles(
            &tiles.drain(..expected_tiles.len()).collect::<Vec<_>>(),
            &expected_tiles,
            false,
        );

        // last 8 (spatial) x 2 (bands) tiles must be no data
        for tile in &tiles[..16] {
            assert_eq!(
                tile.time,
                TimeInterval::new(
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-05-01T00:00:00Z")
                        .unwrap(),
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-06-01T00:00:00Z")
                        .unwrap()
                )
                .unwrap()
            );
            assert!(tile.is_empty());
        }

        Ok(())
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_loads_multi_band_multi_file_mosaics_with_time_gaps_irregular(
        app_ctx: PostgresContext<NoTls>,
    ) -> Result<()> {
        let (ctx, dataset_name) = add_multi_tile_dataset(&app_ctx, false, false).await?;

        let operator = MultiBandGdalSource {
            r#type: Default::default(),
            params: GdalSourceParameters {
                data: dataset_name.into(),
                overview_level: None,
            },
        };

        let execution_context = ctx.execution_context()?;

        let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

        let initialized = OperatorsMultiBandGdalSource::try_from(operator.clone())
            .unwrap()
            .boxed()
            .initialize(workflow_operator_path_root, &execution_context)
            .await?;

        let processor = initialized.query_processor()?;

        let query_ctx = ctx.query_context(WorkflowId::new(), ComputationId::new())?;

        let tiling_spec = execution_context.tiling_specification();

        let tiling_spatial_grid_definition = processor
            .result_descriptor()
            .spatial_grid_descriptor()
            .tiling_grid_definition(tiling_spec);

        let query_tiling_pixel_grid = tiling_spatial_grid_definition
            .tiling_spatial_grid_definition()
            .spatial_bounds_to_compatible_spatial_grid(SpatialPartition2D::new_unchecked(
                (-180., 90.).into(),
                (180.0, -90.).into(),
            ));

        // query a time interval that is greater than the time interval of the tiles and covers a region with a temporal gap
        let query_rect = RasterQueryRectangle::new(
            query_tiling_pixel_grid.grid_bounds(),
            TimeInterval::new(
                geoengine_datatypes::primitives::TimeInstance::from_str("2024-12-01T00:00:00Z")
                    .unwrap(),
                geoengine_datatypes::primitives::TimeInstance::from_str("2025-05-15T00:00:00Z")
                    .unwrap(),
            )
            .unwrap(),
            BandSelection::first_n(2),
        );

        let mut tiles = processor
            .get_u16()
            .unwrap()
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await?;

        // first 8 (spatial) x 2 (bands) tiles must be no data
        for tile in tiles.drain(..16) {
            assert_eq!(
                tile.time,
                TimeInterval::new(
                    geoengine_datatypes::primitives::TimeInstance::MIN,
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-01-01T00:00:00Z")
                        .unwrap(),
                )
                .unwrap()
            );
            assert!(tile.is_empty());
        }

        // next comes data
        let expected_time1 = TimeInterval::new(
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-01-01T00:00:00Z")
                .unwrap(),
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-02-01T00:00:00Z")
                .unwrap(),
        )
        .unwrap();

        let expected_time2 = TimeInterval::new(
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-02-01T00:00:00Z")
                .unwrap(),
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-03-01T00:00:00Z")
                .unwrap(),
        )
        .unwrap();

        let expected_tiles = [
            ("2025-01-01_global_b0_tile_0.tif", 0u32, expected_time1),
            ("2025-01-01_global_b1_tile_0.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_1.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_1.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_2.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_2.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_3.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_3.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_4.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_4.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_5.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_5.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_6.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_6.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_7.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_7.tif", 1, expected_time1),
            ("2025-02-01_global_b0_tile_0.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_0.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_1.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_1.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_2.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_2.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_3.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_3.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_4.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_4.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_5.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_5.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_6.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_6.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_7.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_7.tif", 1, expected_time2),
        ];

        let expected_tiles: Vec<_> = expected_tiles
            .iter()
            .map(|(f, b, t)| {
                raster_tile_from_file::<u16>(
                    test_data!(format!("raster/multi_tile/results/z_index/tiles/{f}")),
                    tiling_spatial_grid_definition,
                    *t,
                    *b,
                )
                .unwrap()
            })
            .collect();

        assert_eq_two_list_of_tiles(
            &tiles.drain(..expected_tiles.len()).collect::<Vec<_>>(),
            &expected_tiles,
            false,
        );

        // next comes a gap of no data
        for tile in tiles.drain(..16) {
            assert_eq!(
                tile.time,
                TimeInterval::new(
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-03-01T00:00:00Z")
                        .unwrap(),
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-04-01T00:00:00Z")
                        .unwrap(),
                )
                .unwrap()
            );
            assert!(tile.is_empty());
        }

        // next comes data again
        let expected_time = TimeInterval::new(
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-04-01T00:00:00Z")
                .unwrap(),
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-05-01T00:00:00Z")
                .unwrap(),
        )
        .unwrap();

        let expected_tiles = [
            ("2025-04-01_global_b0_tile_0.tif", 0u32, expected_time),
            ("2025-04-01_global_b1_tile_0.tif", 1, expected_time),
            ("2025-04-01_global_b0_tile_1.tif", 0, expected_time),
            ("2025-04-01_global_b1_tile_1.tif", 1, expected_time),
            ("2025-04-01_global_b0_tile_2.tif", 0, expected_time),
            ("2025-04-01_global_b1_tile_2.tif", 1, expected_time),
            ("2025-04-01_global_b0_tile_3.tif", 0, expected_time),
            ("2025-04-01_global_b1_tile_3.tif", 1, expected_time),
            ("2025-04-01_global_b0_tile_4.tif", 0, expected_time),
            ("2025-04-01_global_b1_tile_4.tif", 1, expected_time),
            ("2025-04-01_global_b0_tile_5.tif", 0, expected_time),
            ("2025-04-01_global_b1_tile_5.tif", 1, expected_time),
            ("2025-04-01_global_b0_tile_6.tif", 0, expected_time),
            ("2025-04-01_global_b1_tile_6.tif", 1, expected_time),
            ("2025-04-01_global_b0_tile_7.tif", 0, expected_time),
            ("2025-04-01_global_b1_tile_7.tif", 1, expected_time),
        ];

        let expected_tiles: Vec<_> = expected_tiles
            .iter()
            .map(|(f, b, t)| {
                raster_tile_from_file::<u16>(
                    test_data!(format!("raster/multi_tile/results/z_index/tiles/{f}")),
                    tiling_spatial_grid_definition,
                    *t,
                    *b,
                )
                .unwrap()
            })
            .collect();

        assert_eq_two_list_of_tiles(
            &tiles.drain(..expected_tiles.len()).collect::<Vec<_>>(),
            &expected_tiles,
            false,
        );

        // last 8 (spatial) x 2 (bands) tiles must be no data
        for tile in &tiles[..16] {
            assert_eq!(
                tile.time,
                TimeInterval::new(
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-05-01T00:00:00Z")
                        .unwrap(),
                    geoengine_datatypes::primitives::TimeInstance::MAX
                )
                .unwrap()
            );
            assert!(tile.is_empty());
        }

        Ok(())
    }

    #[ge_context::test]
    async fn it_loads_multi_band_multi_file_mosaics_reverse_z_index(
        app_ctx: PostgresContext<NoTls>,
    ) -> Result<()> {
        let (ctx, dataset_name) = add_multi_tile_dataset(&app_ctx, true, false).await?;

        let operator = MultiBandGdalSource {
            r#type: Default::default(),
            params: GdalSourceParameters {
                data: dataset_name.into(),
                overview_level: None,
            },
        };

        let execution_context = ctx.execution_context()?;

        let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

        let initialized = OperatorsMultiBandGdalSource::try_from(operator.clone())
            .unwrap()
            .boxed()
            .initialize(workflow_operator_path_root, &execution_context)
            .await?;

        let processor = initialized.query_processor()?;

        let query_ctx = ctx.query_context(WorkflowId::new(), ComputationId::new())?;

        let tiling_spec = execution_context.tiling_specification();

        let tiling_spatial_grid_definition = processor
            .result_descriptor()
            .spatial_grid_descriptor()
            .tiling_grid_definition(tiling_spec);

        let query_tiling_pixel_grid = tiling_spatial_grid_definition
            .tiling_spatial_grid_definition()
            .spatial_bounds_to_compatible_spatial_grid(SpatialPartition2D::new_unchecked(
                (-180., 90.).into(),
                (180.0, -90.).into(),
            ));

        let query_rect = RasterQueryRectangle::new(
            query_tiling_pixel_grid.grid_bounds(),
            TimeInterval::new_instant(
                geoengine_datatypes::primitives::TimeInstance::from_str("2025-01-01T00:00:00Z")
                    .unwrap(),
            )
            .unwrap(),
            BandSelection::first(),
        );

        let tiles = processor
            .get_u16()
            .unwrap()
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await?;

        let expected_tiles = [
            "2025-01-01_global_b0_tile_0.tif",
            "2025-01-01_global_b0_tile_1.tif",
            "2025-01-01_global_b0_tile_2.tif",
            "2025-01-01_global_b0_tile_3.tif",
            "2025-01-01_global_b0_tile_4.tif",
            "2025-01-01_global_b0_tile_5.tif",
            "2025-01-01_global_b0_tile_6.tif",
            "2025-01-01_global_b0_tile_7.tif",
        ];

        let expected_time = TimeInterval::new(
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-01-01T00:00:00Z")
                .unwrap(),
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-02-01T00:00:00Z")
                .unwrap(),
        )
        .unwrap();

        let expected_tiles: Vec<_> = expected_tiles
            .iter()
            .map(|f| {
                raster_tile_from_file::<u16>(
                    test_data!(format!(
                        "raster/multi_tile/results/z_index_reversed/tiles/{f}"
                    )),
                    tiling_spatial_grid_definition,
                    expected_time,
                    0,
                )
                .unwrap()
            })
            .collect();

        assert_eq_two_list_of_tiles(&tiles, &expected_tiles, false);

        Ok(())
    }

    #[ge_context::test]
    async fn it_loads_multi_band_nodata_only(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let (ctx, dataset_name) = add_multi_tile_dataset(&app_ctx, false, false).await?;

        let operator = MultiBandGdalSource {
            r#type: Default::default(),
            params: GdalSourceParameters {
                data: dataset_name.into(),
                overview_level: None,
            },
        };

        let execution_context = ctx.execution_context()?;

        let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

        let initialized = OperatorsMultiBandGdalSource::try_from(operator.clone())
            .unwrap()
            .boxed()
            .initialize(workflow_operator_path_root, &execution_context)
            .await?;

        let processor = initialized.query_processor()?;

        let query_ctx = ctx.query_context(WorkflowId::new(), ComputationId::new())?;

        let tiling_spec = execution_context.tiling_specification();

        let tiling_spatial_grid_definition = processor
            .result_descriptor()
            .spatial_grid_descriptor()
            .tiling_grid_definition(tiling_spec);

        let query_tiling_pixel_grid = tiling_spatial_grid_definition
            .tiling_spatial_grid_definition()
            .spatial_bounds_to_compatible_spatial_grid(SpatialPartition2D::new_unchecked(
                (-180., 90.).into(),
                (180.0, -90.).into(),
            ));

        let query_rect = RasterQueryRectangle::new(
            query_tiling_pixel_grid.grid_bounds(),
            TimeInterval::new_instant(
                geoengine_datatypes::primitives::TimeInstance::from_str("2024-01-01T00:00:00Z")
                    .unwrap(),
            )
            .unwrap(),
            BandSelection::first(),
        );

        let tiles = processor
            .get_u16()
            .unwrap()
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(tiles.len(), 8);

        for tile in tiles {
            assert!(tile.is_empty());
            assert_eq!(tile.time, geoengine_datatypes::primitives::TimeInterval::new(
                geoengine_datatypes::primitives::TimeInstance::MIN,
                geoengine_datatypes::primitives::TimeInstance::from_str(
                    "2025-01-01T00:00:00Z",
                )
                .unwrap(),
            ).unwrap());
        }

        Ok(())
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_checks_tile_times_regular_before_adding_to_dataset(
        app_ctx: PostgresContext<NoTls>,
    ) -> Result<()> {
        let volume = VolumeName("test_data".to_string());

        // add data
        let create = CreateDataset {
            data_path: DataPath::Volume(volume.clone()),
            definition: DatasetDefinition {
                properties: AddDataset {
                    name: None,
                    display_name: "ndvi (tiled)".to_string(),
                    description: "ndvi".to_string(),
                    source_operator: "MultiBandGdalSource".to_string(),
                    symbology: None,
                    provenance: None,
                    tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
                },
                meta_data: MetaDataDefinition::GdalMultiBand(GdalMultiBand {
                    r#type: Default::default(),
                    result_descriptor: create_ndvi_result_descriptor(true).into(),
                    cache_ttl: None,
                }),
            },
        };

        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let db = ctx.db();

        let req = actix_web::test::TestRequest::post()
            .uri("/dataset")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_payload(serde_json::to_string(&create)?);
        let res = send_test_request(req, app_ctx.clone()).await;

        let DatasetNameResponse { dataset_name } = actix_web::test::read_body_json(res).await;
        let dataset_id = db
            .resolve_dataset_name_to_id(&dataset_name)
            .await
            .unwrap()
            .unwrap();

        assert!(db.load_dataset(&dataset_id).await.is_ok());

        // add tiles
        let tiles = create_ndvi_tiles();

        let req = actix_web::test::TestRequest::post()
            .uri(&format!("/dataset/{dataset_name}/tiles"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_payload(serde_json::to_string(&tiles)?);

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200, "response: {res:?}");

        // try to insert tiles with time that conflicts with existing tiles
        let mut tiles = tiles[..1].to_vec();
        tiles[0].time = TimeInterval::new_unchecked(
            TimeInstance::from_str("2014-01-01T00:00:00Z").unwrap(),
            TimeInstance::from_str("2014-01-03T00:00:00Z").unwrap(),
        )
        .into();

        let req = actix_web::test::TestRequest::post()
            .uri(&format!("/dataset/{dataset_name}/tiles"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_payload(serde_json::to_string(&tiles)?);

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 400, "response: {res:?}");

        let read_body: ErrorResponse = actix_web::test::read_body_json(res).await;

        assert_eq!(read_body, ErrorResponse {
            error: "CannotAddTilesToDataset".to_string(),
            message: "Cannot add tiles to dataset: Dataset tile times `[TimeInterval [1388534400000, 1388707200000)]` conflict with dataset regularity RegularTimeDimension { origin: TimeInstance(0), step: TimeStep { granularity: Months, step: 1 } }".to_string(),
        });

        Ok(())
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_checks_tile_times_irregular_before_adding_to_dataset(
        app_ctx: PostgresContext<NoTls>,
    ) -> Result<()> {
        let volume = VolumeName("test_data".to_string());

        // add data
        let create = CreateDataset {
            data_path: DataPath::Volume(volume.clone()),
            definition: DatasetDefinition {
                properties: AddDataset {
                    name: None,
                    display_name: "ndvi (tiled)".to_string(),
                    description: "ndvi".to_string(),
                    source_operator: "MultiBandGdalSource".to_string(),
                    symbology: None,
                    provenance: None,
                    tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
                },
                meta_data: MetaDataDefinition::GdalMultiBand(GdalMultiBand {
                    r#type: Default::default(),
                    result_descriptor: create_ndvi_result_descriptor(false).into(),
                    cache_ttl: None,
                }),
            },
        };

        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let db = ctx.db();

        let req = actix_web::test::TestRequest::post()
            .uri("/dataset")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_payload(serde_json::to_string(&create)?);
        let res = send_test_request(req, app_ctx.clone()).await;

        let DatasetNameResponse { dataset_name } = actix_web::test::read_body_json(res).await;
        let dataset_id = db
            .resolve_dataset_name_to_id(&dataset_name)
            .await
            .unwrap()
            .unwrap();

        assert!(db.load_dataset(&dataset_id).await.is_ok());

        // add tiles
        let tiles = create_ndvi_tiles();

        let req = actix_web::test::TestRequest::post()
            .uri(&format!("/dataset/{dataset_name}/tiles"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_payload(serde_json::to_string(&tiles)?);

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200, "response: {res:?}");

        // try to insert tiles with time that conflicts with existing tiles
        let mut tiles = tiles[..1].to_vec();
        tiles[0].time = TimeInterval::new_unchecked(
            TimeInstance::from_str("2014-01-01T00:00:00Z").unwrap(),
            TimeInstance::from_str("2014-01-03T00:00:00Z").unwrap(),
        )
        .into();

        let req = actix_web::test::TestRequest::post()
            .uri(&format!("/dataset/{dataset_name}/tiles"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_payload(serde_json::to_string(&tiles)?);

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 400, "response: {res:?}");

        let read_body: ErrorResponse = actix_web::test::read_body_json(res).await;

        assert_eq!(read_body, ErrorResponse {
            error: "CannotAddTilesToDataset".to_string(),
            message: "Cannot add tiles to dataset: Dataset tile time `[TimeInterval [1388534400000, 1388707200000)]` conflict with existing times `[TimeInterval [1388534400000, 1391212800000)]`".to_string(),
        });

        Ok(())
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_checks_tile_z_indexes_before_adding_to_dataset(
        app_ctx: PostgresContext<NoTls>,
    ) -> Result<()> {
        let volume = VolumeName("test_data".to_string());

        // add data
        let create = CreateDataset {
            data_path: DataPath::Volume(volume.clone()),
            definition: DatasetDefinition {
                properties: AddDataset {
                    name: None,
                    display_name: "ndvi (tiled)".to_string(),
                    description: "ndvi".to_string(),
                    source_operator: "MultiBandGdalSource".to_string(),
                    symbology: None,
                    provenance: None,
                    tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
                },
                meta_data: MetaDataDefinition::GdalMultiBand(GdalMultiBand {
                    r#type: Default::default(),
                    result_descriptor: create_ndvi_result_descriptor(true).into(),
                    cache_ttl: None,
                }),
            },
        };

        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let db = ctx.db();

        let req = actix_web::test::TestRequest::post()
            .uri("/dataset")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_payload(serde_json::to_string(&create)?);
        let res = send_test_request(req, app_ctx.clone()).await;

        let DatasetNameResponse { dataset_name } = actix_web::test::read_body_json(res).await;
        let dataset_id = db
            .resolve_dataset_name_to_id(&dataset_name)
            .await
            .unwrap()
            .unwrap();

        assert!(db.load_dataset(&dataset_id).await.is_ok());

        // add tiles
        let tiles = create_ndvi_tiles();

        let req = actix_web::test::TestRequest::post()
            .uri(&format!("/dataset/{dataset_name}/tiles"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_payload(serde_json::to_string(&tiles)?);

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200, "response: {res:?}");

        // try to insert tiles with z index that conflicts with existing tiles
        let tiles = tiles[..1].to_vec();

        let req = actix_web::test::TestRequest::post()
            .uri(&format!("/dataset/{dataset_name}/tiles"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_payload(serde_json::to_string(&tiles)?);

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 400, "response: {res:?}");

        let read_body: ErrorResponse = actix_web::test::read_body_json(res).await;

        let conflict_tile = "raster/modis_ndvi/tiled/MOD13A2_M_NDVI_2014-01-01_1_1.tif";

        assert_eq!(
            read_body,
            ErrorResponse {
                error: "CannotAddTilesToDataset".to_string(),
                message: format!(
                    "Cannot add tiles to dataset: Dataset tile z-index of files `[\"{conflict_tile}\"]` conflict with existing tiles with the same z-indexes",
                ),
            }
        );

        Ok(())
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_extends_dataset_bounds_when_inserting_new_tiles(
        app_ctx: PostgresContext<NoTls>,
    ) -> Result<()> {
        let volume = VolumeName("test_data".to_string());

        let create = CreateDataset {
            data_path: DataPath::Volume(volume.clone()),
            definition: DatasetDefinition {
                properties: AddDataset {
                    name: None,
                    display_name: "fake dataset".to_string(),
                    description: "fake".to_string(),
                    source_operator: "MultiBandGdalSource".to_string(),
                    symbology: None,
                    provenance: None,
                    tags: None,
                },
                meta_data: MetaDataDefinition::GdalMultiBand(GdalMultiBand {
                    r#type: Default::default(),
                    result_descriptor: RasterResultDescriptor {
                        data_type: RasterDataType::U8,
                        spatial_reference: SpatialReferenceOption::SpatialReference(
                            SpatialReference::epsg_4326(),
                        )
                        .into(),
                        time: TimeDescriptor {
                            bounds: None,
                            dimension: TimeDimension::Irregular,
                        },
                        spatial_grid: SpatialGridDescriptor {
                            spatial_grid: SpatialGridDefinition {
                                geo_transform: GeoTransform {
                                    origin_coordinate: Coordinate2D { x: 0., y: 0. },
                                    x_pixel_size: 1.,
                                    y_pixel_size: -1.,
                                },
                                grid_bounds: GridBoundingBox2D {
                                    top_left_idx: GridIdx2D { y_idx: 0, x_idx: 0 },
                                    bottom_right_idx: GridIdx2D {
                                        y_idx: 100,
                                        x_idx: 100,
                                    },
                                },
                            },
                            descriptor: SpatialGridDescriptorState::Source,
                        },
                        bands: RasterBandDescriptors::new(vec![RasterBandDescriptor {
                            name: "band_1".to_string(),
                            measurement: Measurement::Unitless.into(),
                        }])
                        .unwrap(),
                    },
                    cache_ttl: None,
                }),
            },
        };

        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let db = ctx.db();

        let id_and_name = db
            .add_dataset(
                create.definition.properties.into(),
                create.definition.meta_data.into(),
                Some(DataPath::Volume(volume.clone())),
            )
            .await
            .unwrap();

        let tile = AddDatasetTile {
            time: TimeInterval::new_unchecked(
                TimeInstance::from_str("2014-01-01T00:00:00Z").unwrap(),
                TimeInstance::from_str("2014-01-02T00:00:00Z").unwrap(),
            )
            .into(),
            spatial_partition: SpatialPartition2D::new_unchecked(
                (0., 0.).into(),
                (100., -100.).into(),
            )
            .into(),
            band: 0,
            z_index: 0,
            params: GdalDatasetParameters {
                file_path: "fake_path".into(),
                rasterband_channel: 0,
                geo_transform: GeoTransform {
                    origin_coordinate: Coordinate2D { x: 0., y: 0. },
                    x_pixel_size: 1.,
                    y_pixel_size: -1.,
                },
                width: 100,
                height: 100,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value: None,
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options: None,
                allow_alphaband_as_mask: false,
            },
        };

        db.add_dataset_tiles(id_and_name.id, vec![tile]).await?;

        let ds = db.load_dataset(&id_and_name.id).await?;
        let TypedResultDescriptor::Raster(dataset_rd) = ds.result_descriptor else {
            panic!("expected raster dataset");
        };

        let meta_data: Box<
            dyn MetaData<
                    MultiBandGdalLoadingInfo,
                    geoengine_operators::engine::RasterResultDescriptor,
                    MultiBandGdalLoadingInfoQueryRectangle,
                >,
        > = db
            .meta_data(
                &DataId::Internal(InternalDataId {
                    dataset_id: id_and_name.id.into(),
                    r#type:
                        crate::api::model::datatypes::InternalDataIdTypeTag::InternalDataIdTypeTag,
                })
                .into(),
            )
            .await?;
        let metadata_rd = meta_data.result_descriptor().await?;

        assert_eq!(metadata_rd, dataset_rd);

        assert_eq!(
            dataset_rd.spatial_grid,
            SpatialGridDescriptor {
                spatial_grid: SpatialGridDefinition {
                    geo_transform: GeoTransform {
                        origin_coordinate: Coordinate2D { x: 0., y: 0. },
                        x_pixel_size: 1.,
                        y_pixel_size: -1.,
                    },
                    grid_bounds: GridBoundingBox2D {
                        top_left_idx: GridIdx2D { y_idx: 0, x_idx: 0 },
                        bottom_right_idx: GridIdx2D {
                            y_idx: 100,
                            x_idx: 100,
                        },
                    },
                },
                descriptor: SpatialGridDescriptorState::Source,
            }
            .into()
        );

        assert_eq!(
            dataset_rd.time.bounds,
            TimeInterval::new_unchecked(
                TimeInstance::from_str("2014-01-01T00:00:00Z").unwrap(),
                TimeInstance::from_str("2014-01-02T00:00:00Z").unwrap(),
            )
            .into()
        );

        let tile = AddDatasetTile {
            time: TimeInterval::new_unchecked(
                TimeInstance::from_str("2014-01-03T00:00:00Z").unwrap(),
                TimeInstance::from_str("2014-01-04T00:00:00Z").unwrap(),
            )
            .into(),
            spatial_partition: SpatialPartition2D::new_unchecked(
                (-50., 50.).into(),
                (0., 0.).into(),
            )
            .into(),
            band: 0,
            z_index: 0,
            params: GdalDatasetParameters {
                file_path: "fake_path".into(),
                rasterband_channel: 0,
                geo_transform: GeoTransform {
                    origin_coordinate: Coordinate2D { x: -50., y: 50. },
                    x_pixel_size: 1.,
                    y_pixel_size: -1.,
                },
                width: 50,
                height: 50,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value: None,
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options: None,
                allow_alphaband_as_mask: false,
            },
        };

        db.add_dataset_tiles(id_and_name.id, vec![tile]).await?;

        let ds = db.load_dataset(&id_and_name.id).await?;
        let TypedResultDescriptor::Raster(dataset_rd) = ds.result_descriptor else {
            panic!("expected raster dataset");
        };

        let meta_data: Box<
            dyn MetaData<
                    MultiBandGdalLoadingInfo,
                    geoengine_operators::engine::RasterResultDescriptor,
                    MultiBandGdalLoadingInfoQueryRectangle,
                >,
        > = db
            .meta_data(
                &DataId::Internal(InternalDataId {
                    dataset_id: id_and_name.id.into(),
                    r#type:
                        crate::api::model::datatypes::InternalDataIdTypeTag::InternalDataIdTypeTag,
                })
                .into(),
            )
            .await?;
        let metadata_rd = meta_data.result_descriptor().await?;

        assert_eq!(metadata_rd, dataset_rd);

        assert_eq!(
            dataset_rd.spatial_grid,
            SpatialGridDescriptor {
                spatial_grid: SpatialGridDefinition {
                    geo_transform: GeoTransform {
                        origin_coordinate: Coordinate2D { x: 0., y: 0. },
                        x_pixel_size: 1.,
                        y_pixel_size: -1.,
                    },
                    grid_bounds: GridBoundingBox2D {
                        top_left_idx: GridIdx2D {
                            y_idx: -50,
                            x_idx: -50
                        },
                        bottom_right_idx: GridIdx2D {
                            y_idx: 100,
                            x_idx: 100,
                        },
                    },
                },
                descriptor: SpatialGridDescriptorState::Source,
            }
            .into()
        );

        assert_eq!(
            dataset_rd.time.bounds,
            TimeInterval::new_unchecked(
                TimeInstance::from_str("2014-01-01T00:00:00Z").unwrap(),
                TimeInstance::from_str("2014-01-04T00:00:00Z").unwrap(),
            )
            .into()
        );

        Ok(())
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_answers_multiband_time_queries(app_ctx: PostgresContext<NoTls>) -> Result<()> {
        let (ctx, dataset_name) = add_multi_tile_dataset(&app_ctx, false, false).await?;

        let operator = MultiBandGdalSource {
            r#type: Default::default(),
            params: GdalSourceParameters {
                data: dataset_name.into(),
                overview_level: None,
            },
        };

        let execution_context = ctx.execution_context()?;

        let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

        let initialized = OperatorsMultiBandGdalSource::try_from(operator.clone())
            .unwrap()
            .boxed()
            .initialize(workflow_operator_path_root, &execution_context)
            .await?;

        let processor = initialized.query_processor()?;

        let query_ctx = ctx.query_context(WorkflowId::new(), ComputationId::new())?;

        let processor = processor.get_u16().unwrap();

        let time_stream = processor
            .time_query(
                TimeInterval::new(
                    geoengine_datatypes::primitives::TimeInstance::from_str("2024-12-01T00:00:00Z")
                        .unwrap(),
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-05-15T00:00:00Z")
                        .unwrap(),
                )
                .unwrap(),
                &query_ctx,
            )
            .await?;

        let times: Vec<TimeInterval> = time_stream.try_collect().await?;

        assert_eq!(
            times,
            vec![
                TimeInterval::new(
                    geoengine_datatypes::primitives::TimeInstance::MIN,
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-01-01T00:00:00Z")
                        .unwrap(),
                )
                .unwrap(),
                TimeInterval::new(
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-01-01T00:00:00Z")
                        .unwrap(),
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-02-01T00:00:00Z")
                        .unwrap(),
                )
                .unwrap(),
                TimeInterval::new(
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-02-01T00:00:00Z")
                        .unwrap(),
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-03-01T00:00:00Z")
                        .unwrap(),
                )
                .unwrap(),
                TimeInterval::new(
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-03-01T00:00:00Z")
                        .unwrap(),
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-04-01T00:00:00Z")
                        .unwrap(),
                )
                .unwrap(),
                TimeInterval::new(
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-04-01T00:00:00Z")
                        .unwrap(),
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-05-01T00:00:00Z")
                        .unwrap(),
                )
                .unwrap(),
                TimeInterval::new(
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-05-01T00:00:00Z")
                        .unwrap(),
                    geoengine_datatypes::primitives::TimeInstance::MAX
                )
                .unwrap()
            ]
        );

        Ok(())
    }

    #[ge_context::test]
    async fn it_loads_time_gap_in_multi_band_multi_file_mosaics(
        app_ctx: PostgresContext<NoTls>,
    ) -> Result<()> {
        let (ctx, dataset_name) = add_multi_tile_dataset(&app_ctx, false, false).await?;

        let operator = MultiBandGdalSource {
            r#type: Default::default(),
            params: GdalSourceParameters {
                data: dataset_name.into(),
                overview_level: None,
            },
        };

        let execution_context = ctx.execution_context()?;

        let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

        let initialized = OperatorsMultiBandGdalSource::try_from(operator.clone())
            .unwrap()
            .boxed()
            .initialize(workflow_operator_path_root, &execution_context)
            .await?;

        let processor = initialized.query_processor()?;

        let query_ctx = ctx.query_context(WorkflowId::new(), ComputationId::new())?;

        let times = processor
            .get_u16()
            .unwrap()
            .time_query(
                TimeInterval::new(
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-03-01T00:00:00Z")
                        .unwrap(),
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-04-01T00:00:00Z")
                        .unwrap(),
                )
                .unwrap(),
                &query_ctx,
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(
            times,
            vec![
                TimeInterval::new(
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-03-01T00:00:00Z")
                        .unwrap(),
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-04-01T00:00:00Z")
                        .unwrap(),
                )
                .unwrap(),
            ]
        );

        Ok(())
    }
    /// One MD dataset row, declared the way an importer declares it.
    ///
    /// The probe lives on a stacked branch, so these tests state the metadata instead of
    /// deriving it. Constants are transcribed from the fixture manifest in
    /// `test_data/md/generate_md_fixtures.py`.
    struct MdRow {
        path: &'static str,
        array_name: &'static str,
        group: Option<&'static str>,
        band: u32,
        /// position of this file in the concatenated z axis of `band`
        z_index: i64,
        /// first slice of this file in the dataset's global z axis
        z_start: usize,
        slices: usize,
        leading_prefix: Vec<i64>,
    }

    impl MdRow {
        /// A row of the 8x8 `time_series` grid: edges lon 0..240 (30 degree pixels),
        /// lat 0..-8, daily slices from 2000-01-01.
        fn of(path: &'static str, array_name: &'static str, z_start: usize, slices: usize) -> Self {
            Self {
                path,
                array_name,
                group: None,
                band: 0,
                z_index: 0,
                z_start,
                slices,
                leading_prefix: Vec::new(),
            }
        }
    }

    /// The presented footprint of the 8x8 / 30 degree fixture grid: pixel edges 0..240 and
    /// 0..-8, so the box spans lon 0..240 and lat -8..0. No wrap, so the presented transform
    /// is the stored one.
    fn grid_8x8_spatial_bounds() -> SpatialPartition2D {
        let grid = geoengine_datatypes::raster::GridBoundingBox2D::new_unchecked([0, 0], [7, 7]);
        geoengine_datatypes::raster::GeoTransform::new((0.0, 0.0).into(), 30.0, -1.0)
            .grid_to_spatial_bounds(&grid)
    }

    /// A regular daily axis over `steps`, as `MdFileTimes` would have derived it.
    fn daily_descriptor(steps: &[geoengine_datatypes::primitives::TimeInterval]) -> TimeDescriptor {
        TimeDescriptor::from(geoengine_operators::engine::TimeDescriptor::new_regular(
            Some(
                geoengine_datatypes::primitives::TimeInterval::new_unchecked(
                    steps[0].start(),
                    steps[steps.len() - 1].end(),
                ),
            ),
            steps[0].start(),
            geoengine_datatypes::primitives::TimeStep::days(1).expect("one day is a valid step"),
        ))
    }

    fn api_band(name: &str) -> RasterBandDescriptor {
        RasterBandDescriptor {
            name: name.to_owned(),
            measurement: geoengine_datatypes::primitives::Measurement::Unitless.into(),
        }
    }

    const MD_DAY: i64 = 86_400_000;
    const MD_EPOCH_2000: i64 = 946_684_800_000;

    /// The rows plus the `GdalMdMetaData` that describes them, as a caller would post them.
    fn md_dataset_meta(
        rows: &[MdRow],
        bands: Vec<RasterBandDescriptor>,
    ) -> (
        crate::datasets::storage::MetaDataDefinition,
        Vec<AddDatasetMdTile>,
    ) {
        let total = rows.iter().map(|r| r.z_start + r.slices).max().unwrap();
        let time_steps: Vec<TimeInterval> = (0..total)
            .map(|i| {
                TimeInterval::new_unchecked(
                    MD_EPOCH_2000 + i as i64 * MD_DAY,
                    MD_EPOCH_2000 + (i as i64 + 1) * MD_DAY,
                )
            })
            .collect();

        let tiles = rows
            .iter()
            .map(|r| {
                let file_steps = &time_steps[r.z_start..r.z_start + r.slices];
                let params = GdalDatasetParameters {
                    file_path: test_data!(r.path).to_path_buf(),
                    rasterband_channel: 1,
                    // the *stored* grid; the presented one is what the descriptor carries
                    geo_transform: GeoTransform {
                        origin_coordinate: Coordinate2D { x: 0.0, y: 0.0 },
                        x_pixel_size: 30.0,
                        y_pixel_size: -1.0,
                    },
                    width: 8,
                    height: 8,
                    file_not_found_handling: FileNotFoundHandling::NoData,
                    no_data_value: Some(-9999.0),
                    properties_mapping: None,
                    gdal_open_options: None,
                    gdal_config_options: None,
                    allow_alphaband_as_mask: false,
                };
                // the presented footprint of an 8x8 grid at 30 degree pixels: no wrap, so
                // the stored and presented transforms agree
                let bounds = grid_8x8_spatial_bounds();
                AddDatasetMdTile {
                    spatial_partition: bounds.into(),
                    band: r.band,
                    z_index: r.z_index,
                    array_name: r.array_name.to_owned(),
                    array_group: r.group.map(str::to_string),
                    time_descriptor: daily_descriptor(file_steps),
                    time_steps: file_steps
                        .iter()
                        .map(|iv| crate::api::model::datatypes::TimeInterval {
                            start: iv.start().into(),
                            end: iv.end().into(),
                        })
                        .collect(),
                    params,
                    leading_prefix: r.leading_prefix.clone(),
                }
            })
            .collect::<Vec<_>>();

        // the operators descriptor, so `GdalMdMetaData` takes it directly
        let result_descriptor = geoengine_operators::engine::RasterResultDescriptor::new(
            geoengine_datatypes::raster::RasterDataType::F32,
            geoengine_datatypes::spatial_reference::SpatialReference::epsg_4326().into(),
            geoengine_operators::engine::TimeDescriptor::new_regular(
                Some(
                    geoengine_datatypes::primitives::TimeInterval::new_unchecked(
                        time_steps[0].start(),
                        time_steps[time_steps.len() - 1].end(),
                    ),
                ),
                time_steps[0].start(),
                geoengine_datatypes::primitives::TimeStep::days(1)
                    .expect("one day is a valid step"),
            ),
            geoengine_operators::engine::SpatialGridDescriptor::source_from_parts(
                geoengine_datatypes::raster::GeoTransform::new((0.0, 0.0).into(), 30.0, -1.0),
                geoengine_datatypes::raster::GridBoundingBox2D::new_unchecked([0, 0], [7, 7]),
            ),
            geoengine_operators::engine::RasterBandDescriptors::new(
                bands
                    .into_iter()
                    .map(|b| {
                        geoengine_operators::engine::RasterBandDescriptor::new(
                            b.name,
                            b.measurement.into(),
                        )
                    })
                    .collect(),
            )
            .unwrap(),
        );

        (
            geoengine_operators::source::GdalMdMetaData::new(
                result_descriptor,
                geoengine_operators::source::ZRole::Variable,
                false,
                None,
                None,
            )
            .into(),
            tiles,
        )
    }

    /// The MD loading info's time axis must come from the files' *per-slice* intervals, not
    /// from one interval per stored row. A stored row spans a whole file (365 daily slices
    /// for a year of CMIP6), so deriving the axis from row times collapses it to one step
    /// per file and every query then reads the wrong slice. The gap fixtures make it
    /// observable: `gap_a` covers t 0..4, `gap_b` covers t 8..12, t 4..8 is absent.
    /// Fixture values are exact integers in f32
    #[allow(clippy::float_cmp)]
    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn it_reads_md_tiles_across_a_gap_in_the_file_sequence(
        app_ctx: PostgresContext<NoTls>,
    ) -> Result<()> {
        let volume = VolumeName("test_data".to_string());

        // gap_a covers global z 0..4 and gap_b covers 8..12, so z 4..8 is a hole the read
        // path has to gap-fill
        let (meta_data, tiles) = md_dataset_meta(
            &[
                MdRow {
                    z_index: 0,
                    ..MdRow::of("md/time_series_gap_a.nc", "temperature", 0, 4)
                },
                MdRow {
                    z_index: 1,
                    ..MdRow::of("md/time_series_gap_b.nc", "temperature", 8, 4)
                },
            ],
            vec![api_band("temperature")],
        );

        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let create = CreateDataset {
            data_path: DataPath::Volume(volume),
            definition: DatasetDefinition {
                properties: AddDataset {
                    name: None,
                    display_name: "md gap".to_string(),
                    description: "md gap".to_string(),
                    source_operator: "MdGdalSource".to_string(),
                    symbology: None,
                    provenance: None,
                    tags: None,
                },
                meta_data: meta_data.into(),
            },
        };

        let req = actix_web::test::TestRequest::post()
            .uri("/dataset")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_payload(serde_json::to_string(&create)?);
        let res = send_test_request(req, app_ctx.clone()).await;

        let DatasetNameResponse { dataset_name } = actix_web::test::read_body_json(res).await;

        // two rows: one per file, z_index 0 and 1
        assert_eq!(tiles.len(), 2);
        assert_eq!(tiles[0].z_index, 0);
        assert_eq!(tiles[1].z_index, 1);
        // the files store absolute paths; the volume expects them relative to its root
        let rel = |f: &AddDatasetMdTile| {
            let mut f = f.clone();
            f.params.file_path = Path::new("md").join(f.params.file_path.file_name().unwrap());
            f
        };

        let req = actix_web::test::TestRequest::post()
            .uri(&format!("/dataset/{dataset_name}/md-tiles"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .append_header((header::CONTENT_TYPE, "application/json"))
            .set_payload(serde_json::to_string(
                &tiles.iter().map(rel).collect::<Vec<_>>(),
            )?);
        let res = send_test_request(req, app_ctx.clone()).await;
        assert_eq!(
            res.status(),
            200,
            "response: {}",
            String::from_utf8_lossy(&actix_web::test::read_body(res).await)
        );

        // query the whole series in one go: 4 + 1 gap step + 4 = 9 steps
        let operator = geoengine_operators::source::MdGdalSource {
            params: geoengine_operators::source::MdGdalSourceParameters::new(dataset_name.into()),
        };
        let execution_context = ctx.execution_context()?;
        let initialized = operator
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await?;
        let processor = initialized.query_processor()?;
        let query_ctx = ctx.query_context(WorkflowId::new(), ComputationId::new())?;

        let tiling_grid = processor
            .result_descriptor()
            .spatial_grid_descriptor()
            .tiling_grid_definition(execution_context.tiling_specification());
        let query_grid = tiling_grid
            .tiling_spatial_grid_definition()
            .spatial_bounds_to_compatible_spatial_grid(SpatialPartition2D::new_unchecked(
                (0., 0.).into(),
                (240., -8.).into(),
            ));

        // 2000-01-01 .. 2000-01-13 covers t 0..12, i.e. both files plus the hole
        let query_rect = RasterQueryRectangle::new(
            query_grid.grid_bounds(),
            TimeInterval::new(
                geoengine_datatypes::primitives::TimeInstance::from_str("2000-01-01T00:00:00Z")
                    .unwrap(),
                geoengine_datatypes::primitives::TimeInstance::from_str("2000-01-13T00:00:00Z")
                    .unwrap(),
            )
            .unwrap(),
            BandSelection::first(),
        );

        let tiles = processor
            .get_f32()
            .unwrap()
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await?;

        // The query asks for 13 days but only 8 are covered, so the axis is
        // [t0..t3] [gap t4..t8] [t8..t11] - 9 steps, one tile each. The uncovered tail past
        // the last slice is not represented as a step of its own.
        let mut daily = tiles;
        daily.sort_by_key(|t| t.time.start());

        let is_empty = |t: &geoengine_datatypes::raster::RasterTile2D<f32>| {
            matches!(
                t.grid_array,
                geoengine_datatypes::raster::GridOrEmpty::Empty(_)
            )
        };
        let first_value = |t: &geoengine_datatypes::raster::RasterTile2D<f32>| match &t.grid_array {
            geoengine_datatypes::raster::GridOrEmpty::Grid(g) => g.inner_grid.data[0],
            geoengine_datatypes::raster::GridOrEmpty::Empty(_) => f32::NAN,
        };

        assert_eq!(daily.len(), 9, "got {} steps", daily.len());

        // before the gap: one tile per day, each stamped with its own date
        for (i, tile) in daily.iter().enumerate().take(4) {
            assert_eq!(
                tile.time.start().inner(),
                946_684_800_000 + i64::try_from(i).unwrap() * 86_400_000,
                "step {i} must carry its own timestamp"
            );
            assert_eq!(first_value(tile), i as f32 * 1000.0);
        }

        // t 4..8 is absent from the data: one gap interval, and its tile must be empty
        assert_eq!(
            daily[4].time.start().inner(),
            946_684_800_000 + 4 * 86_400_000
        );
        assert!(is_empty(&daily[4]), "the gap step must be an empty tile");

        // after the gap: file b's slices, NOT a repeat of file a's. A z_start taken from a
        // running slice count would put t=8..12 at indices 4..8 and hand back t=3 here.
        assert_eq!(
            daily[5].time.start().inner(),
            946_684_800_000 + 8 * 86_400_000
        );
        assert_eq!(first_value(&daily[5]), 8_000.0);
        assert_eq!(first_value(&daily[8]), 11_000.0);

        Ok(())
    }

    /// `MetaData::time_axis` must answer the same gap-filled axis that `loading_info` does,
    /// without the file rows: a plain time query never asks for a bbox, and an axis derived
    /// from stored *row* times would collapse to one step per file.
    #[ge_context::test]
    async fn it_answers_the_md_time_axis_without_a_bbox(
        app_ctx: PostgresContext<NoTls>,
    ) -> Result<()> {
        let volume = VolumeName("test_data".to_string());

        // gap_a covers global z 0..4 and gap_b covers 8..12, so z 4..8 is a hole the read
        // path has to gap-fill
        let (meta_data, tiles) = md_dataset_meta(
            &[
                MdRow {
                    z_index: 0,
                    ..MdRow::of("md/time_series_gap_a.nc", "temperature", 0, 4)
                },
                MdRow {
                    z_index: 1,
                    ..MdRow::of("md/time_series_gap_b.nc", "temperature", 8, 4)
                },
            ],
            vec![api_band("temperature")],
        );

        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session);
        let db = ctx.db();

        let id_and_name = db
            .add_dataset(
                AddDataset {
                    name: None,
                    display_name: "md time axis".to_string(),
                    description: "md time axis".to_string(),
                    source_operator: "MdGdalSource".to_string(),
                    symbology: None,
                    provenance: None,
                    tags: None,
                }
                .into(),
                meta_data,
                Some(DataPath::Volume(volume)),
            )
            .await?;

        let rel = |f: &AddDatasetMdTile| {
            let mut f = f.clone();
            f.params.file_path = Path::new("md").join(f.params.file_path.file_name().unwrap());
            f
        };
        db.add_md_dataset_tiles(id_and_name.id, tiles.iter().map(rel).collect())
            .await?;

        let meta_data: Box<
            dyn MetaData<
                    geoengine_operators::source::MdLoadingInfo,
                    geoengine_operators::engine::RasterResultDescriptor,
                    RasterQueryRectangle,
                >,
        > = db
            .meta_data(
                &DataId::Internal(InternalDataId {
                    dataset_id: id_and_name.id.into(),
                    r#type:
                        crate::api::model::datatypes::InternalDataIdTypeTag::InternalDataIdTypeTag,
                })
                .into(),
            )
            .await?;

        // 2000-01-01 .. 2000-01-13 spans t 0..12: 4 covered + 1 gap + 4 covered = 9 steps
        let axis = meta_data
            .time_axis(TimeInterval::new_unchecked(
                TimeInstance::from_str("2000-01-01T00:00:00Z").unwrap(),
                TimeInstance::from_str("2000-01-13T00:00:00Z").unwrap(),
            ))
            .await?
            .expect("a Time-role dataset has an axis");

        assert_eq!(axis.len(), 9);
        assert_eq!(axis[0].start().inner(), 946_684_800_000);
        // the gap step starts where file a stopped, and file b resumes after it
        assert_eq!(axis[4].start().inner(), 946_684_800_000 + 4 * 86_400_000);
        assert_eq!(axis[5].start().inner(), 946_684_800_000 + 8 * 86_400_000);

        Ok(())
    }

    /// A dataset's `cache_ttl` must survive the round trip through the database and reach
    /// the loading info, mirroring `MultiBandGdalSource`. A dataset without one must fall
    /// back to the caller's context default instead of a frozen hint.
    #[ge_context::test]
    async fn it_carries_a_dataset_cache_ttl_onto_md_tiles(
        app_ctx: PostgresContext<NoTls>,
    ) -> Result<()> {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session);
        let db = ctx.db();
        let volume = VolumeName("test_data".to_string());

        // `ProbedGdalMdMetaData` is not `Clone`, so probe per case rather than sharing one
        let rel = |f: &AddDatasetMdTile| {
            let mut f = f.clone();
            f.params.file_path = Path::new("md").join(f.params.file_path.file_name().unwrap());
            f
        };

        for (label, cache_ttl) in [("with ttl", Some(1234_u32)), ("without ttl", None)] {
            let (meta_data, tiles) = md_dataset_meta(
                &[MdRow::of("md/time_series_gap_a.nc", "temperature", 0, 4)],
                vec![api_band("temperature")],
            );
            let crate::datasets::storage::MetaDataDefinition::GdalMdMetaData(mut md) = meta_data
            else {
                unreachable!("md_dataset_meta returns MD metadata")
            };
            md.cache_ttl = cache_ttl.map(geoengine_datatypes::primitives::CacheTtlSeconds::new);
            let meta_data = crate::datasets::storage::MetaDataDefinition::GdalMdMetaData(md);

            let id_and_name = db
                .add_dataset(
                    AddDataset {
                        name: None,
                        display_name: format!("md ttl {label}"),
                        description: format!("md ttl {label}"),
                        source_operator: "MdGdalSource".to_string(),
                        symbology: None,
                        provenance: None,
                        tags: None,
                    }
                    .into(),
                    meta_data,
                    Some(DataPath::Volume(volume.clone())),
                )
                .await?;
            db.add_md_dataset_tiles(id_and_name.id, tiles.iter().map(rel).collect())
                .await?;

            let provider: Box<
                dyn MetaData<
                        geoengine_operators::source::MdLoadingInfo,
                        geoengine_operators::engine::RasterResultDescriptor,
                        RasterQueryRectangle,
                    >,
            > = db
                .meta_data(
                    &DataId::Internal(InternalDataId {
                        dataset_id: id_and_name.id.into(),
                        r#type:
                            crate::api::model::datatypes::InternalDataIdTypeTag::InternalDataIdTypeTag,
                    })
                    .into(),
                )
                .await?;

            let loading_info = provider
                .loading_info(RasterQueryRectangle::new(
                    geoengine_datatypes::raster::GridBoundingBox2D::new_unchecked([0, 0], [7, 7]),
                    TimeInterval::new_unchecked(
                        TimeInstance::from_str("2000-01-01T00:00:00Z").unwrap(),
                        TimeInstance::from_str("2000-01-05T00:00:00Z").unwrap(),
                    ),
                    BandSelection::first(),
                ))
                .await?;
            assert!(
                !loading_info.files().is_empty(),
                "a dataset {label} must match its stored rows"
            );

            // a `CacheHint` embeds its own creation time, so compare the expiry the TTL
            // yields rather than two hints built microseconds apart
            let context_ttl = geoengine_datatypes::primitives::CacheTtlSeconds::new(7);
            let expected_ttl = cache_ttl.map_or(context_ttl, |secs| {
                geoengine_datatypes::primitives::CacheTtlSeconds::new(secs)
            });
            let expires_in = loading_info
                .cache_hint(context_ttl)
                .expires()
                .seconds_to_expiration();
            let expected_in = expected_ttl.seconds();
            assert!(
                expires_in.abs_diff(expected_in) <= 2,
                "a dataset {label} cached for {expires_in}s, expected ~{expected_in}s"
            );
        }

        Ok(())
    }

    /// A `ZRole::Variable` dataset stores one row per `(file, variable)`, so the row reader
    /// sees every band's copy of the same intervals. The axis must be built from one band's
    /// rows only: concatenating all of them yields N copies of the timeline, which
    /// `try_time_irregular_range_fill` passes through and turns into a non-monotonic axis.
    #[ge_context::test]
    async fn it_builds_one_md_time_axis_per_variable_not_one_per_band(
        app_ctx: PostgresContext<NoTls>,
    ) -> Result<()> {
        let volume = VolumeName("test_data".to_string());

        let (meta_data, tiles) = md_dataset_meta(
            &[
                MdRow {
                    band: 0,
                    ..MdRow::of("md/variables.nc", "temperature", 0, 8)
                },
                MdRow {
                    band: 1,
                    ..MdRow::of("md/variables.nc", "precipitation", 0, 8)
                },
            ],
            vec![api_band("temperature"), api_band("precipitation")],
        );

        // two rows: one per (file, variable)
        assert_eq!(tiles.len(), 2);

        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session);
        let db = ctx.db();

        let id_and_name = db
            .add_dataset(
                AddDataset {
                    name: None,
                    display_name: "md variables".to_string(),
                    description: "md variables".to_string(),
                    source_operator: "MdGdalSource".to_string(),
                    symbology: None,
                    provenance: None,
                    tags: None,
                }
                .into(),
                meta_data,
                Some(DataPath::Volume(volume)),
            )
            .await?;

        let rel = |f: &AddDatasetMdTile| {
            let mut f = f.clone();
            f.params.file_path = Path::new("md").join(f.params.file_path.file_name().unwrap());
            f
        };
        db.add_md_dataset_tiles(id_and_name.id, tiles.iter().map(rel).collect())
            .await?;

        let meta_data: Box<
            dyn MetaData<
                    geoengine_operators::source::MdLoadingInfo,
                    geoengine_operators::engine::RasterResultDescriptor,
                    RasterQueryRectangle,
                >,
        > = db
            .meta_data(
                &DataId::Internal(InternalDataId {
                    dataset_id: id_and_name.id.into(),
                    r#type:
                        crate::api::model::datatypes::InternalDataIdTypeTag::InternalDataIdTypeTag,
                })
                .into(),
            )
            .await?;

        // `variables.nc` is a single 8-step daily series with 2 data variables, so the
        // dataset has 8 time steps. Concatenating every row's intervals would give 16.
        let axis = meta_data
            .time_axis(TimeInterval::new_unchecked(
                TimeInstance::from_str("2000-01-01T00:00:00Z").unwrap(),
                TimeInstance::from_str("2000-01-09T00:00:00Z").unwrap(),
            ))
            .await?
            .expect("a Variable-role dataset has an axis");

        assert_eq!(
            axis.len(),
            8,
            "one axis per dataset, not one per (file, variable) row"
        );

        // a narrow window clips: `try_time_irregular_range_fill` extends coverage to the
        // query but never trims it, so without a filter this returns all 8 slices
        let window = meta_data
            .time_axis(TimeInterval::new_unchecked(
                TimeInstance::from_str("2000-01-01T00:00:00Z").unwrap(),
                TimeInstance::from_str("2000-01-02T00:00:00Z").unwrap(),
            ))
            .await?
            .expect("a Variable-role dataset has an axis");

        assert_eq!(window.len(), 1, "a one-day window returns one slice");

        Ok(())
    }

    /// `ZRole::Band` stores every row with `band = 0` because the z index *is* the band, so
    /// a `band = ANY(...)` predicate would match nothing and a request for band 2 would
    /// silently come back as an empty tile instead of data.
    #[ge_context::test]
    async fn it_reads_md_band_role_rows_for_any_requested_band(
        app_ctx: PostgresContext<NoTls>,
    ) -> Result<()> {
        let volume = VolumeName("test_data".to_string());

        let (meta_data, tiles) = md_dataset_meta(
            &[MdRow::of("md/bands.nc", "reflectance", 0, 4)],
            (0..4).map(|b| api_band(&format!("band {b}"))).collect(),
        );
        let crate::datasets::storage::MetaDataDefinition::GdalMdMetaData(mut md) = meta_data else {
            unreachable!("md_dataset_meta returns MD metadata")
        };
        md.z_role = geoengine_operators::source::ZRole::Band;
        let meta_data = crate::datasets::storage::MetaDataDefinition::GdalMdMetaData(md);

        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session);
        let db = ctx.db();

        let id_and_name = db
            .add_dataset(
                AddDataset {
                    name: None,
                    display_name: "md bands".to_string(),
                    description: "md bands".to_string(),
                    source_operator: "MdGdalSource".to_string(),
                    symbology: None,
                    provenance: None,
                    tags: None,
                }
                .into(),
                meta_data,
                Some(DataPath::Volume(volume)),
            )
            .await?;

        let rel = |f: &AddDatasetMdTile| {
            let mut f = f.clone();
            f.params.file_path = Path::new("md").join(f.params.file_path.file_name().unwrap());
            f
        };
        db.add_md_dataset_tiles(id_and_name.id, tiles.iter().map(rel).collect())
            .await?;

        let meta_data: Box<
            dyn MetaData<
                    geoengine_operators::source::MdLoadingInfo,
                    geoengine_operators::engine::RasterResultDescriptor,
                    RasterQueryRectangle,
                >,
        > = db
            .meta_data(
                &DataId::Internal(InternalDataId {
                    dataset_id: id_and_name.id.into(),
                    r#type:
                        crate::api::model::datatypes::InternalDataIdTypeTag::InternalDataIdTypeTag,
                })
                .into(),
            )
            .await?;

        // band 2 of a band-role dataset: the row filter must not look at the `band` column
        let loading_info = meta_data
            .loading_info(RasterQueryRectangle::new(
                geoengine_datatypes::raster::GridBoundingBox2D::new_unchecked([0, 0], [7, 7]),
                TimeInterval::new_unchecked(
                    TimeInstance::from_str("2000-01-01T00:00:00Z").unwrap(),
                    TimeInstance::from_str("2000-01-02T00:00:00Z").unwrap(),
                ),
                BandSelection::new_single(2),
            ))
            .await?;

        assert!(
            !loading_info.files().is_empty(),
            "band 2 must match the stored rows, not come back empty"
        );

        Ok(())
    }

    /// The path an MD row's array is opened with. Getting this wrong opened `""` for every
    /// remote row and rejected it as unopenable, while reading the same row worked.
    #[test]
    fn it_opens_an_external_md_row_at_its_own_url() {
        let (_, tiles) = md_dataset_meta(
            &[MdRow::of("md/time_series.nc", "temperature", 0, 8)],
            vec![api_band("temperature")],
        );
        let mut tile = tiles[0].clone();
        tile.params.file_path = "/vsicurl/https://example.invalid/temperature.nc".into();

        // `validate_tile_file_path` hands back an empty path for external data
        let resolved =
            validate_tile_file_path(&tile.params.file_path, &DataPath::External, Path::new(""))
                .expect("a /vsicurl URL is a valid external path");
        assert!(resolved.as_os_str().is_empty(), "{resolved:?}");

        assert_eq!(
            md_tile_open_path(&tile, &DataPath::External, &resolved),
            Path::new("/vsicurl/https://example.invalid/temperature.nc"),
            "an external row is opened at its own URL, not at the empty resolved path"
        );

        // a volume row is opened at the path resolved against the volume root
        let volume = DataPath::Volume(VolumeName("test_data".to_string()));
        assert_eq!(
            md_tile_open_path(&tile, &volume, Path::new("/data/temperature.nc")),
            Path::new("/data/temperature.nc"),
        );
    }

    /// The array check is the only thing standing between a wrong `arrayName` and a dataset
    /// that reads nothing, so it has to run on every data path - including external, where a
    /// wrong CRS would otherwise be a permanent mislabel with nothing to catch it.
    #[test]
    fn it_checks_an_md_row_against_the_array_it_names() {
        let (meta_data, tiles) = md_dataset_meta(
            &[MdRow::of("md/time_series.nc", "temperature", 0, 8)],
            vec![api_band("temperature")],
        );
        let crate::datasets::storage::MetaDataDefinition::GdalMdMetaData(md) = meta_data else {
            panic!("md_dataset_meta returns MD metadata")
        };
        let local = |tile: &AddDatasetMdTile| {
            let mut tile = tile.clone();
            tile.params.file_path = test_data!("md/time_series.nc").to_path_buf();
            tile
        };

        // the declared row matches the fixture
        assert!(
            check_md_tile_against_array(
                &local(&tiles[0]),
                test_data!("md/time_series.nc"),
                &md.result_descriptor
            )
            .is_ok()
        );

        // a name that does not exist cannot be opened
        let mut wrong_name = local(&tiles[0]);
        wrong_name.array_name = "no_such_array".to_owned();
        assert!(matches!(
            check_md_tile_against_array(
                &wrong_name,
                test_data!("md/time_series.nc"),
                &md.result_descriptor
            ),
            Err(AddDatasetMdTilesError::CannotOpenMdTileFile { .. })
        ));

        // a wrong slice count is caught here rather than reading the wrong slices
        let mut wrong_slices = local(&tiles[0]);
        wrong_slices.time_steps.pop();
        assert!(matches!(
            check_md_tile_against_array(
                &wrong_slices,
                test_data!("md/time_series.nc"),
                &md.result_descriptor
            ),
            Err(AddDatasetMdTilesError::MdTileSliceCountMismatch {
                expected: 7,
                found: 8,
                ..
            })
        ));

        // a 3D array declares no prefix, so a non-empty one is wrong
        let mut wrong_prefix = local(&tiles[0]);
        wrong_prefix.leading_prefix = vec![0];
        assert!(matches!(
            check_md_tile_against_array(
                &wrong_prefix,
                test_data!("md/time_series.nc"),
                &md.result_descriptor
            ),
            Err(AddDatasetMdTilesError::MdTileLeadingPrefixMismatch {
                expected: 0,
                found: 1,
                ..
            })
        ));

        // the fixture has no `crs` attribute, so nothing contradicts EPSG:4326
        let mut wrong_grid = local(&tiles[0]);
        wrong_grid.params.width = 7;
        assert!(matches!(
            check_md_tile_against_array(
                &wrong_grid,
                test_data!("md/time_series.nc"),
                &md.result_descriptor
            ),
            Err(AddDatasetMdTilesError::MdTileArraySizeMismatch {
                declared: (7, 8),
                found: (8, 8),
                ..
            })
        ));
    }

    /// A CRS the file actually declares has to match the dataset's: a mismatch is a
    /// mislabelled dataset, and nothing downstream would ever notice.
    #[test]
    fn it_rejects_an_md_row_whose_crs_contradicts_the_dataset() {
        let (meta_data, tiles) = md_dataset_meta(
            &[MdRow::of("md/projected_crs.nc", "elevation", 0, 4)],
            vec![api_band("elevation")],
        );
        let crate::datasets::storage::MetaDataDefinition::GdalMdMetaData(md) = meta_data else {
            panic!("md_dataset_meta returns MD metadata")
        };
        // the fixture declares EPSG:32633 (metre pixels), the descriptor claims EPSG:4326
        assert!(matches!(
            check_md_tile_against_array(
                &tiles[0],
                test_data!("md/projected_crs.nc"),
                &md.result_descriptor
            ),
            Err(AddDatasetMdTilesError::MdTileCrsMismatch { .. })
        ));
    }

    /// External data is never opened while a row is validated, so the declared axis is the
    /// only thing that can be wrong about such a row. A descriptor that disagrees with its
    /// steps is read as `steps` but advertised as `descriptor`, and out-of-order steps derive
    /// an inverted file extent - both have to be rejected on every data path, not just the
    /// ones where a file can be opened.
    #[test]
    fn it_rejects_a_bad_axis_on_external_rows() {
        let (meta_data, tiles) = md_dataset_meta(
            &[MdRow::of("md/variables.nc", "temperature", 0, 8)],
            vec![api_band("temperature")],
        );
        let crate::datasets::storage::MetaDataDefinition::GdalMdMetaData(md) = meta_data else {
            panic!("a probe produces MD metadata");
        };

        let external = |tile: &AddDatasetMdTile| {
            let mut tile = tile.clone();
            // a bare URL is external data; the probe's local path is not
            tile.params.file_path = "/vsicurl/https://example.invalid/variables.nc".into();
            tile
        };

        // `variables.nc` has a uniform daily axis, which has to be advertised as Regular
        let mut tile = external(&tiles[0]);
        tile.time_descriptor = TimeDescriptor {
            bounds: None,
            dimension: TimeDimension::Irregular,
        };
        assert!(matches!(
            validate_md_tile(
                &tile,
                md.wrap,
                &DataPath::External,
                Path::new(""),
                &md.result_descriptor,
                // external rows are the ones whose file is not opened, which is exactly why
                // the axis checks have to be independent of it
                false,
            ),
            Err(AddDatasetMdTilesError::MdTileTimeAxisInconsistent { .. }),
        ));

        // consistent, but reversed: `MdFileTimes::bounds` would take the first and last
        // step and describe the file as covering an inverted window
        let mut tile = external(&tiles[0]);
        tile.time_steps.reverse();
        let times = geoengine_operators::source::MdFileTimes::from_intervals(
            tile.time_steps
                .iter()
                .map(|t| geoengine_datatypes::primitives::TimeInterval::from(*t))
                .collect(),
        );
        tile.time_descriptor = times.descriptor.into();
        assert!(matches!(
            validate_md_tile(
                &tile,
                md.wrap,
                &DataPath::External,
                Path::new(""),
                &md.result_descriptor,
                // external rows are the ones whose file is not opened, which is exactly why
                // the axis checks have to be independent of it
                false,
            ),
            Err(AddDatasetMdTilesError::MdTileTimeAxisUnordered { .. }),
        ));
    }
}
