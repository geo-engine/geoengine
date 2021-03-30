use crate::contexts::Context;
use crate::datasets::storage::DataSetStore;
use crate::datasets::{
    listing::DataSetProvider,
    storage::{CreateDataSet, MetaDataDefinition},
    upload::Upload,
};
use crate::error::Result;
use crate::handlers::authenticate;
use crate::users::session::Session;
use crate::util::user_input::UserInput;
use crate::{
    datasets::{listing::DataSetListOptions, upload::UploadDb},
    util::IdResponse,
};
use warp::Filter;

pub(crate) fn list_datasets_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path("datasets")
        .and(warp::get())
        .and(authenticate(ctx.clone()))
        .and(warp::any().map(move || ctx.clone()))
        .and(warp::query())
        .and_then(list_datasets)
}

// TODO: move into handler once async closures are available?
async fn list_datasets<C: Context>(
    session: Session,
    ctx: C,
    options: DataSetListOptions,
) -> Result<impl warp::Reply, warp::Rejection> {
    let options = options.validated()?;
    let list = ctx
        .data_set_db_ref()
        .await
        .list(session.user.id, options)
        .await?;
    Ok(warp::reply::json(&list))
}

pub(crate) fn create_dataset_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path("dataset")
        .and(warp::post())
        .and(authenticate(ctx.clone()))
        .and(warp::any().map(move || ctx.clone()))
        .and(warp::body::json())
        .and_then(create_dataset)
}

// TODO: move into handler once async closures are available?
async fn create_dataset<C: Context>(
    session: Session,
    ctx: C,
    create: CreateDataSet,
) -> Result<impl warp::Reply, warp::Rejection> {
    let upload = ctx
        .data_set_db_ref()
        .await
        .get_upload(session.user.id, create.upload)
        .await?;

    let mut definition = create.definition;

    adjust_user_path_to_upload_path(&mut definition.meta_data, &upload)?;

    let mut db = ctx.data_set_db_ref_mut().await;
    let meta_data = db.wrap_meta_data(definition.meta_data);
    let id = db
        .add_data_set(
            session.user.id,
            definition.properties.validated()?,
            meta_data,
        )
        .await?;

    Ok(warp::reply::json(&IdResponse::from(id)))
}

fn adjust_user_path_to_upload_path(meta: &mut MetaDataDefinition, upload: &Upload) -> Result<()> {
    match meta {
        crate::datasets::storage::MetaDataDefinition::MockMetaData(_) => {}
        crate::datasets::storage::MetaDataDefinition::OgrMetaData(m) => {
            m.loading_info.file_name = upload.adjust_file_path(&m.loading_info.file_name)?
        }
        crate::datasets::storage::MetaDataDefinition::GdalMetaDataRegular(m) => {
            m.params.file_path = upload.adjust_file_path(&m.params.file_path)?
        }
        crate::datasets::storage::MetaDataDefinition::GdalStatic(m) => {
            m.params.file_path = upload.adjust_file_path(&m.params.file_path)?
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::InMemoryContext;
    use crate::datasets::storage::{AddDataSet, DataSetStore};
    use crate::error::Result;
    use crate::users::user::UserId;
    use crate::util::tests::create_session_helper;
    use crate::util::Identifier;
    use geoengine_datatypes::collections::VectorDataType;
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
    use geoengine_operators::engine::{StaticMetaData, VectorResultDescriptor};
    use geoengine_operators::source::{OgrSourceDataset, OgrSourceErrorSpec};
    use serde_json::json;

    #[tokio::test]
    async fn test_list_datasets() -> Result<()> {
        let ctx = InMemoryContext::default();

        let session = create_session_helper(&ctx).await;

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
        };

        let ds = AddDataSet {
            id: None,
            name: "OgrDataSet".to_string(),
            description: "My Ogr data set".to_string(),
            source_operator: "OgrSource".to_string(),
        };

        let meta = StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: Default::default(),
                layer_name: "".to_string(),
                data_type: None,
                time: Default::default(),
                columns: None,
                default_geometry: None,
                force_ogr_time_filter: false,
                on_error: OgrSourceErrorSpec::Skip,
                provenance: None,
            },
            result_descriptor: descriptor,
        };

        let id = ctx
            .data_set_db_ref_mut()
            .await
            .add_data_set(UserId::new(), ds.validated()?, Box::new(meta))
            .await?;

        let res = warp::test::request()
            .method("GET")
            .path(&format!(
                "/datasets?{}",
                &serde_urlencoded::to_string([
                    ("order", "NameDesc"),
                    ("offset", "0"),
                    ("limit", "2"),
                ])
                .unwrap()
            ))
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .reply(&list_datasets_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();

        assert_eq!(
            body,
            json!([{
                "id": {
                    "Internal": id.internal().unwrap()
                },
                "name": "OgrDataSet",
                "description": "My Ogr data set",
                "tags": [],
                "source_operator": "OgrSource",
                "result_descriptor": {
                    "Vector": {
                        "data_type": "Data",
                        "spatial_reference": "",
                        "columns": {}
                    }
                }
            }])
            .to_string()
        );

        Ok(())
    }

    #[tokio::test]
    async fn create_data_set() {
        let ctx = InMemoryContext::default();

        let session = create_session_helper(&ctx).await;

        let s = r#"{
            "upload": "1f7e3e75-4d20-4c91-9497-7f4df7604b62",
            "definition": {
                "properties": {
                    "id": null,
                    "name": "Uploaded Natural Earth 10m Ports",
                    "description": "Ports from Natural Earth",
                    "source_operator": "OgrSource"
                },
                "meta_data": {
                    "OgrMetaData": {
                        "loading_info": {
                            "file_name": "operators/test-data/vector/data/ne_10m_ports/ne_10m_ports.shp",
                            "layer_name": "ne_10m_ports",
                            "data_type": "MultiPoint",
                            "time": "none",
                            "columns": {
                                "x": "",
                                "y": null,
                                "numeric": ["natlscale"],
                                "decimal": ["scalerank"],
                                "textual": ["featurecla", "name", "website"]
                            },
                            "default_geometry": null,
                            "force_ogr_time_filter": false,
                            "on_error": "skip",
                            "provenance": null
                        },
                        "result_descriptor": {
                            "data_type": "MultiPoint",
                            "spatial_reference": "EPSG:4326",
                            "columns": {
                                "website": "Text",
                                "name": "Text",
                                "natlscale": "Number",
                                "scalerank": "Decimal",
                                "featurecla": "Text"
                            }
                        }
                    }
                }
            }
        }"#;

        let res = warp::test::request()
            .method("POST")
            .path("/dataset")
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .body(s)
            .reply(&create_dataset_handler(ctx))
            .await;

        assert_eq!(res.status(), 500);

        // TODO: add a success test case once it is clear how to upload data from within a test
    }
}
