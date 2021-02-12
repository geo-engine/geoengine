use crate::contexts::Context;
use crate::datasets::listing::DataSetListOptions;
use crate::datasets::listing::DataSetProvider;
use crate::handlers::authenticate;
use crate::users::session::Session;
use crate::util::user_input::UserInput;
use warp::Filter;

pub(crate) fn list_datasets_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::get()
        .and(warp::path("datasets"))
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
        // TODO: use new tests helpers once they are merged
        let ctx = InMemoryContext::default();

        let session = create_session_helper(&ctx).await;

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
        };

        let ds = AddDataSet {
            name: "OgrDataSet".to_string(),
            description: "My Ogr data set".to_string(),
            source_operator: "OgrSource".to_string(),
            result_descriptor: descriptor.clone().into(),
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
}
