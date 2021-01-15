use crate::datasets::listing::DataSetListOptions;
use crate::datasets::listing::DataSetProvider;
use crate::datasets::storage::DataSetDB;
use crate::handlers::{authenticate, Context};
use crate::util::user_input::UserInput;
use geoengine_datatypes::dataset::DataSetProviderId;
use uuid::Uuid;
use warp::Filter;

pub(crate) fn list_data_sets_internal_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(warp::path!("datasets" / "internal"))
        .and(authenticate(ctx))
        .and(warp::body::json())
        .and_then(list_data_sets_internal)
}

// TODO: move into handler once async closures are available?
async fn list_data_sets_internal<C: Context>(
    ctx: C,
    options: DataSetListOptions,
) -> Result<impl warp::Reply, warp::Rejection> {
    let data_sets = ctx
        .data_set_db_ref()
        .await
        .list(
            ctx.session().expect("authenticated").user.id,
            options.validated()?,
        )
        .await?;
    Ok(warp::reply::json(&data_sets))
}

pub(crate) fn list_data_sets_external_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(warp::path!("datasets" / "external" / Uuid).map(DataSetProviderId))
        .and(authenticate(ctx))
        .and(warp::body::json())
        .and_then(list_data_sets_external)
}

// TODO: move into handler once async closures are available?
async fn list_data_sets_external<C: Context>(
    provider: DataSetProviderId,
    ctx: C,
    options: DataSetListOptions,
) -> Result<impl warp::Reply, warp::Rejection> {
    let user_id = ctx.session().expect("authenticated").user.id;
    let data_sets = ctx
        .data_set_db_ref()
        .await
        .data_set_provider(user_id, provider)
        .await?
        .list(user_id, options.validated()?)
        .await?;
    Ok(warp::reply::json(&data_sets))
}

// TODO: upload data

// TODO: import data set (stage, import, unstage)

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::InMemoryContext;
    use crate::datasets::listing::{DataSetListing, OrderBy};
    use crate::users::user::{UserCredentials, UserRegistration};
    use crate::users::userdb::UserDB;

    #[tokio::test]
    async fn list() {
        let ctx = InMemoryContext::default();

        ctx.user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo@bar.de".to_string(),
                    password: "secret123".to_string(),
                    real_name: "Foo Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let session = ctx
            .user_db()
            .write()
            .await
            .login(UserCredentials {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
            })
            .await
            .unwrap();

        let options = DataSetListOptions {
            filter: None,
            order: OrderBy::NameAsc,
            offset: 0,
            limit: 5,
        };

        let res = warp::test::request()
            .method("GET")
            .path("/datasets/internal")
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id.to_string()),
            )
            .json(&options)
            .reply(&list_data_sets_internal_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        let projects = serde_json::from_str::<Vec<DataSetListing>>(&body).unwrap();

        assert_eq!(projects.len(), 1);
        assert_eq!(projects[0].name, "Ports");
        assert_eq!(projects[0].source_operator, "OgrSource");
    }
}
