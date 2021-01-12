use crate::datasets::listing::DataSetListOptions;
use crate::datasets::listing::DataSetProvider;
use crate::datasets::storage::DataSetDB;
use crate::handlers::{authenticate, Context};
use crate::util::user_input::UserInput;
use geoengine_datatypes::dataset::DataSetProviderId;
use uuid::Uuid;
use warp::Filter;

pub fn list_data_sets_internal_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
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

pub fn list_data_sets_external_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
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
