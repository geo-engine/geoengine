//! Runs queries end to end (resolve the source, initialize, plan, query, encode) as one task on
//! the processing runtime. Handlers only parse requests, watch the connection and build responses.

use crate::contexts::SessionContext;
use crate::error::Error;
use futures::future::BoxFuture;
use geoengine_operators::util::spawn_query_task;
use std::future::Future;
use tokio::runtime::Handle;

pub mod ogc_tile;
pub mod plot;
pub mod raster_grid;
pub mod stream;
pub mod vector;

/// Runs `work` as a task on the processing runtime of `ctx`. If `cancel` resolves first, e.g. because
/// the client disconnected, the task is aborted and its query context's abort trigger dropped.
pub async fn run_query<C, T, E, F>(ctx: &C, cancel: BoxFuture<'static, ()>, work: F) -> Result<T, E>
where
    C: SessionContext,
    T: Send + 'static,
    E: From<Error> + Send + 'static,
    F: Future<Output = Result<T, E>> + Send + 'static,
{
    run_on(ctx.processing_runtime().clone(), cancel, work).await
}

fn run_on<T, E, F>(
    runtime: Handle,
    cancel: BoxFuture<'static, ()>,
    work: F,
) -> impl Future<Output = Result<T, E>>
where
    T: Send + 'static,
    E: From<Error> + Send + 'static,
    F: Future<Output = Result<T, E>> + Send + 'static,
{
    // boxed, so the handler's future does not embed the whole query
    let work = Box::pin(work);

    async move {
        let task = spawn_query_task(runtime, async move { Ok(work.await) });

        let result = tokio::select! {
            result = task => result,
            () = cancel => Err(geoengine_operators::error::Error::QueryCanceled),
        };

        result.map_err(Error::from)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::{ApplicationContext, PostgresContext};
    use crate::ge_context;
    use crate::users::UserAuth;
    use futures::FutureExt;
    use std::time::Duration;
    use tokio_postgres::NoTls;

    #[ge_context::test]
    async fn it_runs_queries_on_the_processing_runtime_of_the_context(
        app_ctx: PostgresContext<NoTls>,
    ) {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session);
        let processing = ctx.processing_runtime().id();

        // like an actix worker: a current-thread runtime on its own thread
        let (caller, ran_on) = tokio::task::spawn_blocking(move || {
            let caller = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let ran_on = caller
                .block_on(run_query(&ctx, futures::future::pending().boxed(), async {
                    Ok::<_, Error>(Handle::current().id())
                }))
                .unwrap();
            (caller.handle().id(), ran_on)
        })
        .await
        .unwrap();

        assert_eq!(ran_on, processing);
        assert_ne!(ran_on, caller);
    }

    #[test]
    fn it_runs_the_work_on_the_given_runtime() {
        let processing = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("processing")
            .build()
            .unwrap();
        let caller = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        let thread = caller
            .block_on(run_on(
                processing.handle().clone(),
                futures::future::pending().boxed(),
                async { Ok::<_, Error>(std::thread::current().name().map(ToOwned::to_owned)) },
            ))
            .unwrap();

        assert_eq!(thread.as_deref(), Some("processing"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn it_aborts_the_work_when_canceled() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
        let (mut alive_tx, alive_rx) = tokio::sync::oneshot::channel::<()>();

        let work = async move {
            let _alive = alive_rx; // dropped when the task is aborted
            started_tx.send(()).ok();
            futures::future::pending::<Result<(), Error>>().await
        };
        let cancel = async move {
            started_rx.await.ok();
        }
        .boxed();

        let result = run_on(tokio::runtime::Handle::current(), cancel, work).await;

        assert!(matches!(
            result,
            Err(Error::Operator {
                source: geoengine_operators::error::Error::QueryCanceled
            })
        ));
        let closed = tokio::time::timeout(Duration::from_secs(5), alive_tx.closed()).await;
        assert!(closed.is_ok(), "the task was not aborted");
    }
}
