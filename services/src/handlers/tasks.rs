use crate::error::Result;
use crate::tasks::{TaskListOptions, TaskManager};
use crate::util::user_input::UserInput;
use crate::{contexts::Context, tasks::TaskId};
use actix_web::{web, FromRequest, Responder};
use serde::{Deserialize, Serialize};

pub(crate) fn init_task_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: Context,
    C::Session: FromRequest,
{
    cfg.service(
        web::scope("/tasks")
            .service(web::resource("/list").route(web::get().to(list_handler::<C>)))
            .service(web::resource("/{task_id}/status").route(web::get().to(status_handler::<C>))),
    );
}

/// Create a task somewhere and respond with a task id to query the task status.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TaskResponse {
    pub task_id: TaskId,
}

impl TaskResponse {
    pub fn new(task_id: TaskId) -> Self {
        Self { task_id }
    }
}

/// Retrieve the status of a task.
///
/// # Example
///
/// ```text
/// GET /tasks/420b06de-0a7e-45cb-9c1c-ea901b46ab69/status
/// Authorization: Bearer 4f0d02f9-68e8-46fb-9362-80f862b7db54
/// ```
///
/// Response 1:
///
/// ```json
/// {
///     "status": "running",
///     "pct_complete": 0,
///     "info": (),
/// }
///
/// Response 2:
///
/// ```json
/// {
///     "status": "completed",
///     "info": {
///        "code": 42,
///     }
/// }
///
/// Response 3:
///
/// ```json
/// {
///     "status": "failed",
///     "error": "something went wrong",
/// }
/// ```
async fn status_handler<C: Context>(
    _session: C::Session, // TODO: incorporate
    ctx: web::Data<C>,
    task_id: web::Path<TaskId>,
) -> Result<impl Responder> {
    let task_id = task_id.into_inner();

    let task = ctx.tasks_ref().status(task_id).await?;

    Ok(web::Json(task))
}

/// Retrieve the status of a task.
///
/// # Example
///
/// ```text
/// GET /tasks/list?offset=0&limit=10&filter=completed
/// Authorization: Bearer 4f0d02f9-68e8-46fb-9362-80f862b7db54
/// ```
///
/// Response:
///
/// ```json
/// [
///     {
///         "task_id": "420b06de-0a7e-45cb-9c1c-ea901b46ab69",
///         "status": "completed",
///         "info": "completed",
///     }
/// ]
/// ```
///
async fn list_handler<C: Context>(
    _session: C::Session, // TODO: incorporate
    ctx: web::Data<C>,
    task_list_options: web::Query<TaskListOptions>,
) -> Result<impl Responder> {
    let task_list_options = task_list_options.into_inner().validated()?;

    let task = ctx.tasks_ref().list(task_list_options).await?;

    Ok(web::Json(task))
}

#[cfg(test)]
mod tests {

    use super::*;

    use crate::{
        contexts::{InMemoryContext, Session, SimpleContext},
        tasks::{Task, TaskContext, TaskStatus, TaskStatusInfo},
        util::tests::send_test_request,
    };
    use actix_http::header;
    use actix_web_httpauth::headers::authorization::Bearer;
    use futures::channel::oneshot;
    use geoengine_datatypes::{error::ErrorSource, util::test::TestDefault};
    use std::sync::Arc;

    struct NopTask {
        complete_rx: oneshot::Receiver<()>,
    }

    #[async_trait::async_trait]
    impl<C: TaskContext + 'static> Task<C> for NopTask {
        async fn run(
            self: Box<Self>,
            _ctx: C,
        ) -> Result<Box<dyn TaskStatusInfo>, Box<dyn ErrorSource>> {
            self.complete_rx.await.unwrap();
            Ok("completed".to_string().boxed())
        }

        fn task_type(&self) -> &'static str {
            "nopTask"
        }

        fn task_unique_id(&self) -> Option<String> {
            Some("highlander task".to_string())
        }
    }

    async fn wait_for_task_to_finish<C: TaskContext + 'static>(
        task_manager: Arc<impl TaskManager<C>>,
        task_id: TaskId,
    ) {
        crate::util::retry::retry(3, 100, 2., move || {
            let task_manager = task_manager.clone();
            async move {
                let option =
                    (!task_manager.status(task_id).await.unwrap().is_running()).then(|| ());
                option.ok_or(())
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_get_status() {
        let ctx = InMemoryContext::test_default();
        let session_id = ctx.default_session_ref().await.id();

        let (complete_tx, complete_rx) = oneshot::channel();
        let task_id = ctx
            .tasks_ref()
            .schedule(NopTask { complete_rx }.boxed(), None)
            .await
            .unwrap();

        // 1. initially, we should get a running status

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/tasks/{task_id}/status"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200);

        let status: serde_json::Value = actix_web::test::read_body_json(res).await;
        assert_eq!(
            status,
            serde_json::json!({
                "status": "running",
                "pct_complete": 0,
                "info": (),
            })
        );

        // 2. wait for task to finish

        complete_tx.send(()).unwrap();

        wait_for_task_to_finish(ctx.tasks(), task_id).await;

        // 3. finally, it should complete

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/tasks/{task_id}/status"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let ctx_clone = ctx.clone();

        let res = send_test_request(req, ctx_clone).await;

        assert_eq!(res.status(), 200);

        let status: serde_json::Value = actix_web::test::read_body_json(res).await;

        assert_eq!(
            status,
            serde_json::json!({
                "status": "completed",
                "info": "completed",
            })
        );
    }

    #[tokio::test]
    async fn test_get_list() {
        let ctx = InMemoryContext::test_default();
        let session_id = ctx.default_session_ref().await.id();

        let (complete_tx, complete_rx) = oneshot::channel();
        let task_id = ctx
            .tasks_ref()
            .schedule(NopTask { complete_rx }.boxed(), None)
            .await
            .unwrap();

        complete_tx.send(()).unwrap();

        wait_for_task_to_finish(ctx.tasks(), task_id).await;

        let req = actix_web::test::TestRequest::get()
            .uri("/tasks/list")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200);

        let status: serde_json::Value = actix_web::test::read_body_json(res).await;
        assert_eq!(
            status,
            serde_json::json!([
                {
                    "task_id": task_id,
                    "status": "completed",
                    "info": "completed",
                }
            ])
        );
    }

    #[tokio::test]
    async fn test_duplicate() {
        let ctx = InMemoryContext::test_default();

        let (complete_tx, complete_rx) = oneshot::channel();
        ctx.tasks_ref()
            .schedule(NopTask { complete_rx }.boxed(), None)
            .await
            .unwrap();

        let (_, complete_rx) = oneshot::channel();
        assert!(ctx
            .tasks_ref()
            .schedule(NopTask { complete_rx }.boxed(), None)
            .await
            .is_err());

        complete_tx.send(()).unwrap();
    }

    #[tokio::test]
    async fn test_duplicate_after_finish() {
        let ctx = InMemoryContext::test_default();

        // 1. start first task

        let (complete_tx, complete_rx) = oneshot::channel();
        let task_id = ctx
            .tasks_ref()
            .schedule(NopTask { complete_rx }.boxed(), None)
            .await
            .unwrap();

        // 2. wait for task to finish

        complete_tx.send(()).unwrap();

        wait_for_task_to_finish(ctx.tasks(), task_id).await;

        // 3. start second task

        let (complete_tx, complete_rx) = oneshot::channel();
        assert!(ctx
            .tasks_ref()
            .schedule(NopTask { complete_rx }.boxed(), None)
            .await
            .is_ok());

        complete_tx.send(()).unwrap();
    }

    #[tokio::test]
    async fn test_notify() {
        let ctx = InMemoryContext::test_default();

        // 1. start first task

        let (schedule_complete_tx, schedule_complete_rx) = oneshot::channel();
        let (complete_tx, complete_rx) = oneshot::channel();

        ctx.tasks_ref()
            .schedule(NopTask { complete_rx }.boxed(), Some(schedule_complete_tx))
            .await
            .unwrap();

        // finish task
        complete_tx.send(()).unwrap();

        // wait for completion notification

        assert!(matches!(
            schedule_complete_rx.await.unwrap(),
            TaskStatus::Completed { info: _ }
        ));
    }
}
