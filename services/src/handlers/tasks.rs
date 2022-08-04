use crate::error::Result;
use crate::tasks::{TaskListOptions, TaskManager};
use crate::util::user_input::UserInput;
use crate::{contexts::Context, tasks::TaskId};
use actix_web::{web, FromRequest, HttpResponse, Responder};
use serde::{Deserialize, Serialize};

pub(crate) fn init_task_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: Context,
    C::Session: FromRequest,
{
    cfg.service(
        web::scope("/tasks")
            .service(web::resource("/list").route(web::get().to(list_handler::<C>)))
            .service(
                web::scope("/{task_id}")
                    .service(web::resource("/status").route(web::get().to(status_handler::<C>)))
                    .service(web::resource("/abort").route(web::get().to(abort_handler::<C>))),
            ),
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

/// Retrieve the status of a task.
///
/// # Example
///
/// ```text
/// GET /tasks/420b06de-0a7e-45cb-9c1c-ea901b46ab69/abort
/// Authorization: Bearer 4f0d02f9-68e8-46fb-9362-80f862b7db54
/// ```
///
/// Response 1:
///
/// 200 OK
async fn abort_handler<C: Context>(
    _session: C::Session, // TODO: incorporate
    ctx: web::Data<C>,
    task_id: web::Path<TaskId>,
) -> Result<impl Responder> {
    let task_id = task_id.into_inner();

    ctx.tasks_ref().abort(task_id).await?;

    Ok(HttpResponse::Ok())
}

#[cfg(test)]
mod tests {

    use super::*;

    use crate::{
        contexts::{InMemoryContext, Session, SimpleContext},
        tasks::{
            util::test::wait_for_task_to_finish, Task, TaskContext, TaskStatus, TaskStatusInfo,
        },
        util::tests::send_test_request,
    };
    use actix_http::header;
    use actix_web_httpauth::headers::authorization::Bearer;
    use futures::{channel::oneshot, lock::Mutex};
    use geoengine_datatypes::{error::ErrorSource, util::test::TestDefault};
    use std::{pin::Pin, sync::Arc};

    struct NopTask {
        complete_rx: Arc<Mutex<oneshot::Receiver<()>>>,
    }

    impl NopTask {
        pub fn new_with_sender() -> (Self, oneshot::Sender<()>) {
            let (complete_tx, complete_rx) = oneshot::channel();

            let this = Self {
                complete_rx: Arc::new(Mutex::new(complete_rx)),
            };

            (this, complete_tx)
        }
    }

    #[async_trait::async_trait]
    impl<C: TaskContext + 'static> Task<C> for NopTask {
        async fn run(&self, _ctx: C) -> Result<Box<dyn TaskStatusInfo>, Box<dyn ErrorSource>> {
            let mut complete_rx_lock = self.complete_rx.lock().await;
            let pinned_receiver: Pin<&mut oneshot::Receiver<()>> = Pin::new(&mut complete_rx_lock);

            pinned_receiver.await.unwrap();

            Ok("completed".to_string().boxed())
        }

        async fn cleanup_on_error(&self, ctx: C) -> Result<(), Box<dyn ErrorSource>> {
            todo!()
        }

        fn task_type(&self) -> &'static str {
            "nopTask"
        }

        fn task_unique_id(&self) -> Option<String> {
            Some("highlander task".to_string())
        }
    }

    #[tokio::test]
    async fn test_get_status() {
        let ctx = InMemoryContext::test_default();
        let session_id = ctx.default_session_ref().await.id();

        let (task, complete_tx) = NopTask::new_with_sender();
        let task_id = ctx.tasks_ref().schedule(task.boxed(), None).await.unwrap();

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

        let (task, complete_tx) = NopTask::new_with_sender();
        let task_id = ctx.tasks_ref().schedule(task.boxed(), None).await.unwrap();

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
    async fn test_abort_task() {
        let ctx = InMemoryContext::test_default();
        let session_id = ctx.default_session_ref().await.id();

        // 1. Create task

        let (task, _complete_tx) = NopTask::new_with_sender();
        let task_id = ctx.tasks_ref().schedule(task.boxed(), None).await.unwrap();

        // 2. Abort task

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/tasks/{task_id}/abort"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200, "{:?}", res.response().error());

        // 3. check status

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/tasks/{task_id}/status"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200, "{:?}", res.response().error());

        let status: serde_json::Value = actix_web::test::read_body_json(res).await;

        assert_eq!(
            status,
            serde_json::json!({
                "status": "failed",
                "error": format!("Task was aborted by the user: {task_id}"),
            })
        );
    }

    // TODO: test abort after finish

    #[tokio::test]
    async fn test_duplicate() {
        let ctx = InMemoryContext::test_default();

        let (task, complete_tx) = NopTask::new_with_sender();
        ctx.tasks_ref().schedule(task.boxed(), None).await.unwrap();

        let (task, _) = NopTask::new_with_sender();
        assert!(ctx.tasks_ref().schedule(task.boxed(), None).await.is_err());

        complete_tx.send(()).unwrap();
    }

    #[tokio::test]
    async fn test_duplicate_after_finish() {
        let ctx = InMemoryContext::test_default();

        // 1. start first task

        let (task, complete_tx) = NopTask::new_with_sender();
        let task_id = ctx.tasks_ref().schedule(task.boxed(), None).await.unwrap();

        // 2. wait for task to finish

        complete_tx.send(()).unwrap();

        wait_for_task_to_finish(ctx.tasks(), task_id).await;

        // 3. start second task

        let (task, complete_tx) = NopTask::new_with_sender();
        assert!(ctx.tasks_ref().schedule(task.boxed(), None).await.is_ok());

        complete_tx.send(()).unwrap();
    }

    #[tokio::test]
    async fn test_notify() {
        let ctx = InMemoryContext::test_default();

        // 1. start first task

        let (schedule_complete_tx, schedule_complete_rx) = oneshot::channel();
        let (task, complete_tx) = NopTask::new_with_sender();

        ctx.tasks_ref()
            .schedule(task.boxed(), Some(schedule_complete_tx))
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
