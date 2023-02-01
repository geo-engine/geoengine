use crate::contexts::AdminSession;
use crate::error::Result;
use crate::tasks::{TaskListOptions, TaskManager};
use crate::util::user_input::UserInput;
use crate::{contexts::Context, tasks::TaskId};
use actix_web::{web, Either, FromRequest, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

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
#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
pub struct TaskResponse {
    pub task_id: TaskId,
}

impl TaskResponse {
    pub fn new(task_id: TaskId) -> Self {
        Self { task_id }
    }
}

/// Retrieve the status of a task.
#[utoipa::path(
    tag = "Tasks",
    get,
    path = "/tasks/{id}/status",
    responses(
        (status = 200, description = "Status of the task (running)", body = TaskStatus,
            example = json!({
                "status": "running",
                "pct_complete": 0,
                "info": (),
            })
        )
    ),
    params(
        ("id" = TaskId, description = "Task id")
    ),
    security(
        ("session_token" = [])
    )
)]
async fn status_handler<C: Context>(
    _session: Either<AdminSession, C::Session>, // TODO: check for session auth
    ctx: web::Data<C>,
    task_id: web::Path<TaskId>,
) -> Result<impl Responder> {
    let task_id = task_id.into_inner();

    let task = ctx.tasks_ref().status(task_id).await?;

    Ok(web::Json(task))
}

/// Retrieve the status of all tasks.
#[utoipa::path(
    tag = "Tasks",
    get,
    path = "/tasks/list",
    responses(
        (status = 200, description = "Status of all tasks", body = TaskStatus,
            example = json!([
                {
                    "task_id": "420b06de-0a7e-45cb-9c1c-ea901b46ab69",
                    "status": "completed",
                    "info": "completed",
                }
            ])
        )
    ),
    params(
        TaskListOptions
    ),
    security(
        ("session_token" = [])
    )
)]
async fn list_handler<C: Context>(
    _session: Either<AdminSession, C::Session>, // TODO: check for session auth
    ctx: web::Data<C>,
    task_list_options: web::Query<TaskListOptions>,
) -> Result<impl Responder> {
    let task_list_options = task_list_options.into_inner().validated()?;

    let task = ctx.tasks_ref().list(task_list_options).await?;

    Ok(web::Json(task))
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, IntoParams)]
pub struct TaskAbortOptions {
    #[serde(default)]
    pub force: bool,
}

/// Abort a running task.
///
/// # Parameters
///
/// * `force` - If true, the task will be aborted without clean-up.
///             You can abort a task that is already in the process of aborting.
#[utoipa::path(
    tag = "Tasks",
    get,
    path = "/tasks/{id}/abort",
    responses(
        (status = 200, description = "Task successfully aborted.")
    ),
    params(
        TaskAbortOptions,
        ("id" = TaskId, description = "Task id")
    ),
    security(
        ("session_token" = [])
    )
)]
async fn abort_handler<C: Context>(
    _session: Either<AdminSession, C::Session>, // TODO: check for session auth
    ctx: web::Data<C>,
    task_id: web::Path<TaskId>,
    options: web::Query<TaskAbortOptions>,
) -> Result<impl Responder> {
    let task_id = task_id.into_inner();

    ctx.tasks_ref().abort(task_id, options.force).await?;

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
        unique_id: Option<String>,
    }

    impl NopTask {
        pub fn new_with_sender() -> (Self, oneshot::Sender<()>) {
            let (complete_tx, complete_rx) = oneshot::channel();

            let this = Self {
                complete_rx: Arc::new(Mutex::new(complete_rx)),
                unique_id: None,
            };

            (this, complete_tx)
        }

        pub fn new_with_sender_and_unique_id(unique_id: String) -> (Self, oneshot::Sender<()>) {
            let (complete_tx, complete_rx) = oneshot::channel();

            let this = Self {
                complete_rx: Arc::new(Mutex::new(complete_rx)),
                unique_id: Some(unique_id),
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

        async fn cleanup_on_error(&self, _ctx: C) -> Result<(), Box<dyn ErrorSource>> {
            let mut complete_rx_lock = self.complete_rx.lock().await;
            let pinned_receiver: Pin<&mut oneshot::Receiver<()>> = Pin::new(&mut complete_rx_lock);

            pinned_receiver.await.unwrap();

            Ok(())
        }

        fn task_type(&self) -> &'static str {
            "nopTask"
        }

        fn task_unique_id(&self) -> Option<String> {
            self.unique_id.clone()
        }
    }

    struct TaskTree<T: TaskManager<C>, C: TaskContext + 'static> {
        subtasks: Arc<Mutex<Vec<Box<dyn Task<C>>>>>,
        subtask_ids: Arc<Mutex<Vec<TaskId>>>,
        task_manager: Arc<T>,
        complete_rx: Arc<Mutex<oneshot::Receiver<()>>>,
    }

    impl<T: TaskManager<C>, C: TaskContext + 'static> TaskTree<T, C> {
        pub fn new_with_sender(
            subtasks: Vec<Box<dyn Task<C>>>,
            task_manager: Arc<T>,
        ) -> (Self, oneshot::Sender<()>) {
            let (complete_tx, complete_rx) = oneshot::channel();

            let this = Self {
                subtasks: Arc::new(Mutex::new(subtasks)),
                subtask_ids: Arc::new(Mutex::new(vec![])),
                task_manager,
                complete_rx: Arc::new(Mutex::new(complete_rx)),
            };

            (this, complete_tx)
        }
    }

    #[async_trait::async_trait]
    impl<T: TaskManager<C>, C: TaskContext + 'static> Task<C> for TaskTree<T, C> {
        async fn run(&self, _ctx: C) -> Result<Box<dyn TaskStatusInfo>, Box<dyn ErrorSource>> {
            for subtask in self.subtasks.lock().await.drain(..) {
                let subtask_id = self
                    .task_manager
                    .schedule(subtask, None)
                    .await
                    .map_err(ErrorSource::boxed)?;
                self.subtask_ids.lock().await.push(subtask_id);
            }

            let mut complete_rx_lock = self.complete_rx.lock().await;
            let pinned_receiver: Pin<&mut oneshot::Receiver<()>> = Pin::new(&mut complete_rx_lock);

            pinned_receiver.await.unwrap();

            Ok("completed".to_string().boxed())
        }

        async fn cleanup_on_error(&self, _ctx: C) -> Result<(), Box<dyn ErrorSource>> {
            Ok(())
        }

        fn task_type(&self) -> &'static str {
            stringify!(TaskTree)
        }

        fn task_unique_id(&self) -> Option<String> {
            None
        }

        async fn subtasks(&self) -> Vec<TaskId> {
            self.subtask_ids.lock().await.clone()
        }
    }

    struct FailingTaskWithFailingCleanup;

    #[derive(Debug)]
    struct FailingTaskWithFailingCleanupError;

    impl std::fmt::Display for FailingTaskWithFailingCleanupError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailingTaskWithFailingCleanupError")
        }
    }

    impl std::error::Error for FailingTaskWithFailingCleanupError {}

    #[async_trait::async_trait]
    impl<C: TaskContext + 'static> Task<C> for FailingTaskWithFailingCleanup {
        async fn run(&self, _ctx: C) -> Result<Box<dyn TaskStatusInfo>, Box<dyn ErrorSource>> {
            Err(Box::new(FailingTaskWithFailingCleanupError))
        }

        async fn cleanup_on_error(&self, _ctx: C) -> Result<(), Box<dyn ErrorSource>> {
            Err(Box::new(FailingTaskWithFailingCleanupError))
        }

        fn task_type(&self) -> &'static str {
            "FailingTaskWithFailingCleanup"
        }

        fn task_unique_id(&self) -> Option<String> {
            None
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
                "pct_complete": "0.00%",
                "info": (),
                "time_estimate": "? (± ?)",
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
                "timeTotal": "00:00:00",
            })
        );
    }

    #[tokio::test]
    async fn test_get_status_with_admin_session() {
        crate::util::config::set_config(
            "session.admin_session_token",
            "8aca8875-425a-4ef1-8ee6-cdfc62dd7525",
        )
        .unwrap();

        let ctx = InMemoryContext::test_default();

        let (task, _complete_tx) = NopTask::new_with_sender();
        let task_id = ctx.tasks_ref().schedule(task.boxed(), None).await.unwrap();

        // 1. initially, we should get a running status

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/tasks/{task_id}/status"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((
                header::AUTHORIZATION,
                Bearer::new(AdminSession::default().id().to_string()),
            ));

        let res = send_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200, "{:?}", res.response());

        let status: serde_json::Value = actix_web::test::read_body_json(res).await;
        assert_eq!(
            status,
            serde_json::json!({
                "status": "running",
                "pct_complete": "0.00%",
                "info": (),
                "time_estimate": "? (± ?)",
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
                    "timeTotal": "00:00:00",
                }
            ])
        );
    }

    #[tokio::test]
    async fn test_abort_task() {
        let ctx = InMemoryContext::test_default();
        let session_id = ctx.default_session_ref().await.id();

        // 1. Create task

        let (task, complete_tx) = NopTask::new_with_sender();
        let task_id = ctx.tasks_ref().schedule(task.boxed(), None).await.unwrap();

        // 2. Abort task

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/tasks/{task_id}/abort"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200, "{:?}", res.response().error());

        // 3. Wait for abortion to complete

        complete_tx.send(()).unwrap();

        wait_for_task_to_finish(ctx.tasks(), task_id).await;

        // 4. check status

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
                "status": "aborted",
                "cleanUp": {
                    "status": "completed",
                    "info": null
                },
            })
        );
    }

    #[tokio::test]
    async fn test_force_abort_task() {
        let ctx = InMemoryContext::test_default();
        let session_id = ctx.default_session_ref().await.id();

        // 1. Create task

        let (task, _complete_tx) = NopTask::new_with_sender();
        let task_id = ctx.tasks_ref().schedule(task.boxed(), None).await.unwrap();

        // 2. Abort task

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/tasks/{task_id}/abort?force=true"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200, "{:?}", res.response().error());

        // 3. Wait for abortion to complete

        wait_for_task_to_finish(ctx.tasks(), task_id).await;

        // 4. check status

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
                "status": "aborted",
                "cleanUp": {
                    "status": "noCleanUp"
                },
            })
        );
    }

    #[tokio::test]
    async fn test_abort_after_abort() {
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

        // do not call `_complete_tx`

        // 3. Abort again without force

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/tasks/{task_id}/abort"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 400, "{:?}", res.response().error());

        let status: serde_json::Value = actix_web::test::read_body_json(res).await;

        assert_eq!(
            status,
            serde_json::json!({
                "error": "TaskError",
                "message": format!("TaskError: Task was already aborted by the user: {task_id}"),
            })
        );

        // 5. Abort again with force

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/tasks/{task_id}/abort?force=true"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200, "{:?}", res.response().error());

        // 6. Check abortion status

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
                "status": "aborted",
                "cleanUp": {
                    "status": "noCleanUp"
                },
            })
        );
    }

    #[tokio::test]
    async fn test_duplicate() {
        let unique_id = "highlander".to_string();
        let ctx = InMemoryContext::test_default();

        let (task, complete_tx) = NopTask::new_with_sender_and_unique_id(unique_id.clone());
        ctx.tasks_ref().schedule(task.boxed(), None).await.unwrap();

        let (task, _) = NopTask::new_with_sender_and_unique_id(unique_id.clone());
        assert!(ctx.tasks_ref().schedule(task.boxed(), None).await.is_err());

        complete_tx.send(()).unwrap();
    }

    #[tokio::test]
    async fn test_duplicate_after_finish() {
        let unique_id = "highlander".to_string();
        let ctx = InMemoryContext::test_default();

        // 1. start first task

        let (task, complete_tx) = NopTask::new_with_sender_and_unique_id(unique_id.clone());
        let task_id = ctx.tasks_ref().schedule(task.boxed(), None).await.unwrap();

        // 2. wait for task to finish

        complete_tx.send(()).unwrap();

        wait_for_task_to_finish(ctx.tasks(), task_id).await;

        // 3. start second task

        let (task, complete_tx) = NopTask::new_with_sender_and_unique_id(unique_id.clone());
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
            TaskStatus::Completed { .. }
        ));
    }

    #[tokio::test]
    async fn abort_subtasks() {
        let ctx = InMemoryContext::test_default();

        // 1. start super task

        let (subtask_a, complete_tx_a) = NopTask::new_with_sender();
        let (subtask_b, complete_tx_b) = NopTask::new_with_sender();

        let (task, _complete_tx) =
            TaskTree::new_with_sender(vec![subtask_a.boxed(), subtask_b.boxed()], ctx.tasks());

        let task_id = ctx.tasks_ref().schedule(task.boxed(), None).await.unwrap();

        // 2. wait for all subtasks to schedule

        let all_task_ids: Vec<TaskId> =
            geoengine_operators::util::retry::retry(5, 100, 2., None, || {
                let task_manager = ctx.tasks();
                async move {
                    let task_list = task_manager
                        .list(
                            TaskListOptions {
                                filter: None,
                                offset: 0,
                                limit: 10,
                            }
                            .validated()
                            .unwrap(),
                        )
                        .await
                        .unwrap();

                    if task_list.len() == 3 {
                        Ok(task_list.into_iter().map(|t| t.task_id).collect())
                    } else {
                        Err(())
                    }
                }
            })
            .await
            .unwrap();

        // 3. abort task

        ctx.tasks_ref().abort(task_id, false).await.unwrap();

        // allow clean-up to complete
        complete_tx_a.send(()).unwrap();
        complete_tx_b.send(()).unwrap();

        // 4. wait for completion

        for task_id in all_task_ids {
            wait_for_task_to_finish(ctx.tasks(), task_id).await;
        }

        // 5. check results

        let list = ctx
            .tasks_ref()
            .list(
                TaskListOptions {
                    filter: None,
                    offset: 0,
                    limit: 10,
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(list.len(), 3);
        assert_eq!(
            serde_json::to_value(&list[0].status).unwrap(),
            serde_json::json!({
                "status": "aborted",
                "cleanUp": {"status": "completed", "info": null}
            })
        );
        assert_eq!(
            serde_json::to_value(&list[1].status).unwrap(),
            serde_json::json!({
                "status": "aborted",
                "cleanUp": {"status": "completed", "info": null}
            })
        );
        assert_eq!(
            serde_json::to_value(&list[2].status).unwrap(),
            serde_json::json!({
                "status": "aborted",
                "cleanUp": {"status": "completed", "info": null}
            })
        );
    }

    #[tokio::test]
    async fn test_failing_task_with_failing_cleanup() {
        let ctx = InMemoryContext::test_default();

        // 1. start task

        let task_id = ctx
            .tasks_ref()
            .schedule(FailingTaskWithFailingCleanup.boxed(), None)
            .await
            .unwrap();

        // 2. wait for completion

        wait_for_task_to_finish(ctx.tasks(), task_id).await;

        // 3. check results

        assert_eq!(
            serde_json::to_value(ctx.tasks_ref().status(task_id).await.unwrap()).unwrap(),
            serde_json::json!({
                "status": "failed",
                "error": "FailingTaskWithFailingCleanupError",
                "cleanUp": {"status": "failed", "error": "FailingTaskWithFailingCleanupError"}
            })
        );
    }
}
