use crate::contexts::ApplicationContext;
use crate::error::Result;
use crate::tasks::{TaskListOptions, TaskManager};
use crate::util::user_input::UserInput;
use crate::{contexts::SessionContext, tasks::TaskId};
use actix_web::{web, FromRequest, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

pub(crate) fn init_task_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ApplicationContext,
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
                "pctComplete": 0,
                "timeStarted": "2023-02-16T15:25:45.390Z",
                "estimatedTimeRemaining": "? (± ?)",
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
async fn status_handler<C: ApplicationContext>(
    session: C::Session, // TODO: check for session auth
    app_ctx: web::Data<C>,
    task_id: web::Path<TaskId>,
) -> Result<impl Responder> {
    let task_id = task_id.into_inner();

    let task = app_ctx
        .session_context(session)
        .tasks()
        .get_task_status(task_id)
        .await?;

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
                    "taskId": "420b06de-0a7e-45cb-9c1c-ea901b46ab69",
                    "status": "completed",
                    "info": "completed",
                    "timeTotal": "00:00:30",
                    "timeStarted": "2023-02-16T15:25:45.390Z"
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
async fn list_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    task_list_options: web::Query<TaskListOptions>,
) -> Result<impl Responder> {
    let task_list_options = task_list_options.into_inner().validated()?;

    let task = app_ctx
        .session_context(session)
        .tasks()
        .list_tasks(task_list_options)
        .await?;

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
async fn abort_handler<C: ApplicationContext>(
    session: C::Session, // TODO: check for session auth
    app_ctx: web::Data<C>,
    task_id: web::Path<TaskId>,
    options: web::Query<TaskAbortOptions>,
) -> Result<impl Responder> {
    let task_id = task_id.into_inner();

    app_ctx
        .session_context(session)
        .tasks()
        .abort_tasks(task_id, options.force)
        .await?;

    Ok(HttpResponse::Ok())
}

#[cfg(test)]
mod tests {

    use super::*;

    use crate::util::tests::read_body_json;
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
    use serde_json::json;
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
                    .schedule_task(subtask, None)
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
        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;
        let session = app_ctx.default_session_ref().await.clone();
        let session_id = session.id();

        let (task, complete_tx) = NopTask::new_with_sender();

        let tasks = Arc::new(ctx.tasks());
        let task_id = tasks.schedule_task(task.boxed(), None).await.unwrap();

        // 1. initially, we should get a running status

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/tasks/{task_id}/status"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx.clone()).await;

        let res_status = res.status();
        let res_body = read_body_json(res).await;
        assert_eq!(res_status, 200, "{res_body:?}");

        assert_eq!(res_body["status"], json!("running"));
        assert_eq!(res_body["pctComplete"], json!("0.00%"));
        assert!(res_body["info"].is_null());
        assert_eq!(res_body["estimatedTimeRemaining"], json!("? (± ?)"));
        assert!(res_body["timeStarted"].is_string());

        // 2. wait for task to finish

        complete_tx.send(()).unwrap();

        wait_for_task_to_finish(tasks, task_id).await;

        // 3. finally, it should complete

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/tasks/{task_id}/status"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let app_ctx_clone = app_ctx.clone();

        let res = send_test_request(req, app_ctx_clone).await;

        let res_status = res.status();
        let res_body = read_body_json(res).await;
        assert_eq!(res_status, 200, "{res_body:?}");

        assert_eq!(res_body["status"], json!("completed"));
        assert_eq!(res_body["info"], json!("completed"));
        assert_eq!(res_body["timeTotal"], json!("00:00:00"));
        assert!(res_body["timeStarted"].is_string());
    }

    #[tokio::test]
    async fn test_get_status_with_admin_session() {
        crate::util::config::set_config(
            "session.admin_session_token",
            "8aca8875-425a-4ef1-8ee6-cdfc62dd7525",
        )
        .unwrap();

        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;
        let session = app_ctx.default_session_ref().await.clone();
        let session_id = session.id();

        let (task, _complete_tx) = NopTask::new_with_sender();
        let task_id = ctx.tasks().schedule_task(task.boxed(), None).await.unwrap();

        // 1. initially, we should get a running status

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/tasks/{task_id}/status"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx.clone()).await;

        let res_status = res.status();
        let res_body = read_body_json(res).await;
        assert_eq!(res_status, 200, "{res_body:?}");

        assert_eq!(res_body["status"], json!("running"));
        assert_eq!(res_body["pctComplete"], json!("0.00%"));
        assert!(res_body["info"].is_null());
        assert_eq!(res_body["estimatedTimeRemaining"], json!("? (± ?)"));
        assert!(res_body["timeStarted"].is_string());
    }

    #[tokio::test]
    async fn test_get_list() {
        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;
        let session = app_ctx.default_session_ref().await.clone();
        let session_id = session.id();

        let tasks = Arc::new(ctx.tasks());

        let (task, complete_tx) = NopTask::new_with_sender();
        let task_id = tasks.schedule_task(task.boxed(), None).await.unwrap();

        complete_tx.send(()).unwrap();

        wait_for_task_to_finish(tasks, task_id).await;

        let req = actix_web::test::TestRequest::get()
            .uri("/tasks/list")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx.clone()).await;

        let res_status = res.status();
        let res_body = read_body_json(res).await;
        assert_eq!(res_status, 200, "{res_body:?}");

        let res_body = res_body.get(0).unwrap();
        assert_eq!(res_body["taskId"], json!(task_id));
        assert_eq!(res_body["status"], json!("completed"));
        assert_eq!(res_body["info"], json!("completed"));
        assert_eq!(res_body["timeTotal"], json!("00:00:00"));
        assert!(res_body["timeStarted"].is_string());
    }

    #[tokio::test]
    async fn test_abort_task() {
        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;
        let session = app_ctx.default_session_ref().await.clone();
        let session_id = session.id();

        let tasks = Arc::new(ctx.tasks());

        // 1. Create task

        let (task, complete_tx) = NopTask::new_with_sender();
        let task_id = tasks.schedule_task(task.boxed(), None).await.unwrap();

        // 2. Abort task

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/tasks/{task_id}/abort"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200, "{:?}", res.response().error());

        // 3. Wait for abortion to complete

        complete_tx.send(()).unwrap();

        wait_for_task_to_finish(tasks, task_id).await;

        // 4. check status

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/tasks/{task_id}/status"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200, "{:?}", res.response().error());

        let status = read_body_json(res).await;

        assert_eq!(
            status,
            json!({
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
        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;
        let session = app_ctx.default_session_ref().await.clone();
        let session_id = session.id();

        let tasks = Arc::new(ctx.tasks());

        // 1. Create task

        let (task, _complete_tx) = NopTask::new_with_sender();
        let task_id = tasks.schedule_task(task.boxed(), None).await.unwrap();

        // 2. Abort task

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/tasks/{task_id}/abort?force=true"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200, "{:?}", res.response().error());

        // 3. Wait for abortion to complete

        wait_for_task_to_finish(tasks, task_id).await;

        // 4. check status

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/tasks/{task_id}/status"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200, "{:?}", res.response().error());

        let status = read_body_json(res).await;

        assert_eq!(
            status,
            json!({
                "status": "aborted",
                "cleanUp": {
                    "status": "noCleanUp"
                },
            })
        );
    }

    #[tokio::test]
    async fn test_abort_after_abort() {
        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;
        let session = app_ctx.default_session_ref().await.clone();
        let session_id = session.id();

        let tasks = Arc::new(ctx.tasks());

        // 1. Create task

        let (task, _complete_tx) = NopTask::new_with_sender();
        let task_id = tasks.schedule_task(task.boxed(), None).await.unwrap();

        // 2. Abort task

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/tasks/{task_id}/abort"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200, "{:?}", res.response().error());

        // do not call `_complete_tx`

        // 3. Abort again without force

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/tasks/{task_id}/abort"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 400, "{:?}", res.response().error());

        let status = read_body_json(res).await;

        assert_eq!(
            status,
            json!({
                "error": "TaskError",
                "message": format!("TaskError: Task was already aborted by the user: {task_id}"),
            })
        );

        // 5. Abort again with force

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/tasks/{task_id}/abort?force=true"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200, "{:?}", res.response().error());

        // 6. Check abortion status

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/tasks/{task_id}/status"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200, "{:?}", res.response().error());

        let status = read_body_json(res).await;

        assert_eq!(
            status,
            json!({
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
        let app_ctx = InMemoryContext::test_default();

        let session = app_ctx.default_session_ref().await.clone();

        let tasks = app_ctx.session_context(session).tasks();

        let (task, complete_tx) = NopTask::new_with_sender_and_unique_id(unique_id.clone());
        tasks.schedule_task(task.boxed(), None).await.unwrap();

        let (task, _) = NopTask::new_with_sender_and_unique_id(unique_id.clone());
        assert!(tasks.schedule_task(task.boxed(), None).await.is_err());

        complete_tx.send(()).unwrap();
    }

    #[tokio::test]
    async fn test_duplicate_after_finish() {
        let unique_id = "highlander".to_string();
        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;

        let tasks = Arc::new(ctx.tasks());

        // 1. start first task

        let (task, complete_tx) = NopTask::new_with_sender_and_unique_id(unique_id.clone());
        let task_id = tasks.schedule_task(task.boxed(), None).await.unwrap();

        // 2. wait for task to finish

        complete_tx.send(()).unwrap();

        wait_for_task_to_finish(tasks.clone(), task_id).await;

        // 3. start second task

        let (task, complete_tx) = NopTask::new_with_sender_and_unique_id(unique_id.clone());
        assert!(tasks.schedule_task(task.boxed(), None).await.is_ok());

        complete_tx.send(()).unwrap();
    }

    #[tokio::test]
    async fn test_notify() {
        let app_ctx = InMemoryContext::test_default();

        let session = app_ctx.default_session_ref().await.clone();

        let tasks = app_ctx.session_context(session).tasks();

        // 1. start first task

        let (schedule_complete_tx, schedule_complete_rx) = oneshot::channel();
        let (task, complete_tx) = NopTask::new_with_sender();

        tasks
            .schedule_task(task.boxed(), Some(schedule_complete_tx))
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
        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;

        let tasks = Arc::new(ctx.tasks());

        // 1. start super task

        let (subtask_a, complete_tx_a) = NopTask::new_with_sender();
        let (subtask_b, complete_tx_b) = NopTask::new_with_sender();

        let (task, _complete_tx) =
            TaskTree::new_with_sender(vec![subtask_a.boxed(), subtask_b.boxed()], tasks.clone());

        let task_id = tasks.schedule_task(task.boxed(), None).await.unwrap();

        // 2. wait for all subtasks to schedule

        let all_task_ids: Vec<TaskId> =
            geoengine_operators::util::retry::retry(5, 100, 2., None, || {
                let task_manager = tasks.clone();
                async move {
                    let task_list = task_manager
                        .list_tasks(
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

        tasks.abort_tasks(task_id, false).await.unwrap();

        // allow clean-up to complete
        complete_tx_a.send(()).unwrap();
        complete_tx_b.send(()).unwrap();

        // 4. wait for completion

        for task_id in all_task_ids {
            wait_for_task_to_finish(tasks.clone(), task_id).await;
        }

        // 5. check results

        let list = tasks
            .list_tasks(
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
            json!({
                "status": "aborted",
                "cleanUp": {"status": "completed", "info": null}
            })
        );
        assert_eq!(
            serde_json::to_value(&list[1].status).unwrap(),
            json!({
                "status": "aborted",
                "cleanUp": {"status": "completed", "info": null}
            })
        );
        assert_eq!(
            serde_json::to_value(&list[2].status).unwrap(),
            json!({
                "status": "aborted",
                "cleanUp": {"status": "completed", "info": null}
            })
        );
    }

    #[tokio::test]
    async fn test_failing_task_with_failing_cleanup() {
        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;

        let tasks = Arc::new(ctx.tasks());

        // 1. start task

        let task_id = tasks
            .schedule_task(FailingTaskWithFailingCleanup.boxed(), None)
            .await
            .unwrap();

        // 2. wait for completion

        wait_for_task_to_finish(tasks.clone(), task_id).await;

        // 3. check results

        assert_eq!(
            serde_json::to_value(tasks.get_task_status(task_id).await.unwrap()).unwrap(),
            json!({
                "status": "failed",
                "error": "FailingTaskWithFailingCleanupError",
                "cleanUp": {"status": "failed", "error": "FailingTaskWithFailingCleanupError"}
            })
        );
    }
}
