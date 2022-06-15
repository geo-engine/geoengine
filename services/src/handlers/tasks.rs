use crate::error::Result;
use crate::tasks::TaskDb;
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

/// Create a task somewhere and respond with a task id and a url to query the task status.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TaskResponse {
    pub task_id: TaskId,
    pub url: String,
}

impl TaskResponse {
    pub fn new(task_id: TaskId) -> Self {
        let web_config = crate::util::config::get_config_element::<crate::util::config::Web>();
        let url = web_config.and_then(|web| {
            let external_address = if let Some(external_address) = web.external_address {
                external_address
            } else {
                return Ok(None);
            };

            external_address
                .join(&format!("tasks/status/{task_id}"))
                .map(|url| Some(url.to_string()))
                .map_err(Into::into)
        });

        let url = match url {
            Ok(Some(external_address)) => external_address,
            Ok(None) => {
                log::warn!("Missing external address in config");

                String::new()
            }
            Err(e) => {
                log::warn!("Could not retrieve external address: {e}");

                String::new()
            }
        };

        Self { task_id, url }
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
/// GET /tasks/status/420b06de-0a7e-45cb-9c1c-ea901b46ab69
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
async fn list_handler<C: Context>(
    _session: C::Session, // TODO: incorporate
    ctx: web::Data<C>,
    task_id: web::Path<TaskId>,
) -> Result<impl Responder> {
    let task_id = task_id.into_inner();

    let task = ctx.tasks_ref().status(task_id).await?;

    Ok(web::Json(task))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        contexts::{InMemoryContext, Session, SimpleContext},
        tasks::{Task, TaskContext, TaskStatusInfo},
        util::tests::send_test_request,
    };
    use actix_http::header;
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::{error::ErrorSource, util::test::TestDefault};

    struct NopTask {}

    #[async_trait::async_trait]
    impl<C: TaskContext + 'static> Task<C> for NopTask {
        async fn run(
            self: Box<Self>,
            _ctx: C,
        ) -> Result<Box<dyn TaskStatusInfo>, Box<dyn ErrorSource>> {
            Ok("completed".to_string().boxed())
        }
    }

    #[tokio::test]
    async fn test_get_status() {
        let ctx = InMemoryContext::test_default();
        let session_id = ctx.default_session_ref().await.id();

        let task_id = ctx.tasks_ref().register(NopTask {}.boxed()).await.unwrap();

        let req = actix_web::test::TestRequest::get()
            .uri(&format!("/tasks/{task_id}/status"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        // initially, we should get a running status
        {
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
        }

        // then, it should complete
        let status = crate::util::retry::retry(3, 100, 2., move || {
            let req = actix_web::test::TestRequest::get()
                .uri(&format!("/tasks/{task_id}/status"))
                .append_header((header::CONTENT_LENGTH, 0))
                .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

            let ctx_clone = ctx.clone();

            async move {
                let res = send_test_request(req, ctx_clone).await;

                assert_eq!(res.status(), 200);

                let status: serde_json::Value = actix_web::test::read_body_json(res).await;

                if let Some(status_str) = status.get("status") {
                    if status_str == "completed" {
                        return Ok(status.clone());
                    }
                }

                Err("not completed yet")
            }
        })
        .await
        .unwrap();

        assert_eq!(
            status,
            serde_json::json!({
                "status": "completed",
                "info": "completed",
            })
        );
    }
}
