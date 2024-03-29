use std::sync::Arc;

use actix_web::{web, FromRequest};
use snafu::ensure;

use crate::api::handlers::tasks::TaskResponse;
use crate::contexts::{ApplicationContext, SessionContext};
use crate::error::Result;
use crate::pro::contexts::{ProApplicationContext, ProGeoEngineDb};
use crate::pro::machine_learning::ml_model::MlModelDb;
use crate::pro::machine_learning::{schedule_ml_model_training_task, MLTrainRequest};

pub(crate) fn init_ml_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ProApplicationContext,
    <<C as ApplicationContext>::SessionContext as SessionContext>::GeoEngineDB: ProGeoEngineDb,
    C::Session: FromRequest,
{
    cfg.service(
        web::resource("/ml/train").route(web::post().to(ml_model_from_workflow_handler::<C>)),
    );
}
/// Schedule a machine learning model training process from a specified request.
/// The request contains the relevant information to get the training process started.
#[utoipa::path(
    tag = "Machine Learning",
    post,
    path = "/ml/train",
    request_body = MLTrainRequest,
    responses(
        (
            status = 200, description = "Model training from workflows", body = TaskResponse,
            example = json!({"taskId": "7f8a4cfe-76ab-4972-b347-b197e5ef0f3c"})
        )
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn ml_model_from_workflow_handler<C: ProApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    info: web::Json<MLTrainRequest>,
) -> Result<web::Json<TaskResponse>>
where
    <<C as ApplicationContext>::SessionContext as SessionContext>::GeoEngineDB: MlModelDb,
{
    ensure!(session.is_admin(), crate::error::AccessDenied);

    let ctx = Arc::new(app_ctx.session_context(session));

    let task_id = schedule_ml_model_training_task(ctx, info.into_inner()).await?;

    Ok(web::Json(TaskResponse::new(task_id)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::primitives::{
        BandSelection, CacheHint, ColumnSelection, RasterQueryRectangle,
    };
    use geoengine_operators::engine::{MultipleRasterSources, RasterBandDescriptors};
    use geoengine_operators::{
        engine::QueryProcessor,
        pro::machine_learning::xgboost::{XgboostOperator, XgboostParams},
    };
    use tokio_postgres::NoTls;

    use crate::pro::contexts::ProPostgresContext;
    use crate::pro::ge_context;
    use crate::pro::machine_learning::ModelType::XGBoost;
    use crate::pro::machine_learning::{
        MachineLearningAggregator, TrainingParams, XgboostTrainingParams,
    };
    use crate::pro::users::UserCredentials;
    use crate::tasks::util::test::wait_for_task_to_finish;
    use crate::tasks::TaskStatus;
    use crate::util::tests::read_body_string;
    use crate::workflows::workflow::Workflow;
    use actix_web::{http::header, test};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::primitives::TimeInterval;
    use geoengine_datatypes::raster::{
        GeoTransform, GridBoundingBox2D, GridShape, RasterDataType, TilingSpecification,
    };
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_operators::engine::ExecutionContext;
    use geoengine_operators::engine::SourceOperator;
    use geoengine_operators::engine::{RasterOperator, RasterResultDescriptor};
    use geoengine_operators::mock::{MockRasterSource, MockRasterSourceParams};
    use geoengine_operators::pro::machine_learning::MlModelAccess;
    use geoengine_operators::util::Result;
    use std::sync::Arc;

    use {
        crate::pro::machine_learning::MachineLearningModelFromWorkflowResult,
        geoengine_datatypes::primitives::{BoundingBox2D, VectorQueryRectangle},
        geoengine_operators::engine::WorkflowOperatorPath,
        std::collections::HashMap,
        std::path::PathBuf,
        tokio::{fs::File, io::AsyncReadExt},
    };

    use crate::{
        pro::{users::UserAuth, util::tests::send_pro_test_request},
        tasks::TaskManager,
    };

    use futures::StreamExt;
    use geoengine_datatypes::raster::{Grid2D, MaskedGrid2D, RasterTile2D, TileInformation};

    /// Convenience function to create a `MockRasterSource`
    fn generate_raster_test_data_band_helper(
        data: Vec<Vec<i32>>,
        tile_size_in_pixels: GridShape<[usize; 2]>,
    ) -> SourceOperator<MockRasterSourceParams<i32>> {
        let n_pixels = data
            .first()
            .expect("could not access the first data element")
            .len();

        let n_tiles = data.len();

        let mut tiles = Vec::with_capacity(data.len());

        for (idx, values) in data.into_iter().enumerate() {
            let grid_data = Grid2D::new(tile_size_in_pixels, values).unwrap();
            let grid_mask = Grid2D::new(tile_size_in_pixels, vec![true; n_pixels]).unwrap();
            let masked_grid = MaskedGrid2D::new(grid_data, grid_mask).unwrap().into();

            let global_tile_position = match n_tiles {
                1 => [0, 0].into(),
                _ => [-1, idx as isize].into(),
            };

            tiles.push(RasterTile2D::new_with_tile_info(
                TimeInterval::default(),
                TileInformation {
                    global_geo_transform: TestDefault::test_default(),
                    global_tile_position,
                    tile_size_in_pixels,
                },
                0,
                masked_grid,
                CacheHint::default(),
            ));
        }

        let pixel_bounds = if n_tiles == 1 {
            GridBoundingBox2D::new(
                [0, tile_size_in_pixels.y() as isize],
                [0, tile_size_in_pixels.x() as isize],
            )
            .unwrap()
        } else {
            GridBoundingBox2D::new(
                [tile_size_in_pixels.y() as isize * -1, 0],
                [0, (tile_size_in_pixels.x() * n_tiles) as isize],
            )
            .unwrap()
        };

        MockRasterSource {
            params: MockRasterSourceParams {
                data: tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    geo_transform_x: GeoTransform::test_default(),
                    pixel_bounds_x: pixel_bounds,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
    }

    fn create_dataset_tiling_specification() -> TilingSpecification {
        TilingSpecification {
            tile_size_in_pixels: geoengine_datatypes::raster::GridShape2D::new([4, 2]),
        }
    }

    #[ge_context::test(tiling_spec = "create_dataset_tiling_specification")]
    #[allow(clippy::too_many_lines)]
    async fn ml_model_from_workflow_task_success(app_ctx: ProPostgresContext<NoTls>) {
        use crate::pro::machine_learning::TrainingParams::Xgboost;

        let session = app_ctx
            .login(UserCredentials {
                email: "admin@localhost".into(),
                password: "admin".into(),
            })
            .await
            .unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id;

        let tile_size_in_pixels = [4, 2].into();

        let src_a = generate_raster_test_data_band_helper(
            vec![vec![1, 2, 3, 4, 5, 6, 7, 8]],
            tile_size_in_pixels,
        );
        let src_b = generate_raster_test_data_band_helper(
            vec![vec![9, 10, 11, 12, 13, 14, 15, 16]],
            tile_size_in_pixels,
        );
        let src_target = generate_raster_test_data_band_helper(
            vec![vec![0, 1, 2, 2, 2, 1, 0, 0]],
            tile_size_in_pixels,
        );

        let mut training_config: HashMap<String, String> = HashMap::new();

        training_config.insert("refresh_leaf".into(), "true".into());
        training_config.insert("tree_method".into(), "hist".into());
        training_config.insert("objective".into(), "multi:softmax".into());
        training_config.insert("eta".into(), "0.75".into());
        training_config.insert("num_class".into(), "4".into());
        training_config.insert("max_depth".into(), "10".into());

        let workflow_a = Workflow {
            operator: src_a.boxed().into(),
        };

        let workflow_b = Workflow {
            operator: src_b.boxed().into(),
        };

        let workflow_target = Workflow {
            operator: src_target.boxed().into(),
        };

        let spatial_bounds: geoengine_datatypes::primitives::BoundingBox2D =
            BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap();

        let time_interval = TimeInterval::new_instant(
            geoengine_datatypes::primitives::DateTime::new_utc(2013, 12, 1, 12, 0, 0),
        )
        .unwrap();

        let qry: VectorQueryRectangle = VectorQueryRectangle::with_bounds(
            spatial_bounds,
            time_interval,
            ColumnSelection::all(),
        );

        let xg_train = crate::pro::machine_learning::MLTrainRequest {
            query: qry,
            params: Xgboost(XgboostTrainingParams {
                no_data_value: -1_000.,
                training_config,
                feature_names: vec![Some("a".into()), Some("b".into())],
                label_names: vec![Some("target".into())],
                aggregate_variant: MachineLearningAggregator::Simple,
                memory_limit: Some(1024),
            }),
            input_workflows: vec![workflow_a, workflow_b],
            label_workflows: vec![workflow_target],
            model_type: XGBoost,
        };

        let req = test::TestRequest::post()
            .uri("/ml/train")
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_json(xg_train);

        let res = send_pro_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200, "{:?}", res.response());

        let task_response =
            serde_json::from_str::<TaskResponse>(&read_body_string(res).await).unwrap();

        let tasks = Arc::new(ctx.tasks());

        wait_for_task_to_finish(tasks.clone(), task_response.task_id).await;

        let status = tasks.get_task_status(task_response.task_id).await.unwrap();

        let response = if let TaskStatus::Completed { info, .. } = status {
            info.as_any_arc()
                .downcast::<MachineLearningModelFromWorkflowResult>()
                .unwrap()
                .as_ref()
                .clone()
        } else {
            panic!("Task must be completed");
        };

        let model_id = response.model_id;

        // now use the model_id to get the model content for testing
        let exe_ctx = ctx.execution_context().unwrap();
        let ml_model_access = exe_ctx.extensions().get::<MlModelAccess>().unwrap();
        let model = ml_model_access
            .load_ml_model_by_id(model_id.into())
            .await
            .unwrap();

        // get the content of the reference model on disk to compare against
        let test_model_path = PathBuf::from(geoengine_datatypes::test_data!(
            "pro/ml/xgboost/reference_test_model.json"
        ));

        let mut test_model_bytes: Vec<u8> = Vec::new();
        let mut f = File::open(test_model_path)
            .await
            .expect("Unable to open file of test model");
        f.read_to_end(&mut test_model_bytes)
            .await
            .expect("Could not read file of test model");

        // check that the returned model is as expected
        assert_eq!(&test_model_bytes as &[u8], model.to_string().as_bytes());

        // also check, that the model (which was written after training) to disk is as expected
        let model_from_disk = ml_model_access
            .load_ml_model_by_id(model_id.into())
            .await
            .expect("Could not read specified ml model from disk");

        assert_eq!(&test_model_bytes as &[u8], model_from_disk.as_bytes());
    }

    /// Helper method to prepare a `MLTrainRequest` struct.
    #[allow(clippy::too_many_lines)]
    fn generate_ml_train_request() -> crate::pro::machine_learning::MLTrainRequest {
        let tile_size_in_pixels = [5, 5].into();

        let red = generate_raster_test_data_band_helper(
            vec![
                vec![
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                ],
                vec![
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                ],
                vec![
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                ],
                vec![
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                ],
            ],
            tile_size_in_pixels,
        );

        let green = generate_raster_test_data_band_helper(
            vec![
                vec![
                    0, 0, 0, 0, 0, //
                    0, 255, 255, 255, 255, //
                    0, 255, 255, 255, 255, //
                    0, 255, 255, 255, 255, //
                    0, 0, 0, 0, 0, //
                ],
                vec![
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 255, 255, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 255, 255, //
                    0, 0, 0, 0, 0, //
                ],
                vec![
                    0, 0, 0, 0, 0, //
                    255, 255, 255, 255, 0, //
                    0, 0, 0, 0, 0, //
                    255, 255, 255, 255, 0, //
                    0, 0, 0, 0, 0, //
                ],
                vec![
                    0, 255, 0, 255, 0, //
                    255, 255, 255, 255, 255, //
                    0, 255, 0, 255, 0, //
                    255, 255, 255, 255, 255, //
                    0, 255, 0, 255, 0, //
                ],
            ],
            tile_size_in_pixels,
        );

        let blue = generate_raster_test_data_band_helper(
            vec![
                vec![
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                ],
                vec![
                    0, 0, 0, 0, 0, //
                    255, 255, 255, 0, 0, //
                    255, 255, 255, 255, 255, //
                    255, 255, 255, 0, 0, //
                    0, 0, 0, 0, 0, //
                ],
                vec![
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    255, 255, 255, 255, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                ],
                vec![
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    255, 0, 255, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                ],
            ],
            tile_size_in_pixels,
        );

        let temperature = generate_raster_test_data_band_helper(
            vec![
                vec![
                    30, 30, 30, 30, 30, //
                    30, 15, 15, 15, 15, //
                    30, 15, 15, 15, 15, //
                    30, 15, 15, 15, 15, //
                    30, 30, 30, 30, 30, //
                ],
                vec![
                    30, 30, 30, 30, 30, //
                    5, 5, 5, 15, 15, //
                    5, 5, 5, 5, 5, //
                    5, 5, 5, 15, 15, //
                    30, 30, 30, 30, 30, //
                ],
                vec![
                    30, 30, 30, 30, 30, //
                    15, 15, 15, 15, 30, //
                    5, 5, 5, 5, 30, //
                    15, 15, 15, 15, 30, //
                    30, 30, 30, 30, 30, //
                ],
                vec![
                    30, 15, 30, 15, 30, //
                    15, 15, 15, 15, 15, //
                    5, 15, 5, 15, 30, //
                    15, 15, 15, 15, 15, //
                    30, 15, 30, 15, 30, //
                ],
            ],
            tile_size_in_pixels,
        );

        let target_tiles = vec![
            vec![
                1, 1, 1, 1, 1, //
                1, 2, 2, 2, 2, //
                1, 2, 2, 2, 2, //
                1, 2, 2, 2, 2, //
                1, 1, 1, 1, 1, //
            ],
            vec![
                1, 1, 1, 1, 1, //
                3, 3, 3, 2, 2, //
                3, 3, 3, 3, 3, //
                3, 3, 3, 2, 2, //
                1, 1, 1, 1, 1, //
            ],
            vec![
                1, 1, 1, 1, 1, //
                2, 2, 2, 2, 1, //
                3, 3, 3, 3, 1, //
                2, 2, 2, 2, 1, //
                1, 1, 1, 1, 1, //
            ],
            vec![
                1, 2, 1, 2, 1, //
                2, 2, 2, 2, 2, //
                3, 2, 3, 2, 1, //
                2, 2, 2, 2, 2, //
                1, 2, 1, 2, 1, //
            ],
        ];

        let unique_label_count = target_tiles
            .concat()
            .into_iter()
            .collect::<std::collections::HashSet<_>>()
            .len();

        let src_target = generate_raster_test_data_band_helper(target_tiles, tile_size_in_pixels);

        // use xgboost-rs crate's config builder to generate a training configuration struct
        let tree_params =
            crate::pro::machine_learning::xgb_config_builder::TreeBoosterParametersBuilder::default()
                .eta(0.75)
                .tree_method(crate::pro::machine_learning::xgb_config_builder::TreeMethod::Hist)
                .process_type(crate::pro::machine_learning::xgb_config_builder::ProcessType::Default)
                .max_depth(15)
                .build()
                .expect("could not create tree booster parameters");

        let learning_params =
            crate::pro::machine_learning::xgb_config_builder::LearningTaskParametersBuilder::default()
                .num_class(Some(unique_label_count as u32))
                .objective(crate::pro::machine_learning::xgb_config_builder::Objective::MultiSoftmax)
                .build()
                .expect("could not create learning task parameters");

        // this will be passed to the xgboost crate
        let booster_params =
            crate::pro::machine_learning::xgb_config_builder::BoosterParametersBuilder::default()
                .booster_type(
                    crate::pro::machine_learning::xgb_config_builder::BoosterType::Tree(
                        tree_params,
                    ),
                )
                .learning_params(learning_params)
                .silent(true)
                .build()
                .expect("could not create booster parameters");

        let workflow_red = Workflow {
            operator: red.boxed().into(),
        };

        let workflow_green = Workflow {
            operator: green.boxed().into(),
        };

        let workflow_blue = Workflow {
            operator: blue.boxed().into(),
        };

        let workflow_temperature = Workflow {
            operator: temperature.boxed().into(),
        };

        let workflow_target = Workflow {
            operator: src_target.boxed().into(),
        };

        let spatial_bounds: geoengine_datatypes::primitives::BoundingBox2D =
            BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap();

        let time_interval = TimeInterval::new_instant(
            geoengine_datatypes::primitives::DateTime::new_utc(2013, 12, 1, 12, 0, 0),
        )
        .unwrap();

        let qry: VectorQueryRectangle = VectorQueryRectangle::with_bounds(
            spatial_bounds,
            time_interval,
            ColumnSelection::all(),
        );

        // generate a hashmap of xgboost parameters with the corresponding setting values
        let training_config_vec = booster_params
            .as_string_pairs()
            .expect("could not convert booster parameters to string pairs");
        let training_config: HashMap<String, String> = training_config_vec.into_iter().collect();

        crate::pro::machine_learning::MLTrainRequest {
            query: qry,
            params: TrainingParams::Xgboost(XgboostTrainingParams {
                no_data_value: -1_000.,
                training_config,
                feature_names: vec![
                    Some("red".into()),
                    Some("green".into()),
                    Some("blue".into()),
                    Some("temperature".into()),
                ],
                label_names: vec![Some("target".into())],
                aggregate_variant: MachineLearningAggregator::Simple,
                memory_limit: Some(1024),
            }),
            input_workflows: vec![
                workflow_red,
                workflow_green,
                workflow_blue,
                workflow_temperature,
            ],
            label_workflows: vec![workflow_target],
            model_type: XGBoost,
        }
    }

    fn create_dataset_tiling_specification_5x5() -> TilingSpecification {
        TilingSpecification {
            tile_size_in_pixels: geoengine_datatypes::raster::GridShape2D::new([5, 5]),
        }
    }

    #[ge_context::test(tiling_spec = "create_dataset_tiling_specification_5x5")]
    #[allow(clippy::too_many_lines)]
    async fn complete_train_predict_cycle(app_ctx: ProPostgresContext<NoTls>) {
        // -----------------------------------------------------
        // Training Phase
        // -----------------------------------------------------

        let session = app_ctx
            .login(UserCredentials {
                email: "admin@localhost".into(),
                password: "admin".into(),
            })
            .await
            .unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id;

        // this generates the training config + the training data
        // and puts it in the request struct
        let xg_train: MLTrainRequest = generate_ml_train_request();

        let req = test::TestRequest::post()
            .uri("/ml/train")
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_json(xg_train);

        let res = send_pro_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200, "{:?}", res.response());

        let task_response =
            serde_json::from_str::<TaskResponse>(&read_body_string(res).await).unwrap();

        let tasks = Arc::new(ctx.tasks());

        wait_for_task_to_finish(tasks.clone(), task_response.task_id).await;

        let status = tasks.get_task_status(task_response.task_id).await.unwrap();

        let response = if let TaskStatus::Completed { info, .. } = status {
            info.as_any_arc()
                .downcast::<MachineLearningModelFromWorkflowResult>()
                .unwrap()
                .as_ref()
                .clone()
        } else {
            panic!("Task must be completed");
        };

        let exe_ctx = ctx.execution_context().unwrap();

        // -----------------------------------------------------
        // Prediction Phase
        // -----------------------------------------------------

        let red = generate_raster_test_data_band_helper(
            vec![
                vec![
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                ],
                vec![
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                ],
            ],
            [5, 5].into(),
        )
        .boxed();

        let green = generate_raster_test_data_band_helper(
            vec![
                vec![
                    255, 255, 255, 255, 255, //
                    255, 0, 0, 0, 255, //
                    255, 0, 0, 0, 255, //
                    255, 0, 0, 0, 255, //
                    255, 255, 255, 255, 255, //
                ],
                vec![
                    0, 0, 255, 255, 255, //
                    0, 0, 255, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 255, 0, 0, //
                    255, 255, 255, 0, 0, //
                ],
            ],
            [5, 5].into(),
        )
        .boxed();

        let blue = generate_raster_test_data_band_helper(
            vec![
                vec![
                    0, 0, 0, 0, 0, //
                    0, 0, 255, 0, 0, //
                    0, 255, 255, 255, 0, //
                    0, 0, 255, 0, 0, //
                    0, 0, 0, 0, 0, //
                ],
                vec![
                    0, 255, 0, 0, 0, //
                    255, 0, 0, 255, 255, //
                    255, 255, 0, 255, 255, //
                    255, 255, 0, 0, 255, //
                    0, 0, 0, 255, 0, //
                ],
            ],
            [5, 5].into(),
        )
        .boxed();

        let temperature = generate_raster_test_data_band_helper(
            vec![
                vec![
                    15, 15, 15, 15, 15, //
                    15, 30, 5, 30, 15, //
                    15, 5, 5, 5, 15, //
                    15, 30, 5, 30, 15, //
                    15, 15, 15, 15, 15, //
                ],
                vec![
                    30, 5, 15, 15, 15, //
                    5, 30, 15, 5, 5, //
                    5, 5, 30, 5, 5, //
                    5, 5, 15, 30, 5, //
                    15, 15, 15, 5, 30, //
                ],
            ],
            [5, 5].into(),
        )
        .boxed();

        let srcs = vec![red, green, blue, temperature];

        let xg = XgboostOperator {
            params: XgboostParams {
                model_id: response.model_id.into(),
                no_data_value: -1000.,
            },
            sources: MultipleRasterSources { rasters: srcs },
        };

        let op = RasterOperator::boxed(xg)
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        let processor = op.query_processor().unwrap().get_f32().unwrap();

        let query_rect = RasterQueryRectangle::new_with_grid_bounds(
            GridBoundingBox2D::new([0, 5], [9, -1]).unwrap(),
            TimeInterval::default(),
            BandSelection::first(),
        );

        let query_ctx = ctx.query_context().unwrap();

        // geoengine_operators::engine::QueryProcessor::query(&processor, query_rect, &query_ctx)
        let result_stream = processor.query(query_rect, &query_ctx).await.unwrap();

        let result: Vec<Result<RasterTile2D<f32>>> = result_stream.collect().await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        let mut all_pixels = Vec::new();

        for tile in result {
            let data_of_tile = tile.into_materialized_tile().grid_array.inner_grid.data;
            for pixel in &data_of_tile {
                all_pixels.push(pixel.round());
            }
        }

        // expected result tiles
        //    tile 1      ||      tile 2
        // --------------------------------
        // 1, 1, 1, 1, 1  ||  0, 2, 1, 1, 1
        // 1, 0, 2, 0, 1  ||  2, 0, 1, 2, 2
        // 1, 2, 2, 2, 1  ||  2, 2, 0, 2, 2
        // 1, 0, 2, 0, 1  ||  2, 2, 1, 0, 2
        // 1, 1, 1, 1, 1  ||  1, 1, 1, 2, 0

        let expected = vec![
            // tile 1
            1.0, 1.0, 1.0, 1.0, 1.0, //
            1.0, 0.0, 2.0, 0.0, 1.0, //
            1.0, 2.0, 2.0, 2.0, 1.0, //
            1.0, 0.0, 2.0, 0.0, 1.0, //
            1.0, 1.0, 1.0, 1.0, 1.0, //
            // tile 2
            0.0, 2.0, 1.0, 1.0, 1.0, //
            2.0, 0.0, 1.0, 2.0, 2.0, //
            2.0, 2.0, 0.0, 2.0, 2.0, //
            2.0, 2.0, 1.0, 0.0, 2.0, //
            1.0, 1.0, 1.0, 2.0, 0.0, //
        ];

        assert_eq!(all_pixels, expected);
    }
}
