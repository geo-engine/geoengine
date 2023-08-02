use crate::contexts::SessionContext;
use crate::error::Result;

use crate::machine_learning::data_preparation::{
    accumulate_raster_data, get_operators_from_workflows, get_query_processors,
};
use crate::machine_learning::ml_model::{MlModel, MlModelDb};

use crate::machine_learning::{
    Aggregatable, MachineLearningAggregator, MachineLearningFeature, ModelType,
    ReservoirSamplingAggregator, SimpleAggregator, TrainableModel,
};
#[cfg(feature = "xgboost")]
use crate::tasks::TaskManager;
use crate::tasks::{Task, TaskId, TaskStatusInfo};
use crate::util::config::get_config_element;
use crate::workflows::workflow::Workflow;
use geoengine_datatypes::error::{BoxedResultExt, ErrorSource};
use geoengine_datatypes::ml_model::MlModelId;
use geoengine_datatypes::primitives::VectorQueryRectangle;
use geoengine_datatypes::util::Identifier;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::{ensure, ResultExt};
use std::sync::Arc;
use tokio::fs;
use utoipa::ToSchema;

use super::TrainingParams;

#[derive(Clone, Deserialize, Serialize, ToSchema)]
#[schema(example = json!({"inputWorkflows":[{"operator":{"params":{}}},{"operator":{"params":{}}},],"labelWorkflow":[{"operator":{"params":{}}}],"params":{},"query":{}, "modelInstance":{}}))]
#[serde(rename_all = "camelCase")]
pub struct MLTrainRequest {
    pub params: TrainingParams,
    pub input_workflows: Vec<Workflow>,
    pub label_workflows: Vec<Workflow>,
    pub query: VectorQueryRectangle,
    pub model_instance: ModelType,
}

/// response of the machine learning model from workflow handler
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MachineLearningModelFromWorkflowResult {
    pub model_id: MlModelId,
}

impl TaskStatusInfo for MachineLearningModelFromWorkflowResult {}

pub struct MachineLearningModelFromWorkflowTask<C: SessionContext> {
    pub ctx: Arc<C>,
    pub request: MLTrainRequest,
    pub model_id: MlModelId,
}

impl<C: SessionContext> MachineLearningModelFromWorkflowTask<C> {
    async fn process<A: Aggregatable<Data = f32>>(
        &self,
    ) -> Result<MachineLearningModelFromWorkflowResult> {
        let n_features = self.request.input_workflows.len();
        let n_labels = self.request.label_workflows.len();

        // TODO:: this step can be used to verify the config, and/or create a BoosterParameters instance,
        // but the BP object is not strictly necessary
        // TODO:: This also needs a more generic wrapper, that can handle different kinds of configs.
        // Right now it only allows to check a xgboost config
        //
        // verify the provided training config, before any expensive operations are performed.
        // let _x = BoosterParameters::try_from(self.request.params.training_config.clone()).map_err(
        //     |_| MachineLearningError::InvalidBoosterParameters {
        //         s: "Could not convert from HashMap".to_string(),
        //     },
        // )?;

        // TODO: this assumes, that the config contains these fields for all types of models
        let (feature_names, label_names, training_config, memory_limit) = match &self.request.params
        {
            TrainingParams::Xgboost(params) => (
                &params.feature_names,
                &params.label_names,
                &params.training_config,
                params.memory_limit,
            ),
        };

        // make sure the correct number of feature names is provided
        ensure!(
            n_features >= 1 && (feature_names.len() == n_features),
            super::ml_error::error::WrongNumberOfFeatureNamesProvided {
                n_feature_workflows: self.request.input_workflows.len(),
                feature_names: feature_names.len()
            }
        );

        // make sure the correct number of label names is provided
        ensure!(
            n_labels >= 1 && (label_names.len() == n_labels),
            super::ml_error::error::WrongNumberOfLabelNamesProvided {
                n_label_workflows: self.request.label_workflows.len(),
                label_names: label_names.len()
            }
        );

        let feature_operators = get_operators_from_workflows(&self.request.input_workflows)?;

        let label_operators = get_operators_from_workflows(&self.request.label_workflows)?;

        let exe_ctx = self.ctx.execution_context()?;

        let typed_query_processors_features =
            get_query_processors(feature_operators, &exe_ctx).await?;

        let typed_query_processors_labels = get_query_processors(label_operators, &exe_ctx).await?;

        let query = self.request.query;
        let query_ctx = self.ctx.query_context()?;

        let n_rasters = typed_query_processors_features.len();
        let n_rasters_label = typed_query_processors_labels.len();

        let mut aggregator_vec: Vec<_> = Vec::with_capacity(n_rasters);
        let mut aggregator_vec_labels: Vec<_> = Vec::with_capacity(n_rasters_label);

        (0..n_rasters).for_each(|_| {
            let data = Vec::new();
            let agg: A = Aggregatable::new(data, memory_limit);
            aggregator_vec.push(agg);
        });

        (0..n_rasters_label).for_each(|_| {
            let data = Vec::new();
            let agg: A = Aggregatable::new(data, memory_limit);
            aggregator_vec_labels.push(agg);
        });

        let mut feature_data: Vec<MachineLearningFeature> = accumulate_raster_data(
            feature_names,
            typed_query_processors_features,
            query,
            &query_ctx,
            &mut aggregator_vec,
        )
        .await?;

        let mut label_data: Vec<MachineLearningFeature> = accumulate_raster_data(
            label_names,
            typed_query_processors_labels,
            query,
            &query_ctx,
            &mut aggregator_vec_labels,
        )
        .await?;

        // TODO: inspect, if this is a proper way to handle different kinds of ml models
        // Explanation: the different training methods for each ml type have to be handled differently
        // here is a trait implementation used to choose the correct training method
        let content_of_trained_model: Value = self.request.model_instance.train_model(
            &mut feature_data,
            &mut label_data,
            training_config,
        )?;

        let ml_model = MlModel {
            model_id: self.model_id,
            model_content: content_of_trained_model.to_string(),
        };

        let db = self.ctx.db();

        db.store_ml_model(ml_model).await?;

        Ok(MachineLearningModelFromWorkflowResult {
            model_id: self.model_id,
        })
    }
}

#[async_trait::async_trait]
impl<C: SessionContext> Task<C::TaskContext> for MachineLearningModelFromWorkflowTask<C> {
    async fn run(
        &self,
        _ctx: C::TaskContext,
    ) -> Result<Box<dyn crate::tasks::TaskStatusInfo>, Box<dyn ErrorSource>> {
        let aggregate_variant = match &self.request.params {
            TrainingParams::Xgboost(params) => &params.aggregate_variant,
        };

        match aggregate_variant {
            MachineLearningAggregator::Simple => {
                let response = self.process::<SimpleAggregator>().await;

                response
                    .map(TaskStatusInfo::boxed)
                    .map_err(ErrorSource::boxed)
            }
            MachineLearningAggregator::ReservoirSampling => {
                let response = self.process::<ReservoirSamplingAggregator>().await;

                response
                    .map(TaskStatusInfo::boxed)
                    .map_err(ErrorSource::boxed)
            }
        }
    }

    async fn cleanup_on_error(&self, _ctx: C::TaskContext) -> Result<(), Box<dyn ErrorSource>> {
        let model_defs_path = get_config_element::<crate::util::config::MachineLearning>()
            .boxed_context(crate::machine_learning::ml_error::error::CouldNotGetMlModelConfigPath)
            .map_err(ErrorSource::boxed)?
            .model_defs_path;

        let model_dir = model_defs_path.join(self.model_id.to_string());

        fs::remove_dir_all(model_dir)
            .await
            .context(crate::error::Io)
            .map_err(ErrorSource::boxed)?;

        Ok(())
    }

    fn task_type(&self) -> &'static str {
        "create-ml-model"
    }

    fn task_unique_id(&self) -> Option<String> {
        Some(TaskId::new().to_string())
    }
}

#[cfg(feature = "xgboost")]
pub async fn schedule_ml_model_training_task<C: SessionContext>(
    ctx: Arc<C>,
    ml_train_request: MLTrainRequest,
) -> Result<TaskId> {
    // create a new ModelId. The model will be stored with this id
    // and can be retrieved by this id
    let model_id = MlModelId::new();

    let task = MachineLearningModelFromWorkflowTask {
        ctx: ctx.clone(),
        request: ml_train_request,
        model_id,
    }
    .boxed();

    let task_id = ctx.tasks().schedule_task(task, None).await?;

    Ok(task_id)
}
