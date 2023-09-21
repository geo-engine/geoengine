mod data_preparation;
pub mod ml_model;
mod ml_tasks;

pub mod ml_error;

pub mod xgb_config_builder;

mod xgboost_training;

use crate::error::Result;
use std::collections::HashMap;

pub(crate) use ml_tasks::{schedule_ml_model_training_task, MLTrainRequest};

#[cfg(test)] //TODO: remove test config, once its used outside of tests
pub(crate) use ml_tasks::MachineLearningModelFromWorkflowResult;

use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use typetag::serde;

use utoipa::ToSchema;

#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum TrainingParams {
    Xgboost(XgboostTrainingParams),
}

impl TrainingParams {
    pub fn no_data_value(&self) -> f32 {
        match self {
            TrainingParams::Xgboost(params) => params.no_data_value,
        }
    }
}

/// The `TrainableModel` trait defines the common behavior for all trainable machine learning models.
///
/// This trait is used to provide a generic way to handle different machine learning model types.
/// The goal is to allow any type implementing this trait to be trained in a consistent way.
trait TrainableModel {
    fn train_model(
        &self,
        input_feature_data: &mut [MachineLearningFeature],
        input_label_data: &mut [MachineLearningFeature],
        training_config: &HashMap<String, String>,
    ) -> Result<Value>;
}

/// `GenericModel` is a placeholder model used for feature gating and linting.
///
/// This model does not actually perform any training. Instead, it serves as a stand-in
/// for other models when they are disabled due to feature gating or other reasons.
/// This helps to prevent schema derivation errors and to manage compiler warnings with different feature sets.
#[derive(Clone, Deserialize, Serialize, ToSchema)]
pub struct GenericModel;

impl TrainableModel for GenericModel {
    fn train_model(
        &self,
        input_feature_data: &mut [MachineLearningFeature],
        input_label_data: &mut [MachineLearningFeature],
        training_config: &HashMap<String, String>,
    ) -> Result<Value> {
        // Dummy json data
        let json_data = serde_json::json!({
            "model": "dummy",
            "training_config": training_config,
            "input_feature_data": input_feature_data,
            "input_label_data": input_label_data,
        });

        Ok(json_data)
    }
}

/// The `ModelType` enum provides a way to specify which kind of model is being used.
///
/// This enum is used to route the task to the correct training implementation.
/// Depending on the variant, a different implementation of the `TrainableModel` trait will be used.
#[derive(Clone, Deserialize, Serialize, ToSchema)]
pub enum ModelType {
    XGBoost,
}

impl TrainableModel for ModelType {
    fn train_model(
        &self,
        input_feature_data: &mut [MachineLearningFeature],
        input_label_data: &mut [MachineLearningFeature],
        training_config: &HashMap<String, String>,
    ) -> Result<Value> {
        match self {
            ModelType::XGBoost => {
                xgboost_training::train_model(input_feature_data, input_label_data, training_config)
            }
        }
    }
}

#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct XgboostTrainingParams {
    // A float value that represents missing or null data in the input.
    // Could be used to ignore input labels in the trainig process.
    pub no_data_value: f32,
    pub training_config: HashMap<String, String>,
    pub feature_names: Vec<Option<String>>,
    pub label_names: Vec<Option<String>>,
    pub aggregate_variant: MachineLearningAggregator,
    pub memory_limit: Option<usize>,
}

/// Represents a single feature of a machine learning training data set
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MachineLearningFeature {
    pub feature_name: Option<String>, // e.g. temperature, elevation etc.
    pub feature_data: Vec<f32>,
}

impl MachineLearningFeature {
    pub fn new(feature_name: Option<String>, feature_data: Vec<f32>) -> MachineLearningFeature {
        MachineLearningFeature {
            feature_name,
            feature_data,
        }
    }
}

/// This enum represents the different aggregators that can be used to initialize the
/// different algorithms for collecting the data used in ml training.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum MachineLearningAggregator {
    Simple,
    ReservoirSampling,
}

/// The purpose of this trait is to facilitate a generic aggregation process of raster data.
// TODO: extend the generic implementation to allow for a general use in all kinds of ml methods.
pub trait Aggregatable {
    type Data;

    /// This method should realize the aggregation algorithm of the implementing struct.
    fn aggregate<T>(&mut self, incoming_data: &mut dyn Iterator<Item = Option<T>>)
    where
        f32: From<T>;

    /// Once the aggregation process is finished, return the data for further usage.
    fn finish(&mut self) -> Vec<Self::Data>;

    /// Used to generate a new instance of an aggregatable within a generic context
    fn new(data: Vec<f32>, no_data_value: f32, memory_limit: Option<usize>) -> Self;
}

/// A simple aggregator that just collects all incoming data in a vector.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SimpleAggregator {
    pub data: Vec<f32>,
    no_data_value: f32,
}

impl Aggregatable for SimpleAggregator {
    type Data = f32;

    fn aggregate<T>(&mut self, incoming_data: &mut dyn Iterator<Item = Option<T>>)
    where
        f32: From<T>,
    {
        let out_data_sink = incoming_data
            .into_iter()
            .map(|opt| match opt {
                Some(value) => value.into(),
                None => self.no_data_value,
            })
            .collect::<Vec<f32>>();

        self.data.extend(out_data_sink);
    }

    fn finish(&mut self) -> Vec<f32> {
        // TODO: consume self instead
        std::mem::take(&mut self.data)
    }

    // TODO: implement memory limit
    fn new(data: Vec<f32>, no_data_value: f32, _memory_limit: Option<usize>) -> Self {
        SimpleAggregator {
            data,
            no_data_value,
        }
    }
}

/// A reservoir sampling aggregator that samples a given number of elements from the incoming data.
/// This aggregator can be used, when the data size exceeds the available memory.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReservoirSamplingAggregator {
    pub data: Vec<f32>,
    no_data_value: f32,
}

// TODO: implement reservoir sampling
impl Aggregatable for ReservoirSamplingAggregator {
    type Data = f32;

    fn aggregate<T>(&mut self, incoming_data: &mut dyn Iterator<Item = Option<T>>)
    where
        f32: From<T>,
    {
        let out_data_sink = incoming_data
            .into_iter()
            .flatten()
            .map(Into::into)
            .collect::<Vec<f32>>();
        self.data.extend(out_data_sink);
    }

    fn finish(&mut self) -> Vec<f32> {
        std::mem::take(&mut self.data)
    }

    // TODO: implement memory limit
    fn new(data: Vec<f32>, no_data_value: f32, _memory_limit: Option<usize>) -> Self {
        ReservoirSamplingAggregator {
            data,
            no_data_value,
        }
    }
}
