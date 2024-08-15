use std::borrow::Cow;

use async_trait::async_trait;
use geoengine_datatypes::machine_learning::MlModelName;
use geoengine_operators::pro::machine_learning::MachineLearningError;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use validator::{Validate, ValidationError};

use crate::{
    datasets::upload::UploadId,
    util::config::{get_config_element, MachineLearning},
};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MlModel {
    name: MlModelName,
    display_name: String,
    description: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AddMlModel {
    name: MlModelName,
    display_name: String,
    description: String,
    upload: UploadId,
}

// #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
// #[serde(rename_all = "camelCase")]
// pub struct MlModel {
//     name: MlModelName,
//     display_name: String,
//     description: String,
// }

#[derive(Debug, Deserialize, Serialize, Clone, IntoParams, Validate)]
#[into_params(parameter_in = Query)]
pub struct MlModelListOptions {
    #[param(example = 0)]
    pub offset: u32,
    #[param(example = 2)]
    #[validate(custom = "validate_list_limit")]
    pub limit: u32,
}

fn validate_list_limit(value: u32) -> Result<(), ValidationError> {
    let limit = get_config_element::<MachineLearning>()
        .expect("should exist because it is defined in the default config")
        .list_limit;
    if value <= limit {
        return Ok(());
    }

    let mut err = ValidationError::new("limit (too large)");
    err.add_param::<u32>(Cow::Borrowed("max limit"), &limit);
    Err(err)
}
