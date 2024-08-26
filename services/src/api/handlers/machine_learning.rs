use std::borrow::Cow;

use async_trait::async_trait;
use geoengine_datatypes::machine_learning::MlModelName;
use geoengine_operators::pro::machine_learning::MachineLearningError;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use validator::{Validate, ValidationError};

use crate::{
    api::model::datatypes::RasterDataType,
    datasets::upload::UploadId,
    util::config::{get_config_element, MachineLearning},
};
