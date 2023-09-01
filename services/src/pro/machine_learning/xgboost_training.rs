use std::collections::hash_map::Entry;
use std::collections::HashMap;

use serde_json::Value;
use snafu::{ensure, ResultExt};
use xgboost_rs::{Booster, DMatrix};

use crate::error::Result;
use crate::pro::machine_learning::ml_error as XgModuleError;
use crate::pro::machine_learning::MachineLearningFeature;
use ordered_float::NotNan;

pub fn train_model(
    input_feature_data: &mut [MachineLearningFeature],
    input_label_data: &mut [MachineLearningFeature],
    training_config: &HashMap<String, String>,
) -> Result<Value> {
    // TODO:
    // add functionallity to handle multiple target columns to this function

    ensure!(
        !input_feature_data.is_empty() && !input_label_data.is_empty(),
        XgModuleError::error::MachineLearningMustHaveAtLeastOneFeatureAndOneLabel
    );

    let n_features = input_feature_data.len();

    let raw_feature_data: Vec<&f32> = input_feature_data
        .iter()
        .flat_map(|elem| &elem.feature_data)
        .collect();

    let raw_label_data: Vec<f32> = input_label_data
        .iter()
        .flat_map(|elem| elem.feature_data.clone())
        .collect();

    let n_rows = raw_feature_data.len() / n_features;

    let strides_ax_0 = 1;
    let strides_ax_1 = n_rows;
    let byte_size_ax_0 = std::mem::size_of::<f32>() * strides_ax_0;
    let byte_size_ax_1 = std::mem::size_of::<f32>() * strides_ax_1;

    let mut dmatrix = DMatrix::from_col_major_f32(
        raw_feature_data
            .into_iter()
            .copied()
            .collect::<Vec<f32>>()
            .as_slice(),
        byte_size_ax_0,
        byte_size_ax_1,
        n_rows,
        n_features,
        -1,
        f32::NAN,
    )
    .context(XgModuleError::error::CreateDMatrix)?;

    let lbls_remap = remap_labels(raw_label_data)?;

    dmatrix
        .set_labels(lbls_remap.as_slice())
        .context(XgModuleError::error::DMatrixSetLabels)?; // <- here we need the remapped target values

    let evals = &[(&dmatrix, "train")];

    // TODO:
    // So far, this only accepts a single target column.
    // Add boilerplate code to use xgboost capability to train a model with multiple target columns.
    let bst = Booster::train(
        Some(evals),
        &dmatrix,
        training_config.clone(),
        None, // <- No old model yet
    )
    .context(XgModuleError::error::BoosterTraining)?;

    let model = bst
        .save_to_buffer("json".into())
        .context(XgModuleError::error::ModelStorage)?;

    let res = serde_json::from_str(model.as_str())?;
    Ok(res)
}

/// Maps a vector of labels to a sequence of whole numbers, as required by `XGBoost`.
///
/// `XGBoost` expects labels to be in the form of a sequence of float values, where each value
/// represents the target for a specific data point. Additionally, the labels must be represented
/// as a sequence of whole numbers in the range [0.0, n-1], where n is the number of unique labels.
/// This function takes in a vector of labels and returns a new vector that holds the remapped labels
/// based on its value and order of appearance in the input vector.
///
/// # Arguments
///
/// * `lbls`: A vector of labels in their original form.
///
/// # Returns
///
/// A new vector of labels that have been mapped to whole numbers in the range [0.0, n-1], where
/// n is the number of unique labels in the input vector.
///
/// # Errors
///
/// Returns an error if any of the labels cannot be converted to a `NotNan<f32>`.
///
fn remap_labels(lbls: Vec<f32>) -> Result<Vec<f32>> {
    let mut unique_lbl_counter: f32 = -1.0;

    // TODO: persist this hashmap?
    // currently (april, 2023) it is only possible to use an integer sequence for class prediction
    // with xgboost. This means we need to map class labels like `road`, `forest` and `water` to {0.0,
    // 1.0, 2.0}. In order to provide a better user experience, it would be useful to persist the
    // mapping of this function, for usage later on (i.e. UI).
    let mut unique_lbls_hm: HashMap<NotNan<f32>, f32> = HashMap::new();

    let remapped_values = lbls
        .into_iter()
        .map(|elem| {
            let key: NotNan<f32> = NotNan::new(elem)?;

            match unique_lbls_hm.entry(key) {
                Entry::Vacant(e) => {
                    unique_lbl_counter += 1.0;
                    e.insert(unique_lbl_counter);
                    Ok(unique_lbl_counter)
                }
                Entry::Occupied(e) => {
                    let remapped_val = e.get();
                    Ok(*remapped_val)
                }
            }
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(remapped_values)
}
