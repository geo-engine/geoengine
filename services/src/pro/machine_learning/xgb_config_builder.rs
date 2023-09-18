/// This module contains the configuration builder for xgboost.
/// See [https://xgboost.readthedocs.io/en/stable/parameter.html](https://xgboost.readthedocs.io/en/stable/parameter.html) for more details on the parameters.
/// The `BoosterParameters` struct is the endpoint, that is sent to xgboost.
use derive_builder::Builder;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::default::Default;
use std::fmt::{self, Display};

use serde::{Deserialize, Serialize};

use crate::error::Result;

use super::ml_error::MachineLearningError;

// -----------------------------------------------------------------------------------------------
// XGBoost Config Definitions
// -----------------------------------------------------------------------------------------------

#[derive(Builder, Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[builder(default)]
pub struct BoosterParameters {
    booster_type: BoosterType,

    learning_params: LearningTaskParameters,

    silent: bool,

    threads: Option<u32>,
}

impl BoosterParameters {
    pub fn as_string_pairs(&self) -> Result<Vec<(String, String)>> {
        let mut v = Vec::new();

        v.extend(self.booster_type.as_string_pairs());

        // here could be an option unset, so it needs to be a result value
        let learning_params_res = self.learning_params.as_string_pairs()?;
        v.extend(learning_params_res);

        v.push(("silent".to_owned(), self.silent.to_string()));

        if let Some(nthread) = self.threads {
            v.push(("nthread".to_owned(), nthread.to_string()));
        }

        Ok(v)
    }
}

impl TryFrom<HashMap<String, String>> for BoosterParameters {
    type Error = MachineLearningError;

    #[allow(clippy::too_many_lines)]
    /// This from implementation utilizes the default implementation and adds the custom parameters on top.
    /// This should ensure, that a config is always valid, even if only one parameter is set manually.
    fn try_from(map: HashMap<String, String>) -> Result<Self, Self::Error> {
        let mut tp = TreeBoosterParametersBuilder::default();
        let mut linp = LinearBoosterParametersBuilder::default();
        let mut lp = LearningTaskParametersBuilder::default();
        let mut bp = BoosterParametersBuilder::default();

        // iterate over all keys of the map
        for (key, value) in map {
            let _ = match key.as_str() {
                "booster" => {
                    let bt = BoosterType::try_from(value.as_str()).map_err(|_| {
                        MachineLearningError::UnknownBoosterType { s: value.clone() }
                    })?;
                    bp.booster_type(bt);
                    Ok(())
                }
                "eta" => {
                    let eta = value
                        .parse::<f32>()
                        .map_err(|_| MachineLearningError::InvalidEtaInput { s: value.clone() })?;
                    if !(0.0..=1.0).contains(&eta) {
                        return Err(MachineLearningError::InvalidEtaInput { s: value });
                    }
                    tp.eta(eta);
                    Ok(())
                }
                "hist" | "tree_method" => {
                    let tree_method = TreeMethod::try_from(value.as_str())?;
                    tp.tree_method(tree_method);
                    Ok(())
                }
                "process_type" => {
                    let p = ProcessType::try_from(value.as_str())?;
                    tp.process_type(p);
                    Ok(())
                }
                "max_depth" => {
                    let max_depth = value.parse::<u32>().map_err(|_| {
                        MachineLearningError::InvalidMaxDepthInput { s: value.clone() }
                    })?;
                    tp.max_depth(max_depth);
                    Ok(())
                }
                "objective" => {
                    let objective = Objective::try_from(value.as_str())?;
                    lp.objective(objective);
                    Ok(())
                }
                "silent" => {
                    let silent = value.parse::<bool>().map_err(|_| {
                        MachineLearningError::InvalidDebugLevelInput { s: value.clone() }
                    })?;
                    bp.silent(silent);
                    Ok(())
                }
                "colsample_bylevel" => {
                    let colsample_bylevel = value.parse::<f32>().map_err(|_| {
                        MachineLearningError::InvalidColsampleByLevelInput { s: value.clone() }
                    })?;
                    tp.colsample_bylevel(colsample_bylevel);
                    Ok(())
                }
                "colsample_bytree" => {
                    let colsample_bytree = value.parse::<f32>().map_err(|_| {
                        MachineLearningError::InvalidColsampleByTreeInput { s: value.clone() }
                    })?;
                    tp.colsample_bytree(colsample_bytree);
                    Ok(())
                }
                "colsample_bynode" => {
                    let colsample_bynode = value.parse::<f32>().map_err(|_| {
                        MachineLearningError::InvalidColsampleByNodeInput { s: value.clone() }
                    })?;
                    tp.colsample_bynode(colsample_bynode);
                    Ok(())
                }
                "sketch_eps" => {
                    let sketch_eps = value.parse::<f32>().map_err(|_| {
                        MachineLearningError::InvalidSketchEpsInput { s: value.clone() }
                    })?;
                    tp.sketch_eps(sketch_eps);
                    Ok(())
                }
                "max_bin" => {
                    let max_bin = value.parse::<u32>().map_err(|_| {
                        MachineLearningError::InvalidMaxBinInput { s: value.clone() }
                    })?;
                    tp.max_bin(max_bin);
                    Ok(())
                }
                "max_delta_step" => {
                    let max_delta_step = value.parse::<f32>().map_err(|_| {
                        MachineLearningError::InvalidMaxDeltaStepInput { s: value.clone() }
                    })?;
                    tp.max_delta_step(max_delta_step);
                    Ok(())
                }
                "num_parallel_tree" => {
                    let num_parallel_tree = value.parse::<u32>().map_err(|_| {
                        MachineLearningError::InvalidNumParallelTreeInput { s: value.clone() }
                    })?;
                    tp.num_parallel_tree(num_parallel_tree);
                    Ok(())
                }
                "lambda" => {
                    let lambda = value.parse::<f32>().map_err(|_| {
                        MachineLearningError::InvalidLambdaInput { s: value.clone() }
                    })?;
                    linp.lambda(lambda);
                    Ok(())
                }
                "alpha" => {
                    let alpha = value.parse::<f32>().map_err(|_| {
                        MachineLearningError::InvalidAlphaInput { s: value.clone() }
                    })?;
                    linp.alpha(alpha);
                    Ok(())
                }
                "predictor" => {
                    let predictor = Predictor::try_from(value.as_str())?;
                    tp.predictor(predictor);
                    Ok(())
                }
                "grow_policy" => {
                    let grow_policy = GrowPolicy::try_from(value.as_str()).map_err(|_| {
                        MachineLearningError::UnknownGrowPolicy { s: value.clone() }
                    })?;
                    tp.grow_policy(grow_policy);
                    Ok(())
                }
                "max_leaves" => {
                    let max_leaves = value.parse::<u32>().map_err(|_| {
                        MachineLearningError::InvalidMaxLeavesInput { s: value.clone() }
                    })?;
                    tp.max_leaves(max_leaves);
                    Ok(())
                }
                "refresh_leaf" => match value.as_str() {
                    "1" | "true" => {
                        tp.refresh_leaf(true);
                        Ok(())
                    }
                    "0" | "false" => {
                        tp.refresh_leaf(false);
                        Ok(())
                    }
                    _ => Err(MachineLearningError::InvalidRefreshLeafInput { s: value.clone() }),
                },
                "min_child_weight" => {
                    let min_child_weight = value.parse::<f32>().map_err(|_| {
                        MachineLearningError::InvalidMinChildWeightInput { s: value.clone() }
                    })?;
                    tp.min_child_weight(min_child_weight);
                    Ok(())
                }
                "subsample" => {
                    let subsample = value.parse::<f32>().map_err(|_| {
                        MachineLearningError::InvalidSubsampleInput { s: value.clone() }
                    })?;
                    tp.subsample(subsample);
                    Ok(())
                }
                "scale_pos_weight" => {
                    let scale_pos_weight = value.parse::<f32>().map_err(|_| {
                        MachineLearningError::InvalidScalePosWeightInput { s: value.clone() }
                    })?;
                    tp.scale_pos_weight(scale_pos_weight);
                    Ok(())
                }
                "base_score" => {
                    let base_score = value.parse::<f32>().map_err(|_| {
                        MachineLearningError::InvalidBaseScoreInput { s: value.clone() }
                    })?;
                    lp.base_score(base_score);
                    Ok(())
                }
                "gamma" => {
                    let gamma = value.parse::<f32>().map_err(|_| {
                        MachineLearningError::InvalidGammaInput { s: value.clone() }
                    })?;
                    tp.gamma(gamma);
                    Ok(())
                }
                "seed" => {
                    let seed = value
                        .parse::<u64>()
                        .map_err(|_| MachineLearningError::InvalidSeedInput { s: value.clone() })?;
                    lp.seed(seed);
                    Ok(())
                }
                "num_class" => {
                    let num_class = value.parse::<u32>().map_err(|_| {
                        MachineLearningError::InvalidNumClassInput { s: value.clone() }
                    })?;
                    lp.num_class(Some(num_class));
                    Ok(())
                }
                _ => Err(MachineLearningError::UnknownXGBConfigParameter { s: value.clone() }),
            };
        }

        let lp = lp
            .build()
            .map_err(|_| MachineLearningError::InvalidLearningTaskParameters {
                s: "Failed to build LearningTaskParameters.".to_string(),
            })?;

        let tp = tp
            .build()
            .map_err(|_| MachineLearningError::InvalidTreeBoosterParameters {
                s: "Failed to build TreeBoosterParameters.".to_string(),
            })?;

        let finished_config = bp
            .booster_type(BoosterType::Tree(tp))
            .learning_params(lp)
            .build()
            .map_err(|_| MachineLearningError::InvalidBoosterParameters {
                s: "Failed to build BoosterParameters.".to_string(),
            })?;

        Ok(finished_config)
    }
}

enum Inclusion {
    Open,
    Closed,
}

struct Interval<T> {
    min: T,
    min_inclusion: Inclusion,
    max: T,
    max_inclusion: Inclusion,
}

impl<T: Display> Display for Interval<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let lower = match self.min_inclusion {
            Inclusion::Closed => '[',
            Inclusion::Open => '(',
        };
        let upper = match self.max_inclusion {
            Inclusion::Closed => ']',
            Inclusion::Open => ')',
        };
        write!(f, "{}{}, {}{}", lower, self.min, self.max, upper)
    }
}

impl<T: PartialOrd + Display> Interval<T> {
    fn new(min: T, min_inclusion: Inclusion, max: T, max_inclusion: Inclusion) -> Self {
        Interval {
            min,
            min_inclusion,
            max,
            max_inclusion,
        }
    }

    fn new_open_open(min: T, max: T) -> Self {
        Interval::new(min, Inclusion::Open, max, Inclusion::Open)
    }

    fn new_open_closed(min: T, max: T) -> Self {
        Interval::new(min, Inclusion::Open, max, Inclusion::Closed)
    }

    fn new_closed_closed(min: T, max: T) -> Self {
        Interval::new(min, Inclusion::Closed, max, Inclusion::Closed)
    }

    fn contains(&self, val: &T) -> bool {
        let min = match self.min_inclusion {
            Inclusion::Closed => {
                matches!(
                    val.partial_cmp(&self.min),
                    Some(Ordering::Equal | Ordering::Greater)
                )
            }
            Inclusion::Open => {
                matches!(val.partial_cmp(&self.min), Some(Ordering::Greater))
            }
        };

        let max = match self.max_inclusion {
            Inclusion::Closed => {
                matches!(
                    val.partial_cmp(&self.max),
                    Some(Ordering::Equal | Ordering::Less)
                )
            }
            Inclusion::Open => matches!(val.partial_cmp(&self.max), Some(Ordering::Less)),
        };

        min & max
    }

    fn validate(&self, val: &Option<T>, name: &str) -> Result<(), String> {
        match val {
            Some(ref val) => {
                if self.contains(val) {
                    Ok(())
                } else {
                    Err(format!(
                        "Invalid value for '{}' parameter, {} is not in range {}.",
                        name, &val, self
                    ))
                }
            }
            None => Ok(()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum BoosterType {
    Tree(TreeBoosterParameters),

    Linear(LinearBoosterParameters),
}

impl Default for BoosterType {
    fn default() -> Self {
        BoosterType::Tree(TreeBoosterParameters::default())
    }
}

impl BoosterType {
    fn as_string_pairs(&self) -> Vec<(String, String)> {
        match *self {
            BoosterType::Tree(ref p) => p.as_string_pairs(),
            BoosterType::Linear(ref p) => p.as_string_pairs(),
        }
    }
}

impl<'a> TryFrom<&'a str> for BoosterType {
    type Error = MachineLearningError;
    fn try_from(s: &'a str) -> Result<Self, Self::Error> {
        match s {
            "gbtree" => Ok(BoosterType::Tree(TreeBoosterParameters::default())),
            "gblinear" => Ok(BoosterType::Linear(LinearBoosterParameters::default())),
            _ => Err(MachineLearningError::UnknownBoosterType { s: s.into() }),
        }
    }
}

#[derive(Copy, Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub enum Objective {
    RegLinear,
    #[default]
    RegSquaredError,
    RegLogistic,
    BinaryLogistic,
    BinaryLogisticRaw,
    GpuRegLinear,
    GpuRegLogistic,
    GpuBinaryLogistic,
    GpuBinaryLogisticRaw,
    CountPoisson,
    SurvivalCox,
    MultiSoftmax,
    MultiSoftprob,
    RankPairwise,
    RegGamma,
    RegTweedie(Option<f32>),
}

impl ToString for Objective {
    fn to_string(&self) -> String {
        match *self {
            Objective::RegLinear => "reg:linear".to_owned(),
            Objective::RegSquaredError => "reg:squarederror".to_owned(),
            Objective::RegLogistic => "reg:logistic".to_owned(),
            Objective::BinaryLogistic => "binary:logistic".to_owned(),
            Objective::BinaryLogisticRaw => "binary:logitraw".to_owned(),
            Objective::GpuRegLinear => "gpu:reg:linear".to_owned(),
            Objective::GpuRegLogistic => "gpu:reg:logistic".to_owned(),
            Objective::GpuBinaryLogistic => "gpu:binary:logistic".to_owned(),
            Objective::GpuBinaryLogisticRaw => "gpu:binary:logitraw".to_owned(),
            Objective::CountPoisson => "count:poisson".to_owned(),
            Objective::SurvivalCox => "survival:cox".to_owned(),
            Objective::MultiSoftmax => "multi:softmax".to_owned(), // num_class conf must also be set
            Objective::MultiSoftprob => "multi:softprob".to_owned(), // num_class conf must also be set
            Objective::RankPairwise => "rank:pairwise".to_owned(),
            Objective::RegGamma => "reg:gamma".to_owned(),
            Objective::RegTweedie(_) => "reg:tweedie".to_owned(),
        }
    }
}

impl<'a> TryFrom<&'a str> for Objective {
    type Error = MachineLearningError;
    fn try_from(s: &'a str) -> Result<Self, Self::Error> {
        match s {
            "reg:linear" => Ok(Objective::RegLinear),
            "reg:squarederror" => Ok(Objective::RegSquaredError),
            "reg:logistic" => Ok(Objective::RegLogistic),
            "binary:logistic" => Ok(Objective::BinaryLogistic),
            "binary:logitraw" => Ok(Objective::BinaryLogisticRaw),
            "gpu:reg:linear" => Ok(Objective::GpuRegLinear),
            "gpu:reg:logistic" => Ok(Objective::GpuRegLogistic),
            "gpu:binary:logistic" => Ok(Objective::GpuBinaryLogistic),
            "gpu:binary:logitraw" => Ok(Objective::GpuBinaryLogisticRaw),
            "count:poisson" => Ok(Objective::CountPoisson),
            "survival:cox" => Ok(Objective::SurvivalCox),
            "multi:softmax" => Ok(Objective::MultiSoftmax),
            "multi:softprob" => Ok(Objective::MultiSoftprob),
            "rank:pairwise" => Ok(Objective::RankPairwise),
            "reg:gamma" => Ok(Objective::RegGamma),
            "reg:tweedie" => Ok(Objective::RegTweedie(None)),
            _ => Err(MachineLearningError::UnknownObjective { s: s.into() }),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Metrics {
    Auto,
    Custom(Vec<EvaluationMetric>),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum EvaluationMetric {
    Rmse,
    Mae,
    LogLoss,
    BinaryErrorRate(f32),
    MultiClassErrorRate,
    MultiClassLogLoss,
    Auc,
    Ndcg,
    NdcgCut(u32),
    NdcgNegative,
    NdcgCutNegative(u32),
    Map,
    MapCut(u32),
    MapNegative,
    MapCutNegative(u32),
    PoissonLogLoss,
    GammaLogLoss,
    CoxLogLoss,
    GammaDeviance,
    TweedieLogLoss,
}

impl ToString for EvaluationMetric {
    fn to_string(&self) -> String {
        match *self {
            EvaluationMetric::Rmse => "rmse".to_owned(),
            EvaluationMetric::Mae => "mae".to_owned(),
            EvaluationMetric::LogLoss => "logloss".to_owned(),
            EvaluationMetric::BinaryErrorRate(t) => {
                if (t - 0.5).abs() < std::f32::EPSILON {
                    "error".to_owned()
                } else {
                    format!("error@{t}")
                }
            }
            EvaluationMetric::MultiClassErrorRate => "merror".to_owned(),
            EvaluationMetric::MultiClassLogLoss => "mlogloss".to_owned(),
            EvaluationMetric::Auc => "auc".to_owned(),
            EvaluationMetric::Ndcg => "ndcg".to_owned(),
            EvaluationMetric::NdcgCut(n) => format!("ndcg@{n}"),
            EvaluationMetric::NdcgNegative => "ndcg-".to_owned(),
            EvaluationMetric::NdcgCutNegative(n) => format!("ndcg@{n}-"),
            EvaluationMetric::Map => "map".to_owned(),
            EvaluationMetric::MapCut(n) => format!("map@{n}"),
            EvaluationMetric::MapNegative => "map-".to_owned(),
            EvaluationMetric::MapCutNegative(n) => format!("map@{n}-"),
            EvaluationMetric::PoissonLogLoss => "poisson-nloglik".to_owned(),
            EvaluationMetric::GammaLogLoss => "gamma-nloglik".to_owned(),
            EvaluationMetric::CoxLogLoss => "cox-nloglik".to_owned(),
            EvaluationMetric::GammaDeviance => "gamma-deviance".to_owned(),
            EvaluationMetric::TweedieLogLoss => "tweedie-nloglik".to_owned(),
        }
    }
}

#[derive(Builder, Clone, Debug, Serialize, Deserialize, PartialEq)]
#[builder(build_fn(validate = "Self::validate"))]
#[builder(default)]
pub struct LearningTaskParameters {
    objective: Objective,
    base_score: f32,
    eval_metrics: Metrics,
    seed: u64,
    num_class: Option<u32>,
}

impl Default for LearningTaskParameters {
    fn default() -> Self {
        LearningTaskParameters {
            objective: Objective::default(),
            base_score: 0.5,
            eval_metrics: Metrics::Auto,
            seed: 0,
            num_class: None,
        }
    }
}

impl LearningTaskParameters {
    pub fn as_string_pairs(&self) -> Result<Vec<(String, String)>> {
        let mut v = Vec::new();

        let n = self.num_class.ok_or_else(|| {
            crate::pro::machine_learning::ml_error::MachineLearningError::UndefinedNumberOfClasses
        })?;

        match self.objective {
            Objective::MultiSoftmax | Objective::MultiSoftprob => {
                v.push(("num_class".to_owned(), n.to_string()));
            }
            Objective::RegTweedie(Some(n)) => {
                v.push(("tweedie_variance_power".to_owned(), n.to_string()));
            }
            _ => (),
        }

        v.push(("objective".to_owned(), self.objective.to_string()));
        v.push(("base_score".to_owned(), self.base_score.to_string()));
        v.push(("seed".to_owned(), self.seed.to_string()));

        if let Metrics::Custom(eval_metrics) = &self.eval_metrics {
            for metric in eval_metrics {
                v.push(("eval_metric".to_owned(), metric.to_string()));
            }
        }

        Ok(v)
    }
}

impl LearningTaskParametersBuilder {
    fn validate(&self) -> Result<(), String> {
        if let Some(Objective::RegTweedie(variance_power)) = self.objective {
            Interval::new_closed_closed(1.0, 2.0)
                .validate(&variance_power, "tweedie_variance_power")?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub enum LinearUpdate {
    #[default]
    Shotgun,

    CoordDescent,
}

impl ToString for LinearUpdate {
    fn to_string(&self) -> String {
        match *self {
            LinearUpdate::Shotgun => "shotgun".to_owned(),
            LinearUpdate::CoordDescent => "coord_descent".to_owned(),
        }
    }
}

/// `BoosterParameters` for Linear Booster.
#[derive(Builder, Clone, Debug, Serialize, Deserialize, PartialEq)]
#[builder(default)]
pub struct LinearBoosterParameters {
    lambda: f32,
    alpha: f32,
    updater: LinearUpdate,
}

impl LinearBoosterParameters {
    pub fn as_string_pairs(&self) -> Vec<(String, String)> {
        let v = vec![
            ("booster".to_owned(), "gblinear".to_owned()),
            ("lambda".to_owned(), self.lambda.to_string()),
            ("alpha".to_owned(), self.alpha.to_string()),
            ("updater".to_owned(), self.updater.to_string()),
        ];

        v
    }
}

impl Default for LinearBoosterParameters {
    fn default() -> Self {
        LinearBoosterParameters {
            lambda: 0.0,
            alpha: 0.0,
            updater: LinearUpdate::default(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub enum TreeMethod {
    #[default]
    Auto,
    Exact,
    Approx,
    Hist,
    GpuExact,
    GpuHist,
}

impl ToString for TreeMethod {
    fn to_string(&self) -> String {
        match *self {
            TreeMethod::Auto => "auto".to_owned(),
            TreeMethod::Exact => "exact".to_owned(),
            TreeMethod::Approx => "approx".to_owned(),
            TreeMethod::Hist => "hist".to_owned(),
            TreeMethod::GpuExact => "gpu_exact".to_owned(),
            TreeMethod::GpuHist => "gpu_hist".to_owned(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum TreeUpdater {
    GrowColMaker,
    DistCol,
    GrowHistMaker,
    GrowLocalHistMaker,
    GrowSkMaker,
    Sync,
    Refresh,
    Prune,
}

impl ToString for TreeUpdater {
    fn to_string(&self) -> String {
        match *self {
            TreeUpdater::GrowColMaker => "grow_colmaker".to_owned(),
            TreeUpdater::DistCol => "distcol".to_owned(),
            TreeUpdater::GrowHistMaker => "grow_histmaker".to_owned(),
            TreeUpdater::GrowLocalHistMaker => "grow_local_histmaker".to_owned(),
            TreeUpdater::GrowSkMaker => "grow_skmaker".to_owned(),
            TreeUpdater::Sync => "sync".to_owned(),
            TreeUpdater::Refresh => "refresh".to_owned(),
            TreeUpdater::Prune => "prune".to_owned(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub enum ProcessType {
    #[default]
    Default,
    Update,
}

impl ToString for ProcessType {
    fn to_string(&self) -> String {
        match *self {
            ProcessType::Default => "default".to_owned(),
            ProcessType::Update => "update".to_owned(),
        }
    }
}

impl TryFrom<&str> for ProcessType {
    type Error = MachineLearningError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "default" => Ok(ProcessType::Default),
            "update" => Ok(ProcessType::Update),
            _ => Err(MachineLearningError::UnknownProcessType { s: s.into() }),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub enum GrowPolicy {
    #[default]
    Depthwise,
    LossGuide,
}

impl ToString for GrowPolicy {
    fn to_string(&self) -> String {
        match *self {
            GrowPolicy::Depthwise => "depthwise".to_owned(),
            GrowPolicy::LossGuide => "lossguide".to_owned(),
        }
    }
}

impl<'a> TryFrom<&'a str> for GrowPolicy {
    type Error = MachineLearningError;

    fn try_from(s: &'a str) -> Result<Self, Self::Error> {
        match s {
            "depthwise" => Ok(GrowPolicy::Depthwise),
            "lossguide" => Ok(GrowPolicy::LossGuide),
            _ => Err(MachineLearningError::UnknownGrowPolicy { s: s.into() }),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub enum Predictor {
    #[default]
    Cpu,
    Gpu,
}

impl ToString for Predictor {
    fn to_string(&self) -> String {
        match *self {
            Predictor::Cpu => "cpu_predictor".to_owned(),
            Predictor::Gpu => "gpu_predictor".to_owned(),
        }
    }
}

impl<'a> TryFrom<&'a str> for Predictor {
    type Error = MachineLearningError;

    fn try_from(s: &'a str) -> Result<Self, Self::Error> {
        match s {
            "cpu_predictor" => Ok(Predictor::Cpu),
            "gpu_predictor" => Ok(Predictor::Gpu),
            _ => Err(MachineLearningError::UnknownPredictorType { s: s.into() }),
        }
    }
}

#[derive(Builder, Clone, Debug, Serialize, Deserialize, PartialEq)]
#[builder(build_fn(validate = "Self::validate"))]
#[builder(default)]
pub struct TreeBoosterParameters {
    eta: f32,
    gamma: f32,
    max_depth: u32,
    min_child_weight: f32,
    max_delta_step: f32,
    subsample: f32,
    colsample_bytree: f32,
    colsample_bylevel: f32,
    colsample_bynode: f32,
    lambda: f32,
    alpha: f32,
    tree_method: TreeMethod,
    sketch_eps: f32,
    scale_pos_weight: f32,
    updater: Vec<TreeUpdater>,
    refresh_leaf: bool,
    process_type: ProcessType,
    grow_policy: GrowPolicy,
    max_leaves: u32,
    max_bin: u32,
    num_parallel_tree: u32,
    predictor: Predictor,
}

impl Default for TreeBoosterParameters {
    fn default() -> Self {
        TreeBoosterParameters {
            eta: 0.3,
            gamma: 0.0,
            max_depth: 6,
            min_child_weight: 1.0,
            max_delta_step: 0.0,
            subsample: 1.0,
            colsample_bytree: 1.0,
            colsample_bylevel: 1.0,
            colsample_bynode: 1.0,
            lambda: 1.0,
            alpha: 0.0,
            tree_method: TreeMethod::default(),
            sketch_eps: 0.03,
            scale_pos_weight: 1.0,
            updater: Vec::new(),
            refresh_leaf: true,
            process_type: ProcessType::default(),
            grow_policy: GrowPolicy::default(),
            max_leaves: 0,
            max_bin: 256,
            num_parallel_tree: 1,
            predictor: Predictor::default(),
        }
    }
}

impl TreeBoosterParameters {
    pub fn as_string_pairs(&self) -> Vec<(String, String)> {
        let mut v = vec![
            ("booster".to_owned(), "gbtree".to_owned()),
            ("eta".to_owned(), self.eta.to_string()),
            ("gamma".to_owned(), self.gamma.to_string()),
            ("max_depth".to_owned(), self.max_depth.to_string()),
            (
                "min_child_weight".to_owned(),
                self.min_child_weight.to_string(),
            ),
            ("max_delta_step".to_owned(), self.max_delta_step.to_string()),
            ("subsample".to_owned(), self.subsample.to_string()),
            (
                "colsample_bytree".to_owned(),
                self.colsample_bytree.to_string(),
            ),
            (
                "colsample_bylevel".to_owned(),
                self.colsample_bylevel.to_string(),
            ),
            (
                "colsample_bynode".to_owned(),
                self.colsample_bynode.to_string(),
            ),
            ("lambda".to_owned(), self.lambda.to_string()),
            ("alpha".to_owned(), self.alpha.to_string()),
            ("tree_method".to_owned(), self.tree_method.to_string()),
            ("sketch_eps".to_owned(), self.sketch_eps.to_string()),
            (
                "scale_pos_weight".to_owned(),
                self.scale_pos_weight.to_string(),
            ),
            (
                "refresh_leaf".to_owned(),
                (u8::from(self.refresh_leaf)).to_string(),
            ),
            ("process_type".to_owned(), self.process_type.to_string()),
            ("grow_policy".to_owned(), self.grow_policy.to_string()),
            ("max_leaves".to_owned(), self.max_leaves.to_string()),
            ("max_bin".to_owned(), self.max_bin.to_string()),
            (
                "num_parallel_tree".to_owned(),
                self.num_parallel_tree.to_string(),
            ),
            ("predictor".to_owned(), self.predictor.to_string()),
        ];

        // Don't pass anything to XGBoost if the user didn't specify anything.
        // This allows XGBoost to figure it out on it's own, and suppresses the
        // warning message during training.
        // See: https://github.com/davechallis/rust-xgboost/issues/7
        if !self.updater.is_empty() {
            v.push((
                "updater".to_owned(),
                self.updater
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect::<Vec<String>>()
                    .join(","),
            ));
        }

        v
    }
}

impl TreeBoosterParametersBuilder {
    fn validate(&self) -> Result<(), String> {
        Interval::new_closed_closed(0.0, 1.0).validate(&self.eta, "eta")?;
        Interval::new_open_closed(0.0, 1.0).validate(&self.subsample, "subsample")?;
        Interval::new_open_closed(0.0, 1.0).validate(&self.colsample_bytree, "colsample_bytree")?;
        Interval::new_open_closed(0.0, 1.0)
            .validate(&self.colsample_bylevel, "colsample_bylevel")?;
        Interval::new_open_closed(0.0, 1.0).validate(&self.colsample_bynode, "colsample_bynode")?;
        Interval::new_open_open(0.0, 1.0).validate(&self.sketch_eps, "sketch_eps")?;
        Ok(())
    }
}

impl TryFrom<String> for TreeMethod {
    type Error = MachineLearningError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.as_str() {
            "auto" => Ok(TreeMethod::Auto),
            "exact" => Ok(TreeMethod::Exact),
            "approx" => Ok(TreeMethod::Approx),
            "hist" => Ok(TreeMethod::Hist),
            "gpu_exact" => Ok(TreeMethod::GpuExact),
            "gpu_hist" => Ok(TreeMethod::GpuHist),
            _ => Err(MachineLearningError::UnknownTreeMethod { s }),
        }
    }
}

impl<'a> TryFrom<&'a str> for TreeMethod {
    type Error = MachineLearningError;

    fn try_from(s: &'a str) -> Result<Self, Self::Error> {
        match s {
            "auto" => Ok(TreeMethod::Auto),
            "exact" => Ok(TreeMethod::Exact),
            "approx" => Ok(TreeMethod::Approx),
            "hist" => Ok(TreeMethod::Hist),
            "gpu_exact" => Ok(TreeMethod::GpuExact),
            "gpu_hist" => Ok(TreeMethod::GpuHist),
            _ => Err(MachineLearningError::UnknownTreeMethod { s: s.into() }),
        }
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use crate::pro::machine_learning::xgb_config_builder::{
        BoosterType, Objective, ProcessType, TreeMethod,
    };

    use super::*;

    #[test]
    fn tree_params() {
        let error_margin = f32::EPSILON;
        let p = TreeBoosterParameters::default();
        assert!((p.eta - 0.3).abs() < error_margin);
        let p = TreeBoosterParametersBuilder::default()
            .build()
            .expect("Could not build default tree params.");
        assert!((p.eta - 0.3).abs() < error_margin);
    }

    #[test]
    fn validate_correct_xgb_config() {
        // let the builder generate a config and store it as a hashmap of strings
        let tree_params = TreeBoosterParametersBuilder::default()
            .eta(0.75)
            .tree_method(TreeMethod::Hist)
            .process_type(ProcessType::Default)
            .max_depth(10)
            .build()
            .expect("Could not build tree params.");

        let learning_params = LearningTaskParametersBuilder::default()
            .num_class(Some(4))
            .objective(Objective::MultiSoftmax)
            .build()
            .expect("Could not build learning params.");

        // this will be passed to the xgboost crate
        let booster_params = BoosterParametersBuilder::default()
            .booster_type(BoosterType::Tree(tree_params))
            .learning_params(learning_params)
            .silent(false)
            .build()
            .expect("Could not build booster params.");

        // generate a hashmap of xgboost parameters with the corresponding setting values
        let training_config_vec = booster_params
            .as_string_pairs()
            .expect("Could not generate string pairs from booster parameter config.");
        let training_config_hm: HashMap<String, String> = training_config_vec.into_iter().collect();

        // this is the point, where the config data would be taken from the api, the builder will restore the config from the given hashmap.
        // this should ensure, that the given config is valid.
        let converted_params = BoosterParameters::try_from(training_config_hm)
            .expect("Could not convert hashmap to booster params.");

        assert_eq!(converted_params, booster_params);
    }
}
