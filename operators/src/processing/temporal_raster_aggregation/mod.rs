mod aggregators;
mod first_last_subquery;
mod subquery;
mod temporal_aggregation_operator;

pub use temporal_aggregation_operator::{
    TemporalRasterAggregation, TemporalRasterAggregationParameters,
    TemporalRasterAggregationProcessor,
};
