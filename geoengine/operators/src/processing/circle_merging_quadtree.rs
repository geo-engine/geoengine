mod aggregates;
mod circle_of_points;
mod circle_radius_model;
mod grid;
mod hash_map;
mod node;
mod operator;
mod quadtree;

pub use operator::{
    InitializedVisualPointClustering, VisualPointClustering, VisualPointClusteringParams,
};

/// Compute a weighted mean using parts of Welford's incremental algorithm
fn weighted_mean(a_value: f64, a_n: usize, b_value: f64, b_n: usize) -> f64 {
    let total_n = (a_n + b_n) as f64;
    let b_n = b_n as f64;
    a_value + (b_n / total_n) * (b_value - a_value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::float_cmp)]
    fn it_computes_a_weighted_mean() {
        assert_eq!(weighted_mean(4., 1, 8., 1), 6.);
        assert_eq!(weighted_mean(4., 3, 8., 1), 5.);
    }
}
