use std::fmt::Display;

use geoengine_operators::util::number_statistics::NumberStatistics;
use serde::{Serialize, Serializer};

#[derive(Debug, Clone, Copy)]
pub struct TimeEstimation {
    initial_time_stamp: std::time::Instant,
    estimate_seconds_per_pct: NumberStatistics,
    time_estimate_seconds: (Option<f64>, Option<f64>),
}

impl TimeEstimation {
    pub fn new() -> Self {
        Self {
            initial_time_stamp: std::time::Instant::now(),
            estimate_seconds_per_pct: NumberStatistics::default(),
            time_estimate_seconds: (None, None),
        }
    }

    pub fn update(&mut self, time_stamp: std::time::Instant, pct_complete: f64) {
        let pct_complete = pct_complete.clamp(0., 1.);

        let elapsed_secs = (time_stamp - self.initial_time_stamp).as_secs() as f64;

        // we only update if there is progress in time and pct
        if elapsed_secs <= 0. || pct_complete <= 0. {
            return;
        }

        let seconds_per_pct = elapsed_secs / pct_complete;
        self.estimate_seconds_per_pct.add(seconds_per_pct);

        let pct_remaining = 1. - pct_complete;

        let time_estimate_seconds = {
            let seconds_remaining = pct_remaining * self.estimate_seconds_per_pct.mean();
            if seconds_remaining.is_nan() {
                None
            } else {
                Some(seconds_remaining)
            }
        };

        let time_estimate_seconds_std_dev = {
            let seconds_remaining_std_dev = pct_remaining * self.estimate_seconds_per_pct.std_dev();
            if seconds_remaining_std_dev.is_nan() {
                None
            } else {
                Some(seconds_remaining_std_dev)
            }
        };

        self.time_estimate_seconds = (time_estimate_seconds, time_estimate_seconds_std_dev);
    }

    pub fn update_now(&mut self, pct_complete: f64) {
        self.update(std::time::Instant::now(), pct_complete);
    }

    pub fn time_total(&self) -> String {
        hms_from_secs(self.initial_time_stamp.elapsed().as_secs() as f64)
    }
}

impl Display for TimeEstimation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (mean, std_dev) = self.time_estimate_seconds;

        let mean = if let Some(mean) = mean {
            hms_from_secs(mean)
        } else {
            "?".to_string()
        };

        let std_dev = if let Some(std_dev) = std_dev {
            hms_from_secs(std_dev)
        } else {
            "?".to_string()
        };

        write!(f, "{mean} (± {std_dev})",)
    }
}

impl Serialize for TimeEstimation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

fn hms_from_secs(seconds: f64) -> String {
    let secs = seconds as u64;
    let hours = secs / 3600;
    let secs = secs % 3600;
    let mins = secs / 60;
    let secs = secs % 60;
    format!("{hours:02}:{mins:02}:{secs:02}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_estimation() {
        let mut time_estimation = TimeEstimation::new();

        let next_timestamp =
            time_estimation.initial_time_stamp + std::time::Duration::from_secs(60);
        time_estimation.update(next_timestamp, 0.1);

        assert_eq!(time_estimation.to_string(), "00:09:00 (± ?)");

        let next_timestamp =
            time_estimation.initial_time_stamp + std::time::Duration::from_secs(30);
        time_estimation.update(next_timestamp, 0.2);

        assert_eq!(time_estimation.to_string(), "00:05:00 (± 00:03:00)");

        let next_timestamp =
            time_estimation.initial_time_stamp + std::time::Duration::from_secs(200);
        time_estimation.update(next_timestamp, 1.0);

        assert_eq!(time_estimation.to_string(), "00:00:00 (± 00:00:00)");
    }
}
