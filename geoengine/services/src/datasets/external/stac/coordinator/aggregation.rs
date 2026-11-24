//! Planning of window searches with the STAC aggregation extension.
//!
//! One `datetime_frequency` request tells which times have items at all. A step without any
//! item can then be stored as empty for every cell, and its search is skipped.
use super::StacClient;
use crate::datasets::external::stac::common::closed_end;
use crate::util::{format_stac_wgs84_bbox, retry::RetryPolicy};
use chrono::DateTime;
use geoengine_datatypes::primitives::{
    BoundingBox2D, TimeGranularity, TimeInstance, TimeInterval, TimeStep,
};
use serde::Deserialize;
use tracing::debug;
use url::Url;

/// Statuses with which a server says it does not know aggregations or the request. A request
/// with one of these answers is not retried.
const UNSUPPORTED_STATUSES: [u16; 5] = [400, 404, 405, 422, 501];

/// The bucket of a time step, the finest calendar interval of STAC that is not finer than the
/// step, with its STAC name.
fn bucket_of(step: TimeStep) -> Option<(TimeStep, &'static str)> {
    let (granularity, name) = match step.granularity {
        TimeGranularity::Hours => (TimeGranularity::Hours, "hour"),
        TimeGranularity::Days => (TimeGranularity::Days, "day"),
        TimeGranularity::Months | TimeGranularity::Years => (TimeGranularity::Months, "month"),
        TimeGranularity::Millis | TimeGranularity::Seconds | TimeGranularity::Minutes => {
            return None;
        }
    };
    Some((
        TimeStep {
            granularity,
            step: 1,
        },
        name,
    ))
}

#[derive(Debug, Deserialize)]
struct Response {
    aggregations: Vec<Aggregation>,
}

#[derive(Debug, Deserialize)]
struct Aggregation {
    name: String,
    #[serde(default)]
    buckets: Vec<Bucket>,
    /// Frequency of buckets the server left out. Then counts are missing and nothing can be said.
    #[serde(default)]
    overflow: f64,
}

#[derive(Debug, Deserialize)]
struct Bucket {
    key: String,
    frequency: f64,
}

// This `impl` is here and not next to the struct, so that the request, the types of its answer
// and the check of the buckets stay in one module.
impl StacClient {
    /// The intervals in `range` that have at least one item within `bbox`, from the
    /// `datetime_frequency` aggregation, or `None` if the server cannot tell. A step that overlaps
    /// none of them has no item in `bbox`. Footprint requests use it to skip searches.
    ///
    /// Anything unexpected gives `None`, because a wrongly empty step would hide data.
    pub(crate) async fn datetime_frequency(
        &self,
        aggregate_url: Url,
        bbox: BoundingBox2D,
        range: TimeInterval,
        step: TimeStep,
    ) -> Option<Vec<TimeInterval>> {
        let (bucket, bucket_name) = bucket_of(step)?;
        let datetime = format!(
            "{}/{}",
            range
                .start()
                .as_date_time()?
                .to_datetime_string_with_millis(),
            closed_end(range)?,
        );
        let request = self.get(aggregate_url).query(&[
            ("bbox", format_stac_wgs84_bbox(bbox)),
            ("datetime", datetime),
            ("aggregations", "datetime_frequency".to_owned()),
            ("datetime_frequency_interval", bucket_name.to_owned()),
        ]);

        let response = self
            .execute_json::<Response>(
                request,
                // planning is optional, so a failing server costs one short retry, not a long backoff
                &RetryPolicy::new()
                    .max_retries(1)
                    .initial_delay_ms(200)
                    .stop_on_status(&UNSUPPORTED_STATUSES),
            )
            .await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                // not an error: the caller searches instead, so a request that can be answered
                // without the aggregation does not fail
                debug!("STAC aggregation is not used: {error}");
                return None;
            }
        };

        let aggregation = response
            .aggregations
            .into_iter()
            .find(|aggregation| aggregation.name == "datetime_frequency")?;
        if aggregation.overflow > 0. {
            return None;
        }

        checked_intervals_with_items(&aggregation.buckets, bucket)
    }
}

/// The buckets with items. Only providers whose server is known to answer in the bucket size it
/// is asked for use the aggregation (`datetime_frequency_aggregation` of the provider), because
/// an answer does not tell the size of its buckets, and a larger bucket read as a smaller one
/// would make steps empty that have items.
///
/// As a guard against a provider that is set up wrongly, the distance of the keys is checked: a
/// server sends the buckets from the first to the last one with items, with the empty ones in
/// between. A single bucket cannot be checked. No bucket at all means no item.
fn checked_intervals_with_items(buckets: &[Bucket], bucket: TimeStep) -> Option<Vec<TimeInterval>> {
    let intervals = buckets
        .iter()
        .map(|bucket_of_server| {
            let start = DateTime::parse_from_rfc3339(&bucket_of_server.key).ok()?;
            let start = TimeInstance::from_millis(start.timestamp_millis()).ok()?;
            TimeInterval::new(start, (start + bucket).ok()?).ok()
        })
        .collect::<Option<Vec<_>>>()?;

    if !intervals
        .windows(2)
        .all(|pair| pair[0].end() == pair[1].start())
    {
        debug!("STAC aggregation is not used: its buckets are not {bucket:?}");
        return None;
    }

    Some(
        intervals
            .into_iter()
            .zip(buckets)
            .filter(|(_, bucket_of_server)| bucket_of_server.frequency > 0.)
            .map(|(interval, _)| interval)
            .collect(),
    )
}

/// Whether any of the intervals overlaps the time step.
pub(crate) fn step_has_items(step: TimeInterval, intervals_with_items: &[TimeInterval]) -> bool {
    intervals_with_items
        .iter()
        .any(|interval| interval.start() < step.end() && step.start() < interval.end())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn day(day: u8) -> TimeInterval {
        let start = geoengine_datatypes::primitives::DateTime::new_utc(2026, 1, day, 0, 0, 0);
        let end = geoengine_datatypes::primitives::DateTime::new_utc(2026, 1, day + 1, 0, 0, 0);
        TimeInterval::new(start, end).unwrap()
    }

    #[test]
    fn it_finds_steps_that_overlap_a_bucket() {
        let buckets = [day(3), day(9)];

        assert!(step_has_items(day(3), &buckets));
        assert!(!step_has_items(day(4), &buckets));
        // a step that starts later than the bucket still overlaps it
        let two_days = TimeInterval::new(day(2).start(), day(4).start()).unwrap();
        assert!(step_has_items(two_days, &buckets));
        // the end of a bucket is not part of it
        assert!(!step_has_items(day(10), &buckets));
        assert!(!step_has_items(day(1), &[]));
    }

    #[test]
    fn it_chooses_the_bucket_of_a_time_step() {
        let step = |granularity, step| TimeStep { granularity, step };

        assert_eq!(
            bucket_of(step(TimeGranularity::Hours, 6)),
            Some((step(TimeGranularity::Hours, 1), "hour"))
        );
        assert_eq!(
            bucket_of(step(TimeGranularity::Days, 5)),
            Some((step(TimeGranularity::Days, 1), "day"))
        );
        assert_eq!(
            bucket_of(step(TimeGranularity::Years, 1)),
            Some((step(TimeGranularity::Months, 1), "month"))
        );
        assert_eq!(bucket_of(step(TimeGranularity::Minutes, 10)), None);
    }

    fn buckets(keys_and_frequencies: &[(&str, f64)]) -> Vec<Bucket> {
        keys_and_frequencies
            .iter()
            .map(|(key, frequency)| Bucket {
                key: format!("2026-{key}T00:00:00.000Z"),
                frequency: *frequency,
            })
            .collect()
    }

    const ONE_DAY: TimeStep = TimeStep {
        granularity: TimeGranularity::Days,
        step: 1,
    };

    #[test]
    fn it_reads_day_buckets_with_the_empty_buckets_between_them() {
        // the shape of CODE-DE: no buckets before the first item, empty ones in between
        let answer = buckets(&[("01-03", 6.), ("01-04", 0.), ("01-05", 0.), ("01-06", 2.)]);
        assert_eq!(
            checked_intervals_with_items(&answer, ONE_DAY),
            Some(vec![day(3), day(6)])
        );
    }

    #[test]
    fn it_rejects_buckets_larger_than_asked() {
        let answer = buckets(&[("01-01", 12.), ("02-01", 3.)]);
        assert_eq!(checked_intervals_with_items(&answer, ONE_DAY), None);
    }

    #[test]
    fn it_takes_a_single_bucket_as_the_size_that_was_asked_for() {
        // its size cannot be checked, the provider vouches for its server
        assert_eq!(
            checked_intervals_with_items(&buckets(&[("01-03", 1.)]), ONE_DAY),
            Some(vec![day(3)])
        );
        assert_eq!(
            checked_intervals_with_items(&buckets(&[("01-03", 0.)]), ONE_DAY),
            Some(vec![])
        );
    }

    #[test]
    fn it_no_bucket_means_no_item() {
        assert_eq!(checked_intervals_with_items(&[], ONE_DAY), Some(vec![]));
    }
}
