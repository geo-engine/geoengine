//! Planning of window searches with the STAC aggregation extension.
//!
//! One `datetime_frequency` request tells which times have items at all. A step without any
//! item can then be stored as empty for every cell, and its search is skipped.
use super::{RequestPriority, StacClient};
use crate::util::{format_stac_wgs84_bbox, retry::RetryPolicy};
use chrono::{DateTime, Months, Utc};
use geoengine_datatypes::primitives::{
    BoundingBox2D, TimeGranularity, TimeInstance, TimeInterval, TimeStep,
};
use serde::Deserialize;
use std::time::{Duration, Instant};
use tracing::debug;
use url::Url;

/// Statuses with which a server says it does not know aggregations or the request. A request
/// with one of these answers is not retried.
const UNSUPPORTED_STATUSES: [u16; 5] = [400, 404, 405, 422, 501];
/// Only these say that the server has no aggregation at all. A 400 or 422 can come from one
/// request only, for example an interval the server does not know, so it must not switch
/// aggregation off for the other datasets of the server.
const SERVER_WITHOUT_AGGREGATION: [u16; 3] = [404, 405, 501];

/// How long a server is not asked for aggregations after it showed that it has none.
const PAUSE: Duration = Duration::from_secs(300);

/// A pause of aggregation requests that ends by itself.
#[derive(Debug, Default)]
pub(super) struct Pause(std::sync::Mutex<Option<Instant>>);

impl Pause {
    fn start_for(&self, duration: Duration) {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Instant::now() + duration);
    }

    fn start(&self) {
        self.start_for(PAUSE);
    }

    fn is_active(&self) -> bool {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some_and(|until| Instant::now() < until)
    }
}

/// The size of a bucket, the finest calendar interval that is not finer than a time step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Interval {
    Hour,
    Day,
    Month,
}

impl Interval {
    fn of(step: TimeStep) -> Option<Self> {
        match step.granularity {
            TimeGranularity::Hours => Some(Self::Hour),
            TimeGranularity::Days => Some(Self::Day),
            TimeGranularity::Months | TimeGranularity::Years => Some(Self::Month),
            TimeGranularity::Millis | TimeGranularity::Seconds | TimeGranularity::Minutes => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Hour => "hour",
            Self::Day => "day",
            Self::Month => "month",
        }
    }

    fn end(self, start: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match self {
            Self::Hour => start.checked_add_signed(chrono::Duration::hours(1)),
            Self::Day => start.checked_add_signed(chrono::Duration::days(1)),
            Self::Month => start.checked_add_months(Months::new(1)),
        }
    }
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

impl StacClient {
    /// The intervals in `range` that have at least one item within `bbox`, or `None` if the server
    /// cannot tell. A step that overlaps none of them has no item in `bbox`.
    ///
    /// Anything unexpected gives `None`, because a wrongly empty step would hide data.
    pub(crate) async fn intervals_with_items(
        &self,
        aggregate_url: Url,
        bbox: BoundingBox2D,
        range: TimeInterval,
        step: TimeStep,
    ) -> Option<Vec<TimeInterval>> {
        let channels = self
            .request_registry
            .shared(&self.origin, self.coordinator_authentication.as_ref())
            .ok()?;
        let pause = &channels.aggregation_pause;
        if pause.is_active() {
            return None;
        }
        let interval = Interval::of(step)?;
        let datetime = format!(
            "{}/{}",
            range
                .start()
                .as_date_time()?
                .to_datetime_string_with_millis(),
            range.end().as_date_time()?.to_datetime_string_with_millis(),
        );
        let request = self.get(aggregate_url).query(&[
            ("bbox", format_stac_wgs84_bbox(bbox)),
            ("datetime", datetime),
            ("aggregations", "datetime_frequency".to_owned()),
            ("datetime_frequency_interval", interval.name().to_owned()),
        ]);

        let response = self
            .execute_json_with_priority::<Response>(
                request,
                // planning is optional, so a failing server costs one short retry, not a long backoff
                &RetryPolicy::new()
                    .max_retries(1)
                    .initial_delay_ms(200)
                    .stop_on_status(&UNSUPPORTED_STATUSES),
                RequestPriority::Background,
            )
            .await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                if error
                    .status()
                    .is_some_and(|status| SERVER_WITHOUT_AGGREGATION.contains(&status.as_u16()))
                {
                    pause.start();
                }
                debug!("STAC aggregation is not used: {error}");
                return None;
            }
        };

        let Some(aggregation) = response
            .aggregations
            .into_iter()
            .find(|aggregation| aggregation.name == "datetime_frequency")
        else {
            pause.start();
            return None;
        };
        if aggregation.overflow > 0. {
            return None;
        }

        aggregation
            .buckets
            .iter()
            .filter(|bucket| bucket.frequency > 0.)
            .map(|bucket| {
                let start = DateTime::parse_from_rfc3339(&bucket.key)
                    .ok()?
                    .with_timezone(&Utc);
                TimeInterval::new(
                    TimeInstance::from_millis(start.timestamp_millis()).ok()?,
                    TimeInstance::from_millis(interval.end(start)?.timestamp_millis()).ok()?,
                )
                .ok()
            })
            .collect()
    }
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
    fn a_pause_ends_by_itself() {
        let pause = Pause::default();
        assert!(!pause.is_active());

        pause.start_for(Duration::from_secs(3600));
        assert!(pause.is_active());

        pause.start_for(Duration::ZERO);
        assert!(!pause.is_active());
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
    fn it_chooses_the_bucket_size_of_a_time_step() {
        let step = |granularity, step| TimeStep { granularity, step };

        assert_eq!(
            Interval::of(step(TimeGranularity::Hours, 6)),
            Some(Interval::Hour)
        );
        assert_eq!(
            Interval::of(step(TimeGranularity::Days, 5)),
            Some(Interval::Day)
        );
        assert_eq!(
            Interval::of(step(TimeGranularity::Years, 1)),
            Some(Interval::Month)
        );
        assert_eq!(Interval::of(step(TimeGranularity::Minutes, 10)), None);
    }

    #[test]
    fn it_ends_a_month_bucket_at_the_next_month() {
        let start = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        assert_eq!(
            Interval::Month.end(start).unwrap().to_rfc3339(),
            "2026-02-01T00:00:00+00:00"
        );
        assert_eq!(
            Interval::Day.end(start).unwrap().to_rfc3339(),
            "2026-01-02T00:00:00+00:00"
        );
    }
}
