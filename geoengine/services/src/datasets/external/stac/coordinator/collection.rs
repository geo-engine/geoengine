//! The temporal extent of a STAC collection, the time bounds of its datasets.
use super::StacClient;
use crate::util::retry::RetryPolicy;
use chrono::DateTime;
use geoengine_datatypes::primitives::{TimeInstance, TimeInterval};
use serde::Deserialize;
use tracing::debug;
use url::Url;

#[derive(Debug, Deserialize)]
struct Collection {
    extent: Extent,
}

#[derive(Debug, Deserialize)]
struct Extent {
    temporal: TemporalExtent,
}

#[derive(Debug, Deserialize)]
struct TemporalExtent {
    /// The first interval spans all others; `null` is an open end.
    interval: Vec<[Option<String>; 2]>,
}

impl StacClient {
    /// The time of the collection at `collection_url` from its temporal extent, with open ends as
    /// the earliest and latest instant. `None` if the server cannot tell, which the caller does not
    /// keep, so a later request asks again.
    pub(crate) async fn collection_time_bounds(
        &self,
        collection_url: Url,
    ) -> Result<Option<TimeInterval>, ()> {
        let collection = self
            .execute_json::<Collection>(
                self.get(collection_url),
                &RetryPolicy::new().max_retries(1).initial_delay_ms(200),
            )
            .await
            .map_err(|error| debug!("STAC collection extent is not known: {error}"))?;
        Ok(collection
            .extent
            .temporal
            .interval
            .first()
            .and_then(time_bounds))
    }
}

/// The interval of `[start, end]`, or `None` if both ends are open or a time cannot be read.
/// The end of a STAC extent is closed and that of a time interval is not, so a given end is one
/// millisecond later: an item on the end lies within, and an extent of one instant is not empty.
fn time_bounds([start, end]: &[Option<String>; 2]) -> Option<TimeInterval> {
    if start.is_none() && end.is_none() {
        return None;
    }
    let instant = |time: &Option<String>, open: TimeInstance, shift: i64| match time {
        None => Some(open),
        Some(time) => TimeInstance::from_millis(
            DateTime::parse_from_rfc3339(time)
                .ok()?
                .timestamp_millis()
                .checked_add(shift)?,
        )
        .ok(),
    };
    TimeInterval::new(
        instant(start, TimeInstance::MIN, 0)?,
        instant(end, TimeInstance::MAX, 1)?,
    )
    .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_reads_the_time_bounds_with_open_ends() {
        let time = |text: &str| {
            TimeInstance::from_millis(
                DateTime::parse_from_rfc3339(text)
                    .unwrap()
                    .timestamp_millis(),
            )
            .unwrap()
        };
        assert_eq!(
            time_bounds(&[Some("2015-06-27T00:00:00Z".to_owned()), None]),
            Some(TimeInterval::new(time("2015-06-27T00:00:00Z"), TimeInstance::MAX).unwrap())
        );
        assert_eq!(
            time_bounds(&[
                Some("2018-01-01T00:00:00Z".to_owned()),
                Some("2019-01-01T00:00:00Z".to_owned())
            ]),
            Some(
                TimeInterval::new(
                    time("2018-01-01T00:00:00Z"),
                    time("2019-01-01T00:00:00.001Z")
                )
                .unwrap()
            )
        );
        // an item on the end lies within, and an extent of one instant is not empty
        let instant = Some("2020-05-01T00:00:00Z".to_owned());
        assert_eq!(
            time_bounds(&[instant.clone(), instant]),
            Some(
                TimeInterval::new(
                    time("2020-05-01T00:00:00Z"),
                    time("2020-05-01T00:00:00.001Z")
                )
                .unwrap()
            )
        );
        assert_eq!(time_bounds(&[None, None]), None);
        assert_eq!(time_bounds(&[Some("yesterday".to_owned()), None]), None);
    }
}
