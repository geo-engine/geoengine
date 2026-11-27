//! Which processing graphs of a session currently wait for reads that their data source rate
//! limits, so that the UI can tell users why tiles take long, see
//! [`geoengine_operators::source::gdal_worker_process::ReadStatusListener`].

use crate::contexts::SessionId;
use crate::processing_graphs::processing_graph::ProcessingGraphId;
use dashmap::{DashMap, mapref::entry::Entry};
use geoengine_operators::source::gdal_worker_process::{ReadStatusListener, ThrottleEvent};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The rate limited reads of the queries of all sessions, by session and processing graph.
///
/// Entries only exist while a read is rate limited: each read removes itself when it ends, so the
/// registry does not grow with the number of sessions.
#[derive(Debug)]
pub struct ReadStatusRegistry {
    entries: DashMap<(SessionId, ProcessingGraphId), ThrottledReads>,
    /// An entry without a new event for this long is not reported anymore.
    expiry: Duration,
}

#[derive(Debug, Clone, Copy)]
struct ThrottledReads {
    /// A processing graph without rate limited reads has no entry.
    count: NonZeroUsize,
    since: Instant,
    last_event_at: Instant,
    last_event: ThrottleEvent,
}

/// A processing graph of a session that waits for reads its data source rate limits.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProcessingGraphReadStatus {
    pub processing_graph: ProcessingGraphId,
    /// The number of its reads that are currently rate limited.
    pub throttled_reads: NonZeroUsize,
    /// How long the first of these reads has been rate limited.
    pub throttled_for: Duration,
    /// The latest rate limit.
    pub last_event: ThrottleEvent,
}

impl ReadStatusRegistry {
    pub fn new(expiry: Duration) -> Self {
        Self {
            entries: DashMap::default(),
            expiry,
        }
    }

    /// A listener for the queries of `processing_graph` in `session`.
    pub fn listener(
        self: &Arc<Self>,
        session: SessionId,
        processing_graph: ProcessingGraphId,
    ) -> Arc<dyn ReadStatusListener> {
        Arc::new(QueryReadStatusListener {
            registry: self.clone(),
            key: (session, processing_graph),
        })
    }

    /// The processing graphs of `session` with reads that were rate limited within the expiry.
    pub fn session_status(&self, session: SessionId) -> Vec<ProcessingGraphReadStatus> {
        self.session_status_at(session, Instant::now())
    }

    fn session_status_at(
        &self,
        session: SessionId,
        now: Instant,
    ) -> Vec<ProcessingGraphReadStatus> {
        self.entries
            .iter()
            .filter(|entry| entry.key().0 == session)
            .filter(|entry| now.saturating_duration_since(entry.last_event_at) <= self.expiry)
            .map(|entry| ProcessingGraphReadStatus {
                processing_graph: entry.key().1,
                throttled_reads: entry.count,
                throttled_for: now.saturating_duration_since(entry.since),
                last_event: entry.last_event,
            })
            .collect()
    }

    fn read_throttled(
        &self,
        key: (SessionId, ProcessingGraphId),
        event: ThrottleEvent,
        first: bool,
        now: Instant,
    ) {
        match self.entries.entry(key) {
            Entry::Occupied(mut occupied) => {
                let reads = occupied.get_mut();
                if first {
                    reads.count = reads.count.saturating_add(1);
                }
                reads.last_event_at = now;
                reads.last_event = event;
            }
            // a read that is not `first` only finds no entry if it raced with its own end,
            // so it counts as a new read either way
            Entry::Vacant(vacant) => {
                vacant.insert(ThrottledReads {
                    count: NonZeroUsize::MIN,
                    since: now,
                    last_event_at: now,
                    last_event: event,
                });
            }
        }
    }

    fn throttled_read_ended(&self, key: (SessionId, ProcessingGraphId)) {
        if let Entry::Occupied(mut occupied) = self.entries.entry(key) {
            match NonZeroUsize::new(occupied.get().count.get() - 1) {
                Some(count) => occupied.get_mut().count = count,
                None => {
                    occupied.remove();
                }
            }
        }
    }
}

/// Reports the rate limited reads of one query to the [`ReadStatusRegistry`].
struct QueryReadStatusListener {
    registry: Arc<ReadStatusRegistry>,
    key: (SessionId, ProcessingGraphId),
}

impl ReadStatusListener for QueryReadStatusListener {
    fn read_throttled(&self, event: ThrottleEvent, first: bool) {
        self.registry
            .read_throttled(self.key, event, first, Instant::now());
    }

    fn throttled_read_ended(&self) {
        self.registry.throttled_read_ended(self.key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::Identifier;

    fn event(retry_in_ms: u64) -> ThrottleEvent {
        ThrottleEvent {
            http_status: 429,
            retry_in: Duration::from_millis(retry_in_ms),
        }
    }

    #[test]
    fn it_reports_the_throttled_reads_of_a_session() {
        let registry = ReadStatusRegistry::new(Duration::from_secs(30));
        let session = SessionId::new();
        let other_session = SessionId::new();
        let graph = ProcessingGraphId::new();
        let start = Instant::now();

        registry.read_throttled((session, graph), event(500), true, start);
        registry.read_throttled((session, graph), event(500), true, start);
        registry.read_throttled(
            (session, graph),
            event(1000),
            false,
            start + Duration::from_secs(1),
        );
        registry.read_throttled((other_session, graph), event(500), true, start);

        assert_eq!(
            registry.session_status_at(session, start + Duration::from_secs(2)),
            vec![ProcessingGraphReadStatus {
                processing_graph: graph,
                throttled_reads: NonZeroUsize::new(2).unwrap(),
                throttled_for: Duration::from_secs(2),
                last_event: event(1000),
            }]
        );
    }

    #[test]
    fn it_removes_a_processing_graph_when_its_last_throttled_read_ends() {
        let registry = ReadStatusRegistry::new(Duration::from_secs(30));
        let session = SessionId::new();
        let graph = ProcessingGraphId::new();
        let start = Instant::now();

        registry.read_throttled((session, graph), event(500), true, start);
        registry.read_throttled((session, graph), event(500), true, start);

        registry.throttled_read_ended((session, graph));
        assert_eq!(
            registry.session_status_at(session, start)[0].throttled_reads,
            NonZeroUsize::MIN
        );

        registry.throttled_read_ended((session, graph));
        assert_eq!(registry.session_status_at(session, start), []);
        assert!(registry.entries.is_empty());
    }

    #[test]
    fn it_hides_throttled_reads_without_events_within_the_expiry() {
        let registry = ReadStatusRegistry::new(Duration::from_secs(30));
        let session = SessionId::new();
        let graph = ProcessingGraphId::new();
        let start = Instant::now();

        registry.read_throttled((session, graph), event(500), true, start);

        assert_eq!(
            registry
                .session_status_at(session, start + Duration::from_secs(30))
                .len(),
            1
        );
        assert_eq!(
            registry.session_status_at(session, start + Duration::from_secs(31)),
            []
        );
    }

    #[test]
    fn it_reports_through_the_listener_of_a_query() {
        let registry = Arc::new(ReadStatusRegistry::new(Duration::from_secs(30)));
        let session = SessionId::new();
        let graph = ProcessingGraphId::new();
        let listener = registry.listener(session, graph);

        listener.read_throttled(event(500), true);
        assert_eq!(registry.session_status(session).len(), 1);

        listener.throttled_read_ended();
        assert_eq!(registry.session_status(session), []);
    }
}
