//! Stage times and stream metrics of a run: a global tracing layer that sums the wall time of the
//! DEBUG spans by name (the stages of the map path) and the `stream_metrics` events of the
//! `StreamStatisticsAdapter` by operator path. It records only inside `record`, so the timed
//! runs do not pay for it.

use std::{
    collections::BTreeMap,
    fmt::Debug,
    sync::{
        LazyLock, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
    span,
};
use tracing_subscriber::{
    Layer,
    filter::{LevelFilter, dynamic_filter_fn},
    layer::Context,
    prelude::*,
    registry::LookupSpan,
};

/// Calls `run` `runs` times while recording and prints the mean per run under `name`.
pub fn record(name: &str, runs: u32, mut run: impl FnMut()) {
    SPANS.lock().unwrap().clear();
    STREAMS.lock().unwrap().clear();
    RECORD.store(true, Ordering::Relaxed);
    for _ in 0..runs {
        run();
    }
    RECORD.store(false, Ordering::Relaxed);
    print(name, runs);
}

/// Whether the metrics layer records; only during `record`.
static RECORD: AtomicBool = AtomicBool::new(false);
/// Sum of the wall time and count of closed spans by name
static SPANS: LazyLock<Mutex<BTreeMap<&'static str, (Duration, u64)>>> =
    LazyLock::new(Default::default);
/// Sums of the `stream_metrics` events by operator path and name
static STREAMS: LazyLock<Mutex<BTreeMap<(String, String), StreamSums>>> =
    LazyLock::new(Default::default);

#[derive(Default)]
struct StreamSums {
    streams: u64,
    elements: u64,
    output_bytes: u64,
    poll_time_us: u64,
    wall_time_us: u64,
    dropped: u64,
    failed: u64,
}

/// The fields of one `stream_metrics` event of the `StreamStatisticsAdapter`
#[derive(Default)]
struct StreamEvent {
    event: String,
    operator: String,
    path: String,
    completion: String,
    elements: u64,
    output_bytes: u64,
    poll_time_us: u64,
    wall_time_us: u64,
}

impl Visit for StreamEvent {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "elements" => self.elements = value,
            "output_bytes" => self.output_bytes = value,
            "poll_time_us" => self.poll_time_us = value,
            "wall_time_us" => self.wall_time_us = value,
            _ => {}
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "event" => self.event = value.to_string(),
            "operator" => self.operator = value.to_string(),
            "path" => self.path = value.to_string(),
            "completion" => self.completion = value.to_string(),
            _ => {}
        }
    }

    // `%path` and `%"stream_metrics"` arrive as `Display` values
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.record_str(field, &format!("{value:?}"));
    }
}

/// The span timing of wp0's `map_tile` bench plus a sink for the `stream_metrics` events.
struct Metrics;

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Metrics {
    fn on_new_span(&self, _attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(Instant::now());
        }
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let Some(start) = span.extensions().get::<Instant>().copied() else {
            return;
        };
        let mut spans = SPANS.lock().unwrap();
        let entry = spans.entry(span.name()).or_default();
        entry.0 += start.elapsed();
        entry.1 += 1;
    }

    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut fields = StreamEvent::default();
        event.record(&mut fields);
        if fields.event != "stream_metrics" {
            return;
        }
        let mut streams = STREAMS.lock().unwrap();
        let entry = streams.entry((fields.path, fields.operator)).or_default();
        entry.streams += 1;
        entry.elements += fields.elements;
        entry.output_bytes += fields.output_bytes;
        entry.poll_time_us += fields.poll_time_us;
        entry.wall_time_us += fields.wall_time_us;
        entry.dropped += u64::from(fields.completion == "dropped");
        entry.failed += u64::from(fields.completion == "failed");
    }
}

/// Installs the metrics layer as the global subscriber, recording only while `record` runs.
pub fn install() {
    // `dynamic_filter_fn` is asked per span and event (`filter_fn` caches per callsite), so
    // switching `RECORD` takes effect at once; an
    // error means another bench of this binary installed it already
    let _ = tracing_subscriber::registry()
        .with(
            Metrics.with_filter(
                dynamic_filter_fn(|metadata, _| {
                    RECORD.load(Ordering::Relaxed) && *metadata.level() <= tracing::Level::DEBUG
                })
                .with_max_level_hint(LevelFilter::DEBUG),
            ),
        )
        .try_init();
}

#[allow(clippy::cast_precision_loss, reason = "printed means")]
fn print(name: &str, runs: u32) {
    let runs = f64::from(runs);
    eprintln!("{name}: mean per run over {runs} runs");
    // a span's time is its lifetime, waits included, so overlapping spans add up beyond the run
    eprintln!(
        "  {:<40} {:>8} {:>22}",
        "span", "count", "ms (sum of lifetimes)"
    );
    for (span, (total, count)) in SPANS.lock().unwrap().iter() {
        eprintln!(
            "  {span:<40} {:>8.1} {:>22.3}",
            *count as f64 / runs,
            total.as_secs_f64() * 1000. / runs
        );
    }
    // poll ms: time in the stream's polls, its inputs' polls included, but not work on Rayon or
    // blocking threads; wall ms: first poll to end, waits for the consumer included. Both overlap
    // with the inputs' times, so they neither add up along the path nor are CPU time.
    eprintln!(
        "  {:<40} {:>8} {:>10} {:>10} {:>10} {:>10} {:>8} {:>7}",
        "operator [path]", "streams", "elements", "MB", "poll ms", "wall ms", "dropped", "failed"
    );
    for ((path, operator), s) in STREAMS.lock().unwrap().iter() {
        eprintln!(
            "  {:<40} {:>8.1} {:>10.1} {:>10.2} {:>10.3} {:>10.3} {:>8.1} {:>7.1}",
            format!("{operator} {path}"),
            s.streams as f64 / runs,
            s.elements as f64 / runs,
            s.output_bytes as f64 / runs / 1e6,
            s.poll_time_us as f64 / runs / 1e3,
            s.wall_time_us as f64 / runs / 1e3,
            s.dropped as f64 / runs,
            s.failed as f64 / runs
        );
    }
}
