use snafu::Snafu;
use std::collections::VecDeque;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Duration;

use geoengine_datatypes::raster::{GridBoundingBox2D, GridBounds, Pixel};
use ipc_channel::ipc::{IpcReceiver, IpcSender};
use rustc_hash::FxHasher;

use crate::source::gdal_worker_process::process_common::GdalIpcBytePayload;

use super::{
    GdalProcessPoolAccess,
    process_common::{GdalIpcPayload, IpcChannelMessage, IpcProcessError, IpcProcessRasterResult},
    process_impl::{ChildProcessGuard, WorkerConfig, spawn_ipc_server_process},
};

// --- Core Structural Parameters & Tuning Constants ---

/// Capacity for the broker incoming request multi-producer mpsc channel.
const BROKER_QUEUE_CAPACITY: usize = 8192;

/// Maximum number of commands to read reactively from the mpsc channel per loop iteration.
/// Prevents an incoming flood of requests from starving the dispatch execution logic.
const BATCH_RECEIVE_LIMIT: usize = 256;

/// Staggers process forks in milliseconds so the kernel handles initial
/// page-table allocations cleanly without thrashing.
const INITIAL_SPAWN_DELAY_MS: u64 = 15;

/// Latency budget warning threshold in milliseconds. If an update cycle inside the broker loop
/// blocks the execution context past this point, a performance warning trace is issued.
const THROTTLING_THRESHOLD_MS: u64 = 2;
const THROTTLING_THRESHOLD_DURATION: Duration = Duration::from_millis(THROTTLING_THRESHOLD_MS);

/// Cache Time-To-Live (TTL) threshold for affinity routing data tracking.
/// Set to 30 minutes (1800s) to prioritize heavy S3 / cloud storage network connection caches.
const CACHE_TTL_SECS: f64 = 1800.0;

// Affinity scores rank the idle workers for a request in three tiers:
// 1. Hit (>= ~SCORE_DATASET_MATCH): the worker has the dataset open and reuses it, refined by
//    band and spatial window.
// 2. Fresh (SCORE_FRESH_WORKER_DEFAULT): the worker has never read anything, so using it
//    evicts nothing.
// 3. Eviction ([0, SCORE_EVICTION_MAX]): the worker has to close its dataset. The score grows
//    with idle time, so the least recently used dataset is closed.
// Each worker holds only one open dataset, so idle workers act as an LRU cache of open datasets.

/// Baseline numeric floor used when initiating the maximum-score matrix search.
/// Below every valid score (min. 0.0), so any idle worker is chosen if one exists.
const SCORE_INITIAL_FLOOR: f64 = -1.0;

/// Base affinity score bonus awarded when a worker already has the requested dataset open.
const SCORE_DATASET_MATCH: f64 = 10000.0;

/// Additional score bonus awarded when both the dataset AND the specific raster band match.
const SCORE_BAND_MATCH: f64 = 1000.0;

/// Additional score bonus when the exact spatial window requested matches the worker's last task.
const SCORE_EXACT_WINDOW_MATCH: f64 = 100.0;

/// Maximum base bonus for nearby spatial windows to reward spatial locality.
const SCORE_NEARBY_WINDOW_MAX: f64 = 50.0;

/// Default baseline score given to completely fresh workers.
const SCORE_FRESH_WORKER_DEFAULT: f64 = 0.5;

/// Maximum score of a worker that has to evict its open dataset. Grows with idle time, so the
/// least recently used worker is evicted. Must stay below `SCORE_FRESH_WORKER_DEFAULT`.
const SCORE_EVICTION_MAX: f64 = 0.4;

/// High-affinity immediate dispatch cutoff threshold. If a candidate worker's affinity score
/// matches or exceeds this, we short-circuit the matrix evaluation instantly.
const IMMEDIATE_DISPATCH_THRESHOLD: f64 = SCORE_DATASET_MATCH + SCORE_BAND_MATCH;

/// Max lookahead horizontal threshold for sweeps over unique datasets.
const MAX_ELIGIBLE_SCAN_DEPTH: usize = 16;

/// Interval for logging the broker statistics.
const STATS_REPORT_INTERVAL: Duration = Duration::from_secs(30);

type FastHashMap<K, V> = std::collections::HashMap<K, V, BuildHasherDefault<FxHasher>>;

#[derive(Debug, Snafu, Clone)]
#[snafu(visibility(pub))]
pub enum GdalProcessPoolError {
    IpcProcessError {
        source: IpcProcessError,
    },

    #[snafu(display("IpcError caused by: {error_str}"))]
    IpcError {
        error_str: String,
    },

    WorkerPanic,
}

impl From<IpcProcessError> for GdalProcessPoolError {
    fn from(source: IpcProcessError) -> Self {
        GdalProcessPoolError::IpcProcessError { source }
    }
}

impl From<ipc_channel::IpcError> for GdalProcessPoolError {
    fn from(source: ipc_channel::IpcError) -> Self {
        GdalProcessPoolError::IpcError {
            error_str: source.to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulingStrategy {
    FifoGreedy,
    GlobalMatrix,
}

struct WorkerJob {
    request: IpcChannelMessage,
    respond_to: oneshot::Sender<ReadResult>,
    dataset_hash: u64,
}

#[derive(Debug, Clone)]
struct WorkerAffinity {
    dataset_hash: u64,
    band: usize,
    spatial_window: GridBoundingBox2D,
    timestamp: Instant,
}

impl WorkerAffinity {
    /// Computes the affinity score, decaying the reward linearly over time.
    /// `now` is passed down to bypass the expensive vDSO clock lookup bottleneck inside hot loops.
    #[inline]
    pub fn calculate_score(
        &self,
        dataset_hash: u64,
        band: usize,
        window: &GridBoundingBox2D,
        now: Instant,
    ) -> f64 {
        let idle_duration = now.saturating_duration_since(self.timestamp).as_secs_f64();

        // Cache expired or another dataset: prefer the least recently used worker for eviction.
        // A constant score would make the broker take the first candidate in `idle_workers`.
        // Released workers are pushed last and taken with `swap_remove`, so that is usually the
        // most recently used worker, whose dataset is the most likely to be requested again.
        // Instead, rank by idle time: a worker released just now scores 0.0, and a worker
        // idle for the full TTL scores `SCORE_EVICTION_MAX`.
        if idle_duration > CACHE_TTL_SECS || self.dataset_hash != dataset_hash {
            return SCORE_EVICTION_MAX * (idle_duration / CACHE_TTL_SECS).min(1.0);
        }

        let decay = 1.0 - (idle_duration / CACHE_TTL_SECS);
        let mut score = SCORE_DATASET_MATCH * decay;

        if self.band == band {
            score += SCORE_BAND_MATCH * decay;

            let dist = calculate_grid_distance(&self.spatial_window, window);
            if dist == 0.0 {
                score += SCORE_EXACT_WINDOW_MATCH * decay;
            } else {
                score += (SCORE_NEARBY_WINDOW_MAX / (1.0 + dist)) * decay;
            }
        }

        score
    }
}

struct WorkerProcess {
    _id: usize,
    job_tx: mpsc::UnboundedSender<WorkerJob>,
    child_guard: ChildProcessGuard,
    // A single, unified state representing what the GDAL process last worked on.
    affinity: Option<WorkerAffinity>,
}

impl WorkerProcess {
    #[inline]
    fn score_for_job(
        &self,
        dataset_hash: u64,
        band: usize,
        window: &GridBoundingBox2D,
        now: Instant,
    ) -> f64 {
        match &self.affinity {
            Some(affinity) => affinity.calculate_score(dataset_hash, band, window, now),
            None => SCORE_FRESH_WORKER_DEFAULT,
        }
    }
}

/// Counters of the broker's scheduling decisions, logged at debug level and reset every `STATS_REPORT_INTERVAL`.
/// A dispatch is an affinity hit iff the worker's open dataset is reused (`partial_hash` covers
/// exactly the fields `GdalDatasetHolder::is_hit` compares).
#[derive(Debug, Default)]
struct BrokerStats {
    dispatched: u64,
    affinity_hits: u64,
    fresh: u64,
    evictions: u64,
    cancelled_dropped: u64,
    max_dataset_queue_len: usize,
}

struct DatasetSlot {
    active_count: usize,
    queue: VecDeque<QueuedRequest>,
}

impl DatasetSlot {
    /// Removes canceled requests from the front of the queue and returns how many were removed.
    /// Canceled requests further back are skipped by `peek_first_valid` and removed once they
    /// reach the front, so the queue is never scanned as a whole.
    #[inline]
    pub fn clean_canceled_front(&mut self) -> u64 {
        let mut removed = 0;
        while let Some(req) = self.queue.front() {
            if req.respond_to.is_closed() {
                self.queue.pop_front();
                removed += 1;
            } else {
                break;
            }
        }
        removed
    }

    #[inline]
    pub fn peek_first_valid(&self) -> Option<&QueuedRequest> {
        self.queue.iter().find(|req| !req.respond_to.is_closed())
    }

    /// Takes the first request a caller still waits for, counting the canceled ones removed on
    /// the way. Returns `None` if all callers gave up, which can happen after `peek_first_valid`
    /// found a request, as callers cancel concurrently.
    #[inline]
    pub fn pop_next_valid(&mut self, cancelled_dropped: &mut u64) -> Option<QueuedRequest> {
        *cancelled_dropped += self.clean_canceled_front();
        self.queue.pop_front()
    }
}

struct QueuedRequest {
    dataset_hash: u64,
    request: IpcChannelMessage,
    /// Closed once the leader task drops its receiver because all callers gave up (see
    /// `await_read_or_abandon`). The broker then drops the request instead of reading it.
    respond_to: oneshot::Sender<ReadResult>,
}

/// Result of a read operation, carrying the IPC payload and the companion
/// span so callers can create `follows_from` links. The `Span` handle keeps
/// the companion span alive in the subscriber until all receivers drop it.
struct ReadResult {
    result: Result<IpcProcessRasterResult, GdalProcessPoolError>,
    companion_span: Option<tracing::Span>,
}

enum BrokerCommand {
    Read(Box<QueuedRequest>),
    ReturnWorker {
        worker_id: usize,
        dataset_hash: u64,
        band: usize,
        window: GridBoundingBox2D,
    },
    WorkerDied {
        worker_id: usize,
        dataset_hash: u64,
    },
    WorkerReplaced {
        worker_id: usize,
        child_guard: ChildProcessGuard,
        job_tx: mpsc::UnboundedSender<WorkerJob>,
    },
}

type SharedResult = Option<
    Arc<(
        Result<GdalIpcBytePayload, GdalProcessPoolError>,
        Option<tracing::Span>,
    )>,
>;

/// Sender of an in-flight read. Followers `subscribe()` to it while holding the `in_flight_tiles`
/// lock, so its receiver count only changes under that lock.
type InFlightTile = Arc<tokio::sync::watch::Sender<SharedResult>>;

pub struct GdalProcessPool {
    broker_tx: mpsc::Sender<BrokerCommand>,
    in_flight_tiles: Mutex<FastHashMap<u64, InFlightTile>>,
}

impl GdalProcessPool {
    const STRATEGY: SchedulingStrategy = SchedulingStrategy::GlobalMatrix;

    /// Initializes the GDAL process pool and spawns the broker loop in a dedicated Tokio task.
    /// The broker is responsible for all scheduling decisions and routing of requests to worker processes.
    /// The worker processes themselves are spawned in a blocking thread to avoid stalling the async runtime during fork and initialization.
    /// The number of worker processes is determined by `max_total`, while `max_active_global` and `max_parallel_per_dataset` control the scheduling constraints for concurrent active requests.
    /// Returns an `Arc` to the initialized `GdalProcessPool`, which can be cloned and shared across the application for submitting read requests.
    /// # Parameters
    /// - `max_total`: The total number of GDAL worker processes to spawn and maintain in the pool.
    /// - `max_active_global`: The maximum number of active (in-flight) requests allowed across all datasets at any given time.
    /// - `max_parallel_per_dataset`: The maximum number of active requests allowed concurrently for the same dataset, enforcing per-dataset concurrency limits.
    /// # Panics
    /// This function will panic if any of the worker processes fail to spawn successfully.
    #[allow(clippy::needless_pass_by_value)] // value is cloned into spawn block, ref would need lifetime workaround
    pub fn new_with_tokio_handle(
        handle: &tokio::runtime::Handle,
        number_of_processes: usize,
        max_active_processes: usize,
        max_dataset_processes: usize,
        worker_config: WorkerConfig,
    ) -> Arc<Self> {
        let (broker_tx, broker_rx) = mpsc::channel(BROKER_QUEUE_CAPACITY);
        let b_tx_clone = broker_tx.clone();
        let worker_config_clone = worker_config.clone();

        handle.spawn(async move {
            let b_tx_clone_2 = b_tx_clone.clone();
            let wc = worker_config_clone;
            let wc2 = wc.clone();
            let workers = tokio::task::spawn_blocking(move || {
                let mut w = Vec::with_capacity(number_of_processes);
                for id in 0..number_of_processes {
                    let (child_guard, _join_handle, job_tx) =
                        Self::create_worker_with_id(id, b_tx_clone_2.clone(), &wc)
                            .expect("Failed to create worker");

                    w.push(WorkerProcess {
                        _id: id,
                        job_tx,
                        child_guard,
                        affinity: None,
                    });
                    std::thread::sleep(Duration::from_millis(INITIAL_SPAWN_DELAY_MS));
                }
                w
            })
            .await
            .expect("Failed to initialize static GDAL worker processes");

            Self::broker_loop(
                broker_rx,
                workers,
                max_active_processes,
                max_dataset_processes,
                b_tx_clone,
                wc2,
            )
            .await;
        });

        Arc::new(Self {
            broker_tx,
            in_flight_tiles: Mutex::new(FastHashMap::default()),
        })
    }

    /// Initializes the GDAL process pool and spawns the broker loop in a dedicated Tokio task.
    /// The broker is responsible for all scheduling decisions and routing of requests to worker processes.
    /// The worker processes themselves are spawned in a blocking thread to avoid stalling the async runtime during fork and initialization.
    /// The number of worker processes is determined by `max_total`, while `max_active_global` and `max_parallel_per_dataset` control the scheduling constraints for concurrent active requests.
    /// Returns an `Arc` to the initialized `GdalProcessPool`, which can be cloned and shared across the application for submitting read requests.
    ///
    /// # Parameters
    /// - `number_of_processes`: The total number of GDAL worker processes to spawn and maintain in the pool.
    /// - `max_active_processes`: The maximum number of active (in-flight) requests allowed across all datasets at any given time.
    /// - `max_dataset_processes`: The maximum number of active requests allowed concurrently for the same dataset, enforcing per-dataset concurrency limits.
    ///
    /// # Panics
    /// This function will panic if any of the worker processes fail to spawn successfully.
    /// It is designed to be called during application initialization, and assumes that the system has sufficient resources to spawn the specified number of worker processes.
    ///
    pub fn new(
        number_of_processes: usize,
        max_active_processes: usize,
        max_dataset_processes: usize,
        worker_config: WorkerConfig,
    ) -> Arc<Self> {
        Self::new_with_tokio_handle(
            &tokio::runtime::Handle::current(),
            number_of_processes,
            max_active_processes,
            max_dataset_processes,
            worker_config,
        )
    }

    fn create_worker_with_id(
        worker_id: usize,
        broker_tx: mpsc::Sender<BrokerCommand>,
        worker_config: &WorkerConfig,
    ) -> Result<
        (
            ChildProcessGuard,
            std::thread::JoinHandle<()>,
            mpsc::UnboundedSender<WorkerJob>,
        ),
        GdalProcessPoolError,
    > {
        let mut config = worker_config.clone();
        config.logging.worker_id = worker_id;

        let (guard, tx, rx) =
            spawn_ipc_server_process::<IpcChannelMessage, IpcProcessRasterResult>(&config)?;

        let (job_tx, mut job_rx) = mpsc::unbounded_channel();

        let handle = std::thread::Builder::new()
            .name(format!("gdal-worker-companion-{worker_id}"))
            .spawn(move || {
                Self::worker_companion_loop(worker_id, &tx, &rx, &mut job_rx, &broker_tx);
            })
            .map_err(|_| GdalProcessPoolError::WorkerPanic)?;

        Ok((guard, handle, job_tx))
    }

    fn worker_companion_loop(
        worker_id: usize,
        sender: &IpcSender<IpcChannelMessage>,
        receiver: &IpcReceiver<IpcProcessRasterResult>,
        job_rx: &mut mpsc::UnboundedReceiver<WorkerJob>,
        broker_tx: &mpsc::Sender<BrokerCommand>,
    ) {
        while let Some(job) = job_rx.blocking_recv() {
            let window = job.request.0.read_advise.read_window_bounds;
            let band = job.request.0.dataset_params.rasterband_channel;
            let dataset_hash = job.dataset_hash;

            let _span = tracing::info_span!(
                "gdal_worker_companion",
                worker_id = worker_id,
                read_id = job.request.0.read_id.as_deref().unwrap_or(""),
                dataset = %job.request.0.dataset_params.file_path.display(),
                band = band,
            )
            .entered();

            let companion_span = Some(tracing::Span::current());

            let res = sender
                .send(job.request)
                .map_err(GdalProcessPoolError::from)
                .and_then(|()| receiver.recv().map_err(GdalProcessPoolError::from));

            let is_dead = res.is_err();
            let _ = job.respond_to.send(ReadResult {
                result: res,
                companion_span,
            });

            if is_dead {
                let _ = broker_tx.blocking_send(BrokerCommand::WorkerDied {
                    worker_id,
                    dataset_hash,
                });
                break;
            }
            let _ = broker_tx.blocking_send(BrokerCommand::ReturnWorker {
                worker_id,
                dataset_hash,
                band,
                window,
            });
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn broker_loop(
        mut rx: mpsc::Receiver<BrokerCommand>,
        workers: Vec<WorkerProcess>,
        max_active_processes: usize,
        max_dataset_processes: usize,
        broker_tx: mpsc::Sender<BrokerCommand>,
        worker_config: WorkerConfig,
    ) {
        let mut state = BrokerState::new(
            workers,
            max_dataset_processes,
            max_active_processes,
            Self::STRATEGY,
        );

        let mut stats_tick = tokio::time::interval(STATS_REPORT_INTERVAL);
        stats_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            let first_cmd = tokio::select! {
                cmd = rx.recv() => match cmd {
                    Some(cmd) => cmd,
                    None => break,
                },
                _ = stats_tick.tick() => {
                    state.report_and_reset_stats();
                    continue;
                }
            };
            let start_tick = Instant::now();
            let mut batch = vec![first_cmd];

            while let Ok(cmd) = rx.try_recv() {
                batch.push(cmd);
                if batch.len() >= BATCH_RECEIVE_LIMIT {
                    break;
                }
            }

            for cmd in batch {
                match cmd {
                    BrokerCommand::Read(req) => {
                        state.enqueue_request(req);
                    }
                    BrokerCommand::ReturnWorker {
                        worker_id,
                        dataset_hash,
                        band,
                        window,
                    } => {
                        state.release_worker(worker_id, dataset_hash, band, window);
                    }
                    BrokerCommand::WorkerDied {
                        worker_id,
                        dataset_hash,
                    } => {
                        tracing::error!(
                            "GDAL Worker {} died unexpectedly! Initiating respawn...",
                            worker_id
                        );
                        if let Some(slot) = state.dataset_registry.get_mut(&dataset_hash) {
                            slot.active_count = slot.active_count.saturating_sub(1);
                        }
                        state.global_active_count = state.global_active_count.saturating_sub(1);

                        let b_tx = broker_tx.clone();
                        let wc = worker_config.clone();
                        tokio::task::spawn_blocking(move || {
                            std::thread::sleep(std::time::Duration::from_millis(
                                INITIAL_SPAWN_DELAY_MS,
                            ));

                            match Self::create_worker_with_id(worker_id, b_tx.clone(), &wc) {
                                Ok((child_guard, _join_handle, job_tx)) => {
                                    tracing::info!(
                                        "Successfully respawned GDAL Worker {}",
                                        worker_id
                                    );
                                    if let Err(e) =
                                        b_tx.blocking_send(BrokerCommand::WorkerReplaced {
                                            worker_id,
                                            child_guard,
                                            job_tx,
                                        })
                                    {
                                        tracing::error!(
                                            "Failed to send WorkerReplaced for GDAL Worker {worker_id}: {e:?}",
                                        );
                                    }
                                }
                                Err(e) => {
                                    // Prevent silent failures if the respawn itself crashes
                                    tracing::error!(
                                        "FATAL: Failed to respawn GDAL Worker {}: {:?}",
                                        worker_id,
                                        e
                                    );
                                }
                            }
                        });
                    }
                    BrokerCommand::WorkerReplaced {
                        worker_id,
                        child_guard,
                        job_tx,
                    } => {
                        if let Some(worker) = state.workers.get_mut(worker_id) {
                            worker.child_guard = child_guard;
                            worker.job_tx = job_tx;
                            worker.affinity = None;
                        }
                        state.idle_workers.push(worker_id);
                    }
                }
            }

            state.try_dispatch();

            let elapsed = start_tick.elapsed();
            if elapsed > THROTTLING_THRESHOLD_DURATION {
                let total_pending: usize =
                    state.dataset_registry.values().map(|s| s.queue.len()).sum();
                tracing::warn!(
                    duration_ms = elapsed.as_secs_f64() * 1000.0,
                    queue_len = total_pending,
                    "GDAL broker loop iteration safety warning!"
                );
            }
        }
        tracing::error!("GDAL broker loop: channel closed, exiting");
    }
}

// Encapsulates all state variables and synchronous routing logic inside the broker.
struct BrokerState {
    /// Unordered: `swap_remove` reorders it, so recency comes from `WorkerAffinity::timestamp`.
    idle_workers: Vec<usize>,
    workers: Vec<WorkerProcess>,
    dataset_registry: FastHashMap<u64, DatasetSlot>,
    active_datasets: VecDeque<u64>,
    global_active_count: usize,
    max_dataset_processes: usize,
    max_active_processes: usize,
    strategy: SchedulingStrategy,
    stats: BrokerStats,
}

impl BrokerState {
    pub fn new(
        workers: Vec<WorkerProcess>,
        max_dataset_processes: usize,
        max_active_processes: usize,
        strategy: SchedulingStrategy,
    ) -> Self {
        let idle_workers = (0..workers.len()).collect();
        Self {
            idle_workers,
            workers,
            dataset_registry: FastHashMap::default(),
            active_datasets: VecDeque::new(),
            global_active_count: 0,
            max_dataset_processes,
            max_active_processes,
            strategy,
            stats: BrokerStats::default(),
        }
    }

    fn report_and_reset_stats(&mut self) {
        let stats = std::mem::take(&mut self.stats);
        if stats.dispatched == 0 && stats.cancelled_dropped == 0 {
            return;
        }
        tracing::debug!(
            target: "gdal_pool_stats",
            dispatched = stats.dispatched,
            affinity_hits = stats.affinity_hits,
            fresh = stats.fresh,
            evictions = stats.evictions,
            cancelled_dropped = stats.cancelled_dropped,
            max_dataset_queue_len = stats.max_dataset_queue_len,
            "GDAL process pool statistics"
        );
    }

    #[inline]
    // on BrokerCommand::Read
    pub fn enqueue_request(&mut self, req: Box<QueuedRequest>) {
        if req.respond_to.is_closed() {
            self.stats.cancelled_dropped += 1;
            return;
        }
        let hash = req.dataset_hash;
        let slot = self
            .dataset_registry
            .entry(hash)
            .or_insert_with(|| DatasetSlot {
                active_count: 0,
                queue: VecDeque::new(),
            });

        if slot.queue.is_empty() {
            self.active_datasets.push_back(hash);
        }
        slot.queue.push_back(*req);
        self.stats.max_dataset_queue_len = self.stats.max_dataset_queue_len.max(slot.queue.len());
    }

    #[inline]
    pub fn release_worker(
        &mut self,
        worker_id: usize,
        dataset_hash: u64,
        band: usize,
        window: GridBoundingBox2D,
    ) {
        if let Some(slot) = self.dataset_registry.get_mut(&dataset_hash) {
            slot.active_count = slot.active_count.saturating_sub(1);
        }
        self.global_active_count = self.global_active_count.saturating_sub(1);

        if let Some(worker) = self.workers.get_mut(worker_id) {
            worker.affinity = Some(WorkerAffinity {
                dataset_hash,
                band,
                spatial_window: window,
                timestamp: Instant::now(),
            });
        }

        self.idle_workers.push(worker_id);
    }

    /// Single-pass scheduler execution pass. Inlines scheduling decisions to prevent matrix re-scanning.
    #[allow(clippy::too_many_lines)]
    #[inline]
    pub fn try_dispatch(&mut self) {
        while let Some(&front_hash) = self.active_datasets.front() {
            let should_pop = match self.dataset_registry.get_mut(&front_hash) {
                Some(slot) => {
                    self.stats.cancelled_dropped += slot.clean_canceled_front();
                    slot.queue.is_empty()
                }
                None => true,
            };
            if should_pop {
                self.active_datasets.pop_front();
            } else {
                break;
            }
        }

        // Call the clock precisely ONCE per batch processing window
        let now = Instant::now();

        while !self.idle_workers.is_empty() && !self.active_datasets.is_empty() {
            if self.global_active_count >= self.max_active_processes {
                break;
            }

            let mut best_score = SCORE_INITIAL_FLOOR;
            let mut best_w_matrix_idx = 0;
            let mut best_dataset_hash = 0;
            let mut best_dataset_active_idx = 0;
            let mut datasets_scanned = 0;

            for (idx, &hash) in self.active_datasets.iter().enumerate() {
                let slot = self
                    .dataset_registry
                    .get(&hash)
                    .expect("Active dataset missing from registry - invariant broken");
                if slot.active_count >= self.max_dataset_processes {
                    continue;
                }

                let Some(req) = slot.peek_first_valid() else {
                    continue;
                };

                datasets_scanned += 1;
                let window = &req.request.0.read_advise.read_window_bounds;
                let band = req.request.0.dataset_params.rasterband_channel;

                let mut current_best_w_idx = 0;
                let mut current_best_score = SCORE_INITIAL_FLOOR;

                for (w_idx, &w_id) in self.idle_workers.iter().enumerate() {
                    let w = self
                        .workers
                        .get(w_id)
                        .expect("Idle worker ID missing from registry - invariant broken");
                    let score = w.score_for_job(hash, band, window, now);

                    if score > current_best_score {
                        current_best_score = score;
                        current_best_w_idx = w_idx;
                    }
                    if score >= IMMEDIATE_DISPATCH_THRESHOLD {
                        break;
                    }
                }

                if current_best_score > best_score {
                    best_score = current_best_score;
                    best_w_matrix_idx = current_best_w_idx;
                    best_dataset_hash = hash;
                    best_dataset_active_idx = idx;
                }

                if best_score >= IMMEDIATE_DISPATCH_THRESHOLD {
                    break;
                }

                if self.strategy == SchedulingStrategy::FifoGreedy && best_score >= 0.0 {
                    break;
                }

                if datasets_scanned >= MAX_ELIGIBLE_SCAN_DEPTH {
                    break;
                }
            }

            if best_score == SCORE_INITIAL_FLOOR {
                break;
            }

            // Perform atomic dispatch operation
            let slot = self
                .dataset_registry
                .get_mut(&best_dataset_hash)
                .expect("Selected dataset missing from registry - invariant broken");

            // Take the request before the worker: its callers may have given up since the scan
            let Some(req) = slot.pop_next_valid(&mut self.stats.cancelled_dropped) else {
                self.active_datasets.remove(best_dataset_active_idx);
                continue;
            };

            let worker_id = self.idle_workers.swap_remove(best_w_matrix_idx);
            let worker = self
                .workers
                .get_mut(worker_id)
                .expect("Selected worker ID missing from registry - invariant broken");

            self.global_active_count += 1;
            slot.active_count += 1;

            let affinity = match &worker.affinity {
                None => {
                    self.stats.fresh += 1;
                    "fresh"
                }
                Some(a) if a.dataset_hash == best_dataset_hash => {
                    self.stats.affinity_hits += 1;
                    "hit"
                }
                Some(_) => {
                    self.stats.evictions += 1;
                    "evict"
                }
            };
            self.stats.dispatched += 1;

            {
                let _span = tracing::debug_span!(
                    "gdal_broker_dispatch",
                    worker_id = worker_id,
                    affinity = affinity,
                    dataset = %req.request.0.dataset_params.file_path.display(),
                    read_id = req.request.0.read_id.as_deref().unwrap_or(""),
                )
                .entered();

                let _ = worker.job_tx.send(WorkerJob {
                    request: req.request,
                    respond_to: req.respond_to,
                    dataset_hash: req.dataset_hash,
                });
            }

            self.stats.cancelled_dropped += slot.clean_canceled_front();

            // Maintain FIFO/Round-Robin dataset cycling fairness
            let tracked_hash = self
                .active_datasets
                .remove(best_dataset_active_idx)
                .expect("Best dataset hash missing from active queue - invariant broken");
            if !slot.queue.is_empty() {
                self.active_datasets.push_back(tracked_hash);
            }
        }
    }
}

#[inline]
fn calculate_grid_distance(a: &GridBoundingBox2D, b: &GridBoundingBox2D) -> f64 {
    let a_min = a.min_index();
    let b_min = b.min_index();
    let dy = (a_min.y() - b_min.y()).abs() as f64;
    let dx = (a_min.x() - b_min.x()).abs() as f64;
    dy.max(dx)
}

#[derive(Clone)]
pub struct GdalPoolDispatcher {
    pool: Arc<GdalProcessPool>,
}

struct LeaderCleanupGuard {
    pool: std::sync::Arc<GdalProcessPool>,
    tile_key: u64,
    tx: InFlightTile,
}

impl Drop for LeaderCleanupGuard {
    fn drop(&mut self) {
        // This is guaranteed to run whether the function succeeds, errors,
        // panics, or is abruptly cancelled by Tokio.
        if let Ok(mut lock) = self.pool.in_flight_tiles.lock() {
            remove_in_flight(&mut lock, self.tile_key, &self.tx);
        }
    }
}

/// Removes the entry only if it still belongs to `tx`, never a newer leader's entry for the same key.
fn remove_in_flight(map: &mut FastHashMap<u64, InFlightTile>, tile_key: u64, tx: &InFlightTile) {
    if map.get(&tile_key).is_some_and(|t| Arc::ptr_eq(t, tx)) {
        map.remove(&tile_key);
    }
}

/// Waits for the broker's result. Returns `None` if all callers gave up first: dropping
/// `oneshot_rx` then lets the broker discard the request if it is still queued.
///
/// Every caller, including the one that spawned the leader, holds a receiver of `tx`. A caller
/// gives up when its future is dropped, e.g. when the client aborts the tile request after a
/// pan. If the leader kept `oneshot_rx` until the read finished, the broker would never see a
/// closed request and would read every abandoned tile before the visible ones.
///
/// Abandoning only helps while the request is queued. If a worker already reads it, the read
/// runs to completion and its result is discarded.
async fn await_read_or_abandon(
    pool: &GdalProcessPool,
    tile_key: u64,
    tx: &InFlightTile,
    mut oneshot_rx: oneshot::Receiver<ReadResult>,
) -> Option<(
    Result<GdalIpcBytePayload, GdalProcessPoolError>,
    Option<tracing::Span>,
)> {
    let res = loop {
        tokio::select! {
            res = &mut oneshot_rx => break res,
            () = tx.closed() => {
                let Ok(mut lock) = pool.in_flight_tiles.lock() else {
                    break (&mut oneshot_rx).await;
                };
                // A follower may have subscribed between `closed()` and taking the lock. Followers
                // only subscribe under this lock, so the count is stable here: if it is 0, removing
                // the entry guarantees that later callers start a new leader instead of joining
                // this abandoned one. Otherwise, keep waiting for the result.
                if tx.receiver_count() == 0 {
                    remove_in_flight(&mut lock, tile_key, tx);
                    tracing::trace!("All callers gave up on fetch: {tile_key}");
                    return None;
                }
            }
        }
    };

    Some(match res {
        Ok(read_result) => (
            read_result
                .result
                .map_err(|_| GdalProcessPoolError::WorkerPanic)
                .and_then(|ipc_res| ipc_res.map_err(GdalProcessPoolError::from)),
            read_result.companion_span,
        ),
        Err(_) => (Err(GdalProcessPoolError::WorkerPanic), None),
    })
}

impl GdalPoolDispatcher {
    pub fn new(pool: Arc<GdalProcessPool>) -> Self {
        Self { pool }
    }

    /// Read gdal tiles.
    ///
    /// Identical concurrent tile requests are coalesced via a leader/follower mechanism: the
    /// first request spawns a detached background task that submits the read to the broker;
    /// later requests attach to a shared watch channel. Because the leader task is detached,
    /// cancelling the caller's HTTP request does not abort the leader, so followers still
    /// receive the result. Once all callers are gone, the leader abandons the read, so the
    /// broker drops it if it is still queued. The `LeaderCleanupGuard` ensures the in-flight
    /// entry is removed when the leader task finishes, succeeds, fails, or panics.
    ///
    /// # Panics
    /// This function will panic if a required channel is dropped unexpectedly.
    pub async fn read_data<P: Pixel>(
        &self,
        request: IpcChannelMessage,
    ) -> Result<GdalIpcPayload<P>, GdalProcessPoolError> {
        self.read_data_dedup(request).await
    }

    async fn read_data_dedup<P: Pixel>(
        &self,
        request: IpcChannelMessage,
    ) -> Result<GdalIpcPayload<P>, GdalProcessPoolError> {
        let mut s = rustc_hash::FxHasher::default();
        request.full_hash(&mut s);
        let tile_key = s.finish();

        let mut rx = {
            let mut lock = self
                .pool
                .in_flight_tiles
                .lock()
                .map_err(|_| GdalProcessPoolError::WorkerPanic)?;

            // test if data is already read
            if let Some(tx) = lock.get(&tile_key) {
                tracing::trace!("Follower attached to existing fetch: {tile_key}");
                tx.subscribe()
            } else {
                tracing::trace!("Spawning background fetch task for: {tile_key}");
                let (tx, rx) = tokio::sync::watch::channel(None);
                let tx = Arc::new(tx);
                lock.insert(tile_key, tx.clone());

                let pool_clone = self.pool.clone();
                let request_clone = request.clone();
                let parent_span = tracing::Span::current();

                // DETACHED LEADER: runs to completion as long as at least one caller waits for the result
                tokio::spawn(async move {
                    let _cleanup_guard = LeaderCleanupGuard {
                        pool: pool_clone.clone(),
                        tile_key,
                        tx: tx.clone(),
                    };

                    let mut s_routing = rustc_hash::FxHasher::default();
                    request_clone.0.dataset_params.partial_hash(&mut s_routing);
                    let routing_hash = s_routing.finish();

                    let (oneshot_tx, oneshot_rx) = tokio::sync::oneshot::channel();

                    let dispatched = pool_clone
                        .broker_tx
                        .send(BrokerCommand::Read(Box::new(QueuedRequest {
                            dataset_hash: routing_hash,
                            request: request_clone,
                            respond_to: oneshot_tx,
                        })))
                        .await;

                    let (leader_res, companion_span) = if dispatched.is_ok() {
                        let Some(res) =
                            await_read_or_abandon(&pool_clone, tile_key, &tx, oneshot_rx).await
                        else {
                            return;
                        };
                        res
                    } else {
                        (Err(GdalProcessPoolError::WorkerPanic), None)
                    };

                    // link companion's span to the caller's trace.
                    // The Span handle keeps the companion span alive in the subscriber.
                    if let Some(ref span) = companion_span {
                        span.follows_from(parent_span.id());
                    }

                    // Broadcast to all waiting tasks. (Arc preserves perfect memory alignment)
                    let _ = tx.send(Some(std::sync::Arc::new((leader_res, companion_span))));
                });

                rx
            }
        };

        // ALL requests (including the one that spawned the background task) wait for the task.
        while rx.borrow().is_none() {
            if rx.changed().await.is_err() {
                // Background task panicked before sending
                return Err(GdalProcessPoolError::WorkerPanic);
            }
        }

        let shared_res = rx
            .borrow()
            .as_ref()
            .expect("Watch channel must contain a value after changed() yields")
            .clone();

        // link follower's span to the companion's independent trace.
        // Cloning the Span handle keeps the companion span alive in the subscriber.
        if let Some(ref span) = shared_res.1 {
            tracing::Span::current().follows_from(span.id());
        }

        let byte_payload_ref =
            shared_res
                .0
                .as_ref()
                .map_err(|e| GdalProcessPoolError::IpcProcessError {
                    source: IpcProcessError::IpcOther { msg: e.to_string() },
                })?;

        let payload: GdalIpcPayload<P> =
            byte_payload_ref
                .try_into()
                .map_err(|e: bytemuck::PodCastError| {
                    tracing::error!("DEBUG: Failed to cast payload: {:?}", e);
                    GdalProcessPoolError::IpcProcessError {
                        source: IpcProcessError::PocCastError {
                            error: e.to_string(),
                        },
                    }
                })?;

        Ok(payload)
    }
}

impl GdalProcessPoolAccess for Arc<GdalProcessPool> {
    fn get_gdal_pool(&self) -> &std::sync::Arc<GdalProcessPool> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use float_cmp::assert_approx_eq;
    use geoengine_datatypes::raster::{GridBoundingBox2D, GridIdx2D};
    use geoengine_datatypes::util::NonZeroFiniteF64;

    /// Creates a window from min/max `[row, col]` indices.
    fn window(min: [isize; 2], max: [isize; 2]) -> GridBoundingBox2D {
        GridBoundingBox2D::new(GridIdx2D::new(min), GridIdx2D::new(max)).unwrap()
    }

    // --- calculate_grid_distance ---

    #[test]
    fn same_window() {
        let w = window([0, 0], [7, 7]);
        assert_approx_eq!(f64, calculate_grid_distance(&w, &w), 0.0);
    }

    #[test]
    fn overlapping_windows() {
        let a = window([0, 0], [7, 7]);
        let b = window([4, 4], [11, 11]);
        assert_approx_eq!(f64, calculate_grid_distance(&a, &b), 4.0);
    }

    #[test]
    fn disjoint_far_apart() {
        let a = window([0, 0], [7, 7]);
        let b = window([100, 200], [107, 207]);
        assert_approx_eq!(f64, calculate_grid_distance(&a, &b), 200.0);
    }

    #[test]
    fn single_axis_offset() {
        let a = window([0, 0], [7, 7]);
        let b = window([50, 0], [57, 7]);
        assert_approx_eq!(f64, calculate_grid_distance(&a, &b), 50.0);
    }

    // --- WorkerAffinity::calculate_score ---

    fn make_affinity(dataset_hash: u64, band: usize, window: GridBoundingBox2D) -> WorkerAffinity {
        WorkerAffinity {
            dataset_hash,
            band,
            spatial_window: window,
            timestamp: Instant::now(),
        }
    }

    #[test]
    fn score_exact_match_fresh() {
        let w = window([100, 100], [200, 200]);
        let aff = make_affinity(42, 1, w);
        let score = aff.calculate_score(42, 1, &w, aff.timestamp);

        assert_approx_eq!(
            f64,
            score,
            SCORE_DATASET_MATCH + SCORE_BAND_MATCH + SCORE_EXACT_WINDOW_MATCH
        );
    }

    #[test]
    fn score_nearby_window() {
        let w = window([100, 100], [200, 200]);
        let aff = make_affinity(42, 1, window([100, 101], [200, 201]));
        let score = aff.calculate_score(42, 1, &w, aff.timestamp);

        // Chebyshev(|100-100|, |101-100|) = 1
        let expected =
            SCORE_DATASET_MATCH + SCORE_BAND_MATCH + (SCORE_NEARBY_WINDOW_MAX / (1.0 + 1.0));
        assert_approx_eq!(f64, score, expected);
    }

    #[test]
    fn score_different_band_same_dataset() {
        let w = window([0, 0], [7, 7]);
        let aff = make_affinity(42, 1, w);
        let score = aff.calculate_score(42, 2, &w, aff.timestamp);

        assert_approx_eq!(f64, score, SCORE_DATASET_MATCH);
    }

    #[test]
    fn score_different_dataset() {
        let w = window([0, 0], [7, 7]);
        let aff = make_affinity(42, 1, w);
        let score = aff.calculate_score(99, 1, &w, aff.timestamp);
        assert_approx_eq!(f64, score, 0.0);

        let half_ttl_later = aff.timestamp + Duration::from_secs_f64(CACHE_TTL_SECS / 2.0);
        let score = aff.calculate_score(99, 1, &w, half_ttl_later);
        assert_approx_eq!(f64, score, SCORE_EVICTION_MAX / 2.0);
    }

    #[test]
    fn score_expired_cache() {
        let w = window([0, 0], [7, 7]);
        let mut aff = make_affinity(42, 1, w);
        aff.timestamp = Instant::now()
            .checked_sub(Duration::from_secs_f64(CACHE_TTL_SECS + 1.0))
            .unwrap();
        let score = aff.calculate_score(42, 1, &w, Instant::now());

        assert_approx_eq!(f64, score, SCORE_EVICTION_MAX);
    }

    // --- BrokerState dispatch tests ---

    use crate::source::gdal_worker_process::gdal_dataset_params::{
        FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters,
    };
    use crate::source::gdal_worker_process::process_common::{
        GdalDataGridByteVariant, GdalIpcRasterProperties, GdalReadAdvise, GdalReadWindow,
        IpcChannelMessage, IpcChannelMessagePayload,
    };
    use geoengine_datatypes::primitives::Coordinate2D;
    use geoengine_datatypes::raster::{GridShape2D, RasterDataType};

    /// Spawns an immediately-exited process wrapped in `ChildProcessGuard`.
    /// The `mpsc::unbounded_channel()` receiver captures dispatched `WorkerJob`s.
    fn dummy_worker(id: usize) -> (WorkerProcess, mpsc::UnboundedReceiver<WorkerJob>) {
        let child = std::process::Command::new("true")
            .spawn()
            .expect("failed to spawn dummy process");
        let (job_tx, job_rx) = mpsc::unbounded_channel();
        let wp = WorkerProcess {
            _id: id,
            job_tx,
            child_guard: ChildProcessGuard::from_child(child),
            affinity: None,
        };
        (wp, job_rx)
    }

    fn make_params(file_path: &str, band: usize) -> GdalDatasetParameters {
        GdalDatasetParameters {
            file_path: file_path.into(),
            rasterband_channel: band,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: Coordinate2D::new(0.0, 0.0),
                x_pixel_size: NonZeroFiniteF64::new_unchecked(1.0),
                y_pixel_size: NonZeroFiniteF64::new_unchecked(-1.0),
            },
            width: 256,
            height: 256,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: Some(0.0),
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: false,
            retry: None,
        }
    }

    fn make_request(
        dataset_hash: u64,
        band: usize,
        w: GridBoundingBox2D,
    ) -> (Box<QueuedRequest>, oneshot::Receiver<ReadResult>) {
        let (tx, rx) = oneshot::channel();
        let msg = IpcChannelMessage::new_request_tile_message(IpcChannelMessagePayload {
            dataset_params: make_params("test.tif", band),
            read_advise: GdalReadAdvise {
                gdal_read_widow: GdalReadWindow::new(
                    GridIdx2D::new([0, 0]),
                    GridShape2D::new([256, 256]),
                ),
                read_window_bounds: w,
                bounds_of_target: w,
                flip_y: false,
            },
            data_type: RasterDataType::U8,
            read_id: None,
        });
        let req = Box::new(QueuedRequest {
            dataset_hash,
            request: msg,
            respond_to: tx,
        });
        (req, rx)
    }

    #[test]
    fn dispatch_single_request() {
        let (w0, mut rx0) = dummy_worker(0);
        let (w1, _rx1) = dummy_worker(1);

        let mut state = BrokerState::new(vec![w0, w1], 2, 4, SchedulingStrategy::GlobalMatrix);
        let w = window([0, 0], [255, 255]);
        let (req, _resp_rx) = make_request(100, 1, w);

        state.enqueue_request(req);
        assert_eq!(state.global_active_count, 0);

        state.try_dispatch();

        assert_eq!(state.global_active_count, 1);
        assert!(state.idle_workers.len() < 2);

        let job = rx0.try_recv().expect("worker 0 should have received a job");
        assert_eq!(job.dataset_hash, 100);
    }

    #[test]
    fn release_returns_worker_to_idle() {
        let (w0, mut rx0) = dummy_worker(0);
        let mut state = BrokerState::new(vec![w0], 2, 4, SchedulingStrategy::GlobalMatrix);
        let w = window([0, 0], [255, 255]);
        let (req, _resp_rx) = make_request(100, 1, w);

        state.enqueue_request(req);
        state.try_dispatch();
        let job = rx0.try_recv().unwrap();

        assert_eq!(state.idle_workers.len(), 0);
        assert_eq!(state.global_active_count, 1);

        state.release_worker(0, job.dataset_hash, 1, window([0, 0], [255, 255]));

        assert_eq!(state.idle_workers.len(), 1);
        assert_eq!(state.idle_workers[0], 0);
        assert_eq!(state.global_active_count, 0);
        assert!(state.workers[0].affinity.is_some());
    }

    #[test]
    fn round_robin_across_datasets() {
        let (w0, mut rx0) = dummy_worker(0);
        let mut state = BrokerState::new(vec![w0], 2, 4, SchedulingStrategy::GlobalMatrix);

        let wa = window([0, 0], [255, 255]);
        let wb = window([0, 0], [255, 255]);
        let (req_a, resp_a) = make_request(100, 1, wa);
        let (req_b, resp_b) = make_request(200, 1, wb);

        state.enqueue_request(req_a);
        state.enqueue_request(req_b);

        state.try_dispatch();
        let first = rx0.try_recv().unwrap().dataset_hash;
        assert_eq!(state.global_active_count, 1);

        state.release_worker(0, first, 1, window([0, 0], [255, 255]));
        state.try_dispatch();
        let second = rx0.try_recv().unwrap().dataset_hash;

        assert_ne!(first, second);
        assert_eq!(state.global_active_count, 1);

        drop((resp_a, resp_b));
    }

    #[test]
    fn affinity_preference() {
        let (mut w0, mut rx0) = dummy_worker(0);
        w0.affinity = Some(make_affinity(100, 1, window([0, 0], [255, 255])));
        let (w1, _rx1) = dummy_worker(1);

        let mut state = BrokerState::new(vec![w0, w1], 2, 4, SchedulingStrategy::GlobalMatrix);
        let w = window([0, 0], [255, 255]);
        let (req, _resp) = make_request(100, 1, w);

        state.enqueue_request(req);
        state.try_dispatch();

        let job = rx0
            .try_recv()
            .expect("worker 0 should have received the job (affinity match)");
        assert_eq!(job.dataset_hash, 100);
    }

    #[test]
    fn it_counts_dispatch_affinity() {
        let (mut w0, _rx0) = dummy_worker(0);
        w0.affinity = Some(make_affinity(100, 1, window([0, 0], [255, 255])));
        let (mut w1, _rx1) = dummy_worker(1);
        w1.affinity = Some(make_affinity(300, 1, window([0, 0], [255, 255])));
        let (w2, _rx2) = dummy_worker(2);

        let mut state = BrokerState::new(vec![w0, w1, w2], 4, 4, SchedulingStrategy::GlobalMatrix);
        let w = window([0, 0], [255, 255]);
        let (req_hit, _r1) = make_request(100, 1, w);
        let (req_fresh, _r2) = make_request(200, 1, w);
        let (req_evict, _r3) = make_request(400, 1, w);
        let (req_cancelled, r4) = make_request(500, 1, w);
        drop(r4);

        state.enqueue_request(req_hit);
        state.enqueue_request(req_fresh);
        state.enqueue_request(req_evict);
        state.enqueue_request(req_cancelled);
        state.try_dispatch();

        assert_eq!(state.stats.dispatched, 3);
        assert_eq!(state.stats.affinity_hits, 1);
        assert_eq!(state.stats.fresh, 1);
        assert_eq!(state.stats.evictions, 1);
        assert_eq!(state.stats.cancelled_dropped, 1);
    }

    fn affinity_idle_for(dataset_hash: u64, idle_secs: u64) -> WorkerAffinity {
        let mut aff = make_affinity(dataset_hash, 1, window([0, 0], [255, 255]));
        aff.timestamp = Instant::now()
            .checked_sub(Duration::from_secs(idle_secs))
            .unwrap();
        aff
    }

    #[test]
    fn it_evicts_least_recently_used_worker() {
        let (mut w0, mut rx0) = dummy_worker(0);
        w0.affinity = Some(affinity_idle_for(100, 300));
        let (mut w1, _rx1) = dummy_worker(1);
        w1.affinity = Some(affinity_idle_for(200, 60));
        let (mut w2, _rx2) = dummy_worker(2);
        w2.affinity = Some(affinity_idle_for(300, 1));

        let mut state = BrokerState::new(vec![w0, w1, w2], 4, 4, SchedulingStrategy::GlobalMatrix);
        // most recently released worker first, as `swap_remove` tends to leave it
        state.idle_workers = vec![2, 1, 0];

        let (req, _resp) = make_request(400, 1, window([0, 0], [255, 255]));
        state.enqueue_request(req);
        state.try_dispatch();

        let job = rx0
            .try_recv()
            .expect("the longest idle worker should be evicted");
        assert_eq!(job.dataset_hash, 400);
        assert_eq!(state.stats.evictions, 1);
    }

    #[test]
    fn it_prefers_fresh_worker_over_eviction() {
        let (mut w0, _rx0) = dummy_worker(0);
        w0.affinity = Some(affinity_idle_for(100, 3600));
        let (w1, mut rx1) = dummy_worker(1);

        let mut state = BrokerState::new(vec![w0, w1], 4, 4, SchedulingStrategy::GlobalMatrix);

        let (req, _resp) = make_request(400, 1, window([0, 0], [255, 255]));
        state.enqueue_request(req);
        state.try_dispatch();

        rx1.try_recv().expect("the fresh worker should be used");
        assert_eq!(state.stats.fresh, 1);
    }

    // --- Leader/follower dedup test ---

    fn empty_read_result() -> ReadResult {
        ReadResult {
            result: Ok(Ok(GdalIpcBytePayload {
                dimensions: window([0, 0], [7, 7]),
                properties: GdalIpcRasterProperties {
                    offset: None,
                    scale: None,
                    description: None,
                    properties_map: vec![],
                },
                data_variant: GdalDataGridByteVariant::Empty,
            })),
            companion_span: None,
        }
    }

    /// A pool whose broker forwards every queued read to the returned receiver without answering.
    fn pool_with_holding_broker() -> (
        Arc<GdalProcessPool>,
        mpsc::UnboundedReceiver<Box<QueuedRequest>>,
    ) {
        let (broker_tx, mut broker_rx) = mpsc::channel::<BrokerCommand>(64);
        let (held_tx, held_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(cmd) = broker_rx.recv().await {
                if let BrokerCommand::Read(req) = cmd {
                    let _ = held_tx.send(req);
                }
            }
        });
        let pool = Arc::new(GdalProcessPool {
            broker_tx,
            in_flight_tiles: Mutex::new(FastHashMap::default()),
        });
        (pool, held_rx)
    }

    #[tokio::test]
    async fn it_cancels_queued_read_when_all_callers_drop() {
        let (pool, mut held) = pool_with_holding_broker();
        let dispatcher = GdalPoolDispatcher::new(pool.clone());
        let (msg, _) = make_request(42, 1, window([0, 0], [255, 255]));

        let caller = tokio::spawn(async move { dispatcher.read_data::<u8>(msg.request).await });
        let mut queued = held.recv().await.unwrap();
        assert!(!queued.respond_to.is_closed());

        caller.abort();
        tokio::time::timeout(Duration::from_secs(1), queued.respond_to.closed())
            .await
            .expect("the leader should abandon the read once its only caller is gone");
        assert!(pool.in_flight_tiles.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn it_keeps_read_alive_while_a_follower_waits() {
        let (pool, mut held) = pool_with_holding_broker();
        let dispatcher = GdalPoolDispatcher::new(pool);
        let w = window([0, 0], [255, 255]);
        let (msg_a, _) = make_request(42, 1, w);
        let (msg_b, _) = make_request(42, 1, w);

        let d1 = dispatcher.clone();
        let leader = tokio::spawn(async move { d1.read_data::<u8>(msg_a.request).await });
        let queued = held.recv().await.unwrap();
        let follower = tokio::spawn(async move { dispatcher.read_data::<u8>(msg_b.request).await });
        tokio::task::yield_now().await;

        leader.abort();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!queued.respond_to.is_closed(), "the follower still waits");

        let _ = queued.respond_to.send(empty_read_result());
        assert!(follower.await.unwrap().is_ok());
    }

    #[test]
    fn it_purges_cancelled_requests_before_dispatch() {
        let (w0, mut rx0) = dummy_worker(0);
        let mut state = BrokerState::new(vec![w0], 4, 4, SchedulingStrategy::GlobalMatrix);
        let w = window([0, 0], [255, 255]);
        let (req, resp) = make_request(100, 1, w);

        state.enqueue_request(req);
        drop(resp);
        state.try_dispatch();

        assert!(rx0.try_recv().is_err(), "no job for a cancelled request");
        assert_eq!(state.stats.cancelled_dropped, 1);
        assert_eq!(state.global_active_count, 0);
    }

    /// Callers can cancel between the scan (`peek_first_valid`) and the dispatch, so taking the
    /// request must not assume that one is left.
    #[test]
    fn it_pops_only_requests_with_waiting_callers() {
        let w = window([0, 0], [255, 255]);
        let mut slot = DatasetSlot {
            active_count: 0,
            queue: VecDeque::new(),
        };
        let (cancelled_a, resp_a) = make_request(100, 1, w);
        let (cancelled_b, resp_b) = make_request(100, 1, w);
        slot.queue.push_back(*cancelled_a);
        slot.queue.push_back(*cancelled_b);
        drop((resp_a, resp_b));

        let mut dropped = 0;
        assert!(slot.pop_next_valid(&mut dropped).is_none());
        assert_eq!(dropped, 2);

        let (cancelled, resp_cancelled) = make_request(100, 1, w);
        let (waiting, _resp_waiting) = make_request(100, 2, w);
        slot.queue.push_back(*cancelled);
        slot.queue.push_back(*waiting);
        drop(resp_cancelled);

        let req = slot
            .pop_next_valid(&mut dropped)
            .expect("the request with a waiting caller");
        assert_eq!(req.request.0.dataset_params.rasterband_channel, 2);
        assert_eq!(dropped, 3);
    }

    #[tokio::test]
    async fn dedup_sends_only_one_broker_read() {
        let (broker_tx, mut broker_rx) = mpsc::channel::<BrokerCommand>(64);
        let read_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let read_count_clone = read_count.clone();

        let pool = Arc::new(GdalProcessPool {
            broker_tx,
            in_flight_tiles: Mutex::new(FastHashMap::default()),
        });

        // Mock broker: respond to every Read with a fixed success result.
        tokio::spawn(async move {
            while let Some(cmd) = broker_rx.recv().await {
                if let BrokerCommand::Read(req) = cmd {
                    read_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let _ = req.respond_to.send(empty_read_result());
                }
            }
        });

        let dispatcher = GdalPoolDispatcher::new(pool);

        let w = window([0, 0], [255, 255]);
        let (msg_a, _) = make_request(42, 1, w);
        let (msg_b, _) = make_request(42, 1, w);

        let d1 = dispatcher.clone();
        let d2 = dispatcher.clone();
        let (r1, r2) = tokio::join!(
            async move { d1.read_data::<u8>(msg_a.request).await },
            async move { d2.read_data::<u8>(msg_b.request).await },
        );

        assert!(r1.is_ok());
        assert!(r2.is_ok());
        assert_eq!(
            read_count.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "only one BrokerCommand::Read should have been sent (dedup)"
        );
    }
}
