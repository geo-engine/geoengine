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
    process_common::{
        GdalIpcPayload, IpcChannelMessage, IpcProcessError, IpcProcessRasterResult, WorkerRequest,
        WorkerResponse,
    },
    process_impl::{ChildProcessGuard, WorkerConfig, spawn_ipc_server_process},
};

// --- Core Structural Parameters & Tuning Constants ---

/// Capacity for the broker incoming request multi-producer mpsc channel.
const BROKER_QUEUE_CAPACITY: usize = 8192;

/// Bounds the entire clear operation, including broker submission and worker acknowledgements.
const CLEAR_CACHES_TIMEOUT: Duration = Duration::from_secs(60);

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

    #[snafu(display("Timed out waiting for GDAL worker caches to be cleared"))]
    CacheClearTimeout,
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

enum WorkerCommand {
    Read(Box<WorkerJob>),
    ClearCaches(oneshot::Sender<Result<(), GdalProcessPoolError>>),
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
    /// Block size `(x, y)` of `band`, as reported by the worker's last read of it. `None` until
    /// the worker has read this dataset and band.
    block_size: Option<(usize, usize)>,
}

/// How well an idle worker suits a request; greater is better. Each worker holds one open
/// dataset, so idle workers act as a cache of open datasets, and the rank keeps that cache warm.
/// The derived order compares the variants first (declaration order), then the fields in order,
/// so the tiers can never overlap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum WorkerRank {
    /// The worker has to close its open dataset. Prefer closing a dataset that another worker
    /// also holds, then the least recently used one. Without the idle time, the broker would
    /// take the first candidate in `idle_workers`, usually the most recently used worker.
    Evict { duplicate: bool, idle: Duration },
    /// The worker has nothing open, so using it closes nothing.
    Fresh,
    /// The worker has the dataset open, so reading saves opening it. Prefer the same band, then
    /// the nearest window: their blocks are likely still cached. Idle time does not count, as the
    /// caches only hold blocks of the open dataset and do not expire.
    Hit {
        same_band: bool,
        distance: std::cmp::Reverse<u64>,
    },
}

impl WorkerRank {
    /// Same dataset, band and window: no other worker can be better.
    const BEST: Self = Self::Hit {
        same_band: true,
        distance: std::cmp::Reverse(0),
    };
}

impl WorkerAffinity {
    /// `now` is passed down to avoid a clock lookup per worker inside the dispatch loop.
    /// `holders` counts the workers holding each dataset.
    #[inline]
    fn rank(
        &self,
        dataset_hash: u64,
        band: usize,
        window: &GridBoundingBox2D,
        now: Instant,
        holders: &FastHashMap<u64, usize>,
    ) -> WorkerRank {
        if self.dataset_hash != dataset_hash {
            return WorkerRank::Evict {
                duplicate: holders.get(&self.dataset_hash).copied().unwrap_or(0) > 1,
                idle: now.saturating_duration_since(self.timestamp),
            };
        }

        WorkerRank::Hit {
            same_band: self.band == band,
            distance: std::cmp::Reverse(calculate_grid_distance(&self.spatial_window, window)),
        }
    }
}

struct WorkerProcess {
    _id: usize,
    job_tx: mpsc::UnboundedSender<WorkerCommand>,
    child_guard: ChildProcessGuard,
    // A single, unified state representing what the GDAL process last worked on.
    affinity: Option<WorkerAffinity>,
    pending_clears: usize,
}

impl WorkerProcess {
    #[inline]
    fn rank_for_job(
        &self,
        dataset_hash: u64,
        band: usize,
        window: &GridBoundingBox2D,
        now: Instant,
        holders: &FastHashMap<u64, usize>,
    ) -> WorkerRank {
        match &self.affinity {
            Some(affinity) => affinity.rank(dataset_hash, band, window, now, holders),
            None => WorkerRank::Fresh,
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

type CacheClearAcknowledgement = oneshot::Receiver<Result<(), GdalProcessPoolError>>;

enum BrokerCommand {
    ClearCaches(oneshot::Sender<Vec<CacheClearAcknowledgement>>),
    Read(Box<QueuedRequest>),
    ReturnWorker {
        worker_id: usize,
        dataset_hash: u64,
        band: usize,
        window: GridBoundingBox2D,
        block_size: Option<(usize, usize)>,
        read_failed: bool,
    },
    WorkerCachesCleared {
        worker_id: usize,
    },
    WorkerDied {
        worker_id: usize,
        dataset_hash: u64,
    },
    WorkerDiedDuringClear {
        worker_id: usize,
    },
    WorkerReplaced {
        worker_id: usize,
        child_guard: ChildProcessGuard,
        job_tx: mpsc::UnboundedSender<WorkerCommand>,
    },
}

type SharedResult = Option<
    Arc<(
        Result<GdalIpcBytePayload, GdalProcessPoolError>,
        Option<tracing::Span>,
    )>,
>;

/// Sender of an in-flight read. Followers `subscribe()` to it while holding the `in_flight_tiles`
/// lock, so its receiver count only changes under that lock. The map holds the sender, not a
/// receiver: a stored receiver would keep the count at one or more, so the leader could not
/// notice that all callers gave up.
type InFlightTile = Arc<tokio::sync::watch::Sender<SharedResult>>;

pub struct GdalProcessPool {
    broker_tx: mpsc::Sender<BrokerCommand>,
    /// Reads in progress by tile, to deduplicate them. Used by the callers' tasks on any runtime
    /// thread and by the leader tasks, not by the single-threaded broker, hence the lock. It is
    /// held only briefly and never across an `.await`.
    in_flight_tiles: Mutex<FastHashMap<u64, InFlightTile>>,
}

impl GdalProcessPool {
    /// Ask every worker to close its dataset and clear remote file caches.
    /// Waits for every worker acknowledgement, even if another worker fails.
    /// `CLEAR_CACHES_TIMEOUT` bounds the entire operation, including broker submission and response.
    /// Queued clears may still run after a timeout, and concurrent queries may refill caches.
    pub async fn clear_caches(&self) -> Result<(), GdalProcessPoolError> {
        tokio::time::timeout(CLEAR_CACHES_TIMEOUT, async {
            let (tx, rx) = oneshot::channel();
            self.broker_tx
                .send(BrokerCommand::ClearCaches(tx))
                .await
                .map_err(|_| GdalProcessPoolError::WorkerPanic)?;
            let acknowledgements = rx.await.map_err(|_| GdalProcessPoolError::WorkerPanic)?;
            // Await all workers before reporting an error from any of them.
            let results = futures::future::join_all(acknowledgements).await;
            results
                .into_iter()
                .try_for_each(|ack| ack.unwrap_or(Err(GdalProcessPoolError::WorkerPanic)))
        })
        .await
        .map_err(|_| GdalProcessPoolError::CacheClearTimeout)?
    }

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
                        pending_clears: 0,
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
            mpsc::UnboundedSender<WorkerCommand>,
        ),
        GdalProcessPoolError,
    > {
        let mut config = worker_config.clone();
        config.logging.worker_id = worker_id;

        let (guard, tx, rx) = spawn_ipc_server_process::<WorkerRequest, WorkerResponse>(&config)?;

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
        sender: &IpcSender<WorkerRequest>,
        receiver: &IpcReceiver<WorkerResponse>,
        job_rx: &mut mpsc::UnboundedReceiver<WorkerCommand>,
        broker_tx: &mpsc::Sender<BrokerCommand>,
    ) {
        while let Some(command) = job_rx.blocking_recv() {
            let job = match command {
                WorkerCommand::Read(job) => job,
                WorkerCommand::ClearCaches(reply) => {
                    let result = sender
                        .send(WorkerRequest::ClearCaches)
                        .map_err(GdalProcessPoolError::from)
                        .and_then(|()| receiver.recv().map_err(GdalProcessPoolError::from))
                        .and_then(|response| match response {
                            WorkerResponse::CachesCleared => Ok(()),
                            WorkerResponse::ReadResult(_) => Err(IpcProcessError::IpcOther {
                                msg: "Unexpected worker response: expected CachesCleared, received ReadResult"
                                    .to_owned(),
                            }
                            .into()),
                        });
                    let failed = result.is_err();
                    let notification = if failed {
                        BrokerCommand::WorkerDiedDuringClear { worker_id }
                    } else {
                        BrokerCommand::WorkerCachesCleared { worker_id }
                    };
                    let _ = broker_tx.blocking_send(notification);
                    let _ = reply.send(result);
                    if failed {
                        break;
                    }
                    continue;
                }
            };
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
                .send(WorkerRequest::Read(Box::new(job.request)))
                .map_err(GdalProcessPoolError::from)
                .and_then(|()| receiver.recv().map_err(GdalProcessPoolError::from))
                .and_then(|response| match response {
                    WorkerResponse::ReadResult(result) => Ok(result),
                    WorkerResponse::CachesCleared => Err(IpcProcessError::IpcOther {
                        msg: "Unexpected worker response: expected ReadResult, received CachesCleared"
                            .to_owned(),
                    }
                    .into()),
                });

            let is_dead = res.is_err();
            let read_failed = matches!(res, Ok(Err(_)));
            let block_size = match &res {
                Ok(Ok(payload)) => payload.block_size,
                _ => None,
            };
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
                block_size,
                read_failed,
            });
        }
    }

    fn respawn_worker(
        worker_id: usize,
        state: &mut BrokerState,
        broker_tx: &mpsc::Sender<BrokerCommand>,
        worker_config: &WorkerConfig,
    ) {
        tracing::error!(
            "GDAL Worker {} died unexpectedly! Initiating respawn...",
            worker_id
        );
        state.remove_worker(worker_id);

        let b_tx = broker_tx.clone();
        let wc = worker_config.clone();
        tokio::task::spawn_blocking(move || {
            std::thread::sleep(std::time::Duration::from_millis(INITIAL_SPAWN_DELAY_MS));

            match Self::create_worker_with_id(worker_id, b_tx.clone(), &wc) {
                Ok((child_guard, _join_handle, job_tx)) => {
                    tracing::info!("Successfully respawned GDAL Worker {}", worker_id);
                    if let Err(e) = b_tx.blocking_send(BrokerCommand::WorkerReplaced {
                        worker_id,
                        child_guard,
                        job_tx,
                    }) {
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
                    BrokerCommand::ClearCaches(reply) => {
                        let _ = reply.send(state.clear_caches());
                    }
                    BrokerCommand::Read(req) => {
                        state.enqueue_request(req);
                    }
                    BrokerCommand::ReturnWorker {
                        worker_id,
                        dataset_hash,
                        band,
                        window,
                        block_size,
                        read_failed,
                    } => {
                        state.release_worker(
                            worker_id,
                            dataset_hash,
                            band,
                            window,
                            block_size,
                            read_failed,
                        );
                    }
                    BrokerCommand::WorkerCachesCleared { worker_id } => {
                        state.finish_clearing_worker(worker_id);
                    }
                    BrokerCommand::WorkerDied {
                        worker_id,
                        dataset_hash,
                    } => {
                        state.release_read_capacity(dataset_hash);
                        state.set_affinity(worker_id, None);
                        Self::respawn_worker(worker_id, &mut state, &broker_tx, &worker_config);
                    }
                    BrokerCommand::WorkerDiedDuringClear { worker_id } => {
                        Self::respawn_worker(worker_id, &mut state, &broker_tx, &worker_config);
                    }
                    BrokerCommand::WorkerReplaced {
                        worker_id,
                        child_guard,
                        job_tx,
                    } => {
                        if let Some(worker) = state.workers.get_mut(worker_id) {
                            worker.child_guard = child_guard;
                            worker.job_tx = job_tx;
                            worker.pending_clears = 0;
                        }
                        state.set_affinity(worker_id, None);
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
    /// Number of workers holding each dataset open, to evict duplicates first.
    dataset_holders: FastHashMap<u64, usize>,
}

impl BrokerState {
    pub fn new(
        workers: Vec<WorkerProcess>,
        max_dataset_processes: usize,
        max_active_processes: usize,
        strategy: SchedulingStrategy,
    ) -> Self {
        let idle_workers = (0..workers.len()).collect();
        let mut dataset_holders = FastHashMap::default();
        for affinity in workers.iter().filter_map(|w| w.affinity.as_ref()) {
            *dataset_holders.entry(affinity.dataset_hash).or_default() += 1;
        }
        Self {
            dataset_holders,
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

    fn clear_caches(&mut self) -> Vec<CacheClearAcknowledgement> {
        // Keep reads in the broker until all clears for their worker have finished.
        // On a clear failure, its companion can exit without dropping a queued read.
        self.idle_workers.clear();
        (0..self.workers.len())
            .map(|worker_id| {
                self.set_affinity(worker_id, None);
                let worker = &mut self.workers[worker_id];
                let (tx, rx) = oneshot::channel();
                worker.pending_clears += 1;
                // Retain rx even if sending fails, so the caller reports the disconnection.
                let _ = worker.job_tx.send(WorkerCommand::ClearCaches(tx));
                rx
            })
            .collect()
    }

    fn finish_clearing_worker(&mut self, worker_id: usize) {
        let worker = &mut self.workers[worker_id];
        worker.pending_clears -= 1;
        if worker.pending_clears == 0 {
            self.idle_workers.push(worker_id);
        }
    }

    fn remove_worker(&mut self, worker_id: usize) {
        self.idle_workers.retain(|&id| id != worker_id);
    }

    fn release_read_capacity(&mut self, dataset_hash: u64) {
        if let Some(slot) = self.dataset_registry.get_mut(&dataset_hash) {
            slot.active_count = slot.active_count.saturating_sub(1);
        }
        self.global_active_count = self.global_active_count.saturating_sub(1);
        self.remove_slot_if_unused(dataset_hash);
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

    /// `read_failed`: the worker may not hold the dataset, as a failed open leaves it with none.
    /// A failed read cannot be told apart from a failed open, so both count as holding nothing.
    #[inline]
    pub fn release_worker(
        &mut self,
        worker_id: usize,
        dataset_hash: u64,
        band: usize,
        window: GridBoundingBox2D,
        block_size: Option<(usize, usize)>,
        read_failed: bool,
    ) {
        self.release_read_capacity(dataset_hash);

        // A clear queued behind this read closes the dataset.
        let pending_clear = self.workers[worker_id].pending_clears > 0;
        let affinity = (!read_failed && !pending_clear).then(|| WorkerAffinity {
            dataset_hash,
            band,
            spatial_window: window,
            timestamp: Instant::now(),
            block_size,
        });
        self.set_affinity(worker_id, affinity);

        if self.workers[worker_id].pending_clears == 0 {
            self.idle_workers.push(worker_id);
        }
    }

    /// Removes the dataset's slot when nothing is queued or read, so the registry does not keep
    /// one entry for every dataset ever read. A new request creates the slot again.
    /// An empty queue implies the dataset is not in `active_datasets`.
    fn remove_slot_if_unused(&mut self, dataset_hash: u64) {
        if self
            .dataset_registry
            .get(&dataset_hash)
            .is_some_and(|slot| slot.queue.is_empty() && slot.active_count == 0)
        {
            self.dataset_registry.remove(&dataset_hash);
        }
    }

    /// Records what the worker holds open, keeping `dataset_holders` in sync.
    fn set_affinity(&mut self, worker_id: usize, affinity: Option<WorkerAffinity>) {
        let Some(worker) = self.workers.get_mut(worker_id) else {
            return;
        };
        if let Some(old) = worker.affinity.take()
            && let Some(count) = self.dataset_holders.get_mut(&old.dataset_hash)
        {
            *count -= 1;
            if *count == 0 {
                self.dataset_holders.remove(&old.dataset_hash);
            }
        }
        if let Some(new) = &affinity {
            *self.dataset_holders.entry(new.dataset_hash).or_default() += 1;
        }
        worker.affinity = affinity;
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
                self.remove_slot_if_unused(front_hash);
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

            let mut best_rank: Option<WorkerRank> = None;
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
                let mut current_best_rank: Option<WorkerRank> = None;

                for (w_idx, &w_id) in self.idle_workers.iter().enumerate() {
                    let w = self
                        .workers
                        .get(w_id)
                        .expect("Idle worker ID missing from registry - invariant broken");
                    let rank = w.rank_for_job(hash, band, window, now, &self.dataset_holders);

                    if Some(rank) > current_best_rank {
                        current_best_rank = Some(rank);
                        current_best_w_idx = w_idx;
                    }
                    if rank == WorkerRank::BEST {
                        break;
                    }
                }

                if current_best_rank > best_rank {
                    best_rank = current_best_rank;
                    best_w_matrix_idx = current_best_w_idx;
                    best_dataset_hash = hash;
                    best_dataset_active_idx = idx;
                }

                if best_rank == Some(WorkerRank::BEST) {
                    break;
                }

                if self.strategy == SchedulingStrategy::FifoGreedy && best_rank.is_some() {
                    break;
                }

                if datasets_scanned >= MAX_ELIGIBLE_SCAN_DEPTH {
                    break;
                }
            }

            if best_rank.is_none() {
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
                self.remove_slot_if_unused(best_dataset_hash);
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

            // The worker holds the requested dataset once the read starts, not its previous one.
            // Recorded now, so that a busy worker switching datasets does not count as a holder of
            // its old dataset when ranking duplicates; the release corrects it.
            let band = req.request.0.dataset_params.rasterband_channel;
            let busy_affinity = WorkerAffinity {
                dataset_hash: req.dataset_hash,
                band,
                spatial_window: req.request.0.read_advise.read_window_bounds,
                timestamp: now,
                // a block size only applies to the dataset and band it was read from
                block_size: worker
                    .affinity
                    .as_ref()
                    .filter(|a| a.dataset_hash == req.dataset_hash && a.band == band)
                    .and_then(|a| a.block_size),
            };

            {
                let _span = tracing::debug_span!(
                    "gdal_broker_dispatch",
                    worker_id = worker_id,
                    affinity = affinity,
                    dataset = %req.request.0.dataset_params.file_path.display(),
                    read_id = req.request.0.read_id.as_deref().unwrap_or(""),
                )
                .entered();

                let _ = worker.job_tx.send(WorkerCommand::Read(Box::new(WorkerJob {
                    request: req.request,
                    respond_to: req.respond_to,
                    dataset_hash: req.dataset_hash,
                })));
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

            self.set_affinity(worker_id, Some(busy_affinity));
        }
    }
}

#[inline]
fn calculate_grid_distance(a: &GridBoundingBox2D, b: &GridBoundingBox2D) -> u64 {
    let a_min = a.min_index();
    let b_min = b.min_index();
    let dy = (a_min.y() - b_min.y()).unsigned_abs() as u64;
    let dx = (a_min.x() - b_min.x()).unsigned_abs() as u64;
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
        assert_eq!(calculate_grid_distance(&w, &w), 0);
    }

    #[test]
    fn overlapping_windows() {
        let a = window([0, 0], [7, 7]);
        let b = window([4, 4], [11, 11]);
        assert_eq!(calculate_grid_distance(&a, &b), 4);
    }

    #[test]
    fn disjoint_far_apart() {
        let a = window([0, 0], [7, 7]);
        let b = window([100, 200], [107, 207]);
        assert_eq!(calculate_grid_distance(&a, &b), 200);
    }

    #[test]
    fn single_axis_offset() {
        let a = window([0, 0], [7, 7]);
        let b = window([50, 0], [57, 7]);
        assert_eq!(calculate_grid_distance(&a, &b), 50);
    }

    // --- WorkerAffinity::rank ---

    fn make_affinity(dataset_hash: u64, band: usize, window: GridBoundingBox2D) -> WorkerAffinity {
        WorkerAffinity {
            dataset_hash,
            band,
            spatial_window: window,
            timestamp: Instant::now(),
            block_size: None,
        }
    }

    /// Idle times from just released to one day.
    const IDLE_SECS: [u64; 5] = [0, 1, 600, 1800, 86_400];

    fn rank_after(
        aff: &WorkerAffinity,
        idle_secs: u64,
        dataset_hash: u64,
        band: usize,
    ) -> WorkerRank {
        let w = window([0, 0], [7, 7]);
        let now = aff.timestamp + Duration::from_secs(idle_secs);
        aff.rank(dataset_hash, band, &w, now, &FastHashMap::default())
    }

    #[test]
    fn rank_exact_match_is_best() {
        let w = window([100, 100], [200, 200]);
        let aff = make_affinity(42, 1, w);
        for idle in IDLE_SECS {
            let now = aff.timestamp + Duration::from_secs(idle);
            assert_eq!(
                aff.rank(42, 1, &w, now, &FastHashMap::default()),
                WorkerRank::BEST
            );
        }
    }

    /// Opening a dataset costs the most, so any hit beats a fresh worker, and a fresh worker beats
    /// any eviction, however long the workers were idle.
    #[test]
    fn rank_tiers_never_overlap() {
        let hit = make_affinity(42, 2, window([5000, 5000], [5007, 5007]));
        let evict = make_affinity(99, 1, window([0, 0], [7, 7]));
        let mut duplicates = FastHashMap::default();
        duplicates.insert(99, 2);

        for hit_idle in IDLE_SECS {
            let worst_hit = rank_after(&hit, hit_idle, 42, 1);
            assert!(worst_hit > WorkerRank::Fresh);
            for evict_idle in IDLE_SECS {
                let now = evict.timestamp + Duration::from_secs(evict_idle);
                let best_evict = evict.rank(42, 1, &window([0, 0], [7, 7]), now, &duplicates);
                assert!(WorkerRank::Fresh > best_evict);
            }
        }
    }

    /// The blocks of the same band and window are still cached, however long the worker was idle.
    #[test]
    fn rank_hits_by_band_then_distance_regardless_of_idle_time() {
        let w = window([0, 0], [7, 7]);
        let same_band = make_affinity(42, 1, w);
        let other_band = make_affinity(42, 2, w);
        let near = make_affinity(42, 1, window([512, 0], [519, 7]));
        let far = make_affinity(42, 1, window([1024, 0], [1031, 7]));

        for old in IDLE_SECS {
            for recent in IDLE_SECS {
                assert!(
                    rank_after(&same_band, old, 42, 1) > rank_after(&other_band, recent, 42, 1)
                );
                assert!(rank_after(&near, old, 42, 1) > rank_after(&far, recent, 42, 1));
            }
        }
    }

    #[test]
    fn rank_evicts_duplicates_then_least_recently_used() {
        let w = window([0, 0], [7, 7]);
        let aff = make_affinity(99, 1, w);
        let mut holders = FastHashMap::default();
        holders.insert(99, 1);
        let unique_long_idle = aff.rank(
            42,
            1,
            &w,
            aff.timestamp + Duration::from_secs(3600),
            &holders,
        );
        let unique_short_idle =
            aff.rank(42, 1, &w, aff.timestamp + Duration::from_secs(1), &holders);
        holders.insert(99, 2);
        let duplicate = aff.rank(42, 1, &w, aff.timestamp, &holders);

        assert!(duplicate > unique_long_idle);
        assert!(unique_long_idle > unique_short_idle);
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

    fn read_job(command: WorkerCommand) -> WorkerJob {
        match command {
            WorkerCommand::Read(job) => *job,
            WorkerCommand::ClearCaches(_) => panic!("Expected read command"),
        }
    }

    /// Spawns an immediately-exited process wrapped in `ChildProcessGuard`.
    /// The `mpsc::unbounded_channel()` receiver captures dispatched `WorkerJob`s.
    fn dummy_worker(id: usize) -> (WorkerProcess, mpsc::UnboundedReceiver<WorkerCommand>) {
        let child = std::process::Command::new("true")
            .spawn()
            .expect("failed to spawn dummy process");
        let (job_tx, job_rx) = mpsc::unbounded_channel();
        let wp = WorkerProcess {
            _id: id,
            job_tx,
            child_guard: ChildProcessGuard::from_child(child),
            affinity: None,
            pending_clears: 0,
        };
        (wp, job_rx)
    }

    fn pool_with_broker_channel(
        capacity: usize,
    ) -> (GdalProcessPool, mpsc::Receiver<BrokerCommand>) {
        let (broker_tx, broker_rx) = mpsc::channel(capacity);
        (
            GdalProcessPool {
                broker_tx,
                in_flight_tiles: Mutex::new(FastHashMap::default()),
            },
            broker_rx,
        )
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

    #[test]
    fn it_keeps_reads_queued_until_all_worker_clears_finish() {
        for active_read in [false, true] {
            let (worker, mut jobs) = dummy_worker(0);
            let mut state = BrokerState::new(vec![worker], 1, 1, SchedulingStrategy::GlobalMatrix);
            let bounds = window([0, 0], [255, 255]);
            let (first, _first_response) = make_request(100, 1, bounds);
            if active_read {
                state.enqueue_request(first);
                state.try_dispatch();
                let _job = read_job(jobs.try_recv().unwrap());
            }

            let _first_clear = state.clear_caches();
            let _second_clear = state.clear_caches();
            let WorkerCommand::ClearCaches(_ack0) = jobs.try_recv().unwrap() else {
                panic!("Expected first clear");
            };
            let WorkerCommand::ClearCaches(_ack1) = jobs.try_recv().unwrap() else {
                panic!("Expected second clear");
            };
            if active_read {
                state.release_worker(0, 100, 1, bounds, None, false);
            }
            let (next, _next_response) = make_request(200, 1, bounds);
            state.enqueue_request(next);
            state.try_dispatch();
            assert_eq!(state.idle_workers, [] as [usize; 0]);
            assert_eq!(state.global_active_count, 0);

            state.finish_clearing_worker(0);
            state.try_dispatch();
            assert!(matches!(
                jobs.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
            assert_eq!(state.global_active_count, 0);

            state.finish_clearing_worker(0);
            state.try_dispatch();
            assert_eq!(read_job(jobs.try_recv().unwrap()).dataset_hash, 200);
            assert_eq!(state.global_active_count, 1);
            assert_eq!(state.idle_workers, [] as [usize; 0]);
        }
    }

    #[test]
    fn it_preserves_other_reads_capacity_when_a_clear_fails() {
        let (worker0, mut jobs0) = dummy_worker(0);
        let (worker1, mut jobs1) = dummy_worker(1);
        let mut state = BrokerState::new(
            vec![worker0, worker1],
            1,
            2,
            SchedulingStrategy::GlobalMatrix,
        );
        let bounds = window([0, 0], [255, 255]);
        let (first, _first_response) = make_request(100, 1, bounds);
        let (second, _second_response) = make_request(200, 1, bounds);
        state.enqueue_request(first);
        state.enqueue_request(second);
        state.try_dispatch();
        let first = read_job(jobs0.try_recv().unwrap());
        let second = read_job(jobs1.try_recv().unwrap());

        let _cleared = state.clear_caches();
        state.release_worker(0, first.dataset_hash, 1, bounds, None, false);
        state.remove_worker(0);
        assert_eq!(state.global_active_count, 1);
        assert_eq!(state.dataset_registry[&second.dataset_hash].active_count, 1);
        assert_eq!(state.idle_workers, [] as [usize; 0]);

        // A failed read still releases its reserved capacity.
        state.release_read_capacity(second.dataset_hash);
        state.remove_worker(1);
        assert_eq!(state.global_active_count, 0);
        assert!(!state.dataset_registry.contains_key(&second.dataset_hash));
    }

    #[tokio::test]
    async fn it_replaces_workers_after_clear_failures_without_failing_the_next_read() {
        use super::super::process_common::GdalDataGridVariant;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("data.tif");
        let driver = gdal::DriverManager::get_driver_by_name("GTiff").unwrap();
        let mut dataset = driver
            .create_with_band_type::<u8, _>(&path, 256, 256, 1)
            .unwrap();
        dataset
            .set_geo_transform(&[0., 1., 0., 0., 0., -1.])
            .unwrap();
        dataset.rasterband(1).unwrap().fill(7., None).unwrap();
        dataset.flush_cache().unwrap();
        drop(dataset);

        for failure in ["send", "receive", "unexpected response"] {
            let (worker, mut jobs) = dummy_worker(0);
            let (sender, request_receiver) = ipc_channel::ipc::channel::<WorkerRequest>().unwrap();
            let (response_sender, receiver) =
                ipc_channel::ipc::channel::<WorkerResponse>().unwrap();
            let _request_receiver = (failure != "send").then_some(request_receiver);
            if failure == "unexpected response" {
                response_sender
                    .send(WorkerResponse::ReadResult(Err(
                        IpcProcessError::Disconnected,
                    )))
                    .unwrap();
            }
            drop(response_sender);

            let (pool, broker_rx) = pool_with_broker_channel(16);
            let broker_tx = pool.broker_tx.clone();
            let (finished, completion) = oneshot::channel();
            let companion = std::thread::spawn(move || {
                GdalProcessPool::worker_companion_loop(
                    0, &sender, &receiver, &mut jobs, &broker_tx,
                );
                let _ = finished.send(());
            });
            let broker = tokio::spawn(GdalProcessPool::broker_loop(
                broker_rx,
                vec![worker],
                1,
                1,
                pool.broker_tx.clone(),
                WorkerConfig::default(),
            ));

            let result = tokio::time::timeout(Duration::from_secs(10), pool.clear_caches())
                .await
                .unwrap();
            assert!(result.is_err(), "{failure} must fail clearance");
            if failure == "unexpected response" {
                assert!(matches!(
                    result,
                    Err(GdalProcessPoolError::IpcProcessError {
                        source: IpcProcessError::IpcOther { msg },
                    }) if msg.contains("expected CachesCleared") && msg.contains("received ReadResult")
                ));
            }
            tokio::time::timeout(Duration::from_secs(10), completion)
                .await
                .unwrap()
                .unwrap();
            companion.join().unwrap();

            // The failed clear initiates replacement; the first subsequent read waits for it.
            let (mut request, response) = make_request(100, 1, window([0, 0], [255, 255]));
            request.request.0.dataset_params.file_path = path.clone();
            pool.broker_tx
                .send(BrokerCommand::Read(request))
                .await
                .unwrap();
            let bytes = tokio::time::timeout(Duration::from_secs(10), response)
                .await
                .unwrap()
                .unwrap()
                .result
                .unwrap()
                .unwrap();
            let payload: GdalIpcPayload<u8> = (&bytes).try_into().unwrap();
            match payload.data_variant {
                GdalDataGridVariant::AllValid { data }
                | GdalDataGridVariant::WithNoData { data, .. } => {
                    assert!(data.iter().all(|&value| value == 7));
                }
                other => panic!("Expected pixels, got {other:?}"),
            }
            pool.clear_caches().await.unwrap();
            broker.abort();
        }
    }

    #[tokio::test]
    async fn it_clears_open_datasets_without_restarting_workers() {
        use crate::source::gdal_worker_process::process_common::GdalDataGridVariant;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("data.tif");
        let replacement = directory.path().join("replacement.tif");
        let write = |path: &std::path::Path, value: f64| {
            let driver = gdal::DriverManager::get_driver_by_name("GTiff").unwrap();
            let mut dataset = driver
                .create_with_band_type::<u8, _>(path, 256, 256, 1)
                .unwrap();
            dataset
                .set_geo_transform(&[0., 1., 0., 0., 0., -1.])
                .unwrap();
            dataset.rasterband(1).unwrap().fill(value, None).unwrap();
            dataset.flush_cache().unwrap();
        };
        write(&path, 1.0);
        let pool = GdalProcessPool::new(1, 1, 1, WorkerConfig::default());
        let (mut request, _) = make_request(0, 1, window([0, 0], [255, 255]));
        request.request.0.dataset_params.file_path = path.clone();
        for (iteration, expected) in [1, 1, 2, 2].into_iter().enumerate() {
            let (tx, rx) = oneshot::channel();
            pool.broker_tx
                .send(BrokerCommand::Read(Box::new(QueuedRequest {
                    dataset_hash: 0,
                    request: request.request.clone(),
                    respond_to: tx,
                })))
                .await
                .unwrap();
            let bytes = rx.await.unwrap().result.unwrap().unwrap();
            let payload: GdalIpcPayload<u8> = (&bytes).try_into().unwrap();
            let data = match payload.data_variant {
                GdalDataGridVariant::AllValid { data }
                | GdalDataGridVariant::WithNoData { data, .. } => data,
                other => panic!("Expected pixels, got {other:?}"),
            };
            assert!(data.iter().all(|value| *value == expected));
            if iteration == 0 {
                // Replacing the inode leaves the worker's open dataset pointing at old pixels.
                write(&replacement, 2.0);
                std::fs::rename(&replacement, &path).unwrap();
            } else if iteration == 1 {
                pool.clear_caches().await.unwrap();
            }
        }
    }

    #[tokio::test]
    async fn it_sends_clear_to_every_worker_and_waits_for_acknowledgements() {
        let (worker0, mut jobs0) = dummy_worker(0);
        let (worker1, mut jobs1) = dummy_worker(1);
        let (pool, rx) = pool_with_broker_channel(16);
        let tx = pool.broker_tx.clone();
        let broker = tokio::spawn(GdalProcessPool::broker_loop(
            rx,
            vec![worker0, worker1],
            2,
            2,
            tx.clone(),
            WorkerConfig::default(),
        ));
        let (request, _response) = make_request(0, 1, window([0, 0], [255, 255]));
        tx.send(BrokerCommand::Read(request)).await.unwrap();
        let _active_read = read_job(jobs0.recv().await.unwrap());
        let cleared = pool.clear_caches();
        tokio::pin!(cleared);
        assert!(futures::poll!(&mut cleared).is_pending());
        let WorkerCommand::ClearCaches(ack0) = jobs0.recv().await.unwrap() else {
            panic!("Expected clearance after active read");
        };
        let WorkerCommand::ClearCaches(ack1) = jobs1.recv().await.unwrap() else {
            panic!("Expected clearance for idle worker");
        };
        ack0.send(Ok(())).unwrap();
        assert!(futures::poll!(&mut cleared).is_pending());
        ack1.send(Ok(())).unwrap();
        cleared.await.unwrap();
        // Both existing companion channels remain open; no workers were replaced.
        assert!(matches!(
            jobs0.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            jobs1.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        broker.abort();
    }

    #[tokio::test]
    async fn it_waits_for_all_clear_acknowledgements_after_a_worker_fails() {
        for disconnect in [false, true] {
            let (pool, mut broker_rx) = pool_with_broker_channel(1);
            let cleared = pool.clear_caches();
            tokio::pin!(cleared);
            assert!(futures::poll!(&mut cleared).is_pending());
            let BrokerCommand::ClearCaches(reply) = broker_rx.recv().await.unwrap() else {
                panic!("Expected clearance request");
            };
            let (ack0, rx0) = oneshot::channel();
            let (ack1, rx1) = oneshot::channel();
            reply.send(vec![rx0, rx1]).unwrap();
            if disconnect {
                drop(ack0);
            } else {
                ack0.send(Err(GdalProcessPoolError::IpcError {
                    error_str: "clear failed".to_owned(),
                }))
                .unwrap();
            }
            assert!(futures::poll!(&mut cleared).is_pending());
            ack1.send(Ok(())).unwrap();
            let error = cleared.await.unwrap_err();
            if disconnect {
                assert!(matches!(error, GdalProcessPoolError::WorkerPanic));
            } else {
                assert!(matches!(
                    error,
                    GdalProcessPoolError::IpcError { error_str } if error_str == "clear failed"
                ));
            }
        }
    }

    #[tokio::test]
    async fn it_reports_failed_clear_submission_to_a_worker_after_waiting_for_other_workers() {
        let (worker0, jobs0) = dummy_worker(0);
        let (worker1, mut jobs1) = dummy_worker(1);
        drop(jobs0);
        let (pool, rx) = pool_with_broker_channel(1);
        let broker = tokio::spawn(GdalProcessPool::broker_loop(
            rx,
            vec![worker0, worker1],
            2,
            2,
            pool.broker_tx.clone(),
            WorkerConfig::default(),
        ));
        let cleared = pool.clear_caches();
        tokio::pin!(cleared);
        assert!(futures::poll!(&mut cleared).is_pending());
        let WorkerCommand::ClearCaches(ack1) = jobs1.recv().await.unwrap() else {
            panic!("Expected clearance for surviving worker");
        };
        assert!(futures::poll!(&mut cleared).is_pending());
        ack1.send(Ok(())).unwrap();
        assert!(matches!(
            cleared.await,
            Err(GdalProcessPoolError::WorkerPanic)
        ));
        broker.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn it_times_out_clearance_when_the_broker_queue_is_full() {
        let (pool, _broker_rx) = pool_with_broker_channel(1);
        let (reply, _response) = oneshot::channel();
        pool.broker_tx
            .send(BrokerCommand::ClearCaches(reply))
            .await
            .unwrap();
        let cleared = pool.clear_caches();
        tokio::pin!(cleared);
        assert!(futures::poll!(&mut cleared).is_pending());
        tokio::time::advance(CLEAR_CACHES_TIMEOUT).await;
        assert!(matches!(
            cleared.await,
            Err(GdalProcessPoolError::CacheClearTimeout)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn it_times_out_clearance_waiting_for_the_broker_response() {
        let (pool, mut broker_rx) = pool_with_broker_channel(1);
        let cleared = pool.clear_caches();
        tokio::pin!(cleared);
        assert!(futures::poll!(&mut cleared).is_pending());
        let _command = broker_rx.recv().await.unwrap();
        tokio::time::advance(CLEAR_CACHES_TIMEOUT).await;
        assert!(matches!(
            cleared.await,
            Err(GdalProcessPoolError::CacheClearTimeout)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn it_times_out_clearance_waiting_for_a_worker_acknowledgement() {
        let (pool, mut broker_rx) = pool_with_broker_channel(1);
        let cleared = pool.clear_caches();
        tokio::pin!(cleared);
        assert!(futures::poll!(&mut cleared).is_pending());
        let BrokerCommand::ClearCaches(reply) = broker_rx.recv().await.unwrap() else {
            panic!("Expected clearance request");
        };
        let (ack, rx) = oneshot::channel();
        // Earlier stages consume part of the same deadline; the worker gets no new timeout.
        tokio::time::advance(CLEAR_CACHES_TIMEOUT / 2).await;
        reply.send(vec![rx]).unwrap();
        assert!(futures::poll!(&mut cleared).is_pending());
        tokio::time::advance(CLEAR_CACHES_TIMEOUT / 2).await;
        assert!(matches!(
            cleared.await,
            Err(GdalProcessPoolError::CacheClearTimeout)
        ));
        assert!(ack.send(Ok(())).is_err());
    }

    #[tokio::test]
    async fn it_clears_remote_file_caches_without_restarting_workers() {
        use httptest::{Expectation, Server, matchers::request, responders::status_code};
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("data.tif");
        {
            let driver = gdal::DriverManager::get_driver_by_name("GTiff").unwrap();
            let mut dataset = driver
                .create_with_band_type::<u8, _>(&path, 1, 1, 1)
                .unwrap();
            dataset
                .set_geo_transform(&[0., 1., 0., 0., 0., -1.])
                .unwrap();
            dataset.rasterband(1).unwrap().fill(1., None).unwrap();
            dataset.flush_cache().unwrap();
        }
        let bytes = std::fs::read(path).unwrap();
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("HEAD", "/data.tif"))
                .times(2)
                .respond_with(
                    status_code(200).insert_header("Content-Length", bytes.len().to_string()),
                ),
        );
        server.expect(
            Expectation::matching(request::method_path("GET", "/data.tif"))
                .times(2)
                .respond_with(status_code(200).body(bytes)),
        );
        let pool = GdalProcessPool::new(1, 1, 1, WorkerConfig::default());
        for iteration in 0..3 {
            if iteration == 2 {
                pool.clear_caches().await.unwrap();
            }
            let (mut request, response) = make_request(0, 1, window([0, 0], [0, 0]));
            let params = &mut request.request.0.dataset_params;
            params.file_path = server.url_str("/data.tif").into();
            params.width = 1;
            params.height = 1;
            params.no_data_value = None;
            params.gdal_config_options = Some(vec![
                ("GDAL_DISABLE_READDIR_ON_OPEN".into(), "EMPTY_DIR".into()),
                ("CPL_VSIL_CURL_ALLOWED_EXTENSIONS".into(), ".tif".into()),
            ]);
            request.request.0.read_advise.gdal_read_widow =
                GdalReadWindow::new([0, 0].into(), [1, 1].into());
            pool.broker_tx
                .send(BrokerCommand::Read(request))
                .await
                .unwrap();
            let bytes = response.await.unwrap().result.unwrap().unwrap();
            let payload: GdalIpcPayload<u8> = (&bytes).try_into().unwrap();
            assert!(matches!(payload.data_variant,
                super::super::process_common::GdalDataGridVariant::AllValid { data } if data == vec![1]));
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

        let job = read_job(rx0.try_recv().expect("worker 0 should have received a job"));
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
        let job = read_job(rx0.try_recv().unwrap());

        assert_eq!(state.idle_workers.len(), 0);
        assert_eq!(state.global_active_count, 1);
        // a busy worker already counts as a holder of the dataset it reads
        assert_eq!(state.dataset_holders.get(&100), Some(&1));

        state.release_worker(
            0,
            job.dataset_hash,
            1,
            window([0, 0], [255, 255]),
            None,
            false,
        );

        assert_eq!(state.idle_workers.len(), 1);
        assert_eq!(state.idle_workers[0], 0);
        assert_eq!(state.global_active_count, 0);
        assert!(state.workers[0].affinity.is_some());
        assert!(
            state.dataset_registry.is_empty(),
            "no slot kept after the last read"
        );
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
        let first = read_job(rx0.try_recv().unwrap()).dataset_hash;
        assert_eq!(state.global_active_count, 1);

        state.release_worker(0, first, 1, window([0, 0], [255, 255]), None, false);
        state.try_dispatch();
        let second = read_job(rx0.try_recv().unwrap()).dataset_hash;

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

        let job = read_job(
            rx0.try_recv()
                .expect("worker 0 should have received the job (affinity match)"),
        );
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

        let job = read_job(
            rx0.try_recv()
                .expect("the longest idle worker should be evicted"),
        );
        assert_eq!(job.dataset_hash, 400);
        assert_eq!(state.stats.evictions, 1);
    }

    #[test]
    fn it_prefers_open_dataset_however_long_idle() {
        let (mut w0, mut rx0) = dummy_worker(0);
        w0.affinity = Some(affinity_idle_for(100, 3600));
        let (w1, _rx1) = dummy_worker(1);

        let mut state = BrokerState::new(vec![w0, w1], 4, 4, SchedulingStrategy::GlobalMatrix);

        let (req, _resp) = make_request(100, 1, window([0, 0], [255, 255]));
        state.enqueue_request(req);
        state.try_dispatch();

        rx0.try_recv()
            .expect("the worker with the open dataset should be reused");
        assert_eq!(state.stats.affinity_hits, 1);
    }

    /// A failed open leaves the worker with no dataset, so it must not count as a holder.
    #[test]
    fn release_after_failed_read_holds_nothing() {
        let (w0, mut rx0) = dummy_worker(0);
        let mut state = BrokerState::new(vec![w0], 2, 4, SchedulingStrategy::GlobalMatrix);
        let w = window([0, 0], [255, 255]);
        let (req, _resp_rx) = make_request(100, 1, w);

        state.enqueue_request(req);
        state.try_dispatch();
        let job = read_job(rx0.try_recv().unwrap());
        state.release_worker(0, job.dataset_hash, 1, w, None, true);

        assert!(state.workers[0].affinity.is_none());
        assert!(state.dataset_holders.is_empty());
    }

    #[test]
    fn it_evicts_a_duplicate_before_the_only_copy() {
        let (mut w0, _rx0) = dummy_worker(0);
        w0.affinity = Some(affinity_idle_for(100, 600));
        let (mut w1, mut rx1) = dummy_worker(1);
        w1.affinity = Some(affinity_idle_for(200, 1));
        let (mut w2, _rx2) = dummy_worker(2);
        w2.affinity = Some(affinity_idle_for(200, 1));

        let mut state = BrokerState::new(vec![w0, w1, w2], 4, 4, SchedulingStrategy::GlobalMatrix);

        let (req, _resp) = make_request(400, 1, window([0, 0], [255, 255]));
        state.enqueue_request(req);
        state.try_dispatch();

        rx1.try_recv()
            .expect("a worker holding a copy of dataset 200 should be evicted");
        assert_eq!(state.dataset_holders.get(&100), Some(&1));
    }

    /// A block size belongs to the dataset and band it was read from.
    #[test]
    fn it_keeps_the_block_size_only_for_the_same_dataset_and_band() {
        let w = window([0, 0], [255, 255]);
        let (w0, mut rx0) = dummy_worker(0);
        let mut state = BrokerState::new(vec![w0], 4, 4, SchedulingStrategy::GlobalMatrix);
        let block_size =
            |state: &BrokerState| state.workers[0].affinity.as_ref().unwrap().block_size;

        let mut read = |state: &mut BrokerState, dataset_hash, band| {
            let (req, _resp) = make_request(dataset_hash, band, w);
            state.enqueue_request(req);
            state.try_dispatch();
            rx0.try_recv().unwrap();
        };

        read(&mut state, 100, 1);
        state.release_worker(0, 100, 1, w, Some((1024, 1024)), false);
        assert_eq!(block_size(&state), Some((1024, 1024)));

        read(&mut state, 100, 1);
        assert_eq!(
            block_size(&state),
            Some((1024, 1024)),
            "same dataset and band"
        );
        state.release_worker(0, 100, 1, w, Some((1024, 1024)), false);

        read(&mut state, 100, 2);
        assert_eq!(block_size(&state), None, "other band");
        state.release_worker(0, 100, 2, w, Some((512, 512)), false);

        read(&mut state, 200, 2);
        assert_eq!(block_size(&state), None, "other dataset");
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
                block_size: None,
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
        assert!(
            state.dataset_registry.is_empty(),
            "no slot kept for a dataset with no reads"
        );
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
