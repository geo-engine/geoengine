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

/// Cache Time-To-Live (TTL) threshold for affinity routing data tracking.
/// Set to 30 minutes (1800s) to prioritize heavy S3 / cloud storage network connection caches.
const CACHE_TTL_SECS: f64 = 1800.0;

/// Baseline numeric floor used when initiating the maximum-score matrix search.
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

/// High-affinity immediate dispatch cutoff threshold. If a candidate worker's affinity score
/// matches or exceeds this, we short-circuit the matrix evaluation instantly.
const IMMEDIATE_DISPATCH_THRESHOLD: f64 = SCORE_DATASET_MATCH + SCORE_BAND_MATCH;

/// Max lookahead horizontal threshold for sweeps over unique datasets.
const MAX_ELIGIBLE_SCAN_DEPTH: usize = 16;

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

        if idle_duration > CACHE_TTL_SECS {
            return 0.0; // Cache expired, connections likely closed or dead
        }

        let decay = 1.0 - (idle_duration / CACHE_TTL_SECS);
        let mut score = 0.0;

        if self.dataset_hash == dataset_hash {
            score += SCORE_DATASET_MATCH * decay;

            if self.band == band {
                score += SCORE_BAND_MATCH * decay;

                let dist = calculate_grid_distance(&self.spatial_window, window);
                if dist == 0.0 {
                    score += SCORE_EXACT_WINDOW_MATCH * decay;
                } else {
                    score += (SCORE_NEARBY_WINDOW_MAX / (1.0 + dist)) * decay;
                }
            }
        }

        score
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

struct DatasetSlot {
    active_count: usize,
    queue: VecDeque<QueuedRequest>,
}

impl DatasetSlot {
    #[inline]
    pub fn clean_canceled_front(&mut self) {
        while let Some(req) = self.queue.front() {
            if req.respond_to.is_closed() {
                self.queue.pop_front();
            } else {
                break;
            }
        }
    }

    #[inline]
    pub fn peek_first_valid(&self) -> Option<&QueuedRequest> {
        self.queue.iter().find(|req| !req.respond_to.is_closed())
    }
}

struct QueuedRequest {
    dataset_hash: u64,
    request: IpcChannelMessage,
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

pub struct GdalProcessPool {
    broker_tx: mpsc::Sender<BrokerCommand>,
    in_flight_tiles: Mutex<FastHashMap<u64, tokio::sync::watch::Receiver<SharedResult>>>,
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
    #[allow(clippy::needless_pass_by_value)] // value is cloned into spawn block, ref would need lifetime workaround
    pub fn new(
        number_of_processes: usize,
        max_active_processes: usize,
        max_dataset_processes: usize,
        worker_config: WorkerConfig,
    ) -> Arc<Self> {
        let (broker_tx, broker_rx) = mpsc::channel(BROKER_QUEUE_CAPACITY);
        let b_tx_clone = broker_tx.clone();
        let worker_config_clone = worker_config.clone();

        tokio::spawn(async move {
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

        while let Some(first_cmd) = rx.recv().await {
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
                    } => {
                        state.release_worker(worker_id, dataset_hash, band, window);
                    }
                    BrokerCommand::WorkerCachesCleared { worker_id } => {
                        state.finish_clearing_worker(worker_id);
                    }
                    BrokerCommand::WorkerDied {
                        worker_id,
                        dataset_hash,
                    } => {
                        state.release_read_capacity(dataset_hash);
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
                            worker.affinity = None;
                            worker.pending_clears = 0;
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
    idle_workers: Vec<usize>,
    workers: Vec<WorkerProcess>,
    dataset_registry: FastHashMap<u64, DatasetSlot>,
    active_datasets: VecDeque<u64>,
    global_active_count: usize,
    max_dataset_processes: usize,
    max_active_processes: usize,
    strategy: SchedulingStrategy,
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
        }
    }

    fn clear_caches(&mut self) -> Vec<CacheClearAcknowledgement> {
        // Keep reads in the broker until all clears for their worker have finished.
        // On a clear failure, its companion can exit without dropping a queued read.
        self.idle_workers.clear();
        self.workers
            .iter_mut()
            .map(|worker| {
                let (tx, rx) = oneshot::channel();
                worker.pending_clears += 1;
                worker.affinity = None;
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
    }

    #[inline]
    // on BrokerCommand::Read
    pub fn enqueue_request(&mut self, req: Box<QueuedRequest>) {
        if req.respond_to.is_closed() {
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
    }

    #[inline]
    pub fn release_worker(
        &mut self,
        worker_id: usize,
        dataset_hash: u64,
        band: usize,
        window: GridBoundingBox2D,
    ) {
        self.release_read_capacity(dataset_hash);

        if let Some(worker) = self.workers.get_mut(worker_id) {
            worker.affinity = Some(WorkerAffinity {
                dataset_hash,
                band,
                spatial_window: window,
                timestamp: Instant::now(),
            });
        }

        if self.workers[worker_id].pending_clears == 0 {
            self.idle_workers.push(worker_id);
        }
    }

    /// Single-pass scheduler execution pass. Inlines scheduling decisions to prevent matrix re-scanning.
    #[allow(clippy::too_many_lines)]
    #[inline]
    pub fn try_dispatch(&mut self) {
        while let Some(&front_hash) = self.active_datasets.front() {
            let should_pop = match self.dataset_registry.get_mut(&front_hash) {
                Some(slot) => {
                    slot.clean_canceled_front();
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
            let worker_id = self.idle_workers.swap_remove(best_w_matrix_idx);
            let worker = self
                .workers
                .get_mut(worker_id)
                .expect("Selected worker ID missing from registry - invariant broken");
            let slot = self
                .dataset_registry
                .get_mut(&best_dataset_hash)
                .expect("Selected dataset missing from registry - invariant broken");

            slot.clean_canceled_front();

            let req = slot
                .queue
                .pop_front()
                .expect("Selected request missing from dataset slot - invariant broken");

            self.global_active_count += 1;
            slot.active_count += 1;

            {
                let _span = tracing::debug_span!(
                    "gdal_broker_dispatch",
                    worker_id = worker_id,
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

            slot.clean_canceled_front();

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
}

impl Drop for LeaderCleanupGuard {
    fn drop(&mut self) {
        // This is guaranteed to run whether the function succeeds, errors,
        // panics, or is abruptly cancelled by Tokio.
        if let Ok(mut lock) = self.pool.in_flight_tiles.lock() {
            lock.remove(&self.tile_key);
        }
    }
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
    /// receive the result. The `LeaderCleanupGuard` ensures the in-flight entry is removed
    /// when the leader task finishes, succeeds, fails, or panics.
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
            if let Some(rx) = lock.get(&tile_key) {
                tracing::trace!("Follower attached to existing fetch: {tile_key}");
                rx.clone()
            } else {
                tracing::trace!("Spawning background fetch task for: {tile_key}");
                let (tx, rx) = tokio::sync::watch::channel(None);
                lock.insert(tile_key, rx.clone());

                let pool_clone = self.pool.clone();
                let request_clone = request.clone();
                let parent_span = tracing::Span::current();

                // DETACHED LEADER: Guaranteed to run to completion even if the parent stream is suspended or dropped!
                tokio::spawn(async move {
                    let _cleanup_guard = LeaderCleanupGuard {
                        pool: pool_clone.clone(),
                        tile_key,
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
                        match oneshot_rx.await {
                            Ok(read_result) => {
                                let result = read_result
                                    .result
                                    .map_err(|_| GdalProcessPoolError::WorkerPanic)
                                    .and_then(|ipc_res| {
                                        ipc_res.map_err(GdalProcessPoolError::from)
                                    });
                                (result, read_result.companion_span)
                            }
                            Err(_) => (Err(GdalProcessPoolError::WorkerPanic), None),
                        }
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
    }

    #[test]
    fn score_expired_cache() {
        let w = window([0, 0], [7, 7]);
        let mut aff = make_affinity(42, 1, w);
        aff.timestamp = Instant::now()
            .checked_sub(Duration::from_secs_f64(CACHE_TTL_SECS + 1.0))
            .unwrap();
        let score = aff.calculate_score(42, 1, &w, Instant::now());

        assert_approx_eq!(f64, score, 0.0);
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
                state.release_worker(0, 100, 1, bounds);
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
        state.release_worker(0, first.dataset_hash, 1, bounds);
        state.remove_worker(0);
        assert_eq!(state.global_active_count, 1);
        assert_eq!(state.dataset_registry[&second.dataset_hash].active_count, 1);
        assert_eq!(state.idle_workers, [] as [usize; 0]);

        // A failed read still releases its reserved capacity.
        state.release_read_capacity(second.dataset_hash);
        state.remove_worker(1);
        assert_eq!(state.global_active_count, 0);
        assert_eq!(state.dataset_registry[&second.dataset_hash].active_count, 0);
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
        let first = read_job(rx0.try_recv().unwrap()).dataset_hash;
        assert_eq!(state.global_active_count, 1);

        state.release_worker(0, first, 1, window([0, 0], [255, 255]));
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

    // --- Leader/follower dedup test ---

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
                    let _ = req.respond_to.send(ReadResult {
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
                    });
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
