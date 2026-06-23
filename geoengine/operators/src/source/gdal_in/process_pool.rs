use snafu::Snafu;
use std::collections::VecDeque;
use std::hash::{BuildHasherDefault, DefaultHasher, Hasher};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Duration;

use geoengine_datatypes::raster::{GridBoundingBox2D, GridBounds, Pixel};
use ipc_channel::ipc::{IpcReceiver, IpcSender};
use rustc_hash::FxHasher;

use super::{
    GdalProcessPoolAccess,
    process_common::{GdalIpcPayload, IpcChannelMessage, IpcProcessError, IpcProcessRasterResult},
    process_impl::{ChildProcessGuard, spawn_ipc_server_process},
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

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum GdalProcessPoolError {
    IpcProcessError { source: IpcProcessError },

    IpcError { source: ipc_channel::IpcError },

    WorkerPanic,
}

impl From<IpcProcessError> for GdalProcessPoolError {
    fn from(source: IpcProcessError) -> Self {
        GdalProcessPoolError::IpcProcessError { source }
    }
}

impl From<ipc_channel::IpcError> for GdalProcessPoolError {
    fn from(source: ipc_channel::IpcError) -> Self {
        GdalProcessPoolError::IpcError { source }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulingStrategy {
    FifoGreedy,
    GlobalMatrix,
}

struct WorkerJob {
    request: IpcChannelMessage,
    respond_to: oneshot::Sender<Result<IpcProcessRasterResult, GdalProcessPoolError>>,
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
    respond_to: oneshot::Sender<Result<IpcProcessRasterResult, GdalProcessPoolError>>,
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

pub struct GdalProcessPool {
    broker_tx: mpsc::Sender<BrokerCommand>,
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
    pub fn new_with_tokio_handle(
        handle: &tokio::runtime::Handle,
        max_total: usize,
        max_active_global: usize,
        max_parallel_per_dataset: usize,
    ) -> Arc<Self> {
        let (broker_tx, broker_rx) = mpsc::channel(BROKER_QUEUE_CAPACITY);
        let b_tx_clone = broker_tx.clone();

        handle.spawn(async move {
            let b_tx_clone_2 = b_tx_clone.clone();
            let workers = tokio::task::spawn_blocking(move || {
                let mut w = Vec::with_capacity(max_total);
                for id in 0..max_total {
                    let (guard, tx, rx) =
                        spawn_ipc_server_process::<IpcChannelMessage, IpcProcessRasterResult>()
                            .expect("Error while spawning GDAL worker process");

                    let (job_tx, mut job_rx) = mpsc::unbounded_channel();
                    let b_tx_worker = b_tx_clone_2.clone();

                    std::thread::Builder::new()
                        .name(format!("gdal-worker-companion-{id}"))
                        .spawn(move || {
                            Self::worker_companion_loop(id, &tx, &rx, &mut job_rx, &b_tx_worker);
                        })
                        .expect("Failed to spawn persistent GDAL companion thread");

                    w.push(WorkerProcess {
                        _id: id,
                        job_tx,
                        child_guard: guard,
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
                max_active_global,
                max_parallel_per_dataset,
                b_tx_clone,
            )
            .await;
        });

        Arc::new(Self { broker_tx })
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
    ) -> Arc<Self> {
        let (broker_tx, broker_rx) = mpsc::channel(BROKER_QUEUE_CAPACITY);
        let b_tx_clone = broker_tx.clone();

        tokio::spawn(async move {
            let b_tx_clone_2 = b_tx_clone.clone();
            let workers = tokio::task::spawn_blocking(move || {
                let mut w = Vec::with_capacity(number_of_processes);
                for id in 0..number_of_processes {
                    let (guard, tx, rx) =
                        spawn_ipc_server_process::<IpcChannelMessage, IpcProcessRasterResult>()
                            .expect("Error while spawning GDAL worker process");

                    let (job_tx, mut job_rx) = mpsc::unbounded_channel();
                    let b_tx_worker = b_tx_clone_2.clone();

                    std::thread::Builder::new()
                        .name(format!("gdal-worker-companion-{id}"))
                        .spawn(move || {
                            Self::worker_companion_loop(id, &tx, &rx, &mut job_rx, &b_tx_worker);
                        })
                        .expect("Failed to spawn persistent GDAL companion thread");

                    w.push(WorkerProcess {
                        _id: id,
                        job_tx,
                        child_guard: guard,
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
            )
            .await;
        });

        Arc::new(Self { broker_tx })
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

            let res = sender
                .send(job.request)
                .map_err(|e| GdalProcessPoolError::IpcError { source: e })
                .and_then(|()| {
                    receiver
                        .recv()
                        .map_err(|e| GdalProcessPoolError::IpcError { source: e })
                });

            let is_dead = res.is_err();
            let _ = job.respond_to.send(res);

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

    async fn broker_loop(
        mut rx: mpsc::Receiver<BrokerCommand>,
        workers: Vec<WorkerProcess>,
        max_active_processes: usize,
        max_dataset_processes: usize,
        broker_tx: mpsc::Sender<BrokerCommand>,
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
                        if let Some(slot) = state.dataset_registry.get_mut(&dataset_hash) {
                            slot.active_count = slot.active_count.saturating_sub(1);
                        }
                        state.global_active_count = state.global_active_count.saturating_sub(1);

                        let b_tx = broker_tx.clone();
                        tokio::task::spawn_blocking(move || {
                            if let Ok((guard, tx, rx)) = spawn_ipc_server_process::<
                                IpcChannelMessage,
                                IpcProcessRasterResult,
                            >() {
                                let (job_tx, mut job_rx) = mpsc::unbounded_channel();
                                let b_tx_worker = b_tx.clone();

                                std::thread::Builder::new()
                                    .name(format!("gdal-worker-recovered-{worker_id}"))
                                    .spawn(move || {
                                        Self::worker_companion_loop(
                                            worker_id,
                                            &tx,
                                            &rx,
                                            &mut job_rx,
                                            &b_tx_worker,
                                        );
                                    })
                                    .expect("Failed to spawn replacement companion thread");

                                let _ = b_tx.blocking_send(BrokerCommand::WorkerReplaced {
                                    worker_id,
                                    child_guard: guard,
                                    job_tx,
                                });
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
            let req = slot
                .queue
                .pop_front()
                .expect("Selected request missing from dataset slot - invariant broken");

            self.global_active_count += 1;
            slot.active_count += 1;

            let _ = worker.job_tx.send(WorkerJob {
                request: req.request,
                respond_to: req.respond_to,
                dataset_hash: req.dataset_hash,
            });

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
pub struct GdalPoolWorkerInstance {
    pool: Arc<GdalProcessPool>,
}

impl GdalPoolWorkerInstance {
    pub fn new(pool: Arc<GdalProcessPool>) -> Self {
        Self { pool }
    }

    pub async fn read_data<P: Pixel>(
        &self,
        request: IpcChannelMessage,
    ) -> Result<GdalIpcPayload<P>, GdalProcessPoolError> {
        let mut s = DefaultHasher::new();
        request.0.dataset_params.partial_hash(&mut s);
        let hash = s.finish();

        let (tx, rx) = oneshot::channel();

        self.pool
            .broker_tx
            .send(BrokerCommand::Read(Box::new(QueuedRequest {
                dataset_hash: hash,
                request,
                respond_to: tx,
            })))
            .await
            .map_err(|_| GdalProcessPoolError::WorkerPanic)?;

        let res = rx
            .await
            .map_err(|_| GdalProcessPoolError::WorkerPanic)??
            .map_err(|e| GdalProcessPoolError::IpcProcessError { source: e })
            .inspect_err(|e| tracing::error!("Ipc response error: {e}"))?;

        let payload: GdalIpcPayload<P> = res.into();
        Ok(payload)
    }
}

impl GdalProcessPoolAccess for Arc<GdalProcessPool> {
    fn get_gdal_pool(&self) -> &std::sync::Arc<GdalProcessPool> {
        self
    }
}
