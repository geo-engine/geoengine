use std::collections::VecDeque;
use std::hash::{BuildHasherDefault, DefaultHasher, Hasher};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Duration;

use geoengine_datatypes::raster::{GridBoundingBox2D, GridBounds, Pixel};
use ipc_channel::ipc::{IpcReceiver, IpcSender};
use rustc_hash::FxHasher;

use crate::source::gdal_source::GdalProcessPoolAccess;
use crate::source::gdal_source::process::{GdalIpcPayload, spawn_ipc_server_process};
use crate::source::{GdalSourceError, IpcProcessRasterResult};
use crate::source::{IpcChannelMessage, gdal_source::process::ChildProcessGuard};

const BROKER_QUEUE_CAPACITY: usize = 8192;

// --- Tuning Parameters & Core Constants (Magic Numbers Explained) ---

/// Base score bonus awarded when a worker already has the correct dataset open.
/// This avoids the heavy performance penalty of closing the current dataset handle,
/// freeing its internal caches, and opening a new one.
const SCORE_DATASET_MATCH: f64 = 10000.0;

/// Additional score bonus when both the dataset AND the specific band match.
/// This targets GDAL's band-level block cache layer to prevent band cross-eviction.
const SCORE_BAND_MATCH: f64 = 1000.0;

/// Additional score bonus when the exact spatial window requested matches the worker's last task.
const SCORE_EXACT_WINDOW_MATCH: f64 = 100.0;

/// Maximum base bonus for nearby spatial windows to reward spatial locality.
const SCORE_NEARBY_WINDOW_MAX: f64 = 50.0;

/// Default baseline score given to completely fresh or newly restarted workers
/// to favor them slightly over a worker with a mismatching affinity state.
const SCORE_FRESH_WORKER_DEFAULT: f64 = 0.5;

/// High-affinity immediate dispatch cutoff. If a worker's affinity score matches or exceeds
/// this threshold, we short-circuit the matrix evaluation and instantly assign the job.
const IMMEDIATE_DISPATCH_THRESHOLD: f64 = 11000.0;

/// Maximum number of active datasets to scan when looking for the best worker-job pair.
/// This prevents pathological cases where a large number of active datasets causes the broker loop to stall.
/// In practice, the most relevant scheduling decisions happen within the top of the active dataset queue, so this limit allows us to maintain responsiveness while still capturing most of the affinity benefits.
const MAX_ELIGIBLE_SCAN_DEPTH: usize = 16;

/// Warning threshold for broker loop iteration duration in milliseconds. If a single iteration of the broker loop takes longer than this, we log a warning. This helps us detect potential bottlenecks or issues in the scheduling logic.
/// Tokio scheduling and the broker loop are designed to be very fast, so even under heavy load, we should aim to keep iterations well under this threshold. If we see warnings frequently, it may indicate that we need to optimize the scheduling logic or adjust the `MAX_ELIGIBLE_SCAN_DEPTH`.
const WARNING_SLOW_BROKER_LOOP_MS: Duration = Duration::from_millis(2);

/// The scheduling strategy to use for worker-job assignment. This can be switched to compare the performance of the global matrix approach against a simpler FIFO greedy strategy.
/// - `GlobalMatrix` evaluates all idle worker and pending job combinations to find the best affinity match, which can lead to better cache utilization but is more computationally intensive.
/// - `FifoGreedy` iterates through active datasets in FIFO order and assigns the first idle worker with the best affinity score for that dataset, which is faster but may miss optimal matches
const STRATEGY: SchedulingStrategy = SchedulingStrategy::GlobalMatrix;

// Type alias for zero-cost u64 mapping
type FastHashMap<K, V> = std::collections::HashMap<K, V, BuildHasherDefault<FxHasher>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulingStrategy {
    FifoGreedy,
    GlobalMatrix,
}

struct WorkerJob {
    request: IpcChannelMessage,
    respond_to: oneshot::Sender<Result<IpcProcessRasterResult, GdalSourceError>>,
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
    /// The score calculation moves naturally to the affinity struct itself.
    #[inline]
    fn calculate_score(&self, dataset_hash: u64, band: usize, window: &GridBoundingBox2D) -> f64 {
        let mut score = 0.0;

        if self.dataset_hash == dataset_hash {
            score += SCORE_DATASET_MATCH;
            if self.band == band {
                score += SCORE_BAND_MATCH;
                let dist = calculate_grid_distance(&self.spatial_window, window);
                if dist == 0.0 {
                    score += SCORE_EXACT_WINDOW_MATCH;
                } else {
                    score += SCORE_NEARBY_WINDOW_MAX / (1.0 + dist);
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
    fn score_for_job(&self, dataset_hash: u64, band: usize, window: &GridBoundingBox2D) -> f64 {
        match &self.affinity {
            Some(affinity) => affinity.calculate_score(dataset_hash, band, window),
            // If the worker is fresh/restarted, it gets a slight baseline score
            // over a worker that has a mismatching affinity.
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
    respond_to: oneshot::Sender<Result<IpcProcessRasterResult, GdalSourceError>>,
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
    pub fn new(
        max_total: usize,
        max_active_global: usize,
        max_parallel_per_dataset: usize,
    ) -> Arc<Self> {
        let (broker_tx, broker_rx) = mpsc::channel(BROKER_QUEUE_CAPACITY);
        let b_tx_clone = broker_tx.clone();

        tokio::spawn(async move {
            tracing::info!(
                "Initializing resilient GDAL pool (Strategy={:?}): Capacity={}, Limit={}, Per-Dataset Parallelism={}",
                STRATEGY,
                max_total,
                max_active_global,
                max_parallel_per_dataset
            );
            let b_tx_clone_2 = b_tx_clone.clone();

            let workers = tokio::task::spawn_blocking(move || {
                let mut w = Vec::with_capacity(max_total);
                for id in 0..max_total {
                    let (guard, tx, rx) = spawn_ipc_server_process::<
                        IpcChannelMessage,
                        IpcProcessRasterResult,
                    >()
                    .expect(
                        "Critical initialization failure: Error while spawning GDAL worker process",
                    );

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
                    std::thread::sleep(Duration::from_millis(15));
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
                .map_err(|e| GdalSourceError::IpcSendError { error: e })
                .and_then(|()| {
                    receiver
                        .recv()
                        .map_err(|e| GdalSourceError::IpcReceiveError { error: e })
                });

            let is_dead = res.is_err();
            let _ = job.respond_to.send(res);

            if is_dead {
                let _ = broker_tx.blocking_send(BrokerCommand::WorkerDied { worker_id });
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
        max_active_global: usize,
        max_parallel_per_dataset: usize,
        broker_tx: mpsc::Sender<BrokerCommand>,
    ) {
        let mut state = BrokerState::new(
            workers,
            max_parallel_per_dataset,
            max_active_global,
            STRATEGY,
        );

        while let Some(first_cmd) = rx.recv().await {
            let start_tick = Instant::now();

            let mut batch = vec![first_cmd];
            while let Ok(cmd) = rx.try_recv() {
                batch.push(cmd);
                // TODO: comment this magic number.
                if batch.len() >= 256 {
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
                    BrokerCommand::WorkerDied { worker_id } => {
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
                        let worker = state
                            .workers
                            .get_mut(worker_id)
                            .expect("Replaced worker_id does not exist");
                        worker.child_guard = child_guard;
                        worker.job_tx = job_tx;
                        worker.affinity = None;
                        state.idle_workers.push(worker_id);
                    }
                }
            }

            state.try_dispatch();

            let elapsed = start_tick.elapsed();
            if elapsed > WARNING_SLOW_BROKER_LOOP_MS {
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
    max_parallel_per_dataset: usize,
    max_active_global: usize,
    strategy: SchedulingStrategy,
}

impl BrokerState {
    pub fn new(
        workers: Vec<WorkerProcess>,
        max_parallel_per_dataset: usize,
        max_active_global: usize,
        strategy: SchedulingStrategy,
    ) -> Self {
        let idle_workers = (0..workers.len()).collect();
        Self {
            idle_workers,
            workers,
            dataset_registry: FastHashMap::default(),
            active_datasets: VecDeque::new(),
            global_active_count: 0,
            max_parallel_per_dataset,
            max_active_global,
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
    // on BrokerCommand::ReturnWorker
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

        let worker = self.workers.get_mut(worker_id).unwrap();
        worker.affinity = Some(WorkerAffinity {
            dataset_hash,
            band,
            spatial_window: window,
            timestamp: Instant::now(),
        });

        self.idle_workers.push(worker_id);
    }

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

        while !self.idle_workers.is_empty() && !self.active_datasets.is_empty() {
            if self.global_active_count >= self.max_active_global {
                break;
            }

            let best_pair = match self.strategy {
                SchedulingStrategy::GlobalMatrix => self.find_next_pair_global_matrix(),
                SchedulingStrategy::FifoGreedy => self.find_next_pair_fifo_greedy(),
            };

            let Some((dataset_hash, w_matrix_idx, dataset_idx)) = best_pair else {
                break;
            };

            let slot = self.dataset_registry.get_mut(&dataset_hash).unwrap();
            let req = slot.queue.pop_front().unwrap();
            let tracked_hash = self.active_datasets.remove(dataset_idx).unwrap();

            slot.clean_canceled_front();
            if !slot.queue.is_empty() {
                self.active_datasets.push_back(tracked_hash);
            }

            let worker_id = self.idle_workers.swap_remove(w_matrix_idx);
            let worker = self.workers.get_mut(worker_id).unwrap();

            self.global_active_count += 1;
            slot.active_count += 1;

            let _ = worker.job_tx.send(WorkerJob {
                request: req.request,
                respond_to: req.respond_to,
                dataset_hash: req.dataset_hash,
            });
        }
    }

    fn find_next_pair_global_matrix(&self) -> Option<(u64, usize, usize)> {
        let mut best_score = -1.0;
        let mut best_pair = None;
        let mut datasets_scanned = 0;

        for (idx, &hash) in self.active_datasets.iter().enumerate() {
            let Some(slot) = self.dataset_registry.get(&hash) else {
                continue;
            };
            if slot.active_count >= self.max_parallel_per_dataset {
                continue;
            }

            let Some(req) = slot.peek_first_valid() else {
                continue;
            };

            datasets_scanned += 1;
            if datasets_scanned > MAX_ELIGIBLE_SCAN_DEPTH {
                break;
            }

            let window = req.request.0.read_advise.read_window_bounds;
            let band = req.request.0.dataset_params.rasterband_channel;

            for (w_matrix_idx, &w_id) in self.idle_workers.iter().enumerate() {
                let w = self.workers.get(w_id).unwrap();

                let score = w.score_for_job(hash, band, &window);

                if score > best_score {
                    best_score = score;
                    best_pair = Some((hash, w_matrix_idx, idx));
                }
                if score >= IMMEDIATE_DISPATCH_THRESHOLD {
                    return Some((hash, w_matrix_idx, idx));
                }
            }
        }
        best_pair
    }

    fn find_next_pair_fifo_greedy(&self) -> Option<(u64, usize, usize)> {
        let mut datasets_scanned = 0;

        for (idx, &hash) in self.active_datasets.iter().enumerate() {
            let Some(slot) = self.dataset_registry.get(&hash) else {
                continue;
            };
            if slot.active_count >= self.max_parallel_per_dataset {
                continue;
            }

            let Some(req) = slot.peek_first_valid() else {
                continue;
            };

            datasets_scanned += 1;
            if datasets_scanned > MAX_ELIGIBLE_SCAN_DEPTH {
                break;
            }

            let window = req.request.0.read_advise.read_window_bounds;
            let band = req.request.0.dataset_params.rasterband_channel;

            let mut best_worker_matrix_idx = 0;
            let mut best_score = -1.0;

            for (w_matrix_idx, &w_id) in self.idle_workers.iter().enumerate() {
                let w = self.workers.get(w_id).unwrap();
                let score = w.score_for_job(hash, band, &window);
                if score > best_score {
                    best_score = score;
                    best_worker_matrix_idx = w_matrix_idx;
                }
                if score >= IMMEDIATE_DISPATCH_THRESHOLD {
                    return Some((hash, w_matrix_idx, idx));
                }
            }

            if best_score >= 0.0 {
                return Some((hash, best_worker_matrix_idx, idx));
            }
        }
        None
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
pub struct LazyGdalWorkerInstance {
    pool: Arc<GdalProcessPool>,
}

impl LazyGdalWorkerInstance {
    pub fn new(pool: Arc<GdalProcessPool>) -> Self {
        Self { pool }
    }

    pub async fn read_data<P: Pixel>(
        &self,
        request: IpcChannelMessage,
    ) -> Result<GdalIpcPayload<P>, GdalSourceError> {
        let mut s = DefaultHasher::new();
        // Hash ONLY the file parameters. The band must not be included.
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
            .map_err(|_| GdalSourceError::WorkerPanic)?;

        let res = rx
            .await
            .map_err(|_| GdalSourceError::WorkerPanic)??
            .map_err(|e| GdalSourceError::IpcProcessError { source: e })
            .inspect_err(|e| tracing::error!("Ipc response error: {e}"))?;

        Ok(res.into())
    }
}

impl GdalProcessPoolAccess for Arc<GdalProcessPool> {
    fn get_gdal_pool(&self) -> &std::sync::Arc<GdalProcessPool> {
        self
    }
}
