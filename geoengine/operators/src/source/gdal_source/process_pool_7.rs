use std::collections::{HashMap, VecDeque};
use std::hash::{DefaultHasher, Hasher};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Duration;

use geoengine_datatypes::raster::{GridBoundingBox2D, GridBounds, Pixel};
use ipc_channel::ipc::{IpcReceiver, IpcSender};

use crate::source::gdal_source::GdalProcessPoolAccess;
use crate::source::gdal_source::process::{GdalIpcPayload, spawn_ipc_server_process};
use crate::source::{GdalSourceError, IpcProcessRasterResult};
use crate::source::{IpcChannelMessage, gdal_source::process::ChildProcessGuard};

const BROKER_QUEUE_CAPACITY: usize = 8192;

/// A long-lived, reusable GDAL worker process.
struct WorkerProcess {
    _id: usize,
    // Wrapped in Option so they can be temporarily moved into `spawn_blocking`
    // without requiring Mutex locks on the worker array.
    sender: Option<IpcSender<IpcChannelMessage>>,
    receiver: Option<IpcReceiver<IpcProcessRasterResult>>,
    child_guard: ChildProcessGuard,

    // Affinity Tracking
    last_dataset_hash: Option<u64>,
    last_band: Option<usize>,
    last_spatial_window: Option<GridBoundingBox2D>,
    last_accessed: Instant,
}

impl WorkerProcess {
    /// Evaluates how well this worker's internal GDAL caches match the incoming request.
    fn calculate_affinity_score(
        &self,
        dataset_hash: u64,
        band: usize,
        window: &GridBoundingBox2D,
    ) -> f64 {
        let mut score = 0.0;

        if self.last_dataset_hash == Some(dataset_hash) {
            score += 10000.0; // Prevent GDAL from closing/reopening file handles
            if self.last_band == Some(band) {
                score += 1000.0; // Warm band cache match
                if let Some(last_window) = self.last_spatial_window {
                    let dist = calculate_grid_distance(&last_window, window);
                    if dist == 0.0 {
                        score += 100.0; // Exact spatial match
                    } else {
                        score += 50.0 / (1.0 + dist); // Proximity match
                    }
                }
            }
        }
        score
    }
}

struct QueuedRequest {
    dataset_hash: u64,
    request: IpcChannelMessage,
    respond_to: oneshot::Sender<Result<IpcProcessRasterResult, GdalSourceError>>,
}

enum BrokerCommand {
    Read(Box<QueuedRequest>), // Clippy really wants this boxed to avoid the large size of QueuedRequest on the stack of the broker loop
    ReturnWorker {
        worker_id: usize,
        is_dead: bool,
        dataset_hash: u64,
        band: usize, // Tracked to decrement active band lane counters cleanly
        sender: IpcSender<IpcChannelMessage>,
        receiver: IpcReceiver<IpcProcessRasterResult>,
    },
    WorkerReplaced {
        worker_id: usize,
        child_guard: ChildProcessGuard,
        sender: IpcSender<IpcChannelMessage>,
        receiver: IpcReceiver<IpcProcessRasterResult>,
    },
}

/// Supported strategy patterns for balancing multi-lane queues against warm worker caches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulingStrategy {
    /// Pure FIFO request consumption matching the first unblocked lane's available worker cache.
    FifoGreedy,
    /// Cross-matrix scheduling tracking all requests against all caches to maximize data affinity.
    GlobalMatrix,
    /// Advanced matrix scheduling supporting multi-lane band caps and automated idle bypass valves.
    Balanced,
}

pub struct GdalProcessPool {
    broker_tx: mpsc::Sender<BrokerCommand>,
}

impl GdalProcessPool {
    // PERFORMANCE TOGGLE: Select the scheduling strategy for how the broker loop matches incoming requests to idle workers.
    const SCHEDULING_STRATEGY: SchedulingStrategy = SchedulingStrategy::Balanced;

    /// TOKIO EVENT LOOP PROTECTION: The maximum number of eligible (unblocked) requests
    /// allowed to undergo full cross-matrix worker affinity evaluations per dispatch cycle.
    /// Prevents CPU-pinning and event loop starvation when backlogs scale to thousands of items.
    const MAX_ELIGIBLE_SCAN_DEPTH: usize = 32;

    /// Initializes the GDAL process pool with the specified number of worker processes and maximum parallel requests per dataset.
    ///
    /// Spawns the worker processes sequentially to avoid issues with concurrent temp file creation, and then starts the centralized broker loop to manage request dispatching and worker lifecycle.
    ///
    /// # Panics
    /// Panics if any of the worker processes fail to start, or if the broker loop fails to initialize.
    pub fn new(
        max_total: usize,
        max_active_global: usize,
        max_parallel_per_dataset: usize,
    ) -> Arc<Self> {
        let (broker_tx, broker_rx) = mpsc::channel(BROKER_QUEUE_CAPACITY);
        let b_tx_clone = broker_tx.clone();

        tokio::spawn(async move {
            tracing::info!(
                "Initializing overprovisioned warm GDAL pool: Capacity={}, Execution Limit={}, Per-Dataset Parallelism={}",
                max_total,
                max_active_global,
                max_parallel_per_dataset
            );

            let workers = tokio::task::spawn_blocking(move || {
                let mut w = Vec::with_capacity(max_total);
                for id in 0..max_total {
                    let (guard, tx, rx) =
                        spawn_ipc_server_process::<IpcChannelMessage, IpcProcessRasterResult>()
                            .expect("Error while spawning GDAL worker process");

                    w.push(WorkerProcess {
                        _id: id,
                        sender: Some(tx),
                        receiver: Some(rx),
                        child_guard: guard,
                        last_dataset_hash: None,
                        last_band: None,
                        last_spatial_window: None,
                        last_accessed: Instant::now(),
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

    async fn broker_loop(
        mut rx: mpsc::Receiver<BrokerCommand>,
        mut workers: Vec<WorkerProcess>,
        max_active_global: usize,
        max_parallel_per_dataset: usize,
        broker_tx: mpsc::Sender<BrokerCommand>,
    ) {
        let mut idle_workers: Vec<usize> = (0..workers.len()).collect();
        let mut active_counts: HashMap<u64, usize> = HashMap::new();
        let mut active_bands: HashMap<(u64, usize), usize> = HashMap::new();
        let mut pending_requests: VecDeque<QueuedRequest> = VecDeque::new();

        // Tracks the exact number of worker processes executing tasks inside spawn_blocking
        let mut global_active_count: usize = 0;

        while let Some(cmd) = rx.recv().await {
            match cmd {
                BrokerCommand::Read(req) => {
                    if req.respond_to.is_closed() {
                        continue;
                    }
                    pending_requests.push_back(*req);
                    Self::try_dispatch(
                        &mut idle_workers,
                        &mut workers,
                        &mut active_counts,
                        &mut active_bands,
                        &mut global_active_count,
                        &mut pending_requests,
                        max_parallel_per_dataset,
                        max_active_global,
                        &broker_tx,
                    );
                }
                BrokerCommand::ReturnWorker {
                    worker_id,
                    is_dead,
                    dataset_hash,
                    band,
                    sender,
                    receiver,
                } => {
                    if let Some(count) = active_counts.get_mut(&dataset_hash) {
                        *count = count.saturating_sub(1);
                    }
                    if let Some(b_count) = active_bands.get_mut(&(dataset_hash, band)) {
                        *b_count = b_count.saturating_sub(1);
                    }

                    global_active_count = global_active_count.saturating_sub(1);

                    if is_dead {
                        tracing::warn!(worker_id, "GDAL Worker crashed. Spawning replacement...");
                        let b_tx = broker_tx.clone();
                        tokio::task::spawn_blocking(move || {
                            let (guard, tx, rx) = spawn_ipc_server_process::<
                                IpcChannelMessage,
                                IpcProcessRasterResult,
                            >()
                            .expect("Error spawning replacement GDAL worker process after crash");
                            let _ = b_tx.blocking_send(BrokerCommand::WorkerReplaced {
                                worker_id,
                                child_guard: guard,
                                sender: tx,
                                receiver: rx,
                            });
                        });
                    } else {
                        workers[worker_id].sender = Some(sender);
                        workers[worker_id].receiver = Some(receiver);
                        idle_workers.push(worker_id);
                        Self::try_dispatch(
                            &mut idle_workers,
                            &mut workers,
                            &mut active_counts,
                            &mut active_bands,
                            &mut global_active_count,
                            &mut pending_requests,
                            max_parallel_per_dataset,
                            max_active_global,
                            &broker_tx,
                        );
                    }
                }
                BrokerCommand::WorkerReplaced {
                    worker_id,
                    child_guard,
                    sender,
                    receiver,
                } => {
                    let worker = &mut workers[worker_id];
                    worker.child_guard = child_guard;
                    worker.sender = Some(sender);
                    worker.receiver = Some(receiver);
                    worker.last_dataset_hash = None;

                    idle_workers.push(worker_id);
                    Self::try_dispatch(
                        &mut idle_workers,
                        &mut workers,
                        &mut active_counts,
                        &mut active_bands,
                        &mut global_active_count,
                        &mut pending_requests,
                        max_parallel_per_dataset,
                        max_active_global,
                        &broker_tx,
                    );
                }
            }
        }
    }

    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    fn try_dispatch(
        idle_workers: &mut Vec<usize>,
        workers: &mut [WorkerProcess],
        active_counts: &mut HashMap<u64, usize>,
        active_bands: &mut HashMap<(u64, usize), usize>,
        global_active_count: &mut usize,
        pending_requests: &mut VecDeque<QueuedRequest>,
        max_parallel: usize,
        max_active_global: usize,
        broker_tx: &mpsc::Sender<BrokerCommand>,
    ) {
        pending_requests.retain(|req| !req.respond_to.is_closed());

        while !idle_workers.is_empty() && !pending_requests.is_empty() {
            // NEW: Enforce global execution concurrency ceiling.
            // If the global executing lanes are full, we stop dispatching.
            // Remaining workers left in `idle_workers` act as "Parked" warm caches.
            if *global_active_count >= max_active_global {
                break;
            }

            let (pair, best_score) = match Self::SCHEDULING_STRATEGY {
                SchedulingStrategy::Balanced => Self::find_next_pair_balanced(
                    idle_workers,
                    workers,
                    active_counts,
                    active_bands,
                    pending_requests,
                    max_parallel,
                ),
                SchedulingStrategy::GlobalMatrix => Self::find_next_pair_global_matrix(
                    idle_workers,
                    workers,
                    active_counts,
                    pending_requests,
                    max_parallel,
                ),
                SchedulingStrategy::FifoGreedy => Self::find_next_pair_fifo_greedy(
                    idle_workers,
                    workers,
                    active_counts,
                    pending_requests,
                    max_parallel,
                ),
            };

            let Some((req_idx, w_matrix_idx)) = pair else {
                break;
            };

            let req = pending_requests
                .remove(req_idx)
                .expect("The request should exist at the specified index");
            let worker_id = idle_workers.remove(w_matrix_idx);
            let worker = &mut workers[worker_id];

            let window = req.request.0.read_advise.read_window_bounds;
            let band = req.request.0.dataset_params.rasterband_channel;

            worker.last_dataset_hash = Some(req.dataset_hash);
            worker.last_band = Some(band);
            worker.last_spatial_window = Some(window);
            worker.last_accessed = Instant::now();

            *global_active_count += 1;
            *active_counts.entry(req.dataset_hash).or_insert(0) += 1;
            *active_bands.entry((req.dataset_hash, band)).or_insert(0) += 1;

            let sender = worker
                .sender
                .take()
                .expect("idle worker must have a sender");
            let receiver = worker.receiver.take().expect("worker exists");

            let b_tx = broker_tx.clone();
            let dataset_hash = req.dataset_hash;
            let request_msg = req.request;
            let respond_to = req.respond_to;

            tokio::task::spawn_blocking(move || {
                let res = sender
                    .send(request_msg)
                    .map_err(|e| GdalSourceError::IpcSendError { error: e })
                    .and_then(|()| {
                        receiver
                            .recv()
                            .map_err(|e| GdalSourceError::IpcReceiveError { error: e })
                    });

                let is_dead = res.is_err();
                let _ = respond_to.send(res);

                let _ = b_tx.blocking_send(BrokerCommand::ReturnWorker {
                    worker_id,
                    is_dead,
                    dataset_hash,
                    band,
                    sender,
                    receiver,
                });
            });
            tracing::trace!(
                worker_id,
                dataset_hash = req.dataset_hash,
                band,
                best_score,
                remaining_pending = pending_requests.len(),
                remaining_idle = idle_workers.len(),
                "Worker dispatched"
            );
        }
    }
    /// Selects the first available request from the queue up to the scan limit,
    /// then matches it with the best available worker for that request.
    fn find_next_pair_fifo_greedy(
        idle_workers: &[usize],
        workers: &[WorkerProcess],
        active_counts: &HashMap<u64, usize>,
        pending_requests: &VecDeque<QueuedRequest>,
        max_parallel: usize,
    ) -> (Option<(usize, usize)>, f64) {
        let mut eligible_scanned = 0;

        for (r_idx, req) in pending_requests.iter().enumerate() {
            let active = *active_counts.get(&req.dataset_hash).unwrap_or(&0);
            if active >= max_parallel {
                continue; // Blocked by concurrency limit
            }

            eligible_scanned += 1;
            if eligible_scanned > Self::MAX_ELIGIBLE_SCAN_DEPTH {
                break; // Protect event loop from deep backlog traversal
            }

            let window = req.request.0.read_advise.read_window_bounds;
            let band = req.request.0.dataset_params.rasterband_channel;

            let mut best_worker_matrix_idx = 0;
            let mut best_score = -1.0;

            for (w_matrix_idx, &w_id) in idle_workers.iter().enumerate() {
                let w = &workers[w_id];
                let score = w.calculate_affinity_score(req.dataset_hash, band, &window);

                if score > best_score {
                    best_score = score;
                    best_worker_matrix_idx = w_matrix_idx;
                }
            }

            // Return immediately on the first unblocked request found
            if best_score >= 0.0 {
                return (Some((r_idx, best_worker_matrix_idx)), best_score);
            }
        }
        (None, -1.0)
    }

    /// Evaluates a bounded matrix of eligible workers and requests to find
    /// the absolute best global affinity match, avoiding cache thrashing.
    fn find_next_pair_global_matrix(
        idle_workers: &[usize],
        workers: &[WorkerProcess],
        active_counts: &HashMap<u64, usize>,
        pending_requests: &VecDeque<QueuedRequest>,
        max_parallel: usize,
    ) -> (Option<(usize, usize)>, f64) {
        let mut best_score = -1.0;
        let mut best_pair = None;
        let mut eligible_scanned = 0;

        for (r_idx, req) in pending_requests.iter().enumerate() {
            let active = *active_counts.get(&req.dataset_hash).unwrap_or(&0);
            if active >= max_parallel {
                continue; // Blocked by concurrency limit
            }

            eligible_scanned += 1;
            if eligible_scanned > Self::MAX_ELIGIBLE_SCAN_DEPTH {
                break; // Protect event loop from deep backlog traversal
            }

            let window = req.request.0.read_advise.read_window_bounds;
            let band = req.request.0.dataset_params.rasterband_channel;

            // Age boost gives older requests an advantage when affinity scores are identical
            let age_boost = (pending_requests.len() - r_idx) as f64 * 0.01;

            for (w_matrix_idx, &w_id) in idle_workers.iter().enumerate() {
                let w = &workers[w_id];
                let mut score = w.calculate_affinity_score(req.dataset_hash, band, &window);

                // Prioritize pristine, completely unassigned workers over evicting an active cache
                if w.last_dataset_hash.is_none() {
                    score += 0.5;
                }

                score += age_boost;

                if score > best_score {
                    best_score = score;
                    best_pair = Some((r_idx, w_matrix_idx));
                }
            }
        }

        (best_pair, best_score)
    }

    /// Evaluates the complete cross-matrix of idle workers and pending requests up to the scan limit.
    /// Incorporates a dynamic band holdback penalty to avoid interleaved channel thrashing,
    /// while guaranteeing no worker sits idle if valid work is unfulfilled.
    fn find_next_pair_balanced(
        idle_workers: &[usize],
        workers: &[WorkerProcess],
        active_counts: &HashMap<u64, usize>,
        active_bands: &HashMap<(u64, usize), usize>,
        pending_requests: &VecDeque<QueuedRequest>,
        max_parallel: usize,
    ) -> (Option<(usize, usize)>, f64) {
        const MAX_PARALLEL_LANES_PER_BAND: usize = 2;

        let mut best_score = -1.0;
        let mut best_pair = None;
        let mut eligible_scanned = 0;

        for (r_idx, req) in pending_requests.iter().enumerate() {
            let active = *active_counts.get(&req.dataset_hash).unwrap_or(&0);
            if active >= max_parallel {
                continue;
            }

            eligible_scanned += 1;
            if eligible_scanned > Self::MAX_ELIGIBLE_SCAN_DEPTH {
                break; // Protect event loop from deep backlog traversal
            }

            let window = req.request.0.read_advise.read_window_bounds;
            let band = req.request.0.dataset_params.rasterband_channel;

            let active_on_this_band = *active_bands.get(&(req.dataset_hash, band)).unwrap_or(&0);

            // Allow up to 2 parallel execution lanes per unique band channel before blocking

            let band_lane_is_saturated = active_on_this_band >= MAX_PARALLEL_LANES_PER_BAND;

            let age_boost = (pending_requests.len() - r_idx) as f64 * 0.01;

            for (w_matrix_idx, &w_id) in idle_workers.iter().enumerate() {
                let w = &workers[w_id];

                let is_dataset_match = w.last_dataset_hash == Some(req.dataset_hash);
                let is_band_match = w.last_band == Some(band);

                let mut score = w.calculate_affinity_score(req.dataset_hash, band, &window);

                if w.last_dataset_hash.is_none() {
                    score += 0.5; // Favor completely unassigned workers over cache eviction
                }

                // BALANCED AFFINITY HOLDBACK: If the worker has the correct file handle open
                // but a different band, and our preferred band lane is already saturated,
                // penalize heavily to let this request wait for its true warm worker to return.
                if is_dataset_match
                    && !is_band_match
                    && band_lane_is_saturated
                    && idle_workers.len() > 1
                {
                    score -= 15000.0;
                }

                score += age_boost;

                if score > best_score {
                    best_score = score;
                    best_pair = Some((r_idx, w_matrix_idx));
                }
            }
        }

        // FALLBACK EMERGENCY VALVE: If scheduling penalties caused zero valid pairs to match,
        // but we have workers sitting idle and requests waiting under the concurrency ceilings,
        // bypass affinity constraints completely to maximize compute saturation.
        if best_pair.is_none() && !idle_workers.is_empty() {
            let mut fallback_eligible_scanned = 0;
            for (r_idx, req) in pending_requests.iter().enumerate() {
                let active = *active_counts.get(&req.dataset_hash).unwrap_or(&0);
                if active < max_parallel {
                    fallback_eligible_scanned += 1;
                    if fallback_eligible_scanned > Self::MAX_ELIGIBLE_SCAN_DEPTH {
                        break;
                    }
                    return (Some((r_idx, 0)), 0.0);
                }
            }
        }

        (best_pair, best_score)
    }
}

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
            .inspect_err(|e| tracing::debug!("Ipc response error: {e}"))?;

        let res_2: GdalIpcPayload<P> = res.into();
        Ok(res_2)
    }
}

impl GdalProcessPoolAccess for Arc<GdalProcessPool> {
    fn get_gdal_pool(&self) -> &std::sync::Arc<GdalProcessPool> {
        self
    }
}
