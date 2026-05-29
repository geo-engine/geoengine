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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulingStrategy {
    FifoGreedy,
    GlobalMatrix,
}

struct WorkerProcess {
    _id: usize,
    sender: Option<IpcSender<IpcChannelMessage>>,
    receiver: Option<IpcReceiver<IpcProcessRasterResult>>,
    child_guard: ChildProcessGuard,
    last_dataset_hash: Option<u64>,
    last_band: Option<usize>,
    last_spatial_window: Option<GridBoundingBox2D>,
    last_accessed: Instant,
}

impl WorkerProcess {
    #[inline]
    fn calculate_affinity_score(
        &self,
        dataset_hash: u64,
        band: usize,
        window: &GridBoundingBox2D,
    ) -> f64 {
        let mut score = 0.0;

        if self.last_dataset_hash == Some(dataset_hash) {
            score += 10000.0; 
            if self.last_band == Some(band) {
                score += 1000.0; 
                if let Some(last_window) = self.last_spatial_window {
                    let dist = calculate_grid_distance(&last_window, window);
                    if dist == 0.0 {
                        score += 100.0; 
                    } else {
                        score += 50.0 / (1.0 + dist); 
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
    Read(Box<QueuedRequest>),
    ReturnWorker {
        worker_id: usize,
        is_dead: bool,
        dataset_hash: u64,
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

pub struct GdalProcessPool {
    broker_tx: mpsc::Sender<BrokerCommand>,
}

impl GdalProcessPool {
    const STRATEGY: SchedulingStrategy = SchedulingStrategy::GlobalMatrix;
    const MAX_ELIGIBLE_SCAN_DEPTH: usize = 16;

    pub fn new(
        max_total: usize,
        max_active_global: usize,
        max_parallel_per_dataset: usize,
    ) -> Arc<Self> {
        let (broker_tx, broker_rx) = mpsc::channel(BROKER_QUEUE_CAPACITY);
        let b_tx_clone = broker_tx.clone();

        tokio::spawn(async move {
            tracing::info!(
                "Initializing optimized warm GDAL pool (Strategy={:?}): Capacity={}, Limit={}, Per-Dataset Parallelism={}",
                Self::STRATEGY, max_total, max_active_global, max_parallel_per_dataset
            );

            let workers = tokio::task::spawn_blocking(move || {
                let mut w = Vec::with_capacity(max_total);
                for id in 0..max_total {
                    let (guard, tx, rx) = spawn_ipc_server_process::<IpcChannelMessage, IpcProcessRasterResult>()
                        .expect("Error while spawning GDAL worker process");

                    w.push(WorkerProcess {
                        _id: id,
                        sender: Some(tx), receiver: Some(rx),
                        child_guard: guard,
                        last_dataset_hash: None, last_band: None, last_spatial_window: None,
                        last_accessed: Instant::now(),
                    });
                    std::thread::sleep(Duration::from_millis(15));
                }
                w
            }).await.expect("Failed to initialize static GDAL worker processes");

            Self::broker_loop(broker_rx, workers, max_active_global, max_parallel_per_dataset, b_tx_clone).await;
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
        let mut pending_requests: VecDeque<QueuedRequest> = VecDeque::new();
        let mut global_active_count: usize = 0;

        // OPTIMIZATION 1: Tokio Event Loop Batching
        loop {
            // Await the first message to wake the loop
            let Some(first_cmd) = rx.recv().await else { break; };
            let mut batch = vec![first_cmd];

            // Instantly drain any other messages that arrived during the same micro-burst
            while let Ok(cmd) = rx.try_recv() {
                batch.push(cmd);
                if batch.len() >= 256 { break; } // Prevent channel-drain starvation
            }

            // Apply state updates for the entire burst
            for cmd in batch {
                match cmd {
                    BrokerCommand::Read(req) => {
                        pending_requests.push_back(*req);
                    }
                    BrokerCommand::ReturnWorker { worker_id, is_dead, dataset_hash, sender, receiver } => {
                        if let Some(count) = active_counts.get_mut(&dataset_hash) {
                            *count = count.saturating_sub(1);
                        }
                        global_active_count = global_active_count.saturating_sub(1);

                        if is_dead {
                            tracing::warn!(worker_id, "GDAL Worker crashed. Spawning replacement...");
                            let b_tx = broker_tx.clone();
                            tokio::task::spawn_blocking(move || {
                                let (guard, tx, rx) = spawn_ipc_server_process::<IpcChannelMessage, IpcProcessRasterResult>()
                                    .expect("Error spawning replacement GDAL worker process");
                                let _ = b_tx.blocking_send(BrokerCommand::WorkerReplaced {
                                    worker_id, child_guard: guard, sender: tx, receiver: rx,
                                });
                            });
                        } else {
                            workers[worker_id].sender = Some(sender);
                            workers[worker_id].receiver = Some(receiver);
                            idle_workers.push(worker_id);
                        }
                    }
                    BrokerCommand::WorkerReplaced { worker_id, child_guard, sender, receiver } => {
                        let worker = &mut workers[worker_id];
                        worker.child_guard = child_guard;
                        worker.sender = Some(sender);
                        worker.receiver = Some(receiver);
                        worker.last_dataset_hash = None;
                        idle_workers.push(worker_id);
                    }
                }
            }

            // RUN DISPATCH EXACTLY ONCE PER BURST
            let start = Instant::now();
            Self::try_dispatch(
                &mut idle_workers, &mut workers, &mut active_counts, &mut global_active_count,
                &mut pending_requests, max_parallel_per_dataset, max_active_global, &broker_tx,
            );
            
            let duration = start.elapsed();
            if duration > Duration::from_micros(250) {
                tracing::warn!(?duration, "Tokio Event Loop: Heavy dispatch cycle");
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn try_dispatch(
        idle_workers: &mut Vec<usize>,
        workers: &mut [WorkerProcess],
        active_counts: &mut HashMap<u64, usize>,
        global_active_count: &mut usize,
        pending_requests: &mut VecDeque<QueuedRequest>,
        max_parallel: usize,
        max_active_global: usize,
        broker_tx: &mpsc::Sender<BrokerCommand>,
    ) {
        // Because we batched, this only runs once per burst instead of 64 times
        pending_requests.retain(|req| !req.respond_to.is_closed());

        while !idle_workers.is_empty() && !pending_requests.is_empty() {
            if *global_active_count >= max_active_global { break; }

            let (best_pair, best_score) = match Self::STRATEGY {
                SchedulingStrategy::GlobalMatrix => Self::find_next_pair_global_matrix(
                    idle_workers, workers, active_counts, pending_requests, max_parallel,
                ),
                SchedulingStrategy::FifoGreedy => Self::find_next_pair_fifo_greedy(
                    idle_workers, workers, active_counts, pending_requests, max_parallel,
                ),
            };

            let Some((req_idx, w_matrix_idx)) = best_pair else { break; };

            let req = pending_requests.remove(req_idx).expect("Checked bounds");
            
            // OPTIMIZATION 2: O(1) Memory Swap. 
            // Removes shifting overhead entirely for idle_workers array
            let worker_id = idle_workers.swap_remove(w_matrix_idx);
            let worker = &mut workers[worker_id];

            let window = req.request.0.read_advise.read_window_bounds;
            let band = req.request.0.dataset_params.rasterband_channel;

            worker.last_dataset_hash = Some(req.dataset_hash);
            worker.last_band = Some(band);
            worker.last_spatial_window = Some(window);
            worker.last_accessed = Instant::now();

            *global_active_count += 1;
            *active_counts.entry(req.dataset_hash).or_insert(0) += 1;

            let sender = worker.sender.take().expect("Worker must have a sender");
            let receiver = worker.receiver.take().expect("Worker must have a receiver");

            let b_tx = broker_tx.clone();
            let dataset_hash = req.dataset_hash;
            let request_msg = req.request;
            let respond_to = req.respond_to;

            tokio::task::spawn_blocking(move || {
                let res = sender.send(request_msg)
                    .map_err(|e| GdalSourceError::IpcSendError { error: e })
                    .and_then(|()| receiver.recv().map_err(|e| GdalSourceError::IpcReceiveError { error: e }));

                let is_dead = res.is_err();
                let _ = respond_to.send(res);

                let _ = b_tx.blocking_send(BrokerCommand::ReturnWorker {
                    worker_id, is_dead, dataset_hash, sender, receiver,
                });
            });

            tracing::trace!(worker_id, dataset_hash = req.dataset_hash, band, best_score, "Worker dispatched");
        }
    }

    fn find_next_pair_fifo_greedy(
        idle_workers: &[usize], workers: &[WorkerProcess], active_counts: &HashMap<u64, usize>,
        pending_requests: &VecDeque<QueuedRequest>, max_parallel: usize,
    ) -> (Option<(usize, usize)>, f64) {
        let mut eligible_scanned = 0;
        for (r_idx, req) in pending_requests.iter().enumerate() {
            if *active_counts.get(&req.dataset_hash).unwrap_or(&0) >= max_parallel { continue; }

            eligible_scanned += 1;
            if eligible_scanned > Self::MAX_ELIGIBLE_SCAN_DEPTH { break; }

            let window = req.request.0.read_advise.read_window_bounds;
            let band = req.request.0.dataset_params.rasterband_channel;

            let mut best_worker_matrix_idx = 0;
            let mut best_score = -1.0;

            for (w_matrix_idx, &w_id) in idle_workers.iter().enumerate() {
                let score = workers[w_id].calculate_affinity_score(req.dataset_hash, band, &window);
                if score > best_score {
                    best_score = score;
                    best_worker_matrix_idx = w_matrix_idx;
                    
                    // OPTIMIZATION 3: Short-Circuit! 
                    // Dataset + Band match is enough. Stop checking other idle workers.
                    if score >= 11000.0 {
                        return (Some((r_idx, best_worker_matrix_idx)), best_score);
                    }
                }
            }
            if best_score >= 0.0 {
                return (Some((r_idx, best_worker_matrix_idx)), best_score);
            }
        }
        (None, -1.0)
    }

    fn find_next_pair_global_matrix(
        idle_workers: &[usize], workers: &[WorkerProcess], active_counts: &HashMap<u64, usize>,
        pending_requests: &VecDeque<QueuedRequest>, max_parallel: usize,
    ) -> (Option<(usize, usize)>, f64) {
        let mut best_score = -1.0;
        let mut best_pair = None;
        let mut eligible_scanned = 0;

        for (r_idx, req) in pending_requests.iter().enumerate() {
            if *active_counts.get(&req.dataset_hash).unwrap_or(&0) >= max_parallel { continue; }

            eligible_scanned += 1;
            if eligible_scanned > Self::MAX_ELIGIBLE_SCAN_DEPTH { break; }

            let window = req.request.0.read_advise.read_window_bounds;
            let band = req.request.0.dataset_params.rasterband_channel;
            let age_boost = (pending_requests.len() - r_idx) as f64 * 0.01;

            for (w_matrix_idx, &w_id) in idle_workers.iter().enumerate() {
                let w = &workers[w_id];
                let mut score = w.calculate_affinity_score(req.dataset_hash, band, &window);

                if w.last_dataset_hash.is_none() { score += 0.5; }
                score += age_boost;

                if score > best_score {
                    best_score = score;
                    best_pair = Some((r_idx, w_matrix_idx));
                    
                    // OPTIMIZATION 3: Short-Circuit!
                    if score >= 11000.0 {
                        return (best_pair, best_score);
                    }
                }
            }
        }
        (best_pair, best_score)
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