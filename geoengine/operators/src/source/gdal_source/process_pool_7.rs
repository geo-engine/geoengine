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
    id: usize,
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
    /// Initializes the GDAL process pool with the specified number of worker processes and maximum parallel requests per dataset.
    ///
    /// Spawns the worker processes sequentially to avoid issues with concurrent temp file creation, and then starts the centralized broker loop to manage request dispatching and worker lifecycle.
    ///
    /// # Panics
    /// Panics if any of the worker processes fail to start, or if the broker loop fails to initialize.
    pub fn new(max_total: usize, max_parallel_per_dataset: usize) -> Arc<Self> {
        let (broker_tx, broker_rx) = mpsc::channel(BROKER_QUEUE_CAPACITY);
        let b_tx_clone = broker_tx.clone();

        tokio::spawn(async move {
            tracing::info!(
                "Initializing static GDAL worker pool of {} processes...",
                max_total
            );

            // Spawn worker processes sequentially to avoid problems with concurrent temp file creation in `spawn_ipc_server_process`
            let workers = tokio::task::spawn_blocking(move || {
                let mut w = Vec::with_capacity(max_total);
                for id in 0..max_total {
                    let (guard, tx, rx) =
                        spawn_ipc_server_process::<IpcChannelMessage, IpcProcessRasterResult>()
                            .expect("Error while spawning GDAL worker process");

                    w.push(WorkerProcess {
                        id,
                        sender: Some(tx),
                        receiver: Some(rx),
                        child_guard: guard,
                        last_dataset_hash: None,
                        last_band: None,
                        last_spatial_window: None,
                        last_accessed: Instant::now(),
                    });
                    // Force an OS delay to ensure unique tempfile creation
                    std::thread::sleep(Duration::from_millis(15));
                }
                w
            })
            .await
            .expect("Failed to initialize static GDAL worker processes");

            // Start the centralized, lock-free broker loop
            Self::broker_loop(broker_rx, workers, max_parallel_per_dataset, b_tx_clone).await;
        });

        Arc::new(Self { broker_tx })
    }

    async fn broker_loop(
        mut rx: mpsc::Receiver<BrokerCommand>,
        mut workers: Vec<WorkerProcess>,
        max_parallel_per_dataset: usize,
        broker_tx: mpsc::Sender<BrokerCommand>,
    ) {
        let mut idle_workers: Vec<usize> = (0..workers.len()).collect();
        let mut active_counts: HashMap<u64, usize> = HashMap::new();
        let mut pending_requests: VecDeque<QueuedRequest> = VecDeque::new();

        while let Some(cmd) = rx.recv().await {
            match cmd {
                BrokerCommand::Read(req) => {
                    if req.respond_to.is_closed() {
                        tracing::trace!(
                            dataset_hash = req.dataset_hash,
                            pending_len = pending_requests.len(),
                            "Discarding abandoned request"
                        );
                        continue;
                    } // Discard abandoned requests
                    pending_requests.push_back(*req);
                    Self::try_dispatch(
                        &mut idle_workers,
                        &mut workers,
                        &mut active_counts,
                        &mut pending_requests,
                        max_parallel_per_dataset,
                        &broker_tx,
                    );
                }
                BrokerCommand::ReturnWorker {
                    worker_id,
                    is_dead,
                    dataset_hash,
                    sender,
                    receiver,
                } => {
                    if let Some(count) = active_counts.get_mut(&dataset_hash) {
                        *count = count.saturating_sub(1);
                    }

                    tracing::trace!(
                        worker_id,
                        dataset_hash,
                        remaining_active = active_counts.get(&dataset_hash).copied().unwrap_or(0),
                        is_dead,
                        "Worker returned"
                    );

                    if is_dead {
                        tracing::warn!(
                            worker_id,
                            "GDAL Worker crashed. Spawning replacement sequentially..."
                        );
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
                            &mut pending_requests,
                            max_parallel_per_dataset,
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
                    worker.child_guard = child_guard; // Drops old OS process guard, killing the zombie
                    worker.sender = Some(sender);
                    worker.receiver = Some(receiver);
                    worker.last_dataset_hash = None; // Reset affinity

                    idle_workers.push(worker_id);
                    Self::try_dispatch(
                        &mut idle_workers,
                        &mut workers,
                        &mut active_counts,
                        &mut pending_requests,
                        max_parallel_per_dataset,
                        &broker_tx,
                    );
                }
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn try_dispatch(
        idle_workers: &mut Vec<usize>,
        workers: &mut [WorkerProcess],
        active_counts: &mut HashMap<u64, usize>,
        pending_requests: &mut VecDeque<QueuedRequest>,
        max_parallel: usize,
        broker_tx: &mpsc::Sender<BrokerCommand>,
    ) {
        let mut i = 0;

        while i < pending_requests.len() && !idle_workers.is_empty() {
            let req = &pending_requests[i];

            if req.respond_to.is_closed() {
                pending_requests.remove(i);
                continue;
            }

            let active = *active_counts.get(&req.dataset_hash).unwrap_or(&0);
            tracing::trace!(
                dataset_hash = req.dataset_hash,
                active_for_dataset = active,
                "Evaluating request for dispatch (active: {active}, max_parallel: {max_parallel})"
            );
            if active >= max_parallel {
                tracing::trace!(
                    dataset_hash = req.dataset_hash,
                    queue_pos = i,
                    active,
                    max_parallel,
                    "Request blocked by max_parallel limit"
                );
                i += 1;
                continue;
            }

            let req = pending_requests
                .remove(i)
                .expect("index in pending_requests checked: i < pending_requests.len()");
            let window = req.request.0.read_advise.read_window_bounds;
            let band = req.request.0.dataset_params.rasterband_channel;

            // Calculate the best worker based on affinity heuristics
            let mut best_idx = 0;
            let mut best_score = -1.0;

            for (idx, &w_id) in idle_workers.iter().enumerate() {
                let w = &workers[w_id];
                let mut score = 0.0;

                if w.last_dataset_hash == Some(req.dataset_hash) {
                    score += 10000.0; // Huge boost: Prevents GDAL from reopening the dataset file handle
                    if w.last_band == Some(band) {
                        score += 1000.0;
                        if let Some(last_window) = w.last_spatial_window {
                            let dist = calculate_grid_distance(&last_window, &window);
                            if dist == 0.0 {
                                score += 100.0;
                            } else {
                                score += 50.0 / (1.0 + dist);
                            }
                        }
                    }
                }

                if score > best_score {
                    best_score = score;
                    best_idx = idx;
                }
            }

            tracing::trace!(
                dataset_hash = req.dataset_hash,
                best_score,
                "Dispatching to worker {} with affinity score {}",
                workers[idle_workers[best_idx]].id,
                best_score
            );

            let worker_id = idle_workers.remove(best_idx);
            let worker = &mut workers[worker_id];

            worker.last_dataset_hash = Some(req.dataset_hash);
            worker.last_band = Some(band);
            worker.last_spatial_window = Some(window);
            worker.last_accessed = Instant::now();

            *active_counts.entry(req.dataset_hash).or_insert(0) += 1;

            let sender = worker
                .sender
                .take()
                .expect("idle worker must have a sender");
            let receiver = worker.receiver.take().expect("worker exists");

            let b_tx = broker_tx.clone();
            let dataset_hash = req.dataset_hash;
            let request_msg = req.request;
            let respond_to = req.respond_to;

            // Execute cleanly in isolation
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
                    sender,
                    receiver,
                });
            });
            tracing::trace!(
                worker_id,
                dataset_hash = req.dataset_hash,
                best_score,
                remaining_pending = pending_requests.len(),
                remaining_idle = idle_workers.len(),
                "Worker dispatched"
            );
        }
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
