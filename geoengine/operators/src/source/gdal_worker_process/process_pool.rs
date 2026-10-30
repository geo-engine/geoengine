use snafu::Snafu;
use std::collections::VecDeque;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Duration;

use geoengine_datatypes::raster::{GridBoundingBox2D, GridBounds, GridSize, Pixel};
use ipc_channel::ipc::{IpcReceiver, IpcSender};
use rustc_hash::FxHasher;

use crate::source::gdal_worker_process::process_common::GdalIpcBytePayload;

use super::{
    GdalProcessPoolAccess,
    process_common::{
        GdalIpcPayload, GdalReadAdvise, GdalReadWindow, IpcChannelMessage, IpcProcessError,
        IpcProcessRasterResult,
    },
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

/// Max lookahead horizontal threshold for sweeps over unique datasets.
const MAX_ELIGIBLE_SCAN_DEPTH: usize = 16;

/// Blocks remembered per worker for block-aware affinity. A 1024×1024 block of `UInt16` takes
/// 2 MiB, so 64 blocks stay well within the default `GDAL_CACHEMAX` of 256 MiB per worker.
/// shortcut: fixed instead of derived from `GDAL_CACHEMAX` and the data type, change if the cache
/// size or block sizes differ a lot from that.
const MAX_TRACKED_BLOCKS: usize = 64;

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
    /// Block size `(x, y)` of `band`, as reported by the worker's last read of it. `None` until
    /// the worker has read this dataset and band.
    block_size: Option<(usize, usize)>,
    /// Recently read blocks `(x, y)` of `band`, least recently read first, at most
    /// `MAX_TRACKED_BLOCKS`. GDAL keeps them in the worker's block cache while the dataset is open.
    blocks: VecDeque<(usize, usize)>,
}

/// The blocks a read touches, as inclusive block index ranges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BlockRange {
    min: (usize, usize),
    max: (usize, usize),
}

impl BlockRange {
    fn of_window(window: &GdalReadWindow, (block_x, block_y): (usize, usize)) -> Option<Self> {
        let (start_x, start_y) = window.gdal_window_start();
        let (size_x, size_y) = window.gdal_window_size();
        if size_x == 0 || size_y == 0 || block_x == 0 || block_y == 0 {
            return None;
        }
        let start_x = usize::try_from(start_x).ok()?;
        let start_y = usize::try_from(start_y).ok()?;
        Some(Self {
            min: (start_x / block_x, start_y / block_y),
            max: (
                (start_x + size_x - 1) / block_x,
                (start_y + size_y - 1) / block_y,
            ),
        })
    }

    /// The blocks of the band that a read decodes. `None` for a downsampled read, as GDAL may
    /// then read an overview, whose blocks are not the band's.
    fn of_read(read_advise: &GdalReadAdvise, block_size: (usize, usize)) -> Option<Self> {
        let (size_x, size_y) = read_advise.gdal_read_widow.gdal_window_size();
        let out = &read_advise.read_window_bounds;
        if size_x > out.axis_size_x() || size_y > out.axis_size_y() {
            return None;
        }
        Self::of_window(&read_advise.gdal_read_widow, block_size)
    }

    fn len(&self) -> usize {
        (self.max.0 - self.min.0 + 1) * (self.max.1 - self.min.1 + 1)
    }

    fn contains(&self, (x, y): (usize, usize)) -> bool {
        (self.min.0..=self.max.0).contains(&x) && (self.min.1..=self.max.1).contains(&y)
    }

    fn iter(self) -> impl Iterator<Item = (usize, usize)> {
        (self.min.1..=self.max.1).flat_map(move |y| (self.min.0..=self.max.0).map(move |x| (x, y)))
    }
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
    /// The worker has the dataset open, so reading saves opening it. Prefer the worker that
    /// holds most of the read's blocks in its block cache, as each block then is read and decoded
    /// only once. Then prefer the same band and the nearest window, whose blocks are likely cached
    /// too: `shared_blocks` is 0 while the block size is unknown and for downsampled reads. Idle
    /// time does not count, as the caches only hold blocks of the open dataset and do not expire.
    Hit {
        shared_blocks: usize,
        same_band: bool,
        distance: std::cmp::Reverse<u64>,
    },
}

impl WorkerRank {
    /// Same dataset, band and window as the worker's last read, so it holds all of the read's
    /// blocks: no other worker can be better.
    fn is_best(self) -> bool {
        matches!(
            self,
            Self::Hit {
                same_band: true,
                distance: std::cmp::Reverse(0),
                ..
            }
        )
    }
}

impl WorkerAffinity {
    /// The affinity of a worker that reads `read_advise` of `dataset_hash` and `band` after
    /// `prev`. It keeps the block size and blocks only for the same dataset and band, as a worker
    /// closes its dataset to open another. `block_size` is the one the read reported, if any.
    fn after_read(
        prev: Option<&WorkerAffinity>,
        dataset_hash: u64,
        band: usize,
        read_advise: &GdalReadAdvise,
        block_size: Option<(usize, usize)>,
        timestamp: Instant,
    ) -> Self {
        let same = prev.filter(|a| a.dataset_hash == dataset_hash && a.band == band);
        let block_size = block_size.or(same.and_then(|a| a.block_size));
        let mut affinity = Self {
            dataset_hash,
            band,
            spatial_window: read_advise.read_window_bounds,
            timestamp,
            block_size,
            blocks: same
                .filter(|a| a.block_size == block_size)
                .map(|a| a.blocks.clone())
                .unwrap_or_default(),
        };
        affinity.record_blocks(read_advise);
        affinity
    }

    fn blocks_of(&self, read_advise: &GdalReadAdvise) -> Option<BlockRange> {
        BlockRange::of_read(read_advise, self.block_size?)
    }

    fn record_blocks(&mut self, read_advise: &GdalReadAdvise) {
        let Some(range) = self.blocks_of(read_advise) else {
            return;
        };
        for block in range.iter() {
            if let Some(i) = self.blocks.iter().position(|&b| b == block) {
                self.blocks.remove(i);
            }
            self.blocks.push_back(block);
            if self.blocks.len() > MAX_TRACKED_BLOCKS {
                self.blocks.pop_front();
            }
        }
    }

    /// How many of the read's blocks of `self.band` the worker holds.
    fn shared_blocks(&self, read_advise: &GdalReadAdvise) -> usize {
        self.blocks_of(read_advise).map_or(0, |range| {
            self.blocks.iter().filter(|&&b| range.contains(b)).count()
        })
    }

    /// The read's blocks the worker holds and does not hold, if it knows their block size.
    fn block_hits_and_misses(
        &self,
        dataset_hash: u64,
        band: usize,
        read_advise: &GdalReadAdvise,
    ) -> Option<(u64, u64)> {
        if self.dataset_hash != dataset_hash || self.band != band {
            return None;
        }
        let total = self.blocks_of(read_advise)?.len();
        let hits = self.shared_blocks(read_advise);
        Some((hits as u64, (total - hits) as u64))
    }

    /// `now` is passed down to avoid a clock lookup per worker inside the dispatch loop.
    /// `holders` counts the workers holding each dataset.
    #[inline]
    fn rank(
        &self,
        dataset_hash: u64,
        band: usize,
        read_advise: &GdalReadAdvise,
        now: Instant,
        holders: &FastHashMap<u64, usize>,
    ) -> WorkerRank {
        if self.dataset_hash != dataset_hash {
            return WorkerRank::Evict {
                duplicate: holders.get(&self.dataset_hash).copied().unwrap_or(0) > 1,
                idle: now.saturating_duration_since(self.timestamp),
            };
        }

        let same_band = self.band == band;
        WorkerRank::Hit {
            shared_blocks: if same_band {
                self.shared_blocks(read_advise)
            } else {
                0
            },
            same_band,
            distance: std::cmp::Reverse(calculate_grid_distance(
                &self.spatial_window,
                &read_advise.read_window_bounds,
            )),
        }
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
    fn rank_for_job(
        &self,
        dataset_hash: u64,
        band: usize,
        read_advise: &GdalReadAdvise,
        now: Instant,
        holders: &FastHashMap<u64, usize>,
    ) -> WorkerRank {
        match &self.affinity {
            Some(affinity) => affinity.rank(dataset_hash, band, read_advise, now, holders),
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
    /// Blocks of dispatched reads that the worker held or did not hold, counted only where the
    /// worker knew the block size, i.e. had read the dataset and band before.
    block_hits: u64,
    block_misses: u64,
    /// Reads held back for a busy worker holding their blocks, how many of them were dispatched
    /// only after the wait expired, and their total wait.
    busy_waits: u64,
    busy_wait_timeouts: u64,
    busy_wait: Duration,
    per_worker: FastHashMap<usize, WorkerStats>,
}

#[derive(Debug, Default)]
struct WorkerStats {
    block_hits: u64,
    block_misses: u64,
    busy_waits: u64,
    busy_wait_ms: u128,
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
    /// Since when the broker holds the request back for a busy worker holding its blocks.
    held_since: std::cell::Cell<Option<Instant>>,
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
        read_advise: GdalReadAdvise,
        block_size: Option<(usize, usize)>,
        read_failed: bool,
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
        busy_holder_wait: Duration,
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
                busy_holder_wait,
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
    /// - `busy_holder_wait`: How long a read may wait for a busy worker that holds more of its blocks than any idle worker. Zero never waits.
    ///
    /// # Panics
    /// This function will panic if any of the worker processes fail to spawn successfully.
    /// It is designed to be called during application initialization, and assumes that the system has sufficient resources to spawn the specified number of worker processes.
    ///
    pub fn new(
        number_of_processes: usize,
        max_active_processes: usize,
        max_dataset_processes: usize,
        busy_holder_wait: Duration,
        worker_config: WorkerConfig,
    ) -> Arc<Self> {
        Self::new_with_tokio_handle(
            &tokio::runtime::Handle::current(),
            number_of_processes,
            max_active_processes,
            max_dataset_processes,
            busy_holder_wait,
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
            let read_advise = job.request.0.read_advise;
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
                read_advise,
                block_size,
                read_failed,
            });
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn broker_loop(
        mut rx: mpsc::Receiver<BrokerCommand>,
        workers: Vec<WorkerProcess>,
        max_active_processes: usize,
        max_dataset_processes: usize,
        busy_holder_wait: Duration,
        broker_tx: mpsc::Sender<BrokerCommand>,
        worker_config: WorkerConfig,
    ) {
        let mut state = BrokerState::new(
            workers,
            max_dataset_processes,
            max_active_processes,
            Self::STRATEGY,
        );
        state.busy_holder_wait = busy_holder_wait;

        let mut stats_tick = tokio::time::interval(STATS_REPORT_INTERVAL);
        stats_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            let wake = state.next_wake;
            let first_cmd = tokio::select! {
                cmd = rx.recv() => match cmd {
                    Some(cmd) => cmd,
                    None => break,
                },
                _ = stats_tick.tick() => {
                    state.report_and_reset_stats();
                    continue;
                }
                // a held read's wait for a busy worker expired
                () = tokio::time::sleep_until(wake.unwrap_or_else(Instant::now).into()), if wake.is_some() => {
                    state.try_dispatch();
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
                        read_advise,
                        block_size,
                        read_failed,
                    } => {
                        state.release_worker(
                            worker_id,
                            dataset_hash,
                            band,
                            &read_advise,
                            block_size,
                            read_failed,
                        );
                    }
                    BrokerCommand::WorkerDied {
                        worker_id,
                        dataset_hash,
                    } => {
                        tracing::error!(
                            "GDAL Worker {} died unexpectedly! Initiating respawn...",
                            worker_id
                        );
                        state.finish_read(dataset_hash);
                        state.set_affinity(worker_id, None);

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
    /// How long a read may wait for a busy worker holding more of its blocks than any idle one.
    busy_holder_wait: Duration,
    /// When the earliest held read stops waiting, to dispatch it without another command.
    next_wake: Option<Instant>,
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
            busy_holder_wait: Duration::ZERO,
            next_wake: None,
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
            block_hits = stats.block_hits,
            block_misses = stats.block_misses,
            busy_waits = stats.busy_waits,
            busy_wait_timeouts = stats.busy_wait_timeouts,
            busy_wait_ms = stats.busy_wait.as_millis(),
            per_worker = ?{
                let mut per_worker: Vec<_> = stats.per_worker.into_iter().collect();
                per_worker.sort_unstable_by_key(|(worker_id, _)| *worker_id);
                per_worker
            },
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
        read_advise: &GdalReadAdvise,
        block_size: Option<(usize, usize)>,
        read_failed: bool,
    ) {
        self.finish_read(dataset_hash);

        let affinity = (!read_failed).then(|| {
            WorkerAffinity::after_read(
                self.workers
                    .get(worker_id)
                    .and_then(|w| w.affinity.as_ref()),
                dataset_hash,
                band,
                read_advise,
                block_size,
                Instant::now(),
            )
        });
        self.set_affinity(worker_id, affinity);

        self.idle_workers.push(worker_id);
    }

    /// Counts a read of the dataset as finished, whether the worker survived it or not.
    fn finish_read(&mut self, dataset_hash: u64) {
        if let Some(slot) = self.dataset_registry.get_mut(&dataset_hash) {
            slot.active_count = slot.active_count.saturating_sub(1);
        }
        self.global_active_count = self.global_active_count.saturating_sub(1);
        self.remove_slot_if_unused(dataset_hash);
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

    /// Until when to hold `req` back for a busy worker that holds more of its blocks than the
    /// best idle worker, or `None` to dispatch it now. The wait counts from the first time the
    /// request was held, so it is bounded by `busy_holder_wait` in total.
    fn busy_holder_deadline(
        &self,
        req: &QueuedRequest,
        best_idle: Option<WorkerRank>,
        now: Instant,
    ) -> Option<Instant> {
        if self.busy_holder_wait.is_zero() {
            return None;
        }
        let idle_shared = match best_idle {
            Some(WorkerRank::Hit { shared_blocks, .. }) => shared_blocks,
            _ => 0,
        };
        let band = req.request.0.dataset_params.rasterband_channel;
        // Idle workers share at most `idle_shared` blocks, so a worker sharing more is busy.
        let busy_holder = self
            .workers
            .iter()
            .filter_map(|w| w.affinity.as_ref())
            .filter(|a| a.dataset_hash == req.dataset_hash && a.band == band)
            .any(|a| a.shared_blocks(&req.request.0.read_advise) > idle_shared);
        if !busy_holder {
            return None;
        }
        let held_since = req.held_since.get().unwrap_or(now);
        req.held_since.set(Some(held_since));
        let deadline = held_since + self.busy_holder_wait;
        (now < deadline).then_some(deadline)
    }

    /// Single-pass scheduler execution pass. Inlines scheduling decisions to prevent matrix re-scanning.
    #[allow(clippy::too_many_lines)]
    #[inline]
    pub fn try_dispatch(&mut self) {
        self.next_wake = None;

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
                let read_advise = &req.request.0.read_advise;
                let band = req.request.0.dataset_params.rasterband_channel;

                let mut current_best_w_idx = 0;
                let mut current_best_rank: Option<WorkerRank> = None;

                for (w_idx, &w_id) in self.idle_workers.iter().enumerate() {
                    let w = self
                        .workers
                        .get(w_id)
                        .expect("Idle worker ID missing from registry - invariant broken");
                    let rank = w.rank_for_job(hash, band, read_advise, now, &self.dataset_holders);

                    if Some(rank) > current_best_rank {
                        current_best_rank = Some(rank);
                        current_best_w_idx = w_idx;
                    }
                    if rank.is_best() {
                        break;
                    }
                }

                if let Some(deadline) = self.busy_holder_deadline(req, current_best_rank, now) {
                    self.next_wake = Some(self.next_wake.map_or(deadline, |w| w.min(deadline)));
                    continue;
                }

                if current_best_rank > best_rank {
                    best_rank = current_best_rank;
                    best_w_matrix_idx = current_best_w_idx;
                    best_dataset_hash = hash;
                    best_dataset_active_idx = idx;
                }

                if best_rank.is_some_and(WorkerRank::is_best) {
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

            let worker_stats = self.stats.per_worker.entry(worker_id).or_default();
            if let Some((hits, misses)) = worker.affinity.as_ref().and_then(|a| {
                a.block_hits_and_misses(
                    best_dataset_hash,
                    req.request.0.dataset_params.rasterband_channel,
                    &req.request.0.read_advise,
                )
            }) {
                self.stats.block_hits += hits;
                self.stats.block_misses += misses;
                worker_stats.block_hits += hits;
                worker_stats.block_misses += misses;
            }
            if let Some(held_since) = req.held_since.get() {
                let waited = now.saturating_duration_since(held_since);
                self.stats.busy_waits += 1;
                self.stats.busy_wait += waited;
                if waited >= self.busy_holder_wait {
                    self.stats.busy_wait_timeouts += 1;
                }
                worker_stats.busy_waits += 1;
                worker_stats.busy_wait_ms += waited.as_millis();
            }

            // The worker holds the requested dataset once the read starts, not its previous one.
            // Recorded now, so that a busy worker switching datasets does not count as a holder of
            // its old dataset when ranking duplicates; the release corrects it.
            // Includes the blocks being read, so the next read of them prefers this worker.
            let busy_affinity = WorkerAffinity::after_read(
                worker.affinity.as_ref(),
                req.dataset_hash,
                req.request.0.dataset_params.rasterband_channel,
                &req.request.0.read_advise,
                None,
                now,
            );

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
                            held_since: std::cell::Cell::new(None),
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
            blocks: VecDeque::new(),
        }
    }

    /// A read at native resolution whose file window equals the target window.
    fn advise(w: GridBoundingBox2D) -> GdalReadAdvise {
        GdalReadAdvise {
            gdal_read_widow: GdalReadWindow::new(
                w.min_index(),
                GridShape2D::new([w.axis_size_y(), w.axis_size_x()]),
            ),
            read_window_bounds: w,
            bounds_of_target: w,
            flip_y: false,
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
        aff.rank(dataset_hash, band, &advise(w), now, &FastHashMap::default())
    }

    #[test]
    fn rank_exact_match_is_best() {
        let w = window([100, 100], [200, 200]);
        let aff = make_affinity(42, 1, w);
        for idle in IDLE_SECS {
            let now = aff.timestamp + Duration::from_secs(idle);
            assert!(
                aff.rank(42, 1, &advise(w), now, &FastHashMap::default())
                    .is_best()
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
                let best_evict =
                    evict.rank(42, 1, &advise(window([0, 0], [7, 7])), now, &duplicates);
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
            &advise(w),
            aff.timestamp + Duration::from_secs(3600),
            &holders,
        );
        let unique_short_idle = aff.rank(
            42,
            1,
            &advise(w),
            aff.timestamp + Duration::from_secs(1),
            &holders,
        );
        holders.insert(99, 2);
        let duplicate = aff.rank(42, 1, &advise(w), aff.timestamp, &holders);

        assert!(duplicate > unique_long_idle);
        assert!(unique_long_idle > unique_short_idle);
    }

    // --- BlockRange ---

    const BLOCK: (usize, usize) = (1024, 1024);

    fn file_window(x: isize, y: isize, size_x: usize, size_y: usize) -> GdalReadWindow {
        GdalReadWindow::new(GridIdx2D::new([y, x]), GridShape2D::new([size_y, size_x]))
    }

    fn blocks_of(x: isize, y: isize, size_x: usize, size_y: usize) -> Option<BlockRange> {
        BlockRange::of_window(&file_window(x, y, size_x, size_y), BLOCK)
    }

    fn block_range(min: (usize, usize), max: (usize, usize)) -> BlockRange {
        BlockRange { min, max }
    }

    #[test]
    fn block_range_of_aligned_window() {
        assert_eq!(blocks_of(0, 0, 512, 512), Some(block_range((0, 0), (0, 0))));
        assert_eq!(
            blocks_of(1536, 512, 512, 512),
            Some(block_range((1, 0), (1, 0)))
        );
        assert_eq!(
            blocks_of(0, 0, 2048, 1024),
            Some(block_range((0, 0), (1, 0)))
        );
    }

    /// Neighbouring MGRS tiles start 10000 px apart, which is 9 blocks and 784 px.
    #[test]
    fn block_range_of_window_offset_by_784_px() {
        assert_eq!(
            blocks_of(784, 784, 512, 512),
            Some(block_range((0, 0), (1, 1)))
        );
        assert_eq!(
            blocks_of(784 + 512, 0, 512, 512),
            Some(block_range((1, 0), (1, 0)))
        );
        assert_eq!(
            blocks_of(784, 784, 512, 512).unwrap().len(),
            4,
            "a 512 px tile can touch 4 blocks"
        );
    }

    #[test]
    fn block_range_of_window_on_block_edge() {
        assert_eq!(
            blocks_of(512, 0, 512, 512),
            Some(block_range((0, 0), (0, 0)))
        );
        assert_eq!(
            blocks_of(1024, 1024, 512, 512),
            Some(block_range((1, 1), (1, 1)))
        );
        assert_eq!(blocks_of(1023, 0, 2, 1), Some(block_range((0, 0), (1, 0))));
    }

    /// A 10980 px wide file ends in block 10, which covers only 740 px.
    #[test]
    fn block_range_of_partial_edge_blocks() {
        assert_eq!(
            blocks_of(10240, 10240, 740, 740),
            Some(block_range((10, 10), (10, 10)))
        );
        assert_eq!(
            blocks_of(10000, 0, 980, 512),
            Some(block_range((9, 0), (10, 0)))
        );
    }

    #[test]
    fn block_range_of_empty_or_downsampled_read() {
        assert_eq!(blocks_of(0, 0, 0, 512), None);

        let mut read = advise(window([0, 0], [511, 511]));
        read.gdal_read_widow = file_window(0, 0, 1024, 1024);
        assert_eq!(
            BlockRange::of_read(&read, BLOCK),
            None,
            "may read an overview"
        );

        read.gdal_read_widow = file_window(0, 0, 256, 256);
        assert_eq!(
            BlockRange::of_read(&read, BLOCK),
            Some(block_range((0, 0), (0, 0))),
            "upsampling reads the band itself"
        );
    }

    #[test]
    fn it_tracks_the_most_recently_read_blocks() {
        let mut aff = make_affinity(42, 1, window([0, 0], [511, 511]));
        aff.block_size = Some((1, 1));
        // 100 blocks, more than are tracked
        aff.record_blocks(&advise(window([0, 0], [9, 9])));
        assert_eq!(aff.blocks.len(), MAX_TRACKED_BLOCKS);
        assert_eq!(aff.blocks.back(), Some(&(9, 9)));

        aff.record_blocks(&advise(window([0, 0], [0, 0])));
        assert_eq!(aff.blocks.len(), MAX_TRACKED_BLOCKS, "no duplicate entries");
        assert_eq!(
            aff.blocks.back(),
            Some(&(0, 0)),
            "read again, so most recent"
        );
    }

    // --- block-aware rank ---

    /// A worker on dataset 42 whose last read was `w`, holding `blocks` of 1024 px.
    fn holding_blocks(
        band: usize,
        w: GridBoundingBox2D,
        blocks: &[(usize, usize)],
    ) -> WorkerAffinity {
        let mut aff = make_affinity(42, band, w);
        aff.block_size = Some(BLOCK);
        aff.blocks = blocks.iter().copied().collect();
        aff
    }

    fn rank_for(aff: &WorkerAffinity, band: usize, read: &GdalReadAdvise) -> WorkerRank {
        aff.rank(42, band, read, aff.timestamp, &FastHashMap::default())
    }

    /// Reads blocks (0, 0) to (1, 1) at file pixels 784..1295.
    fn read_across_blocks() -> GdalReadAdvise {
        advise(window([784, 784], [1295, 1295]))
    }

    #[test]
    fn rank_prefers_more_shared_blocks() {
        let read = read_across_blocks();
        let far = window([9000, 9000], [9511, 9511]);
        let near = window([784, 1296], [1295, 1807]);

        let two_far = holding_blocks(1, far, &[(0, 0), (1, 0), (5, 5)]);
        let one_near = holding_blocks(1, near, &[(1, 1), (2, 1)]);
        let other_band_near = holding_blocks(2, near, &[(0, 0), (1, 0), (0, 1), (1, 1)]);

        assert!(rank_for(&two_far, 1, &read) > rank_for(&one_near, 1, &read));
        assert!(rank_for(&one_near, 1, &read) > rank_for(&other_band_near, 1, &read));
        assert!(matches!(
            rank_for(&two_far, 1, &read),
            WorkerRank::Hit {
                shared_blocks: 2,
                ..
            }
        ));
    }

    #[test]
    fn rank_breaks_shared_block_ties_by_band_then_distance() {
        let read = read_across_blocks();
        let far = window([9000, 9000], [9511, 9511]);
        let near = window([784, 1296], [1295, 1807]);

        let same_band_far = holding_blocks(1, far, &[]);
        let other_band_near = holding_blocks(2, near, &[]);
        assert!(rank_for(&same_band_far, 1, &read) > rank_for(&other_band_near, 1, &read));

        let one_far = holding_blocks(1, far, &[(0, 0)]);
        let one_near = holding_blocks(1, near, &[(1, 1)]);
        assert!(rank_for(&one_near, 1, &read) > rank_for(&one_far, 1, &read));
    }

    /// Without a known block size, or for a downsampled read, no blocks count and the rank is the
    /// same as without block awareness.
    #[test]
    fn rank_without_block_size_uses_band_and_distance() {
        let read = read_across_blocks();
        let mut near = holding_blocks(1, window([784, 1296], [1295, 1807]), &[(0, 0)]);
        near.block_size = None;
        assert_eq!(
            rank_for(&near, 1, &read),
            WorkerRank::Hit {
                shared_blocks: 0,
                same_band: true,
                distance: std::cmp::Reverse(512),
            }
        );

        let holding = holding_blocks(1, window([9000, 9000], [9511, 9511]), &[(0, 0)]);
        let mut downsampled = read;
        downsampled.gdal_read_widow = file_window(0, 0, 2048, 2048);
        assert!(matches!(
            rank_for(&holding, 1, &downsampled),
            WorkerRank::Hit {
                shared_blocks: 0,
                ..
            }
        ));
    }

    #[test]
    fn it_dispatches_to_the_worker_holding_the_blocks() {
        let (mut w0, _rx0) = dummy_worker(0);
        w0.affinity = Some(holding_blocks(1, window([0, 512], [511, 1023]), &[(0, 0)]));
        let (mut w1, mut rx1) = dummy_worker(1);
        w1.affinity = Some(holding_blocks(
            1,
            window([3000, 3000], [3511, 3511]),
            &[(0, 0), (1, 0), (0, 1), (1, 1)],
        ));

        let mut state = BrokerState::new(vec![w0, w1], 4, 4, SchedulingStrategy::GlobalMatrix);
        let (mut req, _resp) = make_request(42, 1, window([784, 784], [1295, 1295]));
        req.request.0.read_advise = read_across_blocks();
        state.enqueue_request(req);
        state.try_dispatch();

        rx1.try_recv()
            .expect("the worker holding all four blocks should read them");
        assert_eq!((state.stats.block_hits, state.stats.block_misses), (4, 0));

        // worker 0 holds one of the four blocks
        let (mut req, _resp) = make_request(42, 1, window([784, 784], [1295, 1295]));
        req.request.0.read_advise = read_across_blocks();
        state.enqueue_request(req);
        state.try_dispatch();
        assert_eq!((state.stats.block_hits, state.stats.block_misses), (5, 3));
    }

    // --- waiting for a busy holder ---

    /// Worker 0 is busy and holds all blocks of `read_across_blocks`, idle worker 1 holds none.
    fn state_with_busy_holder(
        wait: Duration,
    ) -> (
        BrokerState,
        mpsc::UnboundedReceiver<WorkerJob>,
        mpsc::UnboundedReceiver<WorkerJob>,
    ) {
        let (mut w0, rx0) = dummy_worker(0);
        w0.affinity = Some(holding_blocks(
            1,
            window([0, 0], [511, 511]),
            &[(0, 0), (1, 0), (0, 1), (1, 1)],
        ));
        let (w1, rx1) = dummy_worker(1);
        let mut state = BrokerState::new(vec![w0, w1], 4, 4, SchedulingStrategy::GlobalMatrix);
        state.idle_workers = vec![1];
        state.global_active_count = 1;
        state.busy_holder_wait = wait;
        (state, rx0, rx1)
    }

    fn enqueue_read_across_blocks(state: &mut BrokerState) -> oneshot::Receiver<ReadResult> {
        let (mut req, resp) = make_request(42, 1, window([784, 784], [1295, 1295]));
        req.request.0.read_advise = read_across_blocks();
        state.enqueue_request(req);
        resp
    }

    fn release_holder(state: &mut BrokerState) {
        state.release_worker(
            0,
            42,
            1,
            &advise(window([0, 0], [511, 511])),
            Some(BLOCK),
            false,
        );
    }

    #[test]
    fn it_waits_for_a_busy_worker_holding_the_blocks() {
        let (mut state, mut rx0, mut rx1) = state_with_busy_holder(Duration::from_secs(60));
        let _resp = enqueue_read_across_blocks(&mut state);

        state.try_dispatch();
        assert!(rx1.try_recv().is_err(), "the idle worker holds no blocks");
        assert!(state.next_wake.is_some());

        release_holder(&mut state);
        state.try_dispatch();
        rx0.try_recv()
            .expect("the holder reads the blocks once it is idle");
        assert!(state.next_wake.is_none());
        assert_eq!(state.stats.busy_waits, 1);
        assert_eq!(state.stats.busy_wait_timeouts, 0);
        assert_eq!(state.stats.block_hits, 4);
        assert_eq!(state.stats.block_misses, 0);
        assert_eq!(state.stats.per_worker[&0].busy_waits, 1);
    }

    #[test]
    fn it_reads_elsewhere_once_the_wait_expired() {
        let (mut state, _rx0, mut rx1) = state_with_busy_holder(Duration::from_millis(20));
        let _resp = enqueue_read_across_blocks(&mut state);

        state.try_dispatch();
        assert!(rx1.try_recv().is_err());
        let wake = state
            .next_wake
            .expect("the broker wakes up when the wait expires");

        std::thread::sleep(wake.saturating_duration_since(Instant::now()));
        state.try_dispatch();
        rx1.try_recv()
            .expect("the idle worker reads once the wait expired");
        assert!(state.next_wake.is_none());
        assert_eq!(state.stats.busy_waits, 1);
        assert_eq!(state.stats.busy_wait_timeouts, 1);
        assert!(state.stats.busy_wait >= Duration::from_millis(20));
        assert_eq!(
            (state.stats.block_hits, state.stats.block_misses),
            (0, 0),
            "worker 1 has not read the dataset, so its block size is unknown"
        );
    }

    #[test]
    fn it_does_not_wait_for_a_busy_holder_by_default() {
        let (mut state, _rx0, mut rx1) = state_with_busy_holder(Duration::ZERO);
        let _resp = enqueue_read_across_blocks(&mut state);

        state.try_dispatch();
        rx1.try_recv().expect("the idle worker reads at once");
    }

    #[test]
    fn it_drops_a_held_read_whose_callers_gave_up() {
        let (mut state, mut rx0, mut rx1) = state_with_busy_holder(Duration::from_secs(60));
        let resp = enqueue_read_across_blocks(&mut state);
        state.try_dispatch();

        drop(resp);
        release_holder(&mut state);
        state.try_dispatch();

        assert!(rx0.try_recv().is_err() && rx1.try_recv().is_err());
        assert_eq!(state.stats.cancelled_dropped, 1);
        assert!(state.dataset_registry.is_empty());
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
            held_since: std::cell::Cell::new(None),
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
        // a busy worker already counts as a holder of the dataset it reads
        assert_eq!(state.dataset_holders.get(&100), Some(&1));

        state.release_worker(
            0,
            job.dataset_hash,
            1,
            &advise(window([0, 0], [255, 255])),
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
        let first = rx0.try_recv().unwrap().dataset_hash;
        assert_eq!(state.global_active_count, 1);

        state.release_worker(
            0,
            first,
            1,
            &advise(window([0, 0], [255, 255])),
            None,
            false,
        );
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
        let job = rx0.try_recv().unwrap();
        state.release_worker(0, job.dataset_hash, 1, &advise(w), None, true);

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
        state.release_worker(0, 100, 1, &advise(w), Some((1024, 1024)), false);
        assert_eq!(block_size(&state), Some((1024, 1024)));

        read(&mut state, 100, 1);
        assert_eq!(
            block_size(&state),
            Some((1024, 1024)),
            "same dataset and band"
        );
        state.release_worker(0, 100, 1, &advise(w), Some((1024, 1024)), false);

        read(&mut state, 100, 2);
        assert_eq!(block_size(&state), None, "other band");
        state.release_worker(0, 100, 2, &advise(w), Some((512, 512)), false);

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
