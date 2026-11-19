#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "okay in benchmarks"
)]

//! Tile load on a `geoengine-server` child process pinned to N cores with `taskset`, shared by the
//! `wms_load` (WMS `GetMap`) and `ogc_tiles_load` (OGC API tiles of a layer) benches.
//!
//! For each core count, the server starts on its own cleared schema. Each client requests 256 px
//! EPSG:3857 tiles of the NDVI test dataset in its own loop, alone and next to a looping export of the
//! dataset; both benches request the same tiles. Criterion reports requests/s. Per case, the bench
//! prints p50/p99 per request, errors, and, per thread group of the server and its GDAL worker
//! processes, the CPU time, context switches and migrations per request from `/proc`, as well as
//! thread counts and peak RSS.
//!
//! Needs Postgres as configured for the server and release builds of the server:
//! `cargo build --release --bin gdalsource-process --bin geoengine-server && cargo bench --bench wms_load`
//! (or `--bench ogc_tiles_load`). Both read these variables:
//!
//! - `TILE_LOAD_SERVER`: the server binary, default `target/release/geoengine-server`. The server runs in
//!   the workspace directory of the binary (`<dir>/target/release/geoengine-server`), so it reads the
//!   settings and test data of that directory.
//! - `TILE_LOAD_CORES`: comma separated core counts, default `8,16,32`. Use it rather than a criterion
//!   filter, which still starts a server per core count.
//! - `TILE_LOAD_CPUS`: comma separated CPUs for the server, default all in order; a core count of N
//!   uses the first N. Pin the bench itself to other CPUs, e.g. with `taskset`, to keep the clients off
//!   the server's CPUs.
//! - `TILE_LOAD_BACKGROUNDS`: comma separated `none` and `export`, default both.
//! - `TILE_LOAD_TILE_MATRIX_SETS` (`ogc_tiles_load` only): comma separated `WebMercatorQuad`, which
//!   requests the same tiles as `wms_load`, and `Custom`, the layer's own tile matrix set at its finest
//!   tile matrix with at most 1024 tiles; default `WebMercatorQuad`.
//! - `TILE_LOAD_COLD=1`: clear the server caches (`POST /cache/clear`) before each case.
//! - `TILE_LOAD_LOG_SPEC`: the log spec of the server, default `warn`.

use criterion::{BenchmarkId, Criterion, SamplingMode, Throughput};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

const ADDRESS: &str = "127.0.0.1:3047";
const CLIENTS: [usize; 4] = [1, 8, 32, 64];
const HALF_WORLD: f64 = 20_037_508.342_789_244;
/// zoom 5 of `WebMercatorQuad`: 32×32 tiles; the 24 rows within ±80° latitude are used
const ZOOM: u8 = 5;
const ZOOM_TILES: u64 = 1 << ZOOM;
const ROWS: u64 = 24;
/// `/proc` reports CPU time in `USER_HZ`, which is 100 on Linux
const TICKS_PER_SECOND: f64 = 100.;
/// the internal layer database: its data connector and root collection
const LAYER_DB: &str = "ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74";
const LAYER_DB_ROOT: &str = "05102bb3-a855-4a37-8a8a-30026a91fef1";
const ANONYMOUS_ROLE: &str = "fd8e87bf-515c-4f36-8da6-1a53702ff102";
const TIME: &str = "2014-04-01T12:00:00.000Z";

/// The server child process, stopped with SIGTERM on drop so that it stops its GDAL workers
struct Server {
    child: Child,
    log: PathBuf,
}

impl Server {
    fn start(binary: &Path, name: &str, cpus: &[usize]) -> Self {
        let cores = cpus.len();
        assert!(
            std::net::TcpListener::bind(ADDRESS).is_ok(),
            "{ADDRESS} is in use, is another server running?"
        );
        let workspace = binary
            .ancestors()
            .nth(3)
            .expect("the server binary should be in `<dir>/target/release`");
        let log = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("{name}-server-{cores}.log"));
        let child = Command::new("taskset")
            .arg("-c")
            .arg(
                cpus.iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
            )
            .arg(binary)
            .current_dir(workspace)
            .env("GEOENGINE__POSTGRES__SCHEMA", name)
            .env("GEOENGINE__POSTGRES__CLEAR_DATABASE_ON_START", "true")
            .env("GEOENGINE__WEB__BIND_ADDRESS", ADDRESS)
            .env(
                "GEOENGINE__LOGGING__LOG_SPEC",
                std::env::var("TILE_LOAD_LOG_SPEC").unwrap_or_else(|_| "warn".to_owned()),
            )
            .stdout(Stdio::null())
            .stderr(File::create(&log).unwrap())
            .spawn()
            .unwrap();
        Self { child, log }
    }

    /// The server process itself, since `taskset` replaces itself with the server
    fn pid(&self) -> u32 {
        self.child.id()
    }

    async fn wait_until_ready(&mut self, client: &reqwest::Client) {
        for _ in 0..120 {
            let info = client
                .get(format!("http://{ADDRESS}/api/info"))
                .send()
                .await;
            if info.is_ok_and(|response| response.status().is_success()) {
                return;
            }
            assert!(
                self.child.try_wait().unwrap().is_none(),
                "the server exited, see {}",
                self.log.display()
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        panic!(
            "the server did not start within 120 s, see {}",
            self.log.display()
        );
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = Command::new("kill").arg(self.pid().to_string()).status();
        for _ in 0..60 {
            if self.child.try_wait().is_ok_and(|status| status.is_some()) {
                return;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        eprintln!("the server did not stop within 60 s of SIGTERM, killing it");
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The REST API with a session token
#[derive(Clone)]
struct Api {
    client: reqwest::Client,
    token: String,
}

impl Api {
    async fn session(client: &reqwest::Client, path: &str, credentials: Option<Value>) -> Self {
        let mut request = client.post(format!("http://{ADDRESS}/api{path}"));
        if let Some(credentials) = credentials {
            request = request.json(&credentials);
        }
        let session: Value = request
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        Self {
            client: client.clone(),
            token: session["id"].as_str().unwrap().to_owned(),
        }
    }

    async fn post(&self, path: &str, body: &Value) -> reqwest::Result<Value> {
        self.client
            .post(format!("http://{ADDRESS}/api{path}"))
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
    }

    async fn put(&self, path: &str, body: &Value) -> reqwest::Result<()> {
        self.client
            .put(format!("http://{ADDRESS}/api{path}"))
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    async fn get(&self, path: &str) -> reqwest::Result<Value> {
        self.client
            .get(format!("http://{ADDRESS}/api{path}"))
            .bearer_auth(&self.token)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
    }

    /// Clears the server caches; needs an admin session
    async fn clear_caches(&self) {
        self.client
            .post(format!("http://{ADDRESS}/api/cache/clear"))
            .bearer_auth(&self.token)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    /// Requests the tile with index `tile` modulo the tiles of `target`, by WMS or as OGC API tile;
    /// returns whether it got a PNG
    async fn get_tile(&self, target: &Target, tile: u64) -> bool {
        let url = if let Some(OgcTiles {
            layer,
            tile_matrix_set,
            tile_matrix,
            width,
            rows,
        }) = &target.ogc
        {
            let tile = tile % (width * (rows.end - rows.start));
            let (row, col) = (rows.start + tile / width, tile % width);
            format!(
                "http://{ADDRESS}/api/ogc/{LAYER_DB}/{layer}/collections/{layer}/map/tiles/{tile_matrix_set}\
                 /{tile_matrix}/{row}/{col}?datetime={TIME}"
            )
        } else {
            let tile = tile % (ZOOM_TILES * ROWS);
            let (row, col) = (4 + tile / ZOOM_TILES, tile % ZOOM_TILES);
            let graph = &target.graph;
            let size = 2. * HALF_WORLD / ZOOM_TILES as f64;
            let x_min = -HALF_WORLD + col as f64 * size;
            let y_max = HALF_WORLD - row as f64 * size;
            format!(
                "http://{ADDRESS}/api/wms/{graph}?service=WMS&version=1.3.0&request=GetMap&layers={graph}\
                 &bbox={x_min},{},{},{y_max}&width=256&height=256&crs=EPSG:3857&format=image/png&styles=\
                 &time={TIME}",
                y_max - size,
                x_min + size,
            )
        };
        match self.client.get(url).bearer_auth(&self.token).send().await {
            Ok(response) if response.status().is_success() => response
                .bytes()
                .await
                .is_ok_and(|png| png.starts_with(b"\x89PNG")),
            _ => false,
        }
    }
}

/// The NDVI processing graph, and for OGC API tiles the tiles of a layer of it that anonymous sessions may read
struct Target {
    graph: String,
    ogc: Option<OgcTiles>,
}

impl Target {
    /// The WMS target, or with a `layer` one OGC API target per tile matrix set
    async fn all(
        api: &Api,
        graph: String,
        layer: Option<String>,
        tile_matrix_sets: &[String],
    ) -> Vec<Self> {
        let Some(layer) = layer else {
            return vec![Self { graph, ogc: None }];
        };
        let mut targets = Vec::new();
        for tile_matrix_set in tile_matrix_sets {
            let ogc = OgcTiles::new(api, &layer, tile_matrix_set).await;
            targets.push(Self {
                graph: graph.clone(),
                ogc: Some(ogc),
            });
        }
        targets
    }

    /// `WebMercatorQuad` keeps the group names of `ogc_tiles_load` from before tile matrix sets
    fn group_name(&self, name: &str, cores: usize, cold: bool) -> String {
        let tile_matrix_set = match &self.ogc {
            Some(ogc) if ogc.tile_matrix_set != "WebMercatorQuad" => {
                format!("{}/", ogc.tile_matrix_set)
            }
            _ => String::new(),
        };
        let cold = if cold { "_cold" } else { "" };
        format!("{name}/{tile_matrix_set}cores_{cores}{cold}")
    }
}

/// OGC API tiles: the rows `rows` of `tile_matrix` in `tile_matrix_set`, `width` tiles wide
struct OgcTiles {
    layer: String,
    tile_matrix_set: String,
    tile_matrix: String,
    width: u64,
    rows: std::ops::Range<u64>,
}

impl OgcTiles {
    /// `WebMercatorQuad` uses the tiles of `wms_load`; other tile matrix sets their finest tile
    /// matrix with at most 1024 tiles, from their definition
    async fn new(api: &Api, layer: &str, tile_matrix_set: &str) -> Self {
        if tile_matrix_set == "WebMercatorQuad" {
            return Self {
                layer: layer.to_owned(),
                tile_matrix_set: tile_matrix_set.to_owned(),
                tile_matrix: ZOOM.to_string(),
                width: ZOOM_TILES,
                rows: 4..4 + ROWS,
            };
        }
        let definition = api
            .get(&format!(
                "/ogc/{LAYER_DB}/{layer}/tileMatrixSets/{tile_matrix_set}"
            ))
            .await
            .unwrap();
        let (tile_matrix, width, height) = definition["tileMatrices"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| {
                (
                    m["id"].as_str().unwrap().to_owned(),
                    m["matrixWidth"].as_u64().unwrap(),
                    m["matrixHeight"].as_u64().unwrap(),
                )
            })
            .filter(|(_, width, height)| width * height <= 1024)
            .max_by_key(|(_, width, height)| width * height)
            .expect("a tile matrix should have at most 1024 tiles");
        println!("{tile_matrix_set}: tile matrix {tile_matrix}, {width}×{height} tiles");
        Self {
            layer: layer.to_owned(),
            tile_matrix_set: tile_matrix_set.to_owned(),
            tile_matrix,
            width,
            rows: 0..height,
        }
    }
}

/// Waits for the server, then returns an anonymous session, an admin session, the NDVI processing
/// graph and, if `ogc`, a layer of it that anonymous sessions may read
async fn prepare(
    server: &mut Server,
    client: &reqwest::Client,
    ogc: bool,
) -> (Api, Api, String, Option<String>) {
    server.wait_until_ready(client).await;
    let api = Api::session(client, "/anonymous", None).await;
    // the default admin of `[user]` in `Settings-default.toml`
    let credentials = json!({"email": "admin@localhost", "password": "adminadmin"});
    let admin = Api::session(client, "/login", Some(credentials)).await;
    let ndvi =
        json!({"type": "Raster", "operator": {"type": "GdalSource", "params": {"data": "ndvi"}}});
    let graph = api.post("/processingGraphs", &ndvi).await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut layer = None;
    if ogc {
        let add = json!({"name": "NDVI", "description": "tile load", "processingGraph": ndvi});
        let path = format!("/layerDb/collections/{LAYER_DB_ROOT}/layers");
        let id = admin.post(&path, &add).await.unwrap()["id"]
            .as_str()
            .unwrap()
            .to_owned();
        let read = json!({"resource": {"type": "layer", "id": id}, "roleId": ANONYMOUS_ROLE, "permission": "Read"});
        admin.put("/permissions", &read).await.unwrap();
        layer = Some(id);
    }
    (api, admin, graph, layer)
}

/// Runs `clients` clients that each send `requests` tile requests one after another; returns latency and success per request
async fn run_clients(
    api: &Api,
    target: &Target,
    clients: usize,
    requests: u64,
    next_tile: &AtomicU64,
) -> Vec<(Duration, bool)> {
    let clients = (0..clients).map(|_| async {
        let mut results = Vec::new();
        for _ in 0..requests {
            // a fixed permutation of the tiles: 7919 is prime, so coprime to their number
            let tile = next_tile.fetch_add(1, Ordering::Relaxed) * 7919;
            let start = Instant::now();
            let ok = api.get_tile(target, tile).await;
            results.push((start.elapsed(), ok));
        }
        results
    });
    futures::future::join_all(clients)
        .await
        .into_iter()
        .flatten()
        .collect()
}

/// Exports the NDVI dataset for six months as a COG, again and again, until `stop` is set.
/// Returns the number of completed exports and panics if one fails, since the load would be wrong.
async fn export_loop(api: Api, graph: String, stop: Arc<AtomicBool>) -> u64 {
    let body = json!({
        "displayName": "wms_load export",
        "query": {
            "spatialBounds": {
                "upperLeftCoordinate": {"x": -180.0, "y": 90.0},
                "lowerRightCoordinate": {"x": 180.0, "y": -90.0},
            },
            "timeInterval": {"start": 1_388_534_400_000_i64, "end": 1_404_172_800_000_i64},
        },
    });
    let export = || async {
        let task = api
            .post(&format!("/datasetFromProcessingGraph/{graph}"), &body)
            .await?;
        let path = format!(
            "/tasks/{}/status",
            task["taskId"].as_str().unwrap_or_default()
        );
        let mut failed_polls = 0;
        loop {
            // the status request is idempotent: retry it if the server closed the connection under load
            match api.get(&path).await {
                Ok(status) if status["status"] != "running" => return reqwest::Result::Ok(status),
                Ok(_) => failed_polls = 0,
                Err(error) if failed_polls < 3 => {
                    eprintln!("export status poll failed, retrying: {error}");
                    failed_polls += 1;
                }
                Err(error) => return Err(error),
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    };
    let mut completed = 0;
    while !stop.load(Ordering::Relaxed) {
        let status = export().await.unwrap();
        assert_eq!(status["status"], "completed", "export failed: {status}");
        completed += 1;
    }
    completed
}

#[derive(Default, Clone, Copy)]
struct Usage {
    cpu_ticks: u64,
    switches: u64,
    migrations: u64,
    threads: u64,
}

/// Usage per thread group (thread name without its number) of `pid` and its child processes, and their summed peak RSS in kB.
/// The CPU time of threads that ended (e.g. the decoding threads `OpenJPEG` starts per read) only shows in the
/// process total, so each process also gets a group `ended threads` with the rest of its total.
fn thread_groups(pid: u32) -> (BTreeMap<String, Usage>, u64) {
    let children = fs::read_dir("/proc").unwrap().filter_map(|entry| {
        let child: u32 = entry.ok()?.file_name().to_str()?.parse().ok()?;
        let stat = fs::read_to_string(format!("/proc/{child}/stat")).ok()?;
        let ppid: u32 = stat[stat.rfind(')')? + 2..]
            .split_whitespace()
            .nth(1)?
            .parse()
            .ok()?;
        (ppid == pid).then_some(child)
    });
    let mut groups = BTreeMap::<String, Usage>::new();
    let mut peak_rss_kb = 0;
    for (process, prefix) in
        std::iter::once((pid, "")).chain(children.map(|child| (child, "gdal-process:")))
    {
        let Ok(tasks) = fs::read_dir(format!("/proc/{process}/task")) else {
            continue;
        };
        let Some(process_ticks) = fs::read_to_string(format!("/proc/{process}/stat"))
            .ok()
            .and_then(|stat| cpu_ticks(&stat))
        else {
            continue; // the process exited
        };
        let mut thread_ticks = 0;
        for task in tasks.flatten() {
            let (Ok(stat), Ok(sched)) = (
                fs::read_to_string(task.path().join("stat")),
                fs::read_to_string(task.path().join("sched")),
            ) else {
                continue; // the thread exited
            };
            let name = stat[stat.find('(').unwrap() + 1..stat.rfind(')').unwrap()]
                .trim_end_matches(|c: char| c.is_ascii_digit());
            let name = name.strip_suffix(['-', '_', ' ']).unwrap_or(name);
            let usage = groups.entry(format!("{prefix}{name}")).or_default();
            let ticks = cpu_ticks(&stat).unwrap();
            usage.cpu_ticks += ticks;
            thread_ticks += ticks;
            usage.switches += proc_value(&sched, "nr_switches");
            usage.migrations += proc_value(&sched, "se.nr_migrations");
            usage.threads += 1;
        }
        groups
            .entry(format!("{prefix}ended threads"))
            .or_default()
            .cpu_ticks += process_ticks.saturating_sub(thread_ticks);
        peak_rss_kb += fs::read_to_string(format!("/proc/{process}/status"))
            .map_or(0, |status| proc_value(&status, "VmHWM"));
    }
    (groups, peak_rss_kb)
}

/// utime plus stime of a `/proc` `stat` file, fields 14 and 15; for a process, they include its ended threads
fn cpu_ticks(stat: &str) -> Option<u64> {
    let mut fields = stat[stat.rfind(')')? + 2..].split_whitespace().skip(11);
    Some(fields.next()?.parse::<u64>().ok()? + fields.next()?.parse::<u64>().ok()?)
}

/// The number after `key` and a colon in a `/proc` file, e.g. `nr_switches : 42` or `VmHWM: 42 kB`
fn proc_value(text: &str, key: &str) -> u64 {
    text.lines()
        .find_map(|line| line.strip_prefix(key)?.trim_start().strip_prefix(':'))
        .and_then(|value| value.split_whitespace().next()?.parse().ok())
        .unwrap_or(0)
}

/// Prints latencies and the usage between `before` and `after` per request
fn print_case(
    id: &str,
    latencies: &[(Duration, bool)],
    elapsed: Duration,
    before: &BTreeMap<String, Usage>,
    after: &(BTreeMap<String, Usage>, u64),
) {
    let mut ok: Vec<Duration> = latencies
        .iter()
        .filter(|(_, ok)| *ok)
        .map(|(latency, _)| *latency)
        .collect();
    ok.sort();
    let quantile = |q: f64| {
        ok.get(((ok.len() as f64 * q) as usize).min(ok.len().saturating_sub(1)))
            .map_or(0., |d| d.as_secs_f64() * 1000.)
    };
    let requests = latencies.len().max(1) as f64;
    let cpu_s = after
        .0
        .values()
        .map(|u| u.cpu_ticks)
        .sum::<u64>()
        .saturating_sub(before.values().map(|u| u.cpu_ticks).sum()) as f64
        / TICKS_PER_SECOND;
    println!(
        "{id}: {} requests, {} errors, p50 {:.1} ms, p99 {:.1} ms, {:.1} busy cores, peak RSS {} MB",
        latencies.len(),
        latencies.len() - ok.len(),
        quantile(0.5),
        quantile(0.99),
        cpu_s / elapsed.as_secs_f64(),
        after.1 / 1024,
    );
    println!(
        "  {:<32} {:>8} {:>12} {:>14} {:>16}",
        "thread group", "threads", "CPU ms/req", "switches/req", "migrations/req"
    );
    for (name, a) in &after.0 {
        let b = before.get(name).copied().unwrap_or_default();
        println!(
            "  {name:<32} {:>8} {:>12.2} {:>14.1} {:>16.1}",
            a.threads,
            a.cpu_ticks.saturating_sub(b.cpu_ticks) as f64 / TICKS_PER_SECOND * 1000. / requests,
            a.switches.saturating_sub(b.switches) as f64 / requests,
            a.migrations.saturating_sub(b.migrations) as f64 / requests,
        );
    }
}

/// The `TILE_LOAD_*` environment variables described at the top
struct Config {
    binary: PathBuf,
    cores: Vec<usize>,
    cpus: Vec<usize>,
    backgrounds: Vec<String>,
    tile_matrix_sets: Vec<String>,
    cold: bool,
}

impl Config {
    fn from_env() -> Self {
        let var = |name: &str, default: &str| -> Vec<String> {
            std::env::var(name)
                .unwrap_or_else(|_| default.to_owned())
                .split(',')
                .map(|v| v.trim().to_owned())
                .collect()
        };
        let binary = std::env::var("TILE_LOAD_SERVER").map_or_else(
            |_| Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/release/geoengine-server"),
            PathBuf::from,
        );
        let all_cpus = (0..std::thread::available_parallelism().unwrap().get())
            .map(|cpu| cpu.to_string())
            .collect::<Vec<_>>()
            .join(",");
        Self {
            binary: fs::canonicalize(&binary)
                .unwrap_or_else(|_| panic!("{} should exist", binary.display())),
            cores: var("TILE_LOAD_CORES", "8,16,32")
                .iter()
                .map(|n| n.parse().unwrap())
                .collect(),
            cpus: var("TILE_LOAD_CPUS", &all_cpus)
                .iter()
                .map(|n| n.parse().unwrap())
                .collect(),
            backgrounds: var("TILE_LOAD_BACKGROUNDS", "none,export"),
            tile_matrix_sets: var("TILE_LOAD_TILE_MATRIX_SETS", "WebMercatorQuad"),
            cold: std::env::var("TILE_LOAD_COLD").is_ok_and(|v| v == "1"),
        }
    }
}

/// Runs the bench `name`, with OGC API tiles if `ogc`, else with WMS `GetMap`
pub fn run(c: &mut Criterion, name: &str, ogc: bool) {
    let Config {
        binary,
        cores,
        cpus,
        backgrounds,
        tile_matrix_sets,
        cold,
    } = Config::from_env();

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();
    let next_tile = AtomicU64::new(0);

    for cores in cores {
        assert!(
            cores <= cpus.len(),
            "{cores} cores, but only {cpus:?} given"
        );
        let mut server = Server::start(&binary, name, &cpus[..cores]);
        let (api, admin, graph, layer) = runtime.block_on(prepare(&mut server, &client, ogc));
        let targets = runtime.block_on(Target::all(&api, graph, layer, &tile_matrix_sets));

        for target in &targets {
            let group_name = target.group_name(name, cores, cold);
            let mut group = c.benchmark_group(&group_name);
            group
                .sampling_mode(SamplingMode::Flat)
                .sample_size(10)
                .warm_up_time(Duration::from_secs(3))
                .measurement_time(Duration::from_secs(20));

            for background in backgrounds.iter().map(String::as_str) {
                for clients in CLIENTS {
                    if cold {
                        runtime.block_on(admin.clear_caches());
                    }
                    let stop = Arc::new(AtomicBool::new(false));
                    let export = (background == "export").then(|| {
                        let export = runtime.spawn(export_loop(
                            api.clone(),
                            target.graph.clone(),
                            stop.clone(),
                        ));
                        std::thread::sleep(Duration::from_secs(5)); // let the export get going
                        export
                    });

                    let latencies = Mutex::new(Vec::new());
                    // taken after each iteration, so that Criterion's analysis does not count
                    let end = Mutex::new(None);
                    let pid = server.pid();
                    let before = thread_groups(pid).0;
                    let case_start = Instant::now();
                    group.throughput(Throughput::Elements(clients as u64));
                    group.bench_function(
                        BenchmarkId::new(background, format!("clients_{clients}")),
                        |b| {
                            b.to_async(&runtime).iter_custom(|iters| {
                                let (api, next_tile, latencies, end) =
                                    (&api, &next_tile, &latencies, &end);
                                async move {
                                    let start = Instant::now();
                                    let results =
                                        run_clients(api, target, clients, iters, next_tile).await;
                                    let elapsed = start.elapsed();
                                    latencies.lock().unwrap().extend(results);
                                    *end.lock().unwrap() =
                                        Some((case_start.elapsed(), thread_groups(pid)));
                                    elapsed
                                }
                            });
                        },
                    );
                    let (elapsed, after) = end.into_inner().unwrap().unwrap();
                    let id = format!("{group_name}/{background}/clients_{clients}");
                    print_case(
                        &id,
                        &latencies.into_inner().unwrap(),
                        elapsed,
                        &before,
                        &after,
                    );

                    stop.store(true, Ordering::Relaxed);
                    if let Some(export) = export {
                        // finish the running export before the next case
                        let completed = runtime.block_on(export).unwrap();
                        println!("  {completed} exports completed");
                    }
                }
            }
            group.finish();
        }
        drop(server);
    }
}
