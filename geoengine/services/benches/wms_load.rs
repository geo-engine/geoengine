#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "okay in benchmarks"
)]

//! WMS `GetMap` load on a `geoengine-server` child process pinned to the first N cores with `taskset`.
//!
//! For each core count, the server starts on its own cleared schema. Each client requests 256 px
//! EPSG:3857 tiles of the NDVI test dataset in its own loop, alone and next to a looping export of the
//! dataset. Criterion reports requests/s. Per case, the bench prints p50/p99 per request, errors, and,
//! per thread group of the server and its GDAL worker processes, the CPU time, context switches and
//! migrations per request from `/proc`, as well as thread counts and peak RSS.
//!
//! Needs Postgres as configured for the server and release builds of the server:
//! `cargo build --release --bin gdalsource-process --bin geoengine-server && cargo bench --bench wms_load`
//!
//! - `WMS_LOAD_SERVER`: the server binary, default `target/release/geoengine-server`. The server runs in
//!   the workspace directory of the binary (`<dir>/target/release/geoengine-server`), so it reads the
//!   settings and test data of that directory.
//! - `WMS_LOAD_CORES`: comma separated core counts, default `8,16,32`. Use it rather than a criterion
//!   filter, which still starts a server per core count.
//! - `WMS_LOAD_COLD=1`: clear the server caches (`POST /cache/clear`) before each case.

use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
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
/// 32×32 tiles; the 24 rows within ±80° latitude are used
const ZOOM_TILES: u64 = 32;
const ROWS: u64 = 24;
/// `/proc` reports CPU time in `USER_HZ`, which is 100 on Linux
const TICKS_PER_SECOND: f64 = 100.;

/// The server child process, stopped with SIGTERM on drop so that it stops its GDAL workers
struct Server {
    child: Child,
    log: PathBuf,
}

impl Server {
    fn start(binary: &Path, cores: usize) -> Self {
        assert!(
            std::net::TcpListener::bind(ADDRESS).is_ok(),
            "{ADDRESS} is in use, is another server running?"
        );
        let workspace = binary
            .ancestors()
            .nth(3)
            .expect("the server binary should be in `<dir>/target/release`");
        let log =
            Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("wms_load-server-{cores}.log"));
        let child = Command::new("taskset")
            .arg("-c")
            .arg(format!("0-{}", cores - 1))
            .arg(binary)
            .current_dir(workspace)
            .env("GEOENGINE__POSTGRES__SCHEMA", "wms_load")
            .env("GEOENGINE__POSTGRES__CLEAR_DATABASE_ON_START", "true")
            .env("GEOENGINE__WEB__BIND_ADDRESS", ADDRESS)
            .env("GEOENGINE__LOGGING__LOG_SPEC", "warn")
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

    /// Requests the tile with index `tile` modulo the tiles in use; returns whether it got a PNG
    async fn get_map(&self, graph: &str, tile: u64) -> bool {
        let size = 2. * HALF_WORLD / ZOOM_TILES as f64;
        let tile = tile % (ZOOM_TILES * ROWS);
        let x_min = -HALF_WORLD + (tile % ZOOM_TILES) as f64 * size;
        let y_max = HALF_WORLD - (4 + tile / ZOOM_TILES) as f64 * size;
        let url = format!(
            "http://{ADDRESS}/api/wms/{graph}?service=WMS&version=1.3.0&request=GetMap&layers={graph}\
             &bbox={x_min},{},{},{y_max}&width=256&height=256&crs=EPSG:3857&format=image/png&styles=\
             &time=2014-04-01T12:00:00.000Z",
            y_max - size,
            x_min + size,
        );
        match self.client.get(url).bearer_auth(&self.token).send().await {
            Ok(response) if response.status().is_success() => response
                .bytes()
                .await
                .is_ok_and(|png| png.starts_with(b"\x89PNG")),
            _ => false,
        }
    }
}

/// Waits for the server, then returns an anonymous session, an admin session and the NDVI processing graph
async fn prepare(server: &mut Server, client: &reqwest::Client) -> (Api, Api, String) {
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
    (api, admin, graph)
}

/// Runs `clients` clients that each send `requests` `GetMap` requests one after another; returns latency and success per request
async fn run_clients(
    api: &Api,
    graph: &str,
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
            let ok = api.get_map(graph, tile).await;
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

/// Exports the NDVI dataset for six months as a COG, again and again, until `stop` is set
async fn export_loop(api: Api, graph: String, stop: Arc<AtomicBool>) {
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
        let status = format!(
            "/tasks/{}/status",
            task["taskId"].as_str().unwrap_or_default()
        );
        while api.get(&status).await?["status"] == "running" {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        reqwest::Result::Ok(())
    };
    while !stop.load(Ordering::Relaxed) {
        if let Err(error) = export().await {
            eprintln!("the background export failed, stopping it: {error}");
            return;
        }
    }
}

#[derive(Default, Clone, Copy)]
struct Usage {
    cpu_ticks: u64,
    switches: u64,
    migrations: u64,
    threads: u64,
}

/// Usage per thread group (thread name without its number) of `pid` and its child processes, and their summed peak RSS in kB
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
            let fields: Vec<&str> = stat[stat.rfind(')').unwrap() + 2..]
                .split_whitespace()
                .collect();
            let usage = groups.entry(format!("{prefix}{name}")).or_default();
            // fields 14 and 15 of `stat` are utime and stime
            usage.cpu_ticks +=
                fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap();
            usage.switches += proc_value(&sched, "nr_switches");
            usage.migrations += proc_value(&sched, "se.nr_migrations");
            usage.threads += 1;
        }
        peak_rss_kb += fs::read_to_string(format!("/proc/{process}/status"))
            .map_or(0, |status| proc_value(&status, "VmHWM"));
    }
    (groups, peak_rss_kb)
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

fn wms_load(c: &mut Criterion) {
    let binary = std::env::var("WMS_LOAD_SERVER").map_or_else(
        |_| Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/release/geoengine-server"),
        PathBuf::from,
    );
    let binary =
        fs::canonicalize(&binary).unwrap_or_else(|_| panic!("{} should exist", binary.display()));
    let cores = std::env::var("WMS_LOAD_CORES").unwrap_or_else(|_| "8,16,32".to_owned());
    let cold = std::env::var("WMS_LOAD_COLD").is_ok_and(|v| v == "1");

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();
    let next_tile = AtomicU64::new(0);

    for cores in cores.split(',').map(|n| n.trim().parse::<usize>().unwrap()) {
        let mut server = Server::start(&binary, cores);
        let (api, admin, graph) = runtime.block_on(prepare(&mut server, &client));

        let mut group = c.benchmark_group(format!(
            "wms_load/cores_{cores}{}",
            if cold { "_cold" } else { "" }
        ));
        group
            .sampling_mode(SamplingMode::Flat)
            .sample_size(10)
            .warm_up_time(Duration::from_secs(3))
            .measurement_time(Duration::from_secs(20));

        for background in ["none", "export"] {
            for clients in CLIENTS {
                if cold {
                    runtime.block_on(admin.clear_caches());
                }
                let stop = Arc::new(AtomicBool::new(false));
                let export = (background == "export").then(|| {
                    let export =
                        runtime.spawn(export_loop(api.clone(), graph.clone(), stop.clone()));
                    std::thread::sleep(Duration::from_secs(5)); // let the export get going
                    export
                });

                let latencies = Mutex::new(Vec::new());
                let before = thread_groups(server.pid()).0;
                let start = Instant::now();
                group.throughput(Throughput::Elements(clients as u64));
                group.bench_function(
                    BenchmarkId::new(background, format!("clients_{clients}")),
                    |b| {
                        b.to_async(&runtime).iter_custom(|iters| {
                            let (api, graph, next_tile, latencies) =
                                (&api, &graph, &next_tile, &latencies);
                            async move {
                                let start = Instant::now();
                                let results =
                                    run_clients(api, graph, clients, iters, next_tile).await;
                                let elapsed = start.elapsed();
                                latencies.lock().unwrap().extend(results);
                                elapsed
                            }
                        });
                    },
                );
                let elapsed = start.elapsed();
                let id = format!(
                    "wms_load/cores_{cores}{}/{background}/clients_{clients}",
                    if cold { "_cold" } else { "" }
                );
                print_case(
                    &id,
                    &latencies.into_inner().unwrap(),
                    elapsed,
                    &before,
                    &thread_groups(server.pid()),
                );

                stop.store(true, Ordering::Relaxed);
                if let Some(export) = export {
                    runtime.block_on(export).unwrap(); // finish the running export before the next case
                }
            }
        }
        group.finish();
        drop(server);
    }
}

criterion_group!(benches, wms_load);
criterion_main!(benches);
