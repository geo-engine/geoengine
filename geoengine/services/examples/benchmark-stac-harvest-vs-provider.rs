use anyhow::{Context, Result, anyhow, bail};
use geoengine_api_client::apis;
use geoengine_api_client::apis::configuration::Configuration;
use geoengine_api_client::models;
use geoengine_datatypes::operations::image::{Colorizer, RasterColorizer, RgbaColor};
use serde_json::Value;
use std::env;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use uuid::Uuid;

const BBOX: &str = "399960.0000000000000000,5590200.0000000000000000,509760.0000000000000000,5700000.0000000000000000";
const CRS: &str = "EPSG:32632";
const TIME: &str = "2026-01-03T00:00:00.000Z";
const IMAGE_WIDTH: i32 = 1830;
const IMAGE_HEIGHT: i32 = 1830;

#[derive(Debug, Clone)]
struct Config {
    runs: u32,
    host: String,
    port: u16,
    stac_url: String,
    stac_s3_endpoint: String,
    stac_s3_access_key: String,
    stac_s3_secret_key: String,
    out_dir: PathBuf,
    import_limit: u32,
    import_bbox: String,
    import_time_start: String,
    import_time_end: String,
}

#[derive(Debug, Clone)]
struct BenchRecord {
    run: u32,
    scenario: &'static str,
    import_ms: u128,
    setup_ms: u128,
    wms_ms: u128,
    total_ms: u128,
    http_status: u16,
}

struct ServerHandle {
    child: Child,
}

impl ServerHandle {
    async fn start(root_dir: &Path, log_file: &Path, base_url: &str) -> Result<Self> {
        let stdout = File::create(log_file)
            .with_context(|| format!("creating log file {}", log_file.display()))?;
        let stderr = stdout
            .try_clone()
            .context("cloning server log file handle")?;

        let child = Command::new("cargo")
            .arg("run")
            .arg("--release")
            .arg("--bin")
            .arg("geoengine-server")
            .env("RUST_LOG", "info")
            .current_dir(root_dir)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .context("starting geoengine-server")?;

        let mut handle = Self { child };
        handle
            .wait_until_ready(base_url, log_file)
            .await
            .context("waiting for geoengine-server readiness")?;

        Ok(handle)
    }

    async fn wait_until_ready(&mut self, base_url: &str, log_file: &Path) -> Result<()> {
        let client = reqwest::Client::new();
        let health_url = format!("{base_url}/swagger-ui/");

        loop {
            if let Some(exit) = self
                .child
                .try_wait()
                .context("checking geoengine-server process status")?
            {
                bail!(
                    "geoengine-server exited early with status {exit}. See {}",
                    log_file.display()
                );
            }

            match client.get(&health_url).send().await {
                Ok(response) if response.status().is_success() => return Ok(()),
                _ => sleep(Duration::from_millis(500)).await,
            }
        }
    }

    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env()?;
    let root_dir = workspace_root();

    let log_dir = config.out_dir.join("logs");
    let png_dir = config.out_dir.join("png");
    fs::create_dir_all(&log_dir).with_context(|| format!("creating {}", log_dir.display()))?;
    fs::create_dir_all(&png_dir).with_context(|| format!("creating {}", png_dir.display()))?;

    let csv_file = config.out_dir.join("results.csv");
    let mut csv_writer =
        File::create(&csv_file).with_context(|| format!("creating {}", csv_file.display()))?;
    writeln!(
        csv_writer,
        "run,scenario,import_ms,setup_ms,wms_ms,total_ms,http_status"
    )?;

    println!("Running benchmark with RUNS={}", config.runs);
    println!("Output directory: {}", config.out_dir.display());

    let mut records = Vec::new();

    for run in 1..=config.runs {
        println!("\n=== Run {run}/{}: harvesting scenario ===", config.runs);
        let harvest = run_harvest_benchmark(run, &config, &root_dir, &log_dir, &png_dir).await?;
        write_record(&mut csv_writer, &harvest)?;
        records.push(harvest);

        println!(
            "\n=== Run {run}/{}: ad-hoc provider scenario ===",
            config.runs
        );
        let provider = run_provider_benchmark(run, &config, &root_dir, &log_dir, &png_dir).await?;
        write_record(&mut csv_writer, &provider)?;
        records.push(provider);
    }

    csv_writer.flush()?;

    println!("\nBenchmark results: {}", csv_file.display());
    print_summary(&records);

    Ok(())
}

async fn run_harvest_benchmark(
    run: u32,
    config: &Config,
    root_dir: &Path,
    log_dir: &Path,
    png_dir: &Path,
) -> Result<BenchRecord> {
    let base_url = config.base_url();
    let log_file = log_dir.join(format!("server_{}_harvest.log", run));
    let _server = ServerHandle::start(root_dir, &log_file, &base_url).await?;

    let mut api = api_configuration(&base_url);

    let token = anonymous_token(&api).await?;
    api.bearer_access_token = Some(token.to_string());

    let import_start = Instant::now();
    run_stac_import(config, root_dir)?;
    let import_ms = import_start.elapsed().as_millis();

    let setup_start = Instant::now();
    let workflow_id = register_harvest_workflow(&api).await?;
    let setup_ms = setup_start.elapsed().as_millis();

    let style = build_wms_style(8, 1051.0, 16015.0)?;

    let wms_start = Instant::now();
    let response = apis::ogcwms_api::wms_handler(
        &api,
        &workflow_id.to_string(),
        models::WmsRequest::GetMap,
        Some(BBOX),
        None,
        Some(CRS),
        None,
        Some("application/json"),
        Some("image/png"),
        Some(IMAGE_HEIGHT),
        None,
        None,
        Some(&workflow_id.to_string()),
        None,
        Some(models::WmsService::Wms),
        None,
        None,
        Some(&style),
        Some(TIME),
        Some(true),
        Some("1.3.0"),
        Some(IMAGE_WIDTH),
    )
    .await
    .map_err(map_api_error("harvest WMS request"))?;

    let status = response.status();
    let png_bytes = response
        .bytes()
        .await
        .context("reading harvest WMS response body")?;
    require_http_200(status, "harvest WMS")?;
    fs::write(png_dir.join(format!("harvest_run_{}.png", run)), png_bytes)
        .context("writing harvest PNG")?;
    let wms_ms = wms_start.elapsed().as_millis();

    Ok(BenchRecord {
        run,
        scenario: "harvest",
        import_ms,
        setup_ms,
        wms_ms,
        total_ms: import_ms + setup_ms + wms_ms,
        http_status: status.as_u16(),
    })
}

async fn run_provider_benchmark(
    run: u32,
    config: &Config,
    root_dir: &Path,
    log_dir: &Path,
    png_dir: &Path,
) -> Result<BenchRecord> {
    let base_url = config.base_url();
    let log_file = log_dir.join(format!("server_{}_provider.log", run));
    let _server = ServerHandle::start(root_dir, &log_file, &base_url).await?;

    let mut api = api_configuration(&base_url);

    let token = anonymous_token(&api).await?;
    api.bearer_access_token = Some(token.to_string());

    let setup_start = Instant::now();

    let provider_definition = build_provider_definition(config, root_dir)?;
    let provider_id = register_stac_provider_fallback(&api, provider_definition).await?;

    let workflow =
        apis::layers_api::layer_to_workflow_id_handler(&api, &provider_id.to_string(), "dataset/2")
            .await
            .map_err(map_api_error("provider layer_to_workflow_id"))?;
    let workflow_id = workflow.id;

    trigger_workflow_metadata_fallback(&api, workflow_id).await?;

    let setup_ms = setup_start.elapsed().as_millis();

    let style = build_wms_style(9, 1051.0, 16015.0)?;

    let wms_start = Instant::now();
    let response = apis::ogcwms_api::wms_handler(
        &api,
        &workflow_id.to_string(),
        models::WmsRequest::GetMap,
        Some(BBOX),
        None,
        Some(CRS),
        None,
        Some("application/json"),
        Some("image/png"),
        Some(IMAGE_HEIGHT),
        None,
        None,
        Some(&workflow_id.to_string()),
        None,
        Some(models::WmsService::Wms),
        None,
        None,
        Some(&style),
        Some(TIME),
        Some(true),
        Some("1.3.0"),
        Some(IMAGE_WIDTH),
    )
    .await
    .map_err(map_api_error("provider WMS request"))?;

    let status = response.status();
    let png_bytes = response
        .bytes()
        .await
        .context("reading provider WMS response body")?;
    require_http_200(status, "provider WMS")?;
    fs::write(png_dir.join(format!("provider_run_{}.png", run)), png_bytes)
        .context("writing provider PNG")?;
    let wms_ms = wms_start.elapsed().as_millis();

    Ok(BenchRecord {
        run,
        scenario: "provider",
        import_ms: 0,
        setup_ms,
        wms_ms,
        total_ms: setup_ms + wms_ms,
        http_status: status.as_u16(),
    })
}

async fn trigger_workflow_metadata_fallback(api: &Configuration, workflow_id: Uuid) -> Result<()> {
    let response = api
        .client
        .get(format!(
            "{}/workflow/{}/metadata",
            api.base_path, workflow_id
        ))
        .bearer_auth(
            api.bearer_access_token
                .as_ref()
                .ok_or_else(|| anyhow!("missing bearer token in API configuration"))?,
        )
        .send()
        .await
        .context("querying workflow metadata")?;

    let status = response.status();
    let body = response
        .text()
        .await
        .context("reading workflow metadata response")?;

    if !status.is_success() {
        bail!("provider metadata failed: status={} body={}", status, body);
    }

    let _: Value = serde_json::from_str(&body).context("decoding workflow metadata JSON")?;

    Ok(())
}

fn api_configuration(base_url: &str) -> Configuration {
    Configuration {
        base_path: base_url.to_string(),
        ..Configuration::default()
    }
}

async fn anonymous_token(api: &Configuration) -> Result<Uuid> {
    let session = apis::session_api::anonymous_handler(api)
        .await
        .map_err(map_api_error("anonymous session"))?;
    Ok(session.id)
}

async fn register_harvest_workflow(api: &Configuration) -> Result<Uuid> {
    let params = models::GdalSourceParameters::new("sentinel-2-l2a_EPSG32632_U16_60".to_owned());
    let source = models::MultiBandGdalSource::new(
        params,
        models::multi_band_gdal_source::Type::MultiBandGdalSource,
    );
    let raster_operator = models::RasterOperator::MultiBandGdalSource(Box::new(source));
    let typed_raster = models::TypedRasterOperator::new(
        raster_operator,
        models::typed_raster_operator::Type::Raster,
    );
    let typed_operator = models::TypedOperator::TypedRasterOperator(Box::new(typed_raster));
    let workflow = models::Workflow::TypedOperator(Box::new(typed_operator));

    let id = apis::workflows_api::register_workflow_handler(api, workflow)
        .await
        .map_err(map_api_error("register harvest workflow"))?;

    Ok(id.id)
}

fn build_wms_style(band: u32, min: f64, max: f64) -> Result<String> {
    let colorizer = Colorizer::linear_gradient(
        vec![
            (min, RgbaColor::new(0, 0, 0, 255)).try_into()?,
            (max, RgbaColor::new(255, 255, 255, 255)).try_into()?,
        ],
        RgbaColor::transparent(),
        RgbaColor::new(246, 250, 254, 255),
        RgbaColor::new(247, 251, 255, 255),
    )?;

    let raster_colorizer = RasterColorizer::SingleBand {
        band,
        band_colorizer: colorizer,
    };

    Ok(format!(
        "custom:{}",
        serde_json::to_string(&raster_colorizer).context("serializing raster colorizer")?
    ))
}

fn run_stac_import(config: &Config, root_dir: &Path) -> Result<()> {
    let status = Command::new("cargo")
        .arg("run")
        .arg("--release")
        .arg("--bin")
        .arg("geoengine-cli")
        .arg("stac-import")
        .arg("--missing-bands-handling")
        .arg("--limit")
        .arg(config.import_limit.to_string())
        .arg("--bbox")
        .arg(&config.import_bbox)
        .arg("--time-start")
        .arg(&config.import_time_start)
        .arg("--time-end")
        .arg(&config.import_time_end)
        .arg("--verbose")
        .arg("--stac-url")
        .arg(&config.stac_url)
        .arg("--s3-endpoint")
        .arg(&config.stac_s3_endpoint)
        .arg("--s3-access-key")
        .arg(&config.stac_s3_access_key)
        .arg("--s3-secret-key")
        .arg(&config.stac_s3_secret_key)
        .current_dir(root_dir)
        .status()
        .context("running geoengine-cli stac-import")?;

    if !status.success() {
        bail!("stac-import failed with status {status}");
    }

    Ok(())
}

fn build_provider_definition(config: &Config, root_dir: &Path) -> Result<Value> {
    let provider_path = root_dir.join("test_data/provider_defs_api/stac_sentinel2.json");
    let provider_text = fs::read_to_string(&provider_path)
        .with_context(|| format!("reading {}", provider_path.display()))?;
    let mut provider_definition: Value =
        serde_json::from_str(&provider_text).context("parsing provider definition JSON")?;

    provider_definition["id"] = Value::String(Uuid::new_v4().to_string());
    provider_definition["s3Config"]["endpoint"] = Value::String(config.stac_s3_endpoint.clone());
    provider_definition["s3Config"]["accessKey"] = Value::String(config.stac_s3_access_key.clone());
    provider_definition["s3Config"]["secretKey"] = Value::String(config.stac_s3_secret_key.clone());

    Ok(provider_definition)
}

async fn register_stac_provider_fallback(
    api: &Configuration,
    provider_definition: Value,
) -> Result<Uuid> {
    let response = api
        .client
        .post(format!("{}/layerDb/providers", api.base_path))
        .bearer_auth(
            api.bearer_access_token
                .as_ref()
                .ok_or_else(|| anyhow!("missing bearer token in API configuration"))?,
        )
        .json(&provider_definition)
        .send()
        .await
        .context("registering STAC provider")?;

    let status = response.status();
    let body = response
        .text()
        .await
        .context("reading provider registration response")?;

    if !status.is_success() {
        bail!(
            "provider registration failed: status={} body={}",
            status,
            body
        );
    }

    let id_response: models::IdResponse =
        serde_json::from_str(&body).context("decoding provider registration response")?;

    Ok(id_response.id)
}

fn map_api_error<T: std::fmt::Debug + Send + Sync + 'static>(
    context_msg: &'static str,
) -> impl FnOnce(apis::Error<T>) -> anyhow::Error {
    move |err| match err {
        apis::Error::ResponseError(content) => anyhow!(
            "{} failed: status={} body={}",
            context_msg,
            content.status,
            content.content
        ),
        other => anyhow!("{} failed: {}", context_msg, other),
    }
}

fn require_http_200(status: reqwest::StatusCode, what: &str) -> Result<()> {
    if status != reqwest::StatusCode::OK {
        bail!("Unexpected HTTP status for {what}: {}", status);
    }

    Ok(())
}

fn write_record(writer: &mut File, record: &BenchRecord) -> Result<()> {
    writeln!(
        writer,
        "{},{},{},{},{},{},{}",
        record.run,
        record.scenario,
        record.import_ms,
        record.setup_ms,
        record.wms_ms,
        record.total_ms,
        record.http_status
    )
    .context("writing CSV row")
}

fn print_summary(records: &[BenchRecord]) {
    #[derive(Default)]
    struct Agg {
        count: u32,
        import_ms: u128,
        setup_ms: u128,
        wms_ms: u128,
        total_ms: u128,
    }

    let mut harvest = Agg::default();
    let mut provider = Agg::default();

    for record in records {
        let agg = if record.scenario == "harvest" {
            &mut harvest
        } else {
            &mut provider
        };

        agg.count += 1;
        agg.import_ms += record.import_ms;
        agg.setup_ms += record.setup_ms;
        agg.wms_ms += record.wms_ms;
        agg.total_ms += record.total_ms;
    }

    for (name, agg) in [("harvest", harvest), ("provider", provider)] {
        if agg.count == 0 {
            continue;
        }

        let count = f64::from(agg.count);
        println!(
            "{name}: runs={} avg_import_ms={:.2} avg_setup_ms={:.2} avg_wms_ms={:.2} total_ms={:.2}",
            agg.count,
            agg.import_ms as f64 / count,
            agg.setup_ms as f64 / count,
            agg.wms_ms as f64 / count,
            agg.total_ms as f64 / count
        );
    }
}

fn workspace_root() -> PathBuf {
    let services_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    services_dir
        .parent()
        .expect("services crate must have a parent directory")
        .to_path_buf()
}

impl Config {
    fn from_env() -> Result<Self> {
        let runs = env_or_default("RUNS", "3")?
            .parse::<u32>()
            .context("parsing RUNS")?;
        let host = env_or_default("HOST", "127.0.0.1")?;
        let port = env_or_default("PORT", "3030")?
            .parse::<u16>()
            .context("parsing PORT")?;
        let stac_url = env_or_default("STAC_URL", "https://stac.nsiscloud.polsa.gov.pl/v1")?;
        let stac_s3_endpoint = env_or_default("STAC_S3_ENDPOINT", "eodata.nsiscloud.polsa.gov.pl")?;
        let stac_s3_access_key = env::var("STAC_S3_ACCESS_KEY")
            .context("missing STAC_S3_ACCESS_KEY environment variable")?;
        let stac_s3_secret_key = env::var("STAC_S3_SECRET_KEY")
            .context("missing STAC_S3_SECRET_KEY environment variable")?;
        let out_dir = PathBuf::from(env_or_default("OUT_DIR", "/tmp/geoengine-stac-benchmark")?);
        let import_limit = env_or_default("IMPORT_LIMIT", "100")?
            .parse::<u32>()
            .context("parsing IMPORT_LIMIT")?;
        let import_bbox = env_or_default("IMPORT_BBOX", "8.766 50.802 8.767 50.803")?;
        let import_time_start = env_or_default("IMPORT_TIME_START", "2026-01-01T00:00:00Z")?;
        let import_time_end = env_or_default("IMPORT_TIME_END", "2026-01-31T23:59:59Z")?;

        Ok(Self {
            runs,
            host,
            port,
            stac_url,
            stac_s3_endpoint,
            stac_s3_access_key,
            stac_s3_secret_key,
            out_dir,
            import_limit,
            import_bbox,
            import_time_start,
            import_time_end,
        })
    }

    fn base_url(&self) -> String {
        format!("http://{}:{}/api", self.host, self.port)
    }
}

fn env_or_default(key: &str, default: &str) -> Result<String> {
    Ok(env::var(key).unwrap_or_else(|_| default.to_owned()))
}
