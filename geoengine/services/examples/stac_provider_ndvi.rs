#![allow(clippy::print_stdout)]

use anyhow::{Context, Result};
use futures::StreamExt;
use geoengine_datatypes::{
    dataset::DataProviderId,
    primitives::{BandSelection, DateTime, RasterQueryRectangle, SpatialPartition2D, TimeInterval},
    raster::{GridBoundingBox2D, GridBounds, SpatialGridDefinition},
    util::Identifier,
};
use geoengine_operators::engine::{ExecutionContext, WorkflowOperatorPath};
use geoengine_services::{
    api::model::services::StacDataProviderDefinition as ApiStacDataProviderDefinition,
    contexts::{ApplicationContext, PostgresContext, SessionContext},
    datasets::external::stac::StacDataProviderDefinition,
    layers::storage::LayerProviderDb,
    users::UserSession,
    util::tests::with_temp_context,
    workflows::workflow::Workflow,
};
use std::{collections::BTreeMap, fs, path::Path};
use tokio_postgres::NoTls;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const PROVIDER_TEMPLATE_ID: &str = "b274275c-373d-4a3f-8b45-9b48e9614329";

const QUERY_MIN_X: f64 = 399_960.0;
const QUERY_MIN_Y: f64 = 5_590_200.0;
const QUERY_MAX_X: f64 = 405_080.0;
const QUERY_MAX_Y: f64 = 5_595_320.0;

const QUERY_TIME_YEAR: i32 = 2026;
const QUERY_TIME_MONTH: u8 = 1;
const QUERY_TIME_DAY: u8 = 1;

#[derive(Debug, Default)]
struct ProfileMetrics {
    file_access_counts: BTreeMap<String, u32>,
    raster_read_time_seconds: f64,
    stac_request_count: u32,
    stac_wait_time_seconds: f64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let log_file = std::env::temp_dir().join("geoengine_stac_ndvi_profile.log");
    let _guard = init_logging(&log_file)?;

    let metrics = with_temp_context(
        move |app_ctx: PostgresContext<NoTls>, _db_config| async move {
            run_profile_query(app_ctx, &log_file).await
        },
    )
    .await?;

    println!("=== STAC NDVI Profile ===");
    println!("stac_requests: {}", metrics.stac_request_count);
    println!(
        "stac_wait_time_seconds: {:.6}",
        metrics.stac_wait_time_seconds
    );
    println!(
        "raster_read_time_seconds: {:.6}",
        metrics.raster_read_time_seconds
    );
    println!("file_accesses:");
    for (path, count) in &metrics.file_access_counts {
        println!("  {count:>5}  {path}");
    }

    Ok(())
}

fn init_logging(log_file: &Path) -> Result<WorkerGuard> {
    let log_file_handle = fs::File::create(log_file)
        .with_context(|| format!("creating log file {}", log_file.display()))?;
    let (non_blocking_writer, guard) = tracing_appender::non_blocking(log_file_handle);

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("debug"))
        .with_ansi(false)
        .with_writer(non_blocking_writer)
        .try_init()
        .map_err(|e| anyhow::anyhow!("initializing tracing subscriber: {e}"))?;

    Ok(guard)
}

async fn run_profile_query(
    app_ctx: PostgresContext<NoTls>,
    log_file: &Path,
) -> Result<ProfileMetrics> {
    let admin_session = UserSession::admin_session();
    let admin_ctx = app_ctx.session_context(admin_session);

    // Use a fresh provider id in the temporary database schema.
    let provider_id = DataProviderId::new();
    let provider = build_provider_definition(provider_id)?;

    admin_ctx
        .db()
        .add_layer_provider(provider.into())
        .await
        .context("registering STAC provider")?;

    let workflow_json = include_str!("../../test_data/api_calls/stac_provider/ndvi-workflow.json")
        .replace(PROVIDER_TEMPLATE_ID, &provider_id.to_string());

    let workflow: Workflow =
        serde_json::from_str(&workflow_json).context("deserializing NDVI workflow")?;

    let operator = workflow
        .operator()
        .context("extracting workflow operator")?
        .get_raster()
        .context("expecting raster workflow")?;

    let execution_ctx = admin_ctx
        .execution_context()
        .context("creating execution context")?;

    let initialized = operator
        .clone()
        .initialize(WorkflowOperatorPath::initialize_root(), &execution_ctx)
        .await
        .context("initializing workflow operator")?;

    let processor = initialized
        .query_processor()
        .context("creating query processor")?
        .get_f32()
        .context("expecting f32 NDVI output")?;

    let query_bounds = SpatialPartition2D::new(
        (QUERY_MIN_X, QUERY_MAX_Y).into(),
        (QUERY_MAX_X, QUERY_MIN_Y).into(),
    )
    .context("creating query bounds")?;

    let full_query_grid = initialized
        .result_descriptor()
        .spatial_grid_descriptor()
        .tiling_grid_definition(execution_ctx.tiling_specification())
        .tiling_spatial_grid_definition()
        .spatial_bounds_to_compatible_spatial_grid(query_bounds);

    // Clamp to a single tile: use only the top-left tile index from the computed grid
    let single_tile_idx = full_query_grid.grid_bounds().min_index();
    let single_tile_grid_bounds =
        GridBoundingBox2D::new(single_tile_idx, single_tile_idx).context("single tile bounds")?;
    let query_grid =
        SpatialGridDefinition::new(full_query_grid.geo_transform(), single_tile_grid_bounds);

    let query_time = TimeInterval::new_instant(DateTime::new_utc(
        QUERY_TIME_YEAR,
        QUERY_TIME_MONTH,
        QUERY_TIME_DAY,
        0,
        0,
        0,
    ))
    .context("creating query time")?;

    let query_rect =
        RasterQueryRectangle::new(query_grid.grid_bounds(), query_time, BandSelection::first());

    let query_ctx = admin_ctx
        .query_context(Uuid::new_v4(), Uuid::new_v4())
        .context("creating query context")?;

    let mut stream = processor
        .raster_query(query_rect, &query_ctx)
        .await
        .context("starting raster query")?;

    let mut tile_count = 0_u64;
    while let Some(tile) = stream.next().await {
        tile.context("query stream returned an error")?;
        tile_count += 1;
    }

    println!("tiles_received: {tile_count}");

    parse_profile_metrics(log_file)
}

fn build_provider_definition(provider_id: DataProviderId) -> Result<StacDataProviderDefinition> {
    let mut provider_definition: ApiStacDataProviderDefinition = serde_json::from_str(
        include_str!("../../test_data/provider_defs_api/stac_sentinel2.json"),
    )
    .context("deserializing STAC provider definition from test data")?;

    provider_definition.id = provider_id.into();

    Ok(provider_definition.into())
}

fn parse_profile_metrics(log_file: &Path) -> Result<ProfileMetrics> {
    let log_text = fs::read_to_string(log_file)
        .with_context(|| format!("reading log file {}", log_file.display()))?;

    let mut metrics = ProfileMetrics::default();

    for line in log_text.lines() {
        let clean_line = strip_ansi_escape_sequences(line);

        if let Some((_, rest)) = clean_line.split_once("Loading raster tile from file:") {
            let mut path = rest.trim();
            if let Some(stripped) = path.strip_prefix('"') {
                path = stripped;
            }
            if let Some(stripped) = path.strip_suffix('"') {
                path = stripped;
            }

            if !path.is_empty() {
                *metrics
                    .file_access_counts
                    .entry(path.to_owned())
                    .or_insert(0) += 1;
            }
        }

        if let Some((_, rest)) = clean_line.split_once("read raster band in")
            && let Some(seconds) = first_f64(rest)
        {
            metrics.raster_read_time_seconds += seconds;
        }

        if clean_line.contains("STAC query first page")
            || clean_line.contains("STAC query next page")
        {
            metrics.stac_request_count += 1;
        }

        if let Some((_, rest)) = clean_line.split_once("STAC response received in")
            && let Some(seconds) = first_f64(rest)
        {
            metrics.stac_wait_time_seconds += seconds;
        }
    }

    Ok(metrics)
}

fn first_f64(input: &str) -> Option<f64> {
    let mut token = String::new();
    for c in input.chars() {
        if c.is_ascii_digit() || matches!(c, '.' | '-' | '+' | 'e' | 'E') {
            token.push(c);
        } else if !token.is_empty() {
            return token.parse::<f64>().ok();
        }
    }

    if token.is_empty() {
        None
    } else {
        token.parse::<f64>().ok()
    }
}

fn strip_ansi_escape_sequences(input: &str) -> String {
    let mut output = String::new();
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' && matches!(chars.peek(), Some('[')) {
            let _ = chars.next();
            for next_ch in chars.by_ref() {
                if ('@'..='~').contains(&next_ch) {
                    break;
                }
            }
            continue;
        }

        output.push(ch);
    }

    output
}
