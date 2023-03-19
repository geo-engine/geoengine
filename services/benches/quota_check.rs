use std::time::Instant;

use actix_http::header::{self, CONTENT_TYPE};
use actix_web_httpauth::headers::authorization::Bearer;
use geoengine_datatypes::{
    operations::image::{Colorizer, DefaultColors, RgbaColor},
    primitives::{TimeGranularity, TimeStep},
    util::test::TestDefault,
};
use geoengine_operators::{
    engine::{RasterOperator, SingleRasterSource, TypedOperator},
    processing::{Aggregation, TemporalRasterAggregation, TemporalRasterAggregationParameters},
    source::{GdalSource, GdalSourceParameters},
};
use geoengine_services::{
    contexts::Context,
    pro::{
        contexts::ProInMemoryContext,
        users::{UserAuth, UserDb, UserSession},
        util::tests::{add_ndvi_to_datasets, send_pro_test_request},
    },
    util::config,
    workflows::{registry::WorkflowRegistry, workflow::Workflow},
};

async fn bench() {
    let ctx = ProInMemoryContext::test_default();

    let session = ctx.create_anonymous_session().await.unwrap();

    let dataset = add_ndvi_to_datasets(&ctx).await;

    let workflow = Workflow {
        operator: TypedOperator::Raster(
            TemporalRasterAggregation {
                params: TemporalRasterAggregationParameters {
                    aggregation: Aggregation::Min {
                        ignore_no_data: false,
                    },
                    window: TimeStep {
                        granularity: TimeGranularity::Days,
                        step: 1,
                    },
                    window_reference: None,
                    output_type: None,
                },
                sources: SingleRasterSource {
                    raster: GdalSource {
                        params: GdalSourceParameters {
                            data: dataset.into(),
                        },
                    }
                    .boxed(),
                },
            }
            .boxed(),
        ),
    };

    let id = ctx
        .db(session.clone())
        .register_workflow(workflow.clone())
        .await
        .unwrap();

    let colorizer = Colorizer::linear_gradient(
        vec![
            (0.0, RgbaColor::white()).try_into().unwrap(),
            (255.0, RgbaColor::black()).try_into().unwrap(),
        ],
        RgbaColor::transparent(),
        DefaultColors::OverUnder {
            over_color: RgbaColor::white(),
            under_color: RgbaColor::black(),
        },
    )
    .unwrap();

    let params = &[
        ("request", "GetMap"),
        ("service", "WMS"),
        ("version", "1.3.0"),
        ("layers", &id.to_string()),
        ("bbox", "-90,-180,90,180"),
        ("width", "2000"),
        ("height", "2000"),
        ("crs", "EPSG:4326"),
        (
            "styles",
            &format!("custom:{}", serde_json::to_string(&colorizer).unwrap()),
        ),
        ("format", "image/png"),
        ("time", "2014-04-01T12:00:00.0Z"),
        ("exceptions", "JSON"),
    ];
    ctx.db(UserSession::admin_session())
        .update_quota_available_by_user(&session.user.id, 9999)
        .await
        .unwrap();

    let req = actix_web::test::TestRequest::get()
        .uri(&format!(
            "/wms/{}?{}",
            id,
            serde_urlencoded::to_string(params).unwrap()
        ))
        .append_header((header::AUTHORIZATION, Bearer::new(session.id.to_string())));
    let res = send_pro_test_request(req, ctx).await;

    assert_eq!(res.status(), 200);
    assert_eq!(
        res.headers().get(&CONTENT_TYPE),
        Some(&header::HeaderValue::from_static("image/png"))
    );
}

#[tokio::main]
async fn main() {
    println!(
        "Starting benchmark, quota check enabled: {}",
        config::get_config_element::<geoengine_services::pro::util::config::User>()
            .unwrap()
            .quota_check
    );
    for i in 0..4 {
        let start = Instant::now();
        bench().await;
        println!("Run {i} time {:?}", start.elapsed());
    }
}
