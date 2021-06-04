use geoengine_datatypes::{
    primitives::BoundingBox2D,
    spatial_reference::{SpatialReference, SpatialReferenceAuthority},
};
use serde::{Deserialize, Serialize};
use warp::Filter;

use crate::handlers::{authenticate, Context};
use crate::{contexts::Session, error};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SpatialReferenceSpecification {
    name: String,
    spatial_reference: SpatialReference,
    proj_string: String,
    extent: BoundingBox2D,
    axis_labels: Option<(String, String)>,
}

pub(crate) fn get_spatial_reference_specification_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("spatialReferenceSpecification" / String)
        .and(warp::get())
        .and(authenticate(ctx))
        .and_then(get_spatial_reference_specification)
}

async fn get_spatial_reference_specification<S: Session>(
    srs_string: String,
    _session: S,
) -> Result<impl warp::Reply, warp::Rejection> {
    // TODO: get specification from Proj or some other source
    let spec = match srs_string.to_uppercase().as_str() {
        "EPSG:4326" => SpatialReferenceSpecification {
            name: "WGS84".to_owned(),
            spatial_reference: SpatialReference::epsg_4326(),
            proj_string: "+proj=longlat +datum=WGS84 +no_defs +type=crs".to_owned(),
            extent: BoundingBox2D::new_unchecked((-180., -90.).into(), (180., 90.).into()),
            axis_labels: Some(("longitude".to_owned(), "latitude".to_owned())),
        },
        "EPSG:3857" => SpatialReferenceSpecification {
            name: "WGS84 Web Mercator".to_owned(),
            spatial_reference: SpatialReference::new(SpatialReferenceAuthority::Epsg, 3857),
            proj_string: "+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m +nadgrids=@null +wktext +no_defs +type=crs".into(),
            extent: BoundingBox2D::new_unchecked((-20_037_508.34, -20_037_508.34).into(),  (20_037_508.34, 20_037_508.34).into()),
            axis_labels: None,
        },
        "EPSG:32632" => SpatialReferenceSpecification {
            name: "WGS 84 / UTM 32 N".to_owned(),
            spatial_reference: SpatialReference::new(SpatialReferenceAuthority::Epsg, 32632),
            proj_string: "+proj=utm +zone=32 +datum=WGS84 +units=m +no_defs +type=crs".into(),
            extent: BoundingBox2D::new_unchecked((166_021.443_1, 0.0).into(),(833_978.556_9, 9_329_005.182_5).into()),
            axis_labels: None,
        },
        "EPSG:32736" => SpatialReferenceSpecification {
            name: "WGS 84 / UTM 36 S".to_owned(),
            spatial_reference: SpatialReference::new(SpatialReferenceAuthority::Epsg, 32736),
            proj_string: "+proj=utm +zone=36 +south +datum=WGS84 +units=m +no_defs".into(),
            extent: BoundingBox2D::new_unchecked((441_867.78, 1_116_915.04).into(), (833_978.56, 10_000_000.0).into()),
            axis_labels: None,
        },
        "EPSG:25832" => SpatialReferenceSpecification {
            name: "ETRS89 / UTM 32 N".to_owned(),
            spatial_reference: SpatialReference::new(SpatialReferenceAuthority::Epsg, 25832),
            proj_string: "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs".into(),
            extent: BoundingBox2D::new_unchecked((265_948.819_1, 6_421_521.225_4).into(),( 677_786.362_9, 7_288_831.701_4).into()),
            axis_labels: None,
        },
        "SR-ORG:81" => SpatialReferenceSpecification {
            name: "GEOS - GEOstationary Satellite".to_owned(),
            spatial_reference: SpatialReference::new(SpatialReferenceAuthority::SrOrg, 81),
            proj_string: "+proj=geos +lon_0=0 +h=-0 +x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs".into(),
            extent: BoundingBox2D::new_unchecked((-5_568_748.276, -5_568_748.276).into(), (5_568_748.276, 5_568_748.276).into()),
            axis_labels: None,
        },
        "EPSG:3035" => SpatialReferenceSpecification {
            name: "ETRS89-LAEA".to_owned(),
            spatial_reference: SpatialReference::new(SpatialReferenceAuthority::Epsg, 3035),
            proj_string: "+proj=laea +lat_0=52 +lon_0=10 +x_0=4321000 +y_0=3210000 +ellps=GRS80 +units=m +no_defs".into(),
            extent: BoundingBox2D::new_unchecked((2_426_378.013_2, 1_528_101.261_8).into(), (6_293_974.621_5, 5_446_513.522_2).into()),
            axis_labels: None,
        },

        _ => return Err(error::Error::UnknownSpatialReference { srs_string }.into()), // TODO: 400 on invalid srsString, 404 not found
    };

    Ok(warp::reply::json(&spec))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::InMemoryContext;
    use crate::contexts::SimpleContext;
    use crate::handlers::handle_rejection;
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use serde_json;

    #[tokio::test]
    async fn get_spatial_reference() {
        let ctx = InMemoryContext::default();
        let session = ctx.default_session().await;

        let response = warp::test::request()
            .method("GET")
            .path("/spatialReferenceSpecification/EPSG:4326")
            .header("Content-Length", "0")
            .header(
                "Authorization",
                format!("Bearer {}", session.id().to_string()),
            )
            .reply(&get_spatial_reference_specification_handler(ctx).recover(handle_rejection))
            .await;

        assert_eq!(response.status(), 200);

        let body: String = String::from_utf8(response.body().to_vec()).unwrap();
        let spec: SpatialReferenceSpecification = serde_json::from_str(&body).unwrap();
        assert_eq!(
            SpatialReferenceSpecification {
                name: "WGS84".to_owned(),
                spatial_reference: SpatialReference::epsg_4326(),
                proj_string: "+proj=longlat +datum=WGS84 +no_defs +type=crs".to_owned(),
                extent: BoundingBox2D::new_unchecked((-180., -90.).into(), (180., 90.).into()),
                axis_labels: Some(("longitude".to_owned(), "latitude".to_owned())),
            },
            spec
        );
    }
}
