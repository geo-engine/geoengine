use crate::api::model::datatypes::{
    AxisLabels, BoundingBox2D, Coordinate2D, SpatialReference, SpatialReferenceAuthority,
};
use crate::contexts::ApplicationContext;
use crate::{error::Error, error::Result};
use actix_web::{FromRequest, Responder, web};
use dashmap::DashMap;
use geoengine_datatypes::spatial_reference::{StaticAxisOrder, StaticEpsgMetadataProvider};
use proj_sys::PJ_PROJ_STRING_TYPE_PJ_PROJ_4;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use utoipa::ToSchema;

pub(crate) fn init_spatial_reference_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ApplicationContext,
    C::Session: FromRequest,
{
    cfg.service(
        web::resource("/spatialReferenceSpecification/{srs_string}")
            .route(web::get().to(get_spatial_reference_specification_handler::<C>)),
    );
}

/// The specification of a spatial reference, where extent and axis labels are given
/// in natural order (x, y) = (east, north)
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SpatialReferenceSpecification {
    pub name: String,
    #[schema(value_type = String)]
    pub spatial_reference: SpatialReference,
    pub proj_string: String,
    pub extent: BoundingBox2D,
    pub axis_labels: Option<AxisLabels>,
    pub axis_order: Option<AxisOrder>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct ProjJson {
    name: String,
    coordinate_system: ProjJsonCoordinateSystem,
}

impl ProjJson {
    /// the axis order of the projection, if one is defined
    pub fn axis_order(&self) -> Option<AxisOrder> {
        match &self.coordinate_system.axis {
            Some(axes) => match *axes.as_slice() {
                [
                    ProjJsonAxis {
                        direction: ProjJsonAxisDirection::North,
                        ..
                    },
                    ProjJsonAxis {
                        direction: ProjJsonAxisDirection::East,
                        ..
                    },
                ] => Some(AxisOrder::NorthEast),
                [
                    ProjJsonAxis {
                        direction: ProjJsonAxisDirection::East,
                        ..
                    },
                    ProjJsonAxis {
                        direction: ProjJsonAxisDirection::North,
                        ..
                    },
                ] => Some(AxisOrder::EastNorth),
                _ => None,
            },
            _ => None,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct ProjJsonCoordinateSystem {
    axis: Option<Vec<ProjJsonAxis>>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct ProjJsonAxis {
    name: String,
    abbreviation: String,
    direction: ProjJsonAxisDirection,
    unit: String,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ProjJsonAxisDirection {
    East,
    North,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum AxisOrder {
    NorthEast,
    EastNorth,
}

impl AxisOrder {
    pub fn xy_to_native_order<T: Copy>(self, input: [T; 2]) -> [T; 2] {
        match &self {
            AxisOrder::EastNorth => input,
            AxisOrder::NorthEast => [input[1], input[0]],
        }
    }
}

#[cfg(test)]
thread_local! {
    /// PROJ contexts created on this thread, for tests that check that requests do not ask PROJ
    static PROJ_CONTEXTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn count_proj_context(srs_string: &str) {
    tracing::debug!(srs_string, "resolving a spatial reference with PROJ");
    #[cfg(test)]
    PROJ_CONTEXTS.with(|n| n.set(n.get() + 1));
}

/// Get the proj json information for the given `srs_string` if it is known.
// TODO: expose method in proj crate instead
fn proj_json(srs_string: &str) -> Option<ProjJson> {
    unsafe {
        let c_definition = std::ffi::CString::new(srs_string).ok()?;

        count_proj_context(srs_string);
        let ctx = proj_sys::proj_context_create();
        let c_proj = proj_sys::proj_create(ctx, c_definition.as_ptr());

        let string = if c_proj.is_null() {
            None
        } else {
            let c_buf = proj_sys::proj_as_projjson(ctx, c_proj, std::ptr::null());

            // null if PROJ cannot export the CRS
            (!c_buf.is_null())
                .then(|| {
                    std::ffi::CStr::from_ptr(c_buf)
                        .to_str()
                        .map(ToOwned::to_owned)
                        .ok()
                })
                .flatten()
        };

        proj_sys::proj_destroy(c_proj);
        proj_sys::proj_context_destroy(ctx);

        string.and_then(|s| serde_json::from_str(&s).ok())
    }
}

/// Get the proj json information for the given `srs_string` if it is known.
// TODO: expose method in proj crate instead
fn proj_proj_string(srs_string: &str) -> Option<String> {
    unsafe {
        let c_definition = std::ffi::CString::new(srs_string).ok()?;

        count_proj_context(srs_string);
        let ctx = proj_sys::proj_context_create();
        let c_proj = proj_sys::proj_create(ctx, c_definition.as_ptr());

        let string = if c_proj.is_null() {
            None
        } else {
            let c_buf = proj_sys::proj_as_proj_string(
                ctx,
                c_proj,
                PJ_PROJ_STRING_TYPE_PJ_PROJ_4,
                std::ptr::null(),
            );

            // null if the CRS has no PROJ string, e.g. EPSG:32600, the UTM grid system
            (!c_buf.is_null())
                .then(|| {
                    std::ffi::CStr::from_ptr(c_buf)
                        .to_str()
                        .map(ToOwned::to_owned)
                        .ok()
                })
                .flatten()
        };

        proj_sys::proj_destroy(c_proj);
        proj_sys::proj_context_destroy(ctx);

        string
    }
}

/// Returns the name, PROJ string, extent and axis order of a spatial reference.
#[allow(
    clippy::unused_async, // the function signature of request handlers requires it
    clippy::no_effect_underscore_binding // need `_session` to quire authentication
)]
#[utoipa::path(
    tag = "Spatial References",
    get,
    path = "/spatialReferenceSpecification/{srsString}",
    responses(
        (status = 200, description = "OK", body = SpatialReferenceSpecification,
            example = json!({
                "name": "WGS 84",
                "spatialReference": "EPSG:4326",
                "projString": "+proj=longlat +datum=WGS84 +no_defs +type=crs",
                "extent": {
                    "lowerLeftCoordinate": {
                        "x": -180.0,
                        "y": -90.0
                    },
                    "upperRightCoordinate": {
                        "x": 180.0,
                        "y": 90.0
                    }
                },
                "axisLabels": [
                    "Geodetic longitude",
                    "Geodetic latitude"
                ],
                "axisOrder": "northEast"
            })
        )
    ),
    params(
        ("srsString" = String, Path, example = "EPSG:4326")
    ),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn get_spatial_reference_specification_handler<C: ApplicationContext>(
    srs_string: web::Path<String>,
    _session: C::Session,
    app_ctx: web::Data<C>,
) -> Result<impl Responder> {
    let spatial_ref = SpatialReference::from_str(&srs_string)?;
    app_ctx
        .spatial_reference_cache()
        .specification(spatial_ref)
        .map(web::Json)
}

/// The spatial reference specifications that PROJ resolved, kept for the lifetime of the
/// application context, so that requests do not ask PROJ again.
///
/// It keeps successes only, so that an unknown spatial reference gives the same error every time.
/// It needs no eviction: it holds at most one entry per CRS in PROJ's database.
#[derive(Clone, Debug, Default)]
pub struct SpatialReferenceCache {
    specifications: Arc<DashMap<SpatialReference, SpatialReferenceSpecification>>,
    axis_orders: Arc<DashMap<SpatialReference, Option<AxisOrder>>>,
}

impl SpatialReferenceCache {
    /// Returns the name, PROJ string, extent and axis order of a spatial reference.
    pub fn specification(
        &self,
        spatial_reference: SpatialReference,
    ) -> Result<SpatialReferenceSpecification> {
        // clone before resolving, so that no shard lock is held while PROJ runs and during the insert
        let cached = self
            .specifications
            .get(&spatial_reference)
            .map(|spec| spec.clone());
        if let Some(spec) = cached {
            return Ok(spec);
        }
        let spec = spatial_reference_specification(spatial_reference)?;
        self.specifications.insert(spatial_reference, spec.clone());
        Ok(spec)
    }

    /// Returns the axis order of a spatial reference, the same as its specification has, but
    /// without building its name, PROJ string and extent.
    ///
    /// Custom references come first, then EPSG codes of the static registry, which needs no
    /// PROJ, then PROJ, asked once per reference.
    pub fn axis_order(&self, spatial_reference: SpatialReference) -> Result<AxisOrder> {
        let axis_order =
            if let Some(spec) = custom_spatial_reference_specification(spatial_reference) {
                spec.axis_order
            } else if let Some(axis_order) =
                StaticEpsgMetadataProvider::axis_order(spatial_reference.into())
            {
                Some(axis_order.into())
            } else {
                self.proj_axis_order(spatial_reference)?
            };
        axis_order.ok_or_else(|| Error::AxisOrderingNotKnownForSrs {
            srs_string: spatial_reference.srs_string(),
        })
    }

    fn proj_axis_order(&self, spatial_reference: SpatialReference) -> Result<Option<AxisOrder>> {
        if let Some(axis_order) = self.axis_orders.get(&spatial_reference).map(|order| *order) {
            return Ok(axis_order);
        }
        let srs_string = spatial_reference.srs_string();
        let axis_order = proj_json(&srs_string)
            .ok_or(Error::UnknownSrsString { srs_string })?
            .axis_order();
        self.axis_orders.insert(spatial_reference, axis_order);
        Ok(axis_order)
    }
}

impl From<StaticAxisOrder> for AxisOrder {
    fn from(axis_order: StaticAxisOrder) -> Self {
        match axis_order {
            StaticAxisOrder::EastNorth => AxisOrder::EastNorth,
            StaticAxisOrder::NorthEast => AxisOrder::NorthEast,
        }
    }
}

/// custom spatial references not known by proj or that shall be overriden
fn custom_spatial_reference_specification(
    spatial_ref: SpatialReference,
) -> Option<SpatialReferenceSpecification> {
    // TODO: provide a generic storage for custom spatial reference specifications
    match spatial_ref.srs_string().to_uppercase().as_str() {
        "SR-ORG:81" => Some(SpatialReferenceSpecification {
            name: "GEOS - GEOstationary Satellite".to_owned(),
            spatial_reference: SpatialReference::new(SpatialReferenceAuthority::SrOrg, 81),
            proj_string: "+proj=geos +lon_0=0 +h=-0 +x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs"
                .into(),
            extent: BoundingBox2D {
                lower_left_coordinate: Coordinate2D {
                    x: -5_568_748.276,
                    y: -5_568_748.276,
                },
                upper_right_coordinate: Coordinate2D {
                    x: 5_568_748.276,
                    y: 5_568_748.276,
                },
            },
            axis_labels: None,
            axis_order: Some(AxisOrder::EastNorth),
        }),
        _ => None,
    }
}

/// Resolves the specification with PROJ; use [`SpatialReferenceCache::specification`] instead.
fn spatial_reference_specification(
    spatial_reference: SpatialReference,
) -> Result<SpatialReferenceSpecification> {
    if let Some(sref) = custom_spatial_reference_specification(spatial_reference) {
        return Ok(sref);
    }

    let spatial_reference: geoengine_datatypes::spatial_reference::SpatialReference =
        spatial_reference.into();
    let srs_string = spatial_reference.srs_string();

    let json = proj_json(&srs_string).ok_or_else(|| Error::UnknownSrsString {
        srs_string: srs_string.clone(),
    })?;
    let proj_string = proj_proj_string(&srs_string).ok_or_else(|| Error::UnknownSrsString {
        srs_string: srs_string.clone(),
    })?;

    let extent: geoengine_datatypes::primitives::BoundingBox2D =
        spatial_reference.area_of_use_projected()?;

    let axis_labels = json.coordinate_system.axis.as_ref().map(|axes| {
        let [a0, a1] = [0, 1].map(|i| axes.get(i).map_or(String::new(), |a| a.name.clone()));

        match json.axis_order() {
            None | Some(AxisOrder::EastNorth) => (a0, a1),
            Some(AxisOrder::NorthEast) => (a1, a0),
        }
    });
    let spec = SpatialReferenceSpecification {
        axis_order: json.axis_order(),
        name: json.name,
        spatial_reference: spatial_reference.into(),
        proj_string,
        extent: extent.into(),
        axis_labels: axis_labels.map(Into::into),
    };
    Ok(spec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::PostgresContext;
    use crate::contexts::Session;
    use crate::ge_context;
    use crate::users::UserAuth;
    use crate::util::tests::send_test_request;
    use actix_web;
    use actix_web::http::header;
    use actix_web_httpauth::headers::authorization::Bearer;
    use float_cmp::assert_approx_eq;
    use geoengine_datatypes::primitives::BoundingBox2D;
    use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceAuthority};
    use tokio_postgres::NoTls;

    /// What `spatial_reference_specification` returned before it was cached; the regression baseline
    /// for the axis order lookups. Extents are compared to 0.001, like the static `crs-constants`.
    #[test]
    #[allow(clippy::too_many_lines, reason = "a table of expected values")]
    fn it_keeps_the_specifications_of_common_spatial_references() {
        let expected = [
            (
                "EPSG:4326",
                "WGS 84",
                "+proj=longlat +datum=WGS84 +no_defs +type=crs",
                [-180., -90., 180., 90.],
                Some(("Geodetic longitude", "Geodetic latitude")),
                Some(AxisOrder::NorthEast),
            ),
            (
                "EPSG:3857",
                "WGS 84 / Pseudo-Mercator",
                "+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m +nadgrids=@null +wktext +no_defs +type=crs",
                [
                    -20_037_508.343,
                    -20_048_966.104,
                    20_037_508.343,
                    20_048_966.104,
                ],
                Some(("Easting", "Northing")),
                Some(AxisOrder::EastNorth),
            ),
            (
                "EPSG:25832",
                "ETRS89 / UTM zone 32N",
                "+proj=utm +zone=32 +ellps=GRS80 +units=m +no_defs +type=crs",
                [239_323.445, 4_290_144.086, 761_545.651, 9_365_801.909],
                Some(("Easting", "Northing")),
                Some(AxisOrder::EastNorth),
            ),
            (
                "EPSG:32632",
                "WGS 84 / UTM zone 32N",
                "+proj=utm +zone=32 +datum=WGS84 +units=m +no_defs +type=crs",
                [166_021.443, 0., 833_978.557, 9_329_005.182],
                Some(("Easting", "Northing")),
                Some(AxisOrder::EastNorth),
            ),
            (
                "EPSG:3035",
                "ETRS89-extended / LAEA Europe",
                "+proj=laea +lat_0=52 +lon_0=10 +x_0=4321000 +y_0=3210000 +ellps=GRS80 +units=m +no_defs +type=crs",
                [-146_473.113, 196_462.701, 7_824_928.564, 6_971_477.485],
                Some(("Easting", "Northing")),
                Some(AxisOrder::NorthEast),
            ),
            (
                "EPSG:4258",
                "ETRS89",
                "+proj=longlat +ellps=GRS80 +no_defs +type=crs",
                [-16.1, 32.88, 40.18, 84.73],
                Some(("Geodetic longitude", "Geodetic latitude")),
                Some(AxisOrder::NorthEast),
            ),
            (
                "EPSG:31467",
                "DHDN / 3-degree Gauss-Kruger zone 3",
                "+proj=tmerc +lat_0=0 +lon_0=9 +k=1 +x_0=3500000 +y_0=0 +ellps=bessel +units=m +no_defs +type=crs",
                [3_386_568.198, 5_236_824.133, 3_614_339.428, 6_107_852.276],
                Some(("Easting", "Northing")),
                Some(AxisOrder::NorthEast),
            ),
            (
                "ESRI:54009",
                "World_Mollweide",
                "+proj=moll +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs +type=crs",
                [
                    -18_040_095.696,
                    -9_020_047.848,
                    18_040_095.696,
                    9_020_047.848,
                ],
                Some(("Easting", "Northing")),
                Some(AxisOrder::EastNorth),
            ),
            (
                "SR-ORG:81",
                "GEOS - GEOstationary Satellite",
                "+proj=geos +lon_0=0 +h=-0 +x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs",
                [-5_568_748.276, -5_568_748.276, 5_568_748.276, 5_568_748.276],
                None,
                Some(AxisOrder::EastNorth),
            ),
        ];
        for (srs, name, proj_string, [x1, y1, x2, y2], axis_labels, axis_order) in expected {
            let spec =
                spatial_reference_specification(SpatialReference::from_str(srs).unwrap().into())
                    .unwrap();
            assert_eq!(
                spec.spatial_reference,
                SpatialReference::from_str(srs).unwrap().into(),
                "{srs}"
            );
            assert_eq!(spec.name, name, "{srs}");
            assert_eq!(spec.proj_string, proj_string, "{srs}");
            assert_approx_eq!(
                BoundingBox2D,
                spec.extent.into(),
                BoundingBox2D::new_unchecked((x1, y1).into(), (x2, y2).into()),
                epsilon = 0.001
            );
            assert_eq!(
                spec.axis_labels,
                axis_labels.map(|(a, b)| (a.to_owned(), b.to_owned()).into()),
                "{srs}"
            );
            assert_eq!(spec.axis_order, axis_order, "{srs}");
        }
    }

    /// Feet units, south-pointing axes and unknown codes are unknown to the `ProjJson` parser
    #[test]
    fn it_keeps_unknown_spatial_references_unknown() {
        for srs in ["EPSG:2263", "EPSG:3413", "EPSG:2299", "EPSG:99999"] {
            let result =
                spatial_reference_specification(SpatialReference::from_str(srs).unwrap().into());
            assert!(
                matches!(&result, Err(Error::UnknownSrsString { srs_string }) if srs_string == srs),
                "{srs}: {result:?}"
            );
        }
    }

    #[test]
    fn it_caches_known_spatial_references_only() {
        let cache = SpatialReferenceCache::default();
        let known = SpatialReference::from_str("ESRI:54009").unwrap().into();
        let first = cache.specification(known).unwrap();
        assert_eq!(cache.specification(known).unwrap(), first);
        assert_eq!(first, spatial_reference_specification(known).unwrap());
        assert_eq!(cache.specifications.len(), 1);

        let unknown = SpatialReference::from_str("EPSG:99999").unwrap().into();
        for _ in 0..2 {
            assert!(matches!(
                cache.specification(unknown),
                Err(Error::UnknownSrsString { .. })
            ));
        }
        assert_eq!(cache.specifications.len(), 1);
    }

    /// The spatial references of the regression tests above, known and unknown
    const BASELINE: [&str; 13] = [
        "EPSG:4326",
        "EPSG:3857",
        "EPSG:25832",
        "EPSG:32632",
        "EPSG:3035",
        "EPSG:4258",
        "EPSG:31467",
        "ESRI:54009",
        "SR-ORG:81",
        "EPSG:2263",
        "EPSG:3413",
        "EPSG:2299",
        "EPSG:99999",
    ];

    #[test]
    fn it_looks_up_the_axis_order_of_the_specification() {
        let cache = SpatialReferenceCache::default();
        for srs in BASELINE {
            let spatial_reference = SpatialReference::from_str(srs).unwrap().into();
            let expected = spatial_reference_specification(spatial_reference).and_then(|spec| {
                spec.axis_order.ok_or(Error::AxisOrderingNotKnownForSrs {
                    srs_string: srs.to_owned(),
                })
            });
            assert_eq!(
                format!("{:?}", cache.axis_order(spatial_reference)),
                format!("{expected:?}"),
                "{srs}"
            );
        }
    }

    #[test]
    fn it_asks_proj_only_for_spatial_references_outside_the_registry() {
        let cache = SpatialReferenceCache::default();
        let count = || PROJ_CONTEXTS.with(std::cell::Cell::get);
        let before = count();
        for srs in ["EPSG:3857", "EPSG:4326", "EPSG:31467"] {
            let spatial_reference = SpatialReference::from_str(srs).unwrap().into();
            for _ in 0..10 {
                let _: BoundingBox2D = crate::api::ogc::util::OgcBoundingBox::new(1., 2., 3., 4.)
                    .bounds(spatial_reference, &cache)
                    .unwrap();
            }
        }
        assert_eq!(
            count() - before,
            0,
            "EPSG codes of the registry need no PROJ"
        );

        let esri = SpatialReference::from_str("ESRI:54009").unwrap().into();
        for _ in 0..10 {
            cache.axis_order(esri).unwrap();
        }
        // one `proj_json`, and no `proj_proj_string`
        assert_eq!(count() - before, 1, "PROJ is asked once per reference");
    }

    /// The registry was generated with pyproj; the server must give the same axis order with the PROJ it links
    #[test]
    fn it_has_the_axis_order_of_proj_for_every_code_of_the_registry() {
        let mut checked = 0;
        let mut mismatches = Vec::new();
        for code in 0..=u32::from(u16::MAX) {
            let spatial_reference = SpatialReference::new(SpatialReferenceAuthority::Epsg, code);
            let Some(static_order) = StaticEpsgMetadataProvider::axis_order(spatial_reference)
            else {
                continue;
            };
            let proj_order =
                proj_json(&spatial_reference.srs_string()).and_then(|json| json.axis_order());
            if proj_order != Some(static_order.into()) {
                mismatches.push((code, static_order, proj_order));
            }
            checked += 1;
        }
        assert!(
            checked > 4000,
            "the registry should know most EPSG codes, got {checked}"
        );
        assert!(
            mismatches.is_empty(),
            "{} of {checked} differ: {mismatches:?}",
            mismatches.len()
        );
    }

    /// PROJ knows these codes but cannot write them as a PROJ string; this crashed the server
    #[test]
    fn it_rejects_spatial_references_without_a_proj_string() {
        let cache = SpatialReferenceCache::default();
        for srs in ["EPSG:22700", "EPSG:32600", "EPSG:32700"] {
            let spatial_reference = SpatialReference::from_str(srs).unwrap().into();
            assert!(
                matches!(
                    cache.specification(spatial_reference),
                    Err(Error::UnknownSrsString { .. })
                ),
                "{srs}"
            );
            assert_eq!(
                cache.axis_order(spatial_reference).unwrap(),
                AxisOrder::EastNorth,
                "{srs}"
            );
        }
    }

    #[ge_context::test]
    async fn get_spatial_reference(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let session_id = session.id();

        let req = actix_web::test::TestRequest::get()
            .uri("/spatialReferenceSpecification/EPSG:4326")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 200);

        let spec: SpatialReferenceSpecification = actix_web::test::read_body_json(res).await;
        assert_eq!(
            SpatialReferenceSpecification {
                name: "WGS 84".to_owned(),
                spatial_reference: SpatialReference::epsg_4326().into(),
                proj_string: "+proj=longlat +datum=WGS84 +no_defs +type=crs".to_owned(),
                extent: BoundingBox2D::new_unchecked((-180., -90.).into(), (180., 90.).into())
                    .into(),
                axis_labels: Some(
                    (
                        "Geodetic longitude".to_owned(),
                        "Geodetic latitude".to_owned()
                    )
                        .into()
                ),
                axis_order: Some(AxisOrder::NorthEast),
            },
            spec
        );
    }

    #[test]
    fn spec_webmercator() {
        let spec = spatial_reference_specification(
            SpatialReference::from_str("EPSG:3857").unwrap().into(),
        )
        .unwrap();
        assert_eq!(spec.name, "WGS 84 / Pseudo-Mercator");
        assert_eq!(
            spec.spatial_reference,
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 3857).into()
        );
        assert_eq!(
            spec.proj_string,
            "+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m +nadgrids=@null +wktext +no_defs +type=crs",
        );
        assert_approx_eq!(
            BoundingBox2D,
            spec.extent.into(),
            BoundingBox2D::new_unchecked(
                (-20_037_508.342_789_244, -20_048_966.104_014_6).into(),
                (20_037_508.342_789_244, 20_048_966.104_014_594).into()
            ),
            epsilon = 0.001 // NOTE: changed to match with shortened numbers in static crs-constants crate.
        );
        assert_eq!(
            spec.axis_labels,
            Some(("Easting".to_owned(), "Northing".to_owned()).into())
        );
        assert_eq!(spec.axis_order, Some(AxisOrder::EastNorth));
    }

    #[test]
    fn spec_wgs84() {
        let spec = spatial_reference_specification(
            SpatialReference::from_str("EPSG:4326").unwrap().into(),
        )
        .unwrap();
        assert_eq!(
            SpatialReferenceSpecification {
                name: "WGS 84".to_owned(),
                spatial_reference: SpatialReference::epsg_4326().into(),
                proj_string: "+proj=longlat +datum=WGS84 +no_defs +type=crs".to_owned(),
                extent: BoundingBox2D::new_unchecked((-180., -90.).into(), (180., 90.).into())
                    .into(),
                axis_labels: Some(
                    (
                        "Geodetic longitude".to_owned(),
                        "Geodetic latitude".to_owned()
                    )
                        .into()
                ),
                axis_order: Some(AxisOrder::NorthEast),
            },
            spec
        );
    }

    #[test]
    fn spec_utm32n() {
        let spec = spatial_reference_specification(
            SpatialReference::from_str("EPSG:32632").unwrap().into(),
        )
        .unwrap();
        assert_eq!(spec.name, "WGS 84 / UTM zone 32N");
        assert_eq!(
            spec.spatial_reference,
            SpatialReference::new(SpatialReferenceAuthority::Epsg, 32632).into()
        );
        assert_eq!(
            spec.proj_string,
            "+proj=utm +zone=32 +datum=WGS84 +units=m +no_defs +type=crs",
        );
        assert_approx_eq!(
            BoundingBox2D,
            spec.extent.into(),
            BoundingBox2D::new_unchecked(
                (166_021.443_080_539_64, 0.0).into(),
                (833_978.556_919_460_4, 9_329_005.182_447_437).into()
            ),
            epsilon = 0.001 // NOTE: changed to match with shortened numbers in static crs-constants crate.
        );

        assert_eq!(
            spec.axis_labels,
            Some(("Easting".to_owned(), "Northing".to_owned()).into())
        );
        assert_eq!(spec.axis_order, Some(AxisOrder::EastNorth));
    }

    #[test]
    fn spec_geos() {
        let spec = spatial_reference_specification(
            SpatialReference::from_str("SR-ORG:81").unwrap().into(),
        )
        .unwrap();
        assert_eq!(
            SpatialReferenceSpecification {
                name: "GEOS - GEOstationary Satellite".to_owned(),
                spatial_reference: SpatialReference::new(SpatialReferenceAuthority::SrOrg, 81)
                    .into(),
                proj_string:
                    "+proj=geos +lon_0=0 +h=-0 +x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs".into(),
                extent: BoundingBox2D::new_unchecked(
                    (-5_568_748.276, -5_568_748.276).into(),
                    (5_568_748.276, 5_568_748.276).into()
                )
                .into(),
                axis_labels: None,
                axis_order: Some(AxisOrder::EastNorth),
            },
            spec
        );
    }
}
