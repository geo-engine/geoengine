pub struct StacFeatureCollection {
    collection_type: String,
    stac_version: String,
    stac_extensions: Vec<String>,
    context: StacContext,
    features: Vec<StacFeature>,
    links: Vec<StacLink>,
}

pub struct StacContext {
    pub page: u64,
    pub limit: u64,
    pub matched: u64,
    pub returned: u64,
}

pub struct StacLink {
    pub rel: String,
    pub title: String,
    pub method: String,
    pub href: String,
}

pub struct StacFeature {
    geo_type: String,
    stac_version: String,
    stac_extensions: Vec<String>,
    id: String,
    bbox: geo::Rect<f64>,
    geometry: geo::Geometry<f64>,
    assets: Vec<StacAsset>,
}

pub struct StacEoBand {
    name: String,
    common_name: String,
    center_wavelenght: f64,
    full_width_half_max: f64,
}

pub struct StackProjShape(i32, i32);
pub struct StackProjTransform(f64, f64, f64, f64, f64, f64, f64, f64);

pub struct StacAsset {
    title: String,
    mime_type: String,
    roles: Vec<String>,
    gsd: Option<f64>,
    eo_bands: Option<Vec<StacEoBand>>,
    href: String,
    proj_shape: Option<StackProjShape>,
    proj_transform: Option<StackProjTransform>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use geojson::{FeatureCollection, GeoJson};

    #[test]
    fn test_name() {
        let b = include_str!("./s2_items_1_1.json");
        let gj = b.parse::<GeoJson>().unwrap();
        dbg!(gj);
    }
}
