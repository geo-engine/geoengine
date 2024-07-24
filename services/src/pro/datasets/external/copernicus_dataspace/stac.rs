use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, DateTime, DateTimeError, Duration, RasterQueryRectangle,
};
use snafu::{ResultExt, Snafu};
use url::Url;

// API limits
const MAX_NUM_PAGES: usize = 100;
const MAX_PAGE_SIZE: usize = 1000;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum CopernicusStacError {
    DateTimeMissing,
    CannotDisplayDateTimeAsString,
    CannotParseDateTimeFromString { source: DateTimeError },
    CannotBuildStacUrl { source: url::ParseError },
    QueryingStacServerFailed { source: reqwest::Error },
    InvalidStacResponse { source: reqwest::Error },
    MissingStacAssetProperty { property: String },
    ProductAlternateS3HrefIsNotAString,
}

fn bbox_time_query(
    query: &RasterQueryRectangle,
) -> Result<[(&'static str, String); 2], CopernicusStacError> {
    // TODO: add query buffer like in Element84 provider?
    let time_start = query.time_interval.start();
    let time_end = query.time_interval.end();

    let query = [
        (
            "bbox",
            format!(
                "{},{},{},{}",
                query.spatial_bounds.lower_left().x,
                query.spatial_bounds.lower_left().y,
                query.spatial_bounds.upper_right().x,
                query.spatial_bounds.upper_right().y
            ),
        ),
        (
            "datetime",
            format!(
                "{}/{}",
                time_start
                    .as_date_time()
                    .ok_or(CopernicusStacError::CannotDisplayDateTimeAsString)?
                    .to_datetime_string(),
                time_end
                    .as_date_time()
                    .ok_or(CopernicusStacError::CannotDisplayDateTimeAsString)?
                    .to_datetime_string()
            ),
        ),
    ];

    Ok(query)
}

pub async fn load_stac_items(
    stac_url: Url,
    collection: &str,
    query: RasterQueryRectangle,
    product_type: &str,
) -> Result<Vec<stac::Item>, CopernicusStacError> {
    // query the STAC API to gather all files
    // TODO: split request up for large time intervals to avoid page limit
    let path = format!("collections/{collection}/items");
    let url = stac_url.join(&path).context(CannotBuildStacUrl)?;
    let query = bbox_time_query(&query)?;

    let client = reqwest::Client::new();

    let mut page = 1;

    let mut stac_items = Vec::new();

    loop {
        log::debug!(
            "Copernicus Dataspace Provider: Requesting page {} of STAC API",
            page
        );

        let response = client
            .get(url.clone())
            .query(&query)
            .query(&("limit", MAX_PAGE_SIZE))
            .query(&("page", page))
            .query(&["sortby", "+datetime"])
            .send()
            .await
            .context(QueryingStacServerFailed)?;

        let new_items = response
            .json::<stac::ItemCollection>()
            .await
            .context(InvalidStacResponse)?;

        let num_items = new_items.items.len();

        // the STAC API does not allow to filter by product type, so we have to do it here
        let filtered_items = new_items.items.into_iter().filter(|item| {
            item.properties
                .additional_fields
                .get("productType")
                .and_then(|v| v.as_str())
                == Some(product_type)
        });

        stac_items.extend(filtered_items);

        if num_items < MAX_PAGE_SIZE {
            // as the page is not full, we are at the end
            break;
        }

        // there may be more items available, so go to next page, if possible

        if page >= MAX_NUM_PAGES {
            log::warn!("Copernicus Data Provider reached maximum number of pages of the STAC API and there may be more items available. This may lead to incomplete results. Try shorter queries.");
            break;
        }

        page += 1;
    }

    Ok(stac_items)
}

/// There can be multiple stac Items for the same product but with different bboxes.
/// This happens e.g. if one satellite image spans multiple files.
/// For now we resolve this by putting the items one after another by adding 1ms to the datetime.
/// A proper solution would be to support multiple files per time slice.
pub fn resolve_datetime_duplicates(items: &mut Vec<stac::Item>) -> Result<(), CopernicusStacError> {
    if items.is_empty() {
        // nothing to do for empty list
        return Ok(());
    }

    items.sort_by(|a, b| a.properties.datetime.cmp(&b.properties.datetime));

    let current_time = items
        .first()
        .expect("first item should exist because list is not empty")
        .properties
        .datetime
        .as_ref()
        .ok_or(CopernicusStacError::DateTimeMissing)?;

    let mut current_time =
        DateTime::parse_from_rfc3339(current_time).context(CannotParseDateTimeFromString)?;

    for item in items.iter_mut().skip(1) {
        let item_time = item
            .properties
            .datetime
            .as_ref()
            .ok_or(CopernicusStacError::DateTimeMissing)?;
        let mut item_time =
            DateTime::parse_from_rfc3339(item_time).context(CannotParseDateTimeFromString)?;

        // ensure that the datetime is strictly increasing with respect to the previous item
        if item_time <= current_time {
            item_time = item_time + Duration::milliseconds(1);
        }

        item.properties.datetime = Some(item_time.to_datetime_string_with_millis());

        current_time = item_time;
    }

    Ok(())
}

pub trait StacItemExt {
    fn datetime(&self) -> Result<DateTime, CopernicusStacError>;
    fn s3_assert_product_url(&self) -> Result<String, CopernicusStacError>;
}

impl StacItemExt for stac::Item {
    fn datetime(&self) -> Result<DateTime, CopernicusStacError> {
        let datetime = self
            .properties
            .datetime
            .as_ref()
            .ok_or(CopernicusStacError::DateTimeMissing)?;

        DateTime::parse_from_rfc3339(datetime).context(CannotParseDateTimeFromString)
    }

    fn s3_assert_product_url(&self) -> Result<String, CopernicusStacError> {
        Ok(self
            .assets
            .get("PRODUCT")
            .ok_or(CopernicusStacError::MissingStacAssetProperty {
                property: "PRODUCT".to_string(),
            })?
            .additional_fields
            .get("alternate")
            .ok_or(CopernicusStacError::MissingStacAssetProperty {
                property: "PRODUCT/altenate".to_string(),
            })?
            .get("s3")
            .ok_or(CopernicusStacError::MissingStacAssetProperty {
                property: "PRODUCT/alternate/s3".to_string(),
            })?
            .get("href")
            .ok_or(CopernicusStacError::MissingStacAssetProperty {
                property: "PRODUCT/alternate/s3/href".to_string(),
            })?
            .as_str()
            .ok_or(CopernicusStacError::ProductAlternateS3HrefIsNotAString)?
            .to_string())
    }
}
