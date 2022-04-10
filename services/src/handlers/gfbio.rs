use crate::contexts::Context;
use crate::error::Result;
use crate::util::config::{get_config_element, GFBio};
use actix_web::{web, FromRequest, Responder};
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;
use geoengine_datatypes::dataset::{DatasetId, ExternalDatasetId};
use geoengine_datatypes::primitives::{DateTime, VectorQueryRectangle};
use geoengine_operators::engine::{
    MetaDataProvider, TypedResultDescriptor, VectorResultDescriptor,
};
use geoengine_operators::source::{AttributeFilter, OgrSourceDataset};
use regex::Regex;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::datasets::external::gfbio::{GfbioDataProvider, GFBIO_PROVIDER_ID};
use crate::datasets::external::pangaea::PANGAEA_PROVIDER_ID;
use crate::datasets::storage::DatasetProviderDb;
use geoengine_datatypes::identifier;
use geoengine_operators::util::input::StringOrNumberRange;

identifier!(BasketId);

pub(crate) fn init_gfbio_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: Context,
    C::Session: FromRequest,
{
    cfg.service(web::resource("/gfbio/basket/{id}").route(web::get().to(get_basket_handler::<C>)));
}

async fn get_basket_handler<C: Context>(
    id: web::Path<BasketId>,
    session: C::Session,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    // Get basket content
    let config = get_config_element::<GFBio>()?;
    let abcd_provider = ctx
        .dataset_db_ref()
        .await
        .dataset_provider(&session, GFBIO_PROVIDER_ID)
        .await
        .ok();

    let abcd_ref = abcd_provider
        .as_ref()
        .and_then(|p| p.as_any().downcast_ref::<GfbioDataProvider>());

    let ec = ctx.execution_context(session)?;

    let url = config.basket_api_base_url.join(&id.to_string())?;
    let client = Client::new();

    let response = client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    let basket = Basket::new::<C>(
        serde_json::from_str::<BasketInternal>(&response)?,
        ec,
        abcd_ref,
        config.group_abcd_units,
    )
    .await?;
    Ok(web::Json(basket))
}

#[derive(Serialize, Debug)]
struct Basket {
    basket_id: BasketId,
    content: Vec<BasketEntry>,
    user_id: Option<String>,
    created: DateTime,
    updated: DateTime,
}

impl Basket {
    async fn new<C: Context>(
        value: BasketInternal,
        ec: <C as Context>::ExecutionContext,
        abcd: Option<&GfbioDataProvider>,
        group_abcd_units: bool,
    ) -> Result<Basket> {
        let mut abcd_entries: Vec<AbcdEntry> = Vec::new();
        let mut pangaea_entries: Vec<PangaeaEntry> = Vec::new();
        let mut error_entries: Vec<BasketEntry> = Vec::new();

        for bi in value.content {
            match TryInto::<TypedBasketEntry>::try_into(bi) {
                Ok(TypedBasketEntry::Abcd(abcd)) => abcd_entries.push(abcd),
                Ok(TypedBasketEntry::Pangaea(pangaea)) => pangaea_entries.push(pangaea),
                Err(title) => error_entries.push(BasketEntry {
                    title,
                    status: BasketEntryStatus::Unavailable,
                }),
            }
        }

        let (mut pangaea, mut abcd) = tokio::join!(
            Self::process_pangaea_entries::<C>(&ec, pangaea_entries),
            Self::process_abcd_entries(abcd, abcd_entries, group_abcd_units)
        );

        abcd.append(&mut pangaea);
        abcd.append(&mut error_entries);

        Ok(Basket {
            basket_id: value.basket_id,
            content: abcd,
            user_id: value.user_id,
            created: value.created,
            updated: value.updated,
        })
    }

    async fn process_pangaea_entries<C: Context>(
        ec: &<C as Context>::ExecutionContext,
        entries: Vec<PangaeaEntry>,
    ) -> Vec<BasketEntry> {
        entries
            .into_iter()
            .map(|entry| Self::process_pangaea_entry::<C>(ec, entry))
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await
    }

    async fn process_pangaea_entry<C: Context>(
        ec: &<C as Context>::ExecutionContext,
        entry: PangaeaEntry,
    ) -> BasketEntry {
        if !entry.visualizable {
            return BasketEntry {
                title: entry.title,
                status: BasketEntryStatus::Unavailable,
            };
        }

        let id = DatasetId::External(ExternalDatasetId {
            provider_id: PANGAEA_PROVIDER_ID,
            dataset_id: entry.doi,
        });
        let mdp = ec as &dyn MetaDataProvider<
            OgrSourceDataset,
            VectorResultDescriptor,
            VectorQueryRectangle,
        >;

        Self::generate_loading_info(entry.title, id, mdp, None).await
    }

    async fn process_abcd_entries(
        provider: Option<&GfbioDataProvider>,
        entries: Vec<AbcdEntry>,
        group_units: bool,
    ) -> Vec<BasketEntry> {
        let entries = if group_units {
            Self::group_abcd_entries(entries)
        } else {
            entries
        };

        match provider {
            Some(provider) => {
                entries
                    .into_iter()
                    .map(|entry| Self::process_abcd_entry(provider, entry))
                    .collect::<FuturesUnordered<_>>()
                    .collect::<Vec<_>>()
                    .await
            }
            None => entries
                .into_iter()
                .map(|entry| BasketEntry {
                    title: entry.title,
                    status: BasketEntryStatus::Unavailable,
                })
                .collect::<Vec<_>>(),
        }
    }

    async fn process_abcd_entry(provider: &GfbioDataProvider, entry: AbcdEntry) -> BasketEntry {
        if !entry.visualizable {
            return BasketEntry {
                title: entry.title,
                status: BasketEntryStatus::Unavailable,
            };
        }

        let sg_id = match provider.resolve_surrogate_key(entry.id.as_str()).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                return BasketEntry {
                    title: entry.title,
                    status: BasketEntryStatus::Unavailable,
                }
            }
            Err(e) => {
                return BasketEntry {
                    title: entry.title,
                    status: BasketEntryStatus::Error {
                        message: format!(
                            "Error looking up dataset \"{}\": {:?}",
                            entry.id.as_str(),
                            e
                        ),
                    },
                }
            }
        };

        let id = DatasetId::External(ExternalDatasetId {
            provider_id: GFBIO_PROVIDER_ID,
            dataset_id: sg_id.to_string(),
        });

        let mdp = provider
            as &dyn MetaDataProvider<
                OgrSourceDataset,
                VectorResultDescriptor,
                VectorQueryRectangle,
            >;

        // Apply filter
        let filter = if entry.units.is_empty() {
            None
        } else {
            let ranges = entry
                .units
                .into_iter()
                .map(|u| StringOrNumberRange::String(u.clone()..=u))
                .collect::<Vec<_>>();

            Some(vec![AttributeFilter {
                attribute: "/DataSets/DataSet/Units/Unit/UnitID".to_string(),
                ranges,
                keep_nulls: false,
            }])
        };

        Self::generate_loading_info(entry.title, id, mdp, filter).await
    }

    fn group_abcd_entries(entries: Vec<AbcdEntry>) -> Vec<AbcdEntry> {
        lazy_static::lazy_static! {
            static ref TITLE_REGEX: Regex = Regex::new(r#"^(.*),\s*a\s*(.*)?record\s*of\s*the\s*(.*)\s*dataset\s*\[ID:\s*(.*)\]\s*$"#).expect("Expression is valid");
        }

        // Group units
        let mut map = HashMap::new();

        for mut e in entries {
            let key = match TITLE_REGEX.captures(&e.title) {
                None => e.title.clone(),
                Some(c) => c
                    .get(1)
                    .expect("First group is present")
                    .as_str()
                    .to_string(),
            };

            match map.entry(key.clone()) {
                std::collections::hash_map::Entry::Vacant(ve) => {
                    e.title = key;
                    ve.insert(e);
                }
                std::collections::hash_map::Entry::Occupied(mut oe) => {
                    oe.get_mut().units.append(&mut e.units);
                }
            }
        }
        let mut result = map.into_values().collect::<Vec<_>>();
        result.sort_by(|l, r| Ord::cmp(&l.title, &r.title));
        result
    }

    async fn generate_loading_info(
        title: String,
        id: DatasetId,
        mdp: &dyn MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
        filter: Option<Vec<AttributeFilter>>,
    ) -> BasketEntry {
        let md = match mdp.meta_data(&id).await {
            Ok(md) => md,
            Err(e) => {
                return BasketEntry {
                    title,
                    status: BasketEntryStatus::Error {
                        message: format!("Unable to load meta data: {:?}", e),
                    },
                };
            }
        };

        let status = match md.result_descriptor().await {
            Err(e) => BasketEntryStatus::Error {
                message: format!("Unable to load meta data: {:?}", e),
            },
            Ok(rd) => BasketEntryStatus::Ok(BasketEntryLoadingDetails {
                dataset_id: id,
                source_operator: "OgrSource".to_string(),
                result_descriptor: TypedResultDescriptor::Vector(rd),
                attribute_filters: filter,
            }),
        };
        BasketEntry { title, status }
    }
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct BasketEntry {
    title: String,
    #[serde(flatten)]
    status: BasketEntryStatus,
}

#[derive(Serialize, Debug)]
#[serde(tag = "status", rename_all = "camelCase")]
enum BasketEntryStatus {
    Unavailable,
    Error { message: String },
    Ok(BasketEntryLoadingDetails),
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct BasketEntryLoadingDetails {
    dataset_id: DatasetId,
    source_operator: String,
    result_descriptor: TypedResultDescriptor,
    #[serde(skip_serializing_if = "Option::is_none")]
    attribute_filters: Option<Vec<AttributeFilter>>,
}

const PANGAEA_PATTERN: &str = "doi.pangaea.de/";

#[derive(Debug, Deserialize)]
struct BasketInternal {
    #[serde(rename = "basketId")]
    basket_id: BasketId,
    content: Vec<BasketEntryInternal>,
    #[serde(rename = "userId")]
    user_id: Option<String>,
    #[serde(rename = "createdAt")]
    created: DateTime,
    #[serde(rename = "updatedAt")]
    updated: DateTime,
}

#[derive(Debug, Deserialize)]
struct BasketEntryInternal {
    title: String,
    #[serde(rename = "metadatalink")]
    metadata_link: String,
    #[serde(rename = "datalink")]
    data_link: Option<String>,
    #[serde(rename = "parentIdentifier")]
    parent_id: Option<String>,
    #[serde(rename = "dcIdentifier")]
    unit_id: Option<String>,
    #[serde(rename = "vat")]
    visualizable: bool,
}

#[derive(Debug, PartialEq)]
enum TypedBasketEntry {
    Pangaea(PangaeaEntry),
    Abcd(AbcdEntry),
}

#[derive(Debug, PartialEq)]
struct PangaeaEntry {
    title: String,
    doi: String,
    visualizable: bool,
}

#[derive(Debug, PartialEq)]
struct AbcdEntry {
    title: String,
    id: String,
    units: Vec<String>,
    visualizable: bool,
}

impl TryFrom<BasketEntryInternal> for TypedBasketEntry {
    type Error = String;

    fn try_from(value: BasketEntryInternal) -> std::result::Result<Self, Self::Error> {
        match value.metadata_link.find(PANGAEA_PATTERN) {
            // Is a pangaea entry
            Some(start) => {
                let doi = value.metadata_link[start + PANGAEA_PATTERN.len()..].to_string();
                Ok(Self::Pangaea(PangaeaEntry {
                    title: value.title,
                    doi,
                    visualizable: value.visualizable,
                }))
            }
            None => {
                let id = value.parent_id.or(value.data_link);
                match id {
                    Some(id) => {
                        let units = match value.unit_id {
                            Some(unit) => vec![unit],
                            None => vec![],
                        };

                        Ok(Self::Abcd(AbcdEntry {
                            title: value.title,
                            id,
                            units,
                            visualizable: value.visualizable,
                        }))
                    }
                    None => Err(value.title),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::handlers::gfbio::{
        AbcdEntry, Basket, BasketEntry, BasketEntryLoadingDetails, BasketEntryStatus,
        BasketInternal, TypedBasketEntry,
    };
    use geoengine_datatypes::collections::VectorDataType;
    use geoengine_datatypes::dataset::{DatasetId, DatasetProviderId, ExternalDatasetId};
    use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceOption};
    use geoengine_operators::engine::{TypedResultDescriptor, VectorResultDescriptor};
    use geoengine_operators::source::AttributeFilter;
    use geoengine_operators::util::input::StringOrNumberRange;
    use uuid::Uuid;

    #[test]
    fn basket_entry_serialization_unavailable() {
        let be = BasketEntry {
            title: "Test".to_string(),
            status: BasketEntryStatus::Unavailable,
        };

        let json = serde_json::json!({
            "title": "Test",
            "status": "unavailable"
        });

        assert_eq!(json, serde_json::to_value(be).unwrap());
    }

    #[test]
    fn basket_entry_serialization_error() {
        let be = BasketEntry {
            title: "Test".to_string(),
            status: BasketEntryStatus::Error {
                message: "Error".to_string(),
            },
        };

        let json = serde_json::json!({
            "title": "Test",
            "status": "error",
            "message": "Error"
        });

        assert_eq!(json, serde_json::to_value(be).unwrap());
    }

    #[test]
    fn basket_entry_serialization_ok() {
        let id = DatasetId::External(ExternalDatasetId {
            provider_id: DatasetProviderId(Uuid::default()),
            dataset_id: "1".to_string(),
        });

        let be = BasketEntry {
            title: "Test".to_string(),
            status: BasketEntryStatus::Ok(BasketEntryLoadingDetails {
                dataset_id: id.clone(),
                source_operator: "OgrSource".to_string(),
                result_descriptor: TypedResultDescriptor::Vector(VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReferenceOption::SpatialReference(
                        SpatialReference::epsg_4326(),
                    ),
                    columns: Default::default(),
                }),
                attribute_filters: None,
            }),
        };

        let json = serde_json::json!({
            "title": "Test",
            "status": "ok",
            "datasetId": id,
            "sourceOperator": "OgrSource",
            "resultDescriptor": {
                "type": "vector",
                "dataType": "MultiPoint",
                "spatialReference": "EPSG:4326",
                "columns": {}
            }
        });

        assert_eq!(json, serde_json::to_value(be).unwrap());
    }

    #[test]
    fn basket_entry_serialization_ok_with_filter() {
        let id = DatasetId::External(ExternalDatasetId {
            provider_id: DatasetProviderId(Uuid::default()),
            dataset_id: "1".to_string(),
        });

        let be = BasketEntry {
            title: "Test".to_string(),
            status: BasketEntryStatus::Ok(BasketEntryLoadingDetails {
                dataset_id: id.clone(),
                source_operator: "OgrSource".to_string(),
                result_descriptor: TypedResultDescriptor::Vector(VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReferenceOption::SpatialReference(
                        SpatialReference::epsg_4326(),
                    ),
                    columns: Default::default(),
                }),
                attribute_filters: Some(vec![AttributeFilter {
                    attribute: "a".to_string(),
                    ranges: vec![StringOrNumberRange::Int(0..=1)],
                    keep_nulls: false,
                }]),
            }),
        };

        let json = serde_json::json!({
            "title": "Test",
            "status": "ok",
            "datasetId": id,
            "sourceOperator": "OgrSource",
            "resultDescriptor": {
                "type": "vector",
                "dataType": "MultiPoint",
                "spatialReference": "EPSG:4326",
                "columns": {}
            },
            "attributeFilters": [
                {
                    "attribute": "a",
                    "ranges": [ [0,1] ],
                    "keepNulls": false,
                }
            ]

        });

        assert_eq!(json, serde_json::to_value(be).unwrap());
    }

    #[test]
    fn pangaea_basket_deserialization() {
        let json = serde_json::json!({
          "content": [
            {
              "id": "PANGAEA.932375",
              "titleUrl": "https://doi.pangaea.de/10.1594/PANGAEA.932375",
              "title": "Vuilleumier, Laurent (2021): Other measurements at 10 m from station Payerne (2020-09)",
              "upperLabels": [
                {
                  "innerInfo": "2021",
                  "tooltip": "Publication year",
                  "colorClass": "bg-label-blue"
                },
                {
                  "innerInfo": "PANGAEA",
                  "tooltip": "This dataset is provided by Data Publisher for Earth; Environmental Science  (PANGAEA).",
                  "colorClass": "bg-goldenrod"
                }
              ],
              "citation": {
                "title": "Other measurements at 10 m from station Payerne (2020-09)",
                "date": "2021-06-08",
                "DOI": "https://doi.pangaea.de/10.1594/PANGAEA.932375",
                "dataCenter": "PANGAEA",
                "source": "Swiss Meteorological Agency, Payerne",
                "creator": [
                  "Vuilleumier, Laurent"
                ]
              },
              "licence": [
                "Other"
              ],
              "vat": false,
              "vatTooltip": "This dataset can be transfered to VAT.",
              "xml": "<dataset xmlns=\"urn:pangaea.de:dataportals\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><dc:title>Other measurements at 10 m from station Payerne (2020-09)</dc:title><dc:creator>Vuilleumier, Laurent</dc:creator><principalInvestigator>Vuilleumier, Laurent</principalInvestigator><dc:source>Swiss Meteorological Agency, Payerne</dc:source><dc:publisher>PANGAEA</dc:publisher><dataCenter>PANGAEA: Data Publisher for Earth &amp; Environmental Science</dataCenter><dc:date>2021-06-08</dc:date><dc:type>Dataset</dc:type><dc:format>text/tab-separated-values, 430587 data points</dc:format><dc:identifier>https://doi.org/10.1594/PANGAEA.932375</dc:identifier><dc:language>en</dc:language><dc:rights>Access constraints: access rights needed</dc:rights><dc:relation>Vuilleumier, Laurent (2021): BSRN Station-to-archive file for station Payerne (2020-09). ftp://ftp.bsrn.awi.de/pay/pay0920.dat.gz</dc:relation><dc:subject type=\"parameter\" xsi:type=\"SubjectType\">DATE/TIME</dc:subject><dc:subject type=\"parameter\" xsi:type=\"SubjectType\">HEIGHT above ground</dc:subject><dc:subject type=\"parameter\" xsi:type=\"SubjectType\">Short-wave upward (REFLEX) radiation</dc:subject><dc:subject type=\"parameter\" xsi:type=\"SubjectType\">Short-wave upward (REFLEX) radiation, standard deviation</dc:subject><dc:subject type=\"parameter\" xsi:type=\"SubjectType\">Short-wave upward (REFLEX) radiation, minimum</dc:subject><dc:subject type=\"parameter\" xsi:type=\"SubjectType\">Short-wave upward (REFLEX) radiation, maximum</dc:subject><dc:subject type=\"parameter\" xsi:type=\"SubjectType\">Long-wave upward radiation</dc:subject><dc:subject type=\"parameter\" xsi:type=\"SubjectType\">Long-wave upward radiation, standard deviation</dc:subject><dc:subject type=\"parameter\" xsi:type=\"SubjectType\">Long-wave upward radiation, minimum</dc:subject><dc:subject type=\"parameter\" xsi:type=\"SubjectType\">Long-wave upward radiation, maximum</dc:subject><dc:subject type=\"parameter\" xsi:type=\"SubjectType\">Temperature, air</dc:subject><dc:subject type=\"parameter\" xsi:type=\"SubjectType\">Humidity, relative</dc:subject><dc:subject type=\"method\" xsi:type=\"SubjectType\">Pyranometer, Kipp &amp; Zonen, CM21, SN 041303, WRMC No. 21074</dc:subject><dc:subject type=\"method\" xsi:type=\"SubjectType\">Pyrgeometer, Eppley Laboratory Inc., PIR, SN 31964F3, WRMC No. 21089</dc:subject><dc:subject type=\"method\" xsi:type=\"SubjectType\">Thermometer</dc:subject><dc:subject type=\"method\" xsi:type=\"SubjectType\">Hygrometer</dc:subject><dc:subject type=\"project\" xsi:type=\"SubjectType\">BSRN: Baseline Surface Radiation Network</dc:subject><dc:subject type=\"sensor\" xsi:type=\"SubjectType\">Monitoring station</dc:subject><dc:subject type=\"sensor\" xsi:type=\"SubjectType\">MONS</dc:subject><dc:subject type=\"feature\" xsi:type=\"SubjectType\">PAY</dc:subject><dc:subject type=\"feature\" xsi:type=\"SubjectType\">Payerne</dc:subject><dc:subject type=\"feature\" xsi:type=\"SubjectType\">WCRP/GEWEX</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">citable</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">author31671</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">campaign32853</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">curlevel30</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">event2536888</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">geocode1599</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">geocode1600</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">geocode1601</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">geocode56349</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">geocode8128</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">inst32</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">method10271</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">method11312</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">method4722</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">method5039</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">method9835</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">param2219</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">param45299</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">param4610</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">param55911</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">param55912</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">param55913</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">param55914</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">param55915</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">param55916</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">param55917</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">pi31671</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">project4094</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">ref108695</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">sourceinst2878</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term1073288</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term21461</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term2663133</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term37764</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term38492</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term40453</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term41019</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term42421</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term43794</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term43863</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term43923</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term43972</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term43975</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term44358</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">topotype8</dc:subject><dc:coverage xsi:type=\"CoverageType\"><northBoundLatitude>46.815</northBoundLatitude><westBoundLongitude>6.944</westBoundLongitude><southBoundLatitude>46.815</southBoundLatitude><eastBoundLongitude>6.944</eastBoundLongitude><location>Switzerland</location><minElevation>10.0 m (HEIGHT above ground)</minElevation><maxElevation>10.0 m (HEIGHT above ground)</maxElevation><startDate>2020-09-01</startDate><endDate>2020-09-30</endDate></dc:coverage><linkage type=\"metadata\">https://doi.pangaea.de/10.1594/PANGAEA.932375</linkage><linkage accessRestricted=\"true\" type=\"data\">https://doi.pangaea.de/10.1594/PANGAEA.932375?format=textfile</linkage><additionalContent>BSRN station no: 21; Surface type: cultivated; Topography type: hilly, rural; Horizon: doi:10.1594/PANGAEA.669523; Station scientist: Laurent Vuilleumier (laurent.vuilleumier@meteoswiss.ch)</additionalContent></dataset>",
              "longitude": 6.944,
              "latitude": 46.815,
              "titleTooltip": "min latitude: 46.815, max longitude: 6.944",
              "metadatalink": "https://doi.pangaea.de/10.1594/PANGAEA.932375",
              "datalink": "https://doi.pangaea.de/10.1594/PANGAEA.932375?format=textfile",
              "dcIdentifier": "https://doi.org/10.1594/PANGAEA.932375",
              "dcType": [
                "Dataset"
              ],
              "linkage": {
                "metadata": "https://doi.pangaea.de/10.1594/PANGAEA.932375",
                "data": "https://doi.pangaea.de/10.1594/PANGAEA.932375?format=textfile"
              },
              "description": [
                {
                  "title": "Parameters:",
                  "value": "DATE/TIME; HEIGHT above ground; Short-wave upward (REFLEX) radiation; Short-wave upward (REFLEX) radiation, standard deviation; Short-wave upward (REFLEX) radiation, minimum; Short-wave upward (REFLEX) radiation, maximum; Long-wave upward radiation; Long-wave upward radiation, standard deviation; Long-wave upward radiation, minimum; Long-wave upward radiation, maximum; Temperature, air; Humidity, relative"
                },
                {
                  "title": "Relation:",
                  "value": "<ul><li>Vuilleumier, Laurent (2021): BSRN Station-to-archive file for station Payerne (2020-09). ftp://ftp.bsrn.awi.de/pay/pay0920.dat.gz</li></ul>"
                }
              ],
              "multimediaObjs": [],
              "color": "#00fa9a",
              "checkbox": true
            }
          ],
          "query": null,
          "keywords": null,
          "filters": null,
          "basketId": "89957503-7c6c-4fd4-9a5a-ca8ddc64f190",
          "userId": null,
          "createdAt": "2022-03-01T00:08:51.000Z",
          "updatedAt": "2022-03-01T00:08:51.000Z"
        });

        let bi = serde_json::from_value::<BasketInternal>(json).unwrap();

        assert_eq!(1, bi.content.len());

        let mapped = bi
            .content
            .into_iter()
            .map(TypedBasketEntry::try_from)
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(1, mapped.len());

        assert!(matches!(
            mapped.get(0).unwrap(),
            TypedBasketEntry::Pangaea(_)
        ));
    }

    #[test]
    fn abcd_basket_deserialization() {
        let json = serde_json::json!({
          "content": [
            {
              "id": "urn:gfbio.org:abcd:2_303_295:M-0116911+/+157135+/+99382",
              "titleUrl": "http://www.botanischestaatssammlung.de/DatabaseClients/BSMmyxcoll/DiversityCollection_BSMmyxcoll_Details.cfm?CollectionSpecimenID=100735",
              "title": "Physarum bogoriense Racib., a preserved specimen record of the The Myxomycetes Collections at the Botanische Staatssammlung München - Collection of Hermann Neubert dataset [ID: M-0116911 / 157135 / 99382 ]",
              "upperLabels": [
                {
                  "innerInfo": "Open Access",
                  "tooltip": "This dataset is open access. You can use primary data and metadata.",
                  "colorClass": "bg-label-green"
                },
                {
                  "innerInfo": "SNSB",
                  "tooltip": "This dataset is provided by Staatliche Naturwissenschaftliche Sammlungen Bayerns; SNSB IT Center, M;nchen (SNSB).",
                  "colorClass": "bg-goldenrod"
                }
              ],
              "citation": {
                "title": "Physarum bogoriense Racib., a preserved specimen record of the The Myxomycetes Collections at the Botanische Staatssammlung München - Collection of Hermann Neubert dataset [ID: M-0116911 / 157135 / 99382 ]",
                "DOI": "http://www.botanischestaatssammlung.de/DatabaseClients/BSMmyxcoll/DiversityCollection_BSMmyxcoll_Details.cfm?CollectionSpecimenID=100735",
                "dataCenter": "SNSB",
                "source": "Triebel, Dagmar (2020). The Myxomycetes Collections at the Botanische Staatssammlung München - Collection of Hermann Neubert. [Dataset]. Version: 20201113. Data Publisher: Staatliche Naturwissenschaftliche Sammlungen Bayerns – SNSB IT Center, München. http://www.botanischestaatssammlung.de/DatabaseClients/BSMmyxcoll/About.cfm.",
                "creator": []
              },
              "licence": [
                "CC BY"
              ],
              "vat": true,
              "vatTooltip": "This dataset can be transfered to VAT.",
              "xml": "<dataset xmlns=\"urn:pangaea.de:dataportals\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"urn:pangaea.de:dataportals http://ws.pangaea.de/schemas/pansimple/pansimple.xsd\">\n  <dc:title>Physarum bogoriense Racib., a preserved specimen record of the The Myxomycetes Collections at the Botanische Staatssammlung München - Collection of Hermann Neubert dataset [ID: M-0116911 / 157135 / 99382 ]</dc:title>\n  <dc:description>http://www.botanischestaatssammlung.de/DatabaseClients/BSMmyxcoll/About.cfm</dc:description>\n  <dc:contributor>Dagmar Triebel</dc:contributor>\n  <dc:contributor>SNSB</dc:contributor>\n  <dc:contributor>Staatliche Naturwissenschaftliche Sammlungen Bayerns – SNSB IT Center, München</dc:contributor>\n  <dc:contributor>Baumann, K.</dc:contributor>\n  <dc:publisher>Data Center SNSB</dc:publisher>\n  <dataCenter>Data Center SNSB</dataCenter>\n  <dc:type>ABCD_Unit</dc:type>\n  <dc:type>preserved specimen</dc:type>\n  <dc:format>text/html</dc:format>\n  <dc:identifier>M-0116911 / 157135 / 99382</dc:identifier>\n  <dc:source>Triebel, Dagmar (2020). The Myxomycetes Collections at the Botanische Staatssammlung München - Collection of Hermann Neubert. [Dataset]. Version: 20201113. Data Publisher: Staatliche Naturwissenschaftliche Sammlungen Bayerns – SNSB IT Center, München. http://www.botanischestaatssammlung.de/DatabaseClients/BSMmyxcoll/About.cfm.</dc:source>\n  <linkage type=\"metadata\">http://www.botanischestaatssammlung.de/DatabaseClients/BSMmyxcoll/DiversityCollection_BSMmyxcoll_Details.cfm?CollectionSpecimenID=100735</linkage>\n  <dc:coverage xsi:type=\"CoverageType\">\n    <northBoundLatitude>55</northBoundLatitude>\n    <westBoundLongitude>-125</westBoundLongitude>\n    <southBoundLatitude>55</southBoundLatitude>\n    <eastBoundLongitude>-125</eastBoundLongitude>\n    <startDate>0002-11-30T00:00:00</startDate>\n    <endDate>0002-11-30T00:00:00</endDate>\n  </dc:coverage>\n  <dc:subject type=\"kingdom\" xsi:type=\"SubjectType\">Protozoa</dc:subject>\n  <dc:subject type=\"taxonomy\" xsi:type=\"SubjectType\">Physarum bogoriense Racib.</dc:subject>\n  <dc:subject type=\"parameter\" xsi:type=\"SubjectType\">Date</dc:subject>\n  <dc:subject type=\"parameter\" xsi:type=\"SubjectType\">Locality</dc:subject>\n  <dc:subject type=\"parameter\" xsi:type=\"SubjectType\">Longitude</dc:subject>\n  <dc:subject type=\"parameter\" xsi:type=\"SubjectType\">Latitude</dc:subject>\n  <dc:rights>CC BY 4.0</dc:rights>\n  <dc:relation>http://www.botanischestaatssammlung.de/DatabaseClients/BSMmyxcoll/About.cfm</dc:relation>\n  <parentIdentifier>urn:gfbio.org:abcd:2_303_295</parentIdentifier>\n  <additionalContent>http://id.snsb.info/snsb/collection/100735/157135/99382, M, BSMneubert, M-0116911 / 157135 / 99382, 2007-01-22T12:39:16, Protozoa, regnum, Physarum bogoriense Racib., PreservedSpecimen, micr. slide, 1980-07, Baumann, K., Kanada, Britisch Kolumbien, 54° Breite, 127° Länge, Canada, CAN, -125, 55, WGS84, http://www.botanischestaatssammlung.de/DatabaseClients/BSMmyxcoll/DiversityCollection_BSMmyxcoll_Details.cfm?CollectionSpecimenID=100735 </additionalContent>\n</dataset>",
              "longitude": -125,
              "latitude": 55,
              "titleTooltip": "min latitude: 55, max longitude: -125",
              "metadatalink": "http://www.botanischestaatssammlung.de/DatabaseClients/BSMmyxcoll/DiversityCollection_BSMmyxcoll_Details.cfm?CollectionSpecimenID=100735",
              "datalink": null,
              "dcIdentifier": "M-0116911 / 157135 / 99382",
              "parentIdentifier": "urn:gfbio.org:abcd:2_303_295",
              "dcType": [
                "ABCD_Unit",
                "preserved specimen"
              ],
              "linkage": {
                "metadata": "http://www.botanischestaatssammlung.de/DatabaseClients/BSMmyxcoll/DiversityCollection_BSMmyxcoll_Details.cfm?CollectionSpecimenID=100735"
              },
              "description": [
                {
                  "title": "Summary:",
                  "value": "<a class=\"text-linkwrap\" href=\"http://www.botanischestaatssammlung.de/DatabaseClients/BSMmyxcoll/About.cfm\" target=\"@target@\">http://www.botanischestaatssammlung.de/DatabaseClients/BSMmyxcoll/About.cfm</a>"
                },
                {
                  "title": "Parameters:",
                  "value": "Date; Locality; Longitude; Latitude"
                },
                {
                  "title": "Relation:",
                  "value": "<ul><li><a href = \"http://www.botanischestaatssammlung.de/DatabaseClients/BSMmyxcoll/About.cfm\" >http://www.botanischestaatssammlung.de/DatabaseClients/BSMmyxcoll/About.cfm</a></li></ul>"
                }
              ],
              "multimediaObjs": [],
              "color": "#00fa9a",
              "checkbox": true
            }
          ],
          "query": null,
          "keywords": null,
          "filters": null,
          "basketId": "0db64d44-1323-45f6-b4f0-cb488037d215",
          "userId": null,
          "createdAt": "2022-03-02T14:43:48.000Z",
          "updatedAt": "2022-03-02T14:43:48.000Z"
        });
        let bi = serde_json::from_value::<BasketInternal>(json).unwrap();

        assert_eq!(1, bi.content.len());

        let mapped = bi
            .content
            .into_iter()
            .map(TypedBasketEntry::try_from)
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(1, mapped.len());

        assert!(matches!(mapped.get(0).unwrap(), TypedBasketEntry::Abcd(_)));
    }

    #[test]
    fn test_unit_grouping() {
        let entries = vec![
            AbcdEntry {
                title: "Title, a record of the xyz Dataset dataset [ID: a]".to_string(),
                id: "1".to_string(),
                units: vec!["a".to_string()],
                visualizable: true,
            },
            AbcdEntry {
                title: "Title, a record of the xyz Dataset dataset [ID: b]".to_string(),
                id: "1".to_string(),
                units: vec!["b".to_string()],
                visualizable: true,
            },
            AbcdEntry {
                title: "Title2, a record of the xyz Dataset dataset [ID: c]".to_string(),
                id: "1".to_string(),
                units: vec!["c".to_string()],
                visualizable: true,
            },
        ];

        let grouped = Basket::group_abcd_entries(entries);

        assert_eq!(2, grouped.len());

        assert_eq!(
            vec![
                AbcdEntry {
                    title: "Title".to_string(),
                    id: "1".to_string(),
                    units: vec!["a".to_string(), "b".to_string()],
                    visualizable: true
                },
                AbcdEntry {
                    title: "Title2".to_string(),
                    id: "1".to_string(),
                    units: vec!["c".to_string()],
                    visualizable: true
                },
            ],
            grouped
        );
    }
}
