use crate::contexts::Context;
use crate::error::Result;
use crate::util::config::{get_config_element, GFBio};
use actix_web::{web, FromRequest, Responder};
use chrono::{DateTime, Utc};
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;
use geoengine_datatypes::dataset::{DatasetId, DatasetProviderId, ExternalDatasetId};
use geoengine_datatypes::primitives::VectorQueryRectangle;
use geoengine_operators::engine::{
    MetaDataProvider, TypedResultDescriptor, VectorResultDescriptor,
};
use geoengine_operators::source::OgrSourceDataset;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

pub(crate) fn init_gfbio_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: Context,
    C::Session: FromRequest,
{
    cfg.service(web::resource("/gfbio/basket/{id}").route(web::get().to(get_basket_handler::<C>)));
}

async fn get_basket_handler<C: Context>(
    id: web::Path<u64>,
    session: C::Session,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    // Get basket content
    let config = get_config_element::<GFBio>()?;
    let ec = ctx.execution_context(session)?;

    //
    // let url = config.basket_api_base_url.join(&id.to_string())?;
    // let client = Client::new();
    //
    // let response = client
    //     .get(url)
    //     .send()
    //     .await?
    //     .error_for_status()?
    //     .text()
    //     .await?;
    //
    // let basket = Basket::new::<C>(
    //     serde_json::from_str::<BasketInternal>(&response)?,
    //     ec,
    //     config.pangaea_provider_id,
    //     config.gfbio_provider_id,
    // )
    // .await?;

    let bi = BasketInternal {
        basket_id: id.into_inner(),
        created: Utc::now(),
        updated: Utc::now(),
        user_id: None,
        content: BasketContentInternal {
            selected: vec![BasketEntryInternal {
                    title: "test-data".to_string(),
                    visualizable: true,
                    metadata_link: "https://doi.pangaea.de/10.1594/PANGAEA.921338".to_string(),
                },
               BasketEntryInternal {
                   title: "A very long dataset name which makes no sense at all. No really no sense. I swear!".to_string(),
                   visualizable: true,
                   metadata_link: "https://doi.pangaea.de/10.1594/PANGAEA.933024".to_string(),
               },
               BasketEntryInternal {
                   title: "A error entry.".to_string(),
                   visualizable: true,
                   metadata_link: "https://doi.pangaea.de/10.1594/PANGAEA.ERROR".to_string(),
               },
               BasketEntryInternal {
                   title: "A non-visualizable entry.".to_string(),
                   visualizable: false,
                   metadata_link: "https://doi.pangaea.de/10.1594/PANGAEA.921338".to_string(),
               },
               BasketEntryInternal {
                   title: "An ABCD entry".to_string(),
                   visualizable: false,
                   metadata_link: "https://other-stuff/10.1594/PANGAEA.921338".to_string(),
               }
            ],
        },
    };

    let basket =
        Basket::new::<C>(bi, ec, config.pangaea_provider_id, config.gfbio_provider_id).await?;
    Ok(web::Json(basket))
}

#[derive(Serialize, Debug)]
struct Basket {
    basket_id: u64,
    content: Vec<BasketEntry>,
    user_id: Option<String>,
    created: chrono::DateTime<Utc>,
    updated: chrono::DateTime<Utc>,
}

impl Basket {
    async fn new<C: Context>(
        value: BasketInternal,
        ec: <C as Context>::ExecutionContext,
        pangaea_id: DatasetProviderId,
        abcd_id: DatasetProviderId,
    ) -> Result<Basket> {
        let elems = value.content.selected.len();
        let mut entries = Vec::with_capacity(elems);
        let mut futures = value
            .content
            .selected
            .into_iter()
            .map(|x| x.to_basket_entry::<C>(&ec, pangaea_id, abcd_id))
            .collect::<FuturesUnordered<_>>();

        while let Some(r) = futures.next().await {
            entries.push(r);
        }

        Ok(Basket {
            basket_id: value.basket_id,
            content: entries,
            user_id: value.user_id,
            created: value.created,
            updated: value.updated,
        })
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
}

const PANGAEA_PATTERN: &'static str = "doi.pangaea.de/";

#[derive(Debug, Deserialize)]
struct BasketInternal {
    #[serde(rename = "basketId")]
    basket_id: u64,
    #[serde(with = "json_string")]
    content: BasketContentInternal,
    #[serde(rename = "userId")]
    user_id: Option<String>,
    #[serde(rename = "createdAt")]
    created: chrono::DateTime<Utc>,
    #[serde(rename = "updatedAt")]
    updated: chrono::DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct BasketContentInternal {
    selected: Vec<BasketEntryInternal>,
}

#[derive(Debug, Deserialize)]
struct BasketEntryInternal {
    title: String,
    #[serde(rename = "metadatalink")]
    metadata_link: String,
    #[serde(rename = "vat")]
    visualizable: bool,
}

impl BasketEntryInternal {
    async fn to_basket_entry<C: Context>(
        self,
        ec: &<C as Context>::ExecutionContext,
        pangaea_id: DatasetProviderId,
        abcd_id: DatasetProviderId,
    ) -> BasketEntry {
        let status = if !self.visualizable {
            BasketEntryStatus::Unavailable
        } else {
            match self.metadata_link.find(PANGAEA_PATTERN) {
                // Is a pangaea entry
                Some(start) => {
                    let doi = self.metadata_link[start + PANGAEA_PATTERN.len()..].to_string();
                    let id = DatasetId::External(ExternalDatasetId {
                        provider_id: pangaea_id,
                        dataset_id: doi,
                    });
                    Self::handle_pangaea_entry::<C>(id, ec).await
                }
                // Is an ABCD entry
                None => BasketEntryStatus::Unavailable,
            }
        };
        BasketEntry {
            title: self.title,
            status,
        }
    }

    async fn handle_pangaea_entry<C: Context>(
        id: DatasetId,
        ec: &<C as Context>::ExecutionContext,
    ) -> BasketEntryStatus {
        let mdp = ec as &dyn MetaDataProvider<
            OgrSourceDataset,
            VectorResultDescriptor,
            VectorQueryRectangle,
        >;

        let md = match mdp.meta_data(&id).await {
            Ok(md) => md,
            Err(e) => {
                return BasketEntryStatus::Error {
                    message: format!("Unable to load meta data: {:?}", e),
                }
            }
        };

        match md.result_descriptor().await {
            Err(e) => BasketEntryStatus::Error {
                message: format!("Unable to load meta data: {:?}", e),
            },
            Ok(rd) => BasketEntryStatus::Ok(BasketEntryLoadingDetails {
                dataset_id: id,
                source_operator: "OgrSource".to_string(),
                result_descriptor: TypedResultDescriptor::Vector(rd),
            }),
        }
    }
}

mod json_string {
    use serde::de::{self, Deserialize, DeserializeOwned, Deserializer};
    use serde::ser::{self, Serialize, Serializer};
    use serde_json;

    pub fn serialize<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: Serialize,
        S: Serializer,
    {
        let j = serde_json::to_string(value).map_err(ser::Error::custom)?;
        j.serialize(serializer)
    }

    pub fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
    where
        T: DeserializeOwned,
        D: Deserializer<'de>,
    {
        let j = String::deserialize(deserializer)?;
        serde_json::from_str(&j).map_err(de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use crate::handlers::gfbio::{BasketEntry, BasketEntryLoadingDetails, BasketEntryStatus};
    use geoengine_datatypes::collections::VectorDataType;
    use geoengine_datatypes::dataset::{DatasetId, DatasetProviderId, ExternalDatasetId};
    use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceOption};
    use geoengine_operators::engine::{TypedResultDescriptor, VectorResultDescriptor};
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
}
