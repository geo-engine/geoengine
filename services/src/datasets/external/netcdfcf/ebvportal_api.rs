//! GEO BON EBV Portal catalog lookup service
//!
//! Connects to <https://portal.geobon.org/api/v1/>.

use crate::datasets::external::netcdfcf::{NetCdfOverview, error};
use crate::error::Result;
use error::NetCdfCf4DProviderError;
use geoengine_datatypes::dataset::DataProviderId;
use log::debug;
#[allow(unused_imports)]
use serde::{Deserialize, Serialize};
use url::Url;

mod portal_responses {
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    pub struct EbvClassesResponse {
        pub data: Vec<EbvClassesResponseClass>,
    }

    #[derive(Debug, Deserialize)]
    pub struct EbvClassesResponseClass {
        pub ebv_class: String,
        pub ebv_name: Vec<String>,
    }

    #[derive(Debug, Deserialize)]
    pub struct EbvDatasetsResponse {
        pub data: Vec<EbvDatasetsResponseData>,
    }

    #[derive(Debug, Deserialize)]
    pub struct EbvDatasetsResponseData {
        pub id: String,
        pub title: String,
        pub summary: String,
        pub creator: EbvDatasetsResponseCreator,
        pub license: String,
        pub dataset: EbvDatasetsResponseDataset,
        pub ebv: EbvDatasetsResponseEbv,
        pub ebv_scenario: EbvDatasetsResponseEbvScenario,
    }

    #[derive(Debug, Deserialize)]
    pub struct EbvDatasetsResponseCreator {
        pub creator_name: String,
        pub creator_institution: String,
    }

    #[derive(Debug, Deserialize)]
    pub struct EbvDatasetsResponseDataset {
        pub pathname: String,
    }

    #[derive(Debug, Deserialize)]
    pub struct EbvDatasetsResponseEbv {
        pub ebv_class: String,
        pub ebv_name: String,
    }

    #[derive(Debug, Deserialize)]
    #[serde(untagged)]
    #[allow(dead_code)]
    pub enum EbvDatasetsResponseEbvScenario {
        String(String),
        Value(EbvDatasetsResponseEbvScenarioValue),
    }

    #[derive(Debug, Deserialize)]
    #[allow(dead_code)]
    pub struct EbvDatasetsResponseEbvScenarioValue {
        pub ebv_scenario_classification_name: String,
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EbvClass {
    pub name: String,
    pub ebv_names: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct EbvClasses {
    classes: Vec<EbvClass>,
}

#[derive(Debug)]
pub struct EbvPortalApi {
    base_url: Url,
}

impl EbvPortalApi {
    pub fn new(base_url: Url) -> Self {
        Self { base_url }
    }

    pub async fn get_classes(&self) -> Result<Vec<EbvClass>> {
        let url = format!("{}/ebv-map", self.base_url);

        debug!("Calling {url}");

        let response = reqwest::get(url)
            .await?
            .json::<portal_responses::EbvClassesResponse>()
            .await?;

        Ok(response
            .data
            .into_iter()
            .map(|c| EbvClass {
                name: c.ebv_class,
                ebv_names: c.ebv_name,
            })
            .collect())
    }

    pub async fn get_ebv_datasets(&self, ebv_name: &str) -> Result<Vec<EbvDataset>> {
        let url = format!("{}/datasets/filter", self.base_url);

        debug!("Calling {url}");

        let response = reqwest::Client::new()
            .get(url)
            .query(&[("ebvName", ebv_name)])
            .send()
            .await?
            .json::<portal_responses::EbvDatasetsResponse>()
            .await?;

        Ok(response
            .data
            .into_iter()
            .map(|data| EbvDataset {
                id: data.id,
                name: data.title,
                author_name: data.creator.creator_name,
                author_institution: data.creator.creator_institution,
                description: data.summary,
                license: data.license,
                dataset_path: data.dataset.pathname,
                ebv_class: data.ebv.ebv_class,
                ebv_name: data.ebv.ebv_name,
                has_scenario: matches!(
                    data.ebv_scenario,
                    portal_responses::EbvDatasetsResponseEbvScenario::Value(_)
                ),
            })
            .collect())
    }

    pub async fn get_dataset_metadata(&self, id: &str) -> Result<EbvDataset> {
        let url = format!("{}/datasets/{id}", self.base_url);

        debug!("Calling {url}");

        let response = reqwest::get(url)
            .await?
            .json::<portal_responses::EbvDatasetsResponse>()
            .await?;

        let dataset: Option<EbvDataset> = response
            .data
            .into_iter()
            .map(|data| EbvDataset {
                id: data.id,
                name: data.title,
                author_name: data.creator.creator_name,
                author_institution: data.creator.creator_institution,
                description: data.summary,
                license: data.license,
                dataset_path: data.dataset.pathname,
                ebv_class: data.ebv.ebv_class,
                ebv_name: data.ebv.ebv_name,
                has_scenario: matches!(
                    data.ebv_scenario,
                    portal_responses::EbvDatasetsResponseEbvScenario::Value(_)
                ),
            })
            .next();

        match dataset {
            Some(dataset) => Ok(dataset),
            None => Err(NetCdfCf4DProviderError::CannotLookupDataset { id: id.to_string() }.into()),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EbvDataset {
    pub id: String,
    pub name: String,
    pub author_name: String,
    pub author_institution: String,
    pub description: String,
    pub license: String,
    pub dataset_path: String,
    pub ebv_class: String,
    pub ebv_name: String,
    pub has_scenario: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EbvHierarchy {
    pub provider_id: DataProviderId,
    pub tree: NetCdfOverview,
}

#[cfg(test)]
mod tests {
    use super::portal_responses::EbvDatasetsResponseEbvScenario;

    #[test]
    fn it_parses_ebv_scenario() {
        let value: EbvDatasetsResponseEbvScenario = serde_json::from_str(
            r#"{
                "ebv_scenario_classification_name": "foo",
                "other_field": "bar"
            }"#,
        )
        .unwrap();

        matches!(value, EbvDatasetsResponseEbvScenario::Value(_));

        let value: EbvDatasetsResponseEbvScenario = serde_json::from_str(r#""N/A""#).unwrap();

        matches!(value, EbvDatasetsResponseEbvScenario::String(_));
    }
}
