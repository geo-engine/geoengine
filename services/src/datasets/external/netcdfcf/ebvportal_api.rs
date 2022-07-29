//! GEO BON EBV Portal catalog lookup service
//!
//! Connects to <https://portal.geobon.org/api/v1/>.

use crate::datasets::external::netcdfcf::NetCdfCfDataProvider;
use crate::datasets::external::netcdfcf::{error, NetCdfOverview, NETCDF_CF_PROVIDER_ID};
use crate::error::Result;
use crate::util::config::get_config_element;
use error::NetCdfCf4DProviderError;
use geoengine_datatypes::dataset::DataProviderId;
use log::debug;
use serde::Serialize;
use snafu::ResultExt;
use std::path::PathBuf;
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
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EbvClass {
    pub name: String,
    pub ebv_names: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct EbvClasses {
    classes: Vec<EbvClass>,
}

pub async fn get_classes() -> Result<Vec<EbvClass>> {
    let base_url = get_config_element::<crate::util::config::Ebv>()?.api_base_url;
    let url = format!("{base_url}/ebv");

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
}

pub async fn get_ebv_datasets(ebv_name: &str) -> Result<Vec<EbvDataset>> {
    let base_url = get_config_element::<crate::util::config::Ebv>()?.api_base_url;
    let url = format!("{base_url}/datasets/filter");

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
        })
        .collect())
}

async fn get_dataset_metadata(base_url: &Url, id: &str) -> Result<EbvDataset> {
    let url = format!("{base_url}/datasets/{id}");

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
        })
        .next();

    match dataset {
        Some(dataset) => Ok(dataset),
        None => Err(NetCdfCf4DProviderError::CannotLookupDataset { id: id.to_string() }.into()),
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EbvHierarchy {
    pub provider_id: DataProviderId,
    pub tree: NetCdfOverview,
}

pub async fn get_ebv_subdatasets(
    provider_paths: NetCdfCfDataProviderPaths,
    dataset_id: &str,
) -> Result<EbvHierarchy> {
    let base_url = get_config_element::<crate::util::config::Ebv>()?.api_base_url;

    let dataset = get_dataset_metadata(&base_url, dataset_id).await?;

    let listing = {
        let dataset_path = PathBuf::from(dataset.dataset_path.trim_start_matches('/'));

        debug!("Accessing dataset {}", dataset_path.display());

        crate::util::spawn_blocking(move || {
            NetCdfCfDataProvider::build_netcdf_tree(
                &provider_paths.provider_path,
                Some(&provider_paths.overview_path),
                &dataset_path,
                false,
            )
        })
        .await?
        .map_err(|e| Box::new(e) as _)
        .context(error::CannotParseNetCdfFile)?
    };

    Ok(EbvHierarchy {
        provider_id: NETCDF_CF_PROVIDER_ID,
        tree: listing,
    })
}

pub struct NetCdfCfDataProviderPaths {
    pub provider_path: PathBuf,
    pub overview_path: PathBuf,
}
