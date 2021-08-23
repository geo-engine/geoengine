use futures::FutureExt;
use log::debug;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::marker::PhantomData;
use std::path::PathBuf;

use crate::datasets::provenance::{ProvenanceOutput, ProvenanceProvider};
use crate::error::Error;
use crate::{datasets::listing::DatasetListOptions, error::Result};
use crate::{
    datasets::{
        listing::{DatasetListing, DatasetProvider},
        storage::DatasetProviderDefinition,
    },
    error,
    util::user_input::Validated,
};
use async_trait::async_trait;
use futures::future::join_all;
use futures::TryFutureExt;
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::dataset::{DatasetId, DatasetProviderId, ExternalDatasetId};
use geoengine_datatypes::primitives::{FeatureDataType, TimeGranularity, TimeStep};
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_operators::engine::{
    typed_external_dataset_id, StaticMetaData, TypedResultDescriptor,
};
use geoengine_operators::source::{
    OgrSourceColumnSpec, OgrSourceDatasetTimeType, OgrSourceDurationSpec, OgrSourceErrorSpec,
    OgrSourceTimeFormat,
};
use geoengine_operators::{
    engine::{
        MetaData, MetaDataProvider, RasterQueryRectangle, RasterResultDescriptor,
        VectorQueryRectangle, VectorResultDescriptor,
    },
    mock::MockDatasetDataSourceLoadingInfo,
    source::{GdalLoadingInfo, OgrSourceDataset},
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Bexis2DataProviderDefinition {
    id: DatasetProviderId,
    name: String,
    base_url: String,
}

#[typetag::serde]
#[async_trait]
impl DatasetProviderDefinition for Bexis2DataProviderDefinition {
    async fn initialize(self: Box<Self>) -> crate::error::Result<Box<dyn DatasetProvider>> {
        Ok(Box::new(Bexis2DataProvider {
            id: self.id,
            base_url: self.base_url,
        }))
    }

    fn type_name(&self) -> String {
        "Bexis2".to_owned()
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> DatasetProviderId {
        self.id
    }
}

pub struct Bexis2DataProvider {
    id: DatasetProviderId,
    base_url: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Georef {
    loading_info: GeorefLoadingInfo,
    result_descriptor: GeorefResultDescriptor,
}

impl Georef {
    fn create_result_descriptor(&self) -> VectorResultDescriptor {
        VectorResultDescriptor {
            data_type: self.loading_info.data_type,
            spatial_reference: self.result_descriptor.spatial_reference.into(),
            columns: self
                .loading_info
                .columns
                .iter()
                .flat_map(|(&k, v)| {
                    let data_type: FeatureDataType = k.into();
                    v.iter()
                        .map(move |column_name| (column_name.clone(), data_type))
                })
                .collect(),
        }
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct GeorefLoadingInfo {
    columns: HashMap<ColumnType, Vec<String>>,
    x: String,
    y: String,
    data_type: VectorDataType,
    layer_name: String,
    on_error: OgrSourceErrorSpec,
    time: TimeSpec,
}

#[derive(Deserialize, Debug)]
struct TimeSpec {
    start: TimeSpecStart,
}

impl TryFrom<TimeSpec> for OgrSourceDatasetTimeType {
    type Error = Error;
    fn try_from(value: TimeSpec) -> Result<Self> {
        // TODO: support more time types
        // ensure!(
        //     value.start.start_format.format == "custom" && !value.start.start_format.is_empty(),
        //     error::Bexis2TimeFormatInvalid
        // );

        Ok(OgrSourceDatasetTimeType::Start {
            start_field: value.start.start_field,
            start_format: OgrSourceTimeFormat::Auto, // TODO
            // OgrSourceTimeFormat::Custom {
            //     custom_format: value.start.start_format.custom_format.expect("checked"),
            // },
            duration: OgrSourceDurationSpec::Value(TimeStep {
                granularity: TimeGranularity::Seconds,
                step: value.start.duration,
            }),
        })
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct TimeSpecStart {
    duration: u32,
    start_field: String,
    start_format: TimeSpecFormat,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct TimeSpecFormat {
    custom_format: Option<String>,
    format: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct GeorefResultDescriptor {
    spatial_reference: SpatialReference,
}

#[derive(Clone, Copy, Deserialize, Debug, Hash, PartialEq, Eq)]
enum ColumnType {
    String,
    Decimal,
    Double,
    DateTime,
}

impl From<ColumnType> for FeatureDataType {
    fn from(value: ColumnType) -> Self {
        match value {
            ColumnType::String | ColumnType::DateTime => FeatureDataType::Text, // TODO: change DateTime when we support it
            ColumnType::Decimal => FeatureDataType::Int,
            ColumnType::Double => FeatureDataType::Float,
        }
    }
}

#[async_trait]
impl DatasetProvider for Bexis2DataProvider {
    async fn list(&self, _options: Validated<DatasetListOptions>) -> Result<Vec<DatasetListing>> {
        // TODO: pagination
        let georef_ids = self.load_georef_ids().await?;

        let georefs = georef_ids.iter().map(|georef_id| {
            self.load_georef(*georef_id).map(move |georef| {
                if georef.is_err() {
                    debug!("Could not list dataset {}", georef_id);
                }

                Ok(self.loading_info(georef?, *georef_id))
            })
        });
        let georefs: Vec<Result<DatasetListing>> = join_all(georefs).await;

        Ok(georefs.into_iter().filter_map(Result::ok).collect())
    }

    async fn load(
        &self,
        _dataset: &geoengine_datatypes::dataset::DatasetId,
    ) -> crate::error::Result<crate::datasets::storage::Dataset> {
        Err(error::Error::NotYetImplemented)
    }
}

#[async_trait]
impl ProvenanceProvider for Bexis2DataProvider {
    async fn provenance(&self, dataset: &DatasetId) -> Result<ProvenanceOutput> {
        Ok(ProvenanceOutput {
            dataset: dataset.clone(),
            provenance: None,
        })
    }
}

impl Bexis2DataProvider {
    async fn load_georef(&self, id: usize) -> Result<Georef> {
        Client::new()
            .get(format!("{}/georef/{}", self.base_url, id))
            .send()
            .await?
            .json()
            .await
            .context(error::Reqwest)
    }

    async fn load_georef_ids(&self) -> Result<Vec<usize>> {
        Client::new()
            .get(format!("{}/georef", self.base_url))
            .send()
            .await?
            .json()
            .await
            .context(error::Reqwest)
    }

    fn data_url(&self, georef_id: usize) -> String {
        format!(
            r#"CSV:/vsicurl_streaming/{}/data/{}"#,
            self.base_url, georef_id
        )
    }

    fn loading_info(&self, georef: Georef, id: usize) -> DatasetListing {
        DatasetListing {
            id: DatasetId::External(ExternalDatasetId {
                provider_id: self.id,
                dataset_id: id.to_string(),
            }),
            result_descriptor: TypedResultDescriptor::Vector(georef.create_result_descriptor()),
            name: georef.loading_info.layer_name,
            description: "".to_string(),
            tags: vec![],
            source_operator: "OgrSource".to_owned(),
            symbology: None,
        }
    }
}

#[async_trait]
impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for Bexis2DataProvider
{
    async fn meta_data(
        &self,
        dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        let georef_id: usize = typed_external_dataset_id(dataset)?;

        let georef = self
            .load_georef(georef_id)
            .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            })
            .await?;

        let result_descriptor = georef.create_result_descriptor();
        Ok(Box::new(StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: PathBuf::from(&self.data_url(georef_id)),
                layer_name: georef_id.to_string(),
                data_type: Some(georef.loading_info.data_type),
                // TODO: use time once API is fixed
                // time: georef.loading_info.time.try_into().map_err(|e| {
                //     geoengine_operators::error::Error::LoadingInfo {
                //         source: Box::new(e),
                //     }
                // })?,
                time: OgrSourceDatasetTimeType::None,
                columns: Some(OgrSourceColumnSpec {
                    x: georef.loading_info.x,
                    y: Some(georef.loading_info.y),
                    int: georef
                        .loading_info
                        .columns
                        .get(&ColumnType::Decimal)
                        .cloned()
                        .unwrap_or_default(),
                    float: georef
                        .loading_info
                        .columns
                        .get(&ColumnType::Double)
                        .cloned()
                        .unwrap_or_default(),
                    text: georef
                        .loading_info
                        .columns
                        .get(&ColumnType::String)
                        .cloned()
                        .unwrap_or_default()
                        .into_iter()
                        .chain(
                            georef
                                .loading_info
                                .columns
                                .get(&ColumnType::DateTime)
                                .cloned()
                                .unwrap_or_default()
                                .into_iter(),
                        ) // TODO: handle datetimes when we support them as column type
                        .collect(),
                    rename: None,
                }),
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: georef.loading_info.on_error,
                sql_query: None,
                attribute_query: None,
            },
            result_descriptor,
            phantom: PhantomData::default(),
        }))
    }
}

#[async_trait]
impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for Bexis2DataProvider
{
    async fn meta_data(
        &self,
        _dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotYetImplemented)
    }
}

#[async_trait]
impl
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for Bexis2DataProvider
{
    async fn meta_data(
        &self,
        _dataset: &DatasetId,
    ) -> Result<
        Box<
            dyn MetaData<
                MockDatasetDataSourceLoadingInfo,
                VectorResultDescriptor,
                VectorQueryRectangle,
            >,
        >,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotYetImplemented)
    }
}
