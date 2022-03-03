use std::path::Path;

use crate::datasets::listing::ProvenanceOutput;
use crate::error::Error;
use crate::util::parsing::{deserialize_base_url, string_or_string_array};
use crate::util::retry::retry;
use crate::{datasets::listing::DatasetListOptions, error::Result};
use crate::{
    datasets::{
        listing::{DatasetListing, ExternalDatasetProvider},
        storage::ExternalDatasetProviderDefinition,
    },
    error,
    util::user_input::Validated,
};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use futures::future::join_all;
use gdal::DatasetOptions;
use gdal::Metadata;
use geoengine_datatypes::dataset::{DatasetId, DatasetProviderId, ExternalDatasetId};
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use geoengine_operators::engine::TypedResultDescriptor;
use geoengine_operators::source::{
    GdalMetaDataStatic, GdalMetadataFixedTimes, GdalSourceTimePlaceholder, TimeReference,
};
use geoengine_operators::util::gdal::{
    gdal_open_dataset_ex, gdal_parameters_from_dataset, raster_descriptor_from_dataset,
};
use geoengine_operators::{
    engine::{MetaData, MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor},
    mock::MockDatasetDataSourceLoadingInfo,
    source::{GdalLoadingInfo, OgrSourceDataset},
};
use log::info;
use quick_xml::events::Event;
use quick_xml::Reader;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
//use std::collections::HashMap;

use geoengine_datatypes::hashmap;
use url::Url;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Nature40DataProviderDefinition {
    id: DatasetProviderId,
    name: String,
    #[serde(deserialize_with = "deserialize_base_url")]
    base_url: Url,
    user: String,
    password: String,
    #[serde(default)]
    request_retries: RequestRetries,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestRetries {
    number_of_retries: usize,
    initial_delay_ms: u64,
    exponential_backoff_factor: f64,
}

impl Default for RequestRetries {
    // TODO: find good defaults
    fn default() -> Self {
        Self {
            number_of_retries: 3,
            initial_delay_ms: 125,
            exponential_backoff_factor: 2.,
        }
    }
}

#[typetag::serde]
#[async_trait]
impl ExternalDatasetProviderDefinition for Nature40DataProviderDefinition {
    async fn initialize(self: Box<Self>) -> crate::error::Result<Box<dyn ExternalDatasetProvider>> {
        Ok(Box::new(Nature40DataProvider {
            id: self.id,
            base_url: self.base_url,
            user: self.user,
            password: self.password,
            request_retries: self.request_retries,
        }))
    }

    fn type_name(&self) -> String {
        "Nature4.0".to_owned()
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> DatasetProviderId {
        self.id
    }
}

#[derive(Debug)]
pub struct Nature40DataProvider {
    id: DatasetProviderId,
    base_url: Url,
    user: String,
    password: String,
    request_retries: RequestRetries,
}

#[derive(Deserialize, Debug)]
struct RasterDb {
    name: String,
    title: String,
    #[serde(deserialize_with = "string_or_string_array", default)]
    tags: Vec<String>,
}

impl RasterDb {
    fn url_from_name(base_url: &Url, name: &str) -> Result<String> {
        let raster_url = base_url.join(&format!(
            "/rasterdb/{name}/wcs?VERSION=1.0.0&COVERAGE={name}",
            name = name
        ))?;
        Ok(format!("WCS:{}", raster_url))
    }

    fn url_from_name_with_time(base_url: &Url, name: &str) -> Result<String> {
        let raster_url = base_url.join(&format!(
            "/rasterdb/{name}/wcs?VERSION=1.0.0&COVERAGE={name}&TIME=%TIME%",
            name = name
        ))?;
        Ok(format!("WCS:{}", raster_url))
    }

    fn url(&self, base_url: &Url) -> Result<String> {
        Self::url_from_name(base_url, &self.name)
    }
}

#[derive(Deserialize, Debug)]
struct RasterDbs {
    rasterdbs: Vec<RasterDb>,
    // #[serde(deserialize_with = "string_or_string_array", default)]
    // tags: Vec<String>, // TODO: use
    // session: String, // TODO: incorporate into requests
}

#[async_trait]
impl ExternalDatasetProvider for Nature40DataProvider {
    async fn list(&self, _options: Validated<DatasetListOptions>) -> Result<Vec<DatasetListing>> {
        // TODO: query the other dbs as well
        let raster_dbs = self.load_raster_dbs().await?;

        let mut listing = vec![];

        let datasets = raster_dbs
            .rasterdbs
            .iter()
            .flat_map(|db| db.url(&self.base_url))
            .map(|dataset_url| self.load_dataset(dataset_url));
        let datasets: Vec<Result<gdal::Dataset>> = join_all(datasets).await;

        for (db, dataset) in raster_dbs.rasterdbs.iter().zip(datasets) {
            if let Ok(dataset) = dataset {
                let (dataset, band_labels, _time_positions) =
                    self.get_band_labels_and_time_positions(dataset).await?;

                for band_index in 1..=dataset.raster_count() {
                    if let Ok(result_descriptor) =
                        raster_descriptor_from_dataset(&dataset, band_index, None)
                    {
                        listing.push(Ok(DatasetListing {
                            id: DatasetId::External(ExternalDatasetId {
                                provider_id: self.id,
                                dataset_id: format!("{}:{}", db.name.clone(), band_index),
                            }),
                            name: db.title.clone(),
                            description: format!(
                                "Band {}: {}",
                                band_index,
                                band_labels
                                    .get((band_index - 1) as usize)
                                    .unwrap_or(&"".to_owned())
                            ),
                            tags: db.tags.clone(),
                            source_operator: "GdalSource".to_owned(),
                            result_descriptor: TypedResultDescriptor::Raster(result_descriptor),
                            symbology: None, // TODO: build symbology
                        }));
                    } else {
                        info!(
                            "Could not create restult descriptor for band {} of {}",
                            band_index, db.name
                        );
                    }
                }
            } else {
                info!("Could not open dataset {}", db.name);
            }
        }

        let mut listing: Vec<_> = listing
            .into_iter()
            .filter_map(|d: Result<DatasetListing>| if let Ok(d) = d { Some(d) } else { None })
            .collect();
        listing.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(listing)
    }

    async fn provenance(&self, dataset: &DatasetId) -> Result<ProvenanceOutput> {
        Ok(ProvenanceOutput {
            dataset: dataset.clone(),
            provenance: None,
        })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl Nature40DataProvider {
    fn auth(&self) -> [String; 2] {
        [
            format!("UserPwd={}:{}", self.user, self.password),
            "HttpAuth=BASIC".to_owned(),
        ]
    }

    async fn load_dataset(&self, db_url: String) -> Result<gdal::Dataset> {
        retry(
            self.request_retries.number_of_retries,
            self.request_retries.initial_delay_ms,
            self.request_retries.exponential_backoff_factor,
            || self.try_load_dataset(db_url.clone()),
        )
        .await
    }

    async fn try_load_dataset(&self, db_url: String) -> Result<gdal::Dataset> {
        let auth = self.auth();
        crate::util::spawn_blocking(move || {
            let dataset = gdal_open_dataset_ex(
                Path::new(&db_url),
                DatasetOptions {
                    open_options: Some(&[&auth[0], &auth[1]]),
                    ..DatasetOptions::default()
                },
            )?;
            Ok(dataset)
        })
        .await
        .context(error::TokioJoin)?
    }

    async fn load_raster_dbs(&self) -> Result<RasterDbs> {
        Client::new()
            .get(self.base_url.join("rasterdbs.json")?)
            .basic_auth(&self.user, Some(&self.password))
            .send()
            .await?
            .json()
            .await
            .context(error::Reqwest)
    }

    /// Get the band labels for a dataset from the raw WCS xml in the Gdal WCS cache.
    /// Note: as the data is possibly not written to disk yet, we close and reopen the
    /// dataset if no band labels are found during parsing. In order to close it we
    /// need to take ownership of the dataset.
    async fn get_band_labels_and_time_positions(
        &self,
        dataset: gdal::Dataset,
    ) -> Result<(gdal::Dataset, Vec<String>, Vec<String>)> {
        let (labels, time_positions) = Self::parse_band_labels_and_time_positions(&dataset)?;

        if labels.is_empty() {
            // no labels found during parsing, try to reopen the dataset to flush the cache and try again
            let name = dataset
                .metadata_item("label", "")
                .ok_or(Error::Nature40WcsDatasetMissingLabelInMetadata)?;

            drop(dataset);

            let dataset = self
                .load_dataset(RasterDb::url_from_name(&self.base_url, &name)?)
                .await?;

            let (labels, time_positions) = Self::parse_band_labels_and_time_positions(&dataset)?;

            Ok((dataset, labels, time_positions))
        } else {
            Ok((dataset, labels, time_positions))
        }
    }

    fn parse_band_labels_and_time_positions(
        dataset: &gdal::Dataset,
    ) -> Result<(Vec<String>, Vec<String>)> {
        let mut reader = Reader::from_file(&dataset.description()?)?;
        reader.trim_text(true);
        let mut labels = Vec::new();
        let mut time_positions = Vec::new();
        let mut buf = Vec::new();

        let mut first = true;

        loop {
            match reader.read_event(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    if e.name() == b"label" {
                        if first {
                            first = false; // skip first label which is the coverage label
                        } else {
                            labels.push(
                                reader
                                    .read_text(e.name(), &mut Vec::new())
                                    .unwrap_or_else(|_| "".to_owned()),
                            );
                        }
                    }
                    if e.name() == b"timePosition" {
                        time_positions.push(
                            reader
                                .read_text(e.name(), &mut Vec::new())
                                .unwrap_or_else(|_| "".to_owned()),
                        );
                    }
                }
                Ok(Event::Eof) => break,
                _ => (),
            }
            buf.clear();
        }
        Ok((labels, time_positions))
    }
}

#[async_trait]
impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for Nature40DataProvider
{
    async fn meta_data(
        &self,
        dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        let dataset = dataset
            .external()
            .ok_or(geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(Error::InvalidExternalDatasetId { provider: self.id }),
            })?;
        let split: Vec<_> = dataset.dataset_id.split(':').collect();

        let (db_name, band_index) = match *split.as_slice() {
            [db, band_index] => {
                if let Ok(band_index) = band_index.parse::<usize>() {
                    (db, band_index)
                } else {
                    return Err(geoengine_operators::error::Error::LoadingInfo {
                        source: Box::new(Error::InvalidExternalDatasetId { provider: self.id }),
                    });
                }
            }
            _ => {
                return Err(geoengine_operators::error::Error::LoadingInfo {
                    source: Box::new(Error::InvalidExternalDatasetId { provider: self.id }),
                })
            }
        };

        let db_url = RasterDb::url_from_name(&self.base_url, db_name).map_err(|e| {
            geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            }
        })?;
        let dataset = self.load_dataset(db_url.clone()).await.map_err(|e| {
            geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            }
        })?;

        let (dataset, _band_labels, time_positions) = self
            .get_band_labels_and_time_positions(dataset)
            .await
            .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            })?;

        if time_positions.is_empty() {
            Ok(Box::new(GdalMetaDataStatic {
                time: None,
                params: gdal_parameters_from_dataset(
                    &dataset,
                    band_index,
                    Path::new(&db_url),
                    None,
                    Some(self.auth().to_vec()),
                )?,
                result_descriptor: raster_descriptor_from_dataset(
                    &dataset,
                    band_index as isize,
                    None,
                )?,
            }))
        } else {
            let time_steps = time_positions
                .iter()
                .map(|time_position| {
                    NaiveDateTime::parse_from_str(time_position, "%Y-%m-%dT%H:%M").unwrap()
                })
                .map(TimeInstance::from)
                .map(|time_istance| TimeInterval::new_instant(time_istance).unwrap())
                .collect::<Vec<_>>();

            let db_url =
                RasterDb::url_from_name_with_time(&self.base_url, db_name).map_err(|e| {
                    geoengine_operators::error::Error::LoadingInfo {
                        source: Box::new(e),
                    }
                })?;

            Ok(Box::new(GdalMetadataFixedTimes {
                time_steps,
                params: gdal_parameters_from_dataset(
                    &dataset,
                    band_index,
                    Path::new(&db_url),
                    None,
                    Some(self.auth().to_vec()),
                )?,
                result_descriptor: raster_descriptor_from_dataset(
                    &dataset,
                    band_index as isize,
                    None,
                )?,
                time_placeholders: hashmap! {
                    "%TIME%".to_string() => GdalSourceTimePlaceholder {
                        format: "%Y-%m-%dT%H:%M".to_string(),
                        reference: TimeReference::Start,
                    },
                },
            }))
        }
    }
}

#[async_trait]
impl
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for Nature40DataProvider
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

#[async_trait]
impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for Nature40DataProvider
{
    async fn meta_data(
        &self,
        _dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotYetImplemented)
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Read, path::PathBuf, str::FromStr};

    use geoengine_datatypes::{
        primitives::{
            Measurement, QueryRectangle, SpatialPartition2D, SpatialResolution, TimeInterval,
        },
        raster::RasterDataType,
        spatial_reference::{SpatialReference, SpatialReferenceAuthority},
    };
    use geoengine_operators::source::{
        FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters,
        GdalLoadingInfoTemporalSlice, GdalLoadingInfoTemporalSliceIterator,
    };
    use httptest::{
        all_of,
        matchers::{contains, lowercase, request, url_decoded},
        responders::{json_encoded, status_code},
        Expectation, Server,
    };
    use serde_json::json;

    use crate::{datasets::listing::OrderBy, test_data, util::user_input::UserInput};

    use super::*;

    #[allow(clippy::too_many_lines)]
    fn expect_geonode_requests(server: &mut Server) {
        server.expect(
            Expectation::matching(all_of![
                request::headers(contains((
                    lowercase("authorization"),
                    "Basic Z2VvZW5naW5lOnB3ZA=="
                ))),
                request::method_path("GET", "/rasterdb/geonode_ortho_muf_1m/wcs",),
                request::query(url_decoded(contains(("REQUEST", "GetCapabilities"))))
            ])
            .respond_with(status_code(200).body(
                r#"
                <WCS_Capabilities version="1.0.0">
                    <Service>
                        <name>RSDB WCS</name>
                        <label>geonode_ortho_muf_1m</label>
                    </Service>
                    <ContentMetadata>
                        <CoverageOfferingBrief>
                            <name>geonode_ortho_muf_1m</name>
                            <label>geonode_ortho_muf_1m</label>
                        </CoverageOfferingBrief>
                    </ContentMetadata>
                </WCS_Capabilities>"#,
            )),
        );

        server.expect(
            Expectation::matching(all_of![
                request::headers(contains((
                    lowercase("authorization"),
                    "Basic Z2VvZW5naW5lOnB3ZA=="
                ))),
                request::method_path("GET", "/rasterdb/geonode_ortho_muf_1m/wcs",),
                request::query(url_decoded(contains(("REQUEST", "DescribeCoverage"))))
            ])
            .respond_with(status_code(200).body(
                r#"
                <CoverageDescription version="1.0.0">
                    <CoverageOffering>
                        <name>RSDB WCS</name>
                        <label>geonode_ortho_muf_1m</label>
                        <domainSet>
                            <spatialDomain>
                                <gml:Envelope srsName="EPSG:3044">
                                    <gml:pos>475927.0 5630630.0</gml:pos>
                                    <gml:pos>478886.0 5633083.0</gml:pos>
                                </gml:Envelope>
                                <gml:RectifiedGrid dimension="2">
                                    <gml:limits>
                                        <gml:GridEnvelope>
                                            <gml:low>0 0</gml:low>
                                            <gml:high>2958 2452</gml:high>
                                        </gml:GridEnvelope>
                                    </gml:limits>
                                    <gml:axisName>x</gml:axisName>
                                    <gml:axisName>y</gml:axisName>
                                    <gml:origin>
                                        <gml:pos>475927.0 5633083.0</gml:pos>
                                    </gml:origin>
                                    <gml:offsetVector>1.0 0.0</gml:offsetVector>
                                    <gml:offsetVector>0.0 -1.0</gml:offsetVector>
                                </gml:RectifiedGrid>
                            </spatialDomain>
                        </domainSet>
                        <rangeSet>
                            <RangeSet>
                                <name>1</name>
                                <label>band1</label>
                                <name>2</name>
                                <label>band2</label>
                                <name>3</name>
                                <label>band3</label>
                            </RangeSet>
                        </rangeSet>
                        <supportedCRSs>
                            <requestResponseCRSs>EPSG:3044</requestResponseCRSs>
                            <nativeCRSs>EPSG:3044</nativeCRSs>
                        </supportedCRSs>
                        <supportedFormats>
                            <formats>GeoTIFF</formats>
                        </supportedFormats>
                    </CoverageOffering>
                </CoverageDescription>"#,
            )),
        );

        let mut geonode_ortho_muf_1m_bytes = vec![];
        File::open(test_data!("nature40/geonode_ortho_muf_1m.tiff"))
            .unwrap()
            .read_to_end(&mut geonode_ortho_muf_1m_bytes)
            .unwrap();

        server.expect(
            Expectation::matching(all_of![
                request::headers(contains((
                    lowercase("authorization"),
                    "Basic Z2VvZW5naW5lOnB3ZA=="
                ))),
                request::method_path("GET", "/rasterdb/geonode_ortho_muf_1m/wcs",),
                request::query(url_decoded(contains(("REQUEST", "GetCoverage"))))
            ])
            .respond_with(
                status_code(200)
                    .append_header("Content-Type", "image/tiff")
                    .body(geonode_ortho_muf_1m_bytes),
            ),
        );
    }

    fn expect_lidar_requests(server: &mut Server) {
        server.expect(
            Expectation::matching(all_of![
                request::headers(contains((
                    lowercase("authorization"),
                    "Basic Z2VvZW5naW5lOnB3ZA=="
                ))),
                request::method_path("GET", "/rasterdb/lidar_2018_wetness_1m/wcs",),
                request::query(url_decoded(contains(("REQUEST", "GetCapabilities"))))
            ])
            .respond_with(status_code(200).body(
                r#"
                <WCS_Capabilities version="1.0.0">
                    <Service>
                        <name>RSDB WCS</name>
                        <label>lidar_2018_wetness_1m</label>
                    </Service>
                    <ContentMetadata>
                        <CoverageOfferingBrief>
                            <name>lidar_2018_wetness_1m</name>
                            <label>lidar_2018_wetness_1m</label>
                        </CoverageOfferingBrief>
                    </ContentMetadata>
                </WCS_Capabilities>"#,
            )),
        );

        server.expect(
            Expectation::matching(all_of![
                request::headers(contains((
                    lowercase("authorization"),
                    "Basic Z2VvZW5naW5lOnB3ZA=="
                ))),
                request::method_path("GET", "/rasterdb/lidar_2018_wetness_1m/wcs",),
                request::query(url_decoded(contains(("REQUEST", "DescribeCoverage"))))
            ])
            .respond_with(status_code(200).body(
                r#"
                <CoverageDescription version="1.0.0">
                    <CoverageOffering>
                        <name>RSDB WCS</name>
                        <label>lidar_2018_wetness_1m</label>
                        <domainSet>
                        <spatialDomain>
                            <gml:Envelope srsName="EPSG:25832">
                                <gml:pos>473923.0 5630763.0</gml:pos>
                                <gml:pos>478218.0 5634057.0</gml:pos>
                            </gml:Envelope>
                            <gml:RectifiedGrid dimension="2">
                                <gml:limits>
                                    <gml:GridEnvelope>
                                        <gml:low>0 0</gml:low>
                                        <gml:high>4294 3293</gml:high>
                                    </gml:GridEnvelope>
                                </gml:limits>
                                <gml:axisName>x</gml:axisName>
                                <gml:axisName>y</gml:axisName>
                                <gml:origin>
                                    <gml:pos>473923.0 5634057.0</gml:pos>
                                </gml:origin>
                                <gml:offsetVector>1.0 0.0</gml:offsetVector>
                                <gml:offsetVector>0.0 -1.0</gml:offsetVector>
                            </gml:RectifiedGrid>
                        </spatialDomain>
                        </domainSet>
                        <rangeSet>
                            <RangeSet>
                                <name>1</name>
                                <label>wetness</label>
                            </RangeSet>
                        </rangeSet>
                        <supportedCRSs>
                            <requestResponseCRSs>EPSG:25832</requestResponseCRSs>
                            <nativeCRSs>EPSG:25832</nativeCRSs>
                        </supportedCRSs>
                        <supportedFormats>
                            <formats>GeoTIFF</formats>
                        </supportedFormats>
                    </CoverageOffering>
                </CoverageDescription>"#,
            )),
        );

        let mut lidar_2018_wetness_1m = vec![];
        File::open(test_data!("nature40/lidar_2018_wetness_1m.tiff"))
            .unwrap()
            .read_to_end(&mut lidar_2018_wetness_1m)
            .unwrap();

        server.expect(
            Expectation::matching(all_of![
                request::headers(contains((
                    lowercase("authorization"),
                    "Basic Z2VvZW5naW5lOnB3ZA=="
                ))),
                request::method_path("GET", "/rasterdb/lidar_2018_wetness_1m/wcs",),
                request::query(url_decoded(contains(("REQUEST", "GetCoverage"))))
            ])
            .respond_with(
                status_code(200)
                    .append_header("Content-Type", "image/tiff")
                    .body(lidar_2018_wetness_1m),
            ),
        );
    }

    #[allow(clippy::too_many_lines)]
    fn expect_uas_requests(server: &mut Server) {
        server.expect(
            Expectation::matching(all_of![
                request::headers(contains((
                    lowercase("authorization"),
                    "Basic Z2VvZW5naW5lOnB3ZA=="
                ))),
                request::method_path("GET", "/rasterdb/uas_orthomosaics_2020/wcs",),
                request::query(url_decoded(contains(("REQUEST", "GetCapabilities"))))
            ])
            .respond_with(status_code(200).body(
                r#"
                <WCS_Capabilities version="1.0.0">
                    <Service>
                        <name>RSDB WCS</name>
                        <label>uas_orthomosaics_2020</label>
                    </Service>
                    <ContentMetadata>
                        <CoverageOfferingBrief>
                            <name>uas_orthomosaics_2020</name>
                            <label>uas_orthomosaics_2020</label>
                        </CoverageOfferingBrief>
                    </ContentMetadata>
                </WCS_Capabilities>"#,
            )),
        );

        server.expect(
            Expectation::matching(all_of![
                request::headers(contains((
                    lowercase("authorization"),
                    "Basic Z2VvZW5naW5lOnB3ZA=="
                ))),
                request::method_path("GET", "/rasterdb/uas_orthomosaics_2020/wcs",),
                request::query(url_decoded(contains(("REQUEST", "DescribeCoverage"))))
            ])
            .respond_with(status_code(200).body(
                r#"
                <CoverageDescription version="1.0.0">
                    <CoverageOffering>
                        <name>RSDB WCS</name>
                        <label>uas_orthomosaics_2020</label>
                        <domainSet>
                            <spatialDomain>
                                <gml:Envelope srsName="EPSG:25832">
                                    <gml:pos>477626.5 5632063.5</gml:pos>
                                    <gml:pos>478182.0 5632531.0</gml:pos>
                                </gml:Envelope>
                                <gml:RectifiedGrid dimension="2">
                                    <gml:limits>
                                        <gml:GridEnvelope>
                                            <gml:low>0 0</gml:low>
                                            <gml:high>7937 6679</gml:high>
                                        </gml:GridEnvelope>
                                    </gml:limits>
                                    <gml:axisName>x</gml:axisName>
                                    <gml:axisName>y</gml:axisName>
                                    <gml:origin>
                                        <gml:pos>477626.5 5632531.0</gml:pos>
                                    </gml:origin>
                                    <gml:offsetVector>0.07 0.0</gml:offsetVector>
                                    <gml:offsetVector>0.0 -0.07</gml:offsetVector>
                                </gml:RectifiedGrid>
                            </spatialDomain>
                            <temporalDomain>
                                <gml:timePosition>2020-09-02T00:00</gml:timePosition>
                                <gml:timePosition>2020-09-15T00:00</gml:timePosition>                    
                            </temporalDomain>
                        </domainSet>
                        <rangeSet>
                            <RangeSet>
                                <name>1</name>
                                <label>red</label>
                                <name>2</name>
                                <label>green</label>
                                <name>3</name>
                                <label>blue</label>
                            </RangeSet>
                        </rangeSet>
                        <supportedCRSs>
                            <requestResponseCRSs>EPSG:25832</requestResponseCRSs>
                            <nativeCRSs>EPSG:25832</nativeCRSs>
                        </supportedCRSs>
                        <supportedFormats>
                            <formats>GeoTIFF</formats>
                        </supportedFormats>
                    </CoverageOffering>
                </CoverageDescription>"#,
            )),
        );

        let mut uas_orthomosaics_2020 = vec![];
        File::open(test_data!("nature40/uas_orthomosaics_2020.tiff"))
            .unwrap()
            .read_to_end(&mut uas_orthomosaics_2020)
            .unwrap();

        server.expect(
            Expectation::matching(all_of![
                request::headers(contains((
                    lowercase("authorization"),
                    "Basic Z2VvZW5naW5lOnB3ZA=="
                ))),
                request::method_path("GET", "/rasterdb/uas_orthomosaics_2020/wcs",),
                request::query(url_decoded(contains(("REQUEST", "GetCoverage"))))
            ])
            .respond_with(
                status_code(200)
                    .append_header("Content-Type", "image/tiff")
                    .body(uas_orthomosaics_2020),
            ),
        );
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn it_lists() {
        let mut server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::headers(contains((
                    lowercase("authorization"),
                    "Basic Z2VvZW5naW5lOnB3ZA=="
                ))),
                request::method_path("GET", "/rasterdbs.json")
            ])
            .respond_with(json_encoded(json!({
                "rasterdbs": [{
                    "name": "geonode_ortho_muf_1m",
                    "title": "MOF Luftbild",
                    "tags": "natur40"
                },
                {
                    "name": "lidar_2018_wetness_1m",
                    "title": "Topografic Wetness index",
                    "tags": "natur40"
                },
                {
                    "name": "uas_orthomosaics_2020",
                    "title": "UAS Orthomosaics Fall 2020",
                    "tags": ["UAV","natur40"]
                }],
                "tags": ["UAV", "natur40"],
                "session": "lhtdVm"
            }))),
        );

        expect_geonode_requests(&mut server);
        expect_lidar_requests(&mut server);
        expect_uas_requests(&mut server);

        let provider = Box::new(Nature40DataProviderDefinition {
            id: DatasetProviderId::from_str("2cb964d5-b9fa-4f8f-ab6f-f6c7fb47d4cd").unwrap(),
            name: "Nature40".to_owned(),
            base_url: Url::parse(&server.url_str("")).unwrap(),
            user: "geoengine".to_owned(),
            password: "pwd".to_owned(),
            request_retries: Default::default(),
        })
        .initialize()
        .await
        .unwrap();

        let listing = provider
            .list(
                DatasetListOptions {
                    filter: None,
                    order: OrderBy::NameAsc,
                    offset: 0,
                    limit: 10,
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            listing,
            vec![
                DatasetListing {
                    id: DatasetId::External(ExternalDatasetId {
                        provider_id: DatasetProviderId::from_str(
                            "2cb964d5-b9fa-4f8f-ab6f-f6c7fb47d4cd"
                        )
                        .unwrap(),
                        dataset_id: "geonode_ortho_muf_1m:1".to_owned()
                    }),
                    name: "MOF Luftbild".to_owned(),
                    description: "Band 1: band1".to_owned(),
                    tags: vec!["natur40".to_owned()],
                    source_operator: "GdalSource".to_owned(),
                    result_descriptor: TypedResultDescriptor::Raster(RasterResultDescriptor {
                        data_type: RasterDataType::F32,
                        spatial_reference: SpatialReference::new(
                            SpatialReferenceAuthority::Epsg,
                            3044
                        )
                        .into(),
                        measurement: Measurement::Unitless,
                        no_data_value: None
                    }),
                    symbology: None
                },
                DatasetListing {
                    id: DatasetId::External(ExternalDatasetId {
                        provider_id: DatasetProviderId::from_str(
                            "2cb964d5-b9fa-4f8f-ab6f-f6c7fb47d4cd"
                        )
                        .unwrap(),
                        dataset_id: "geonode_ortho_muf_1m:2".to_owned()
                    }),
                    name: "MOF Luftbild".to_owned(),
                    description: "Band 2: band2".to_owned(),
                    tags: vec!["natur40".to_owned()],
                    source_operator: "GdalSource".to_owned(),
                    result_descriptor: TypedResultDescriptor::Raster(RasterResultDescriptor {
                        data_type: RasterDataType::F32,
                        spatial_reference: SpatialReference::new(
                            SpatialReferenceAuthority::Epsg,
                            3044
                        )
                        .into(),
                        measurement: Measurement::Unitless,
                        no_data_value: None
                    }),
                    symbology: None
                },
                DatasetListing {
                    id: DatasetId::External(ExternalDatasetId {
                        provider_id: DatasetProviderId::from_str(
                            "2cb964d5-b9fa-4f8f-ab6f-f6c7fb47d4cd"
                        )
                        .unwrap(),
                        dataset_id: "geonode_ortho_muf_1m:3".to_owned()
                    }),
                    name: "MOF Luftbild".to_owned(),
                    description: "Band 3: band3".to_owned(),
                    tags: vec!["natur40".to_owned()],
                    source_operator: "GdalSource".to_owned(),
                    result_descriptor: TypedResultDescriptor::Raster(RasterResultDescriptor {
                        data_type: RasterDataType::F32,
                        spatial_reference: SpatialReference::new(
                            SpatialReferenceAuthority::Epsg,
                            3044
                        )
                        .into(),
                        measurement: Measurement::Unitless,
                        no_data_value: None
                    }),
                    symbology: None
                },
                DatasetListing {
                    id: DatasetId::External(ExternalDatasetId {
                        provider_id: DatasetProviderId::from_str(
                            "2cb964d5-b9fa-4f8f-ab6f-f6c7fb47d4cd"
                        )
                        .unwrap(),
                        dataset_id: "lidar_2018_wetness_1m:1".to_owned()
                    }),
                    name: "Topografic Wetness index".to_owned(),
                    description: "Band 1: wetness".to_owned(),
                    tags: vec!["natur40".to_owned()],
                    source_operator: "GdalSource".to_owned(),
                    result_descriptor: TypedResultDescriptor::Raster(RasterResultDescriptor {
                        data_type: RasterDataType::F32,
                        spatial_reference: SpatialReference::new(
                            SpatialReferenceAuthority::Epsg,
                            25832
                        )
                        .into(),
                        measurement: Measurement::Unitless,
                        no_data_value: None
                    }),
                    symbology: None
                },
                DatasetListing {
                    id: DatasetId::External(ExternalDatasetId {
                        provider_id: DatasetProviderId::from_str(
                            "2cb964d5-b9fa-4f8f-ab6f-f6c7fb47d4cd"
                        )
                        .unwrap(),
                        dataset_id: "uas_orthomosaics_2020:1".to_owned()
                    }),
                    name: "UAS Orthomosaics Fall 2020".to_owned(),
                    description: "Band 1: red".to_owned(),
                    tags: vec!["UAV".to_owned(), "natur40".to_owned()],
                    source_operator: "GdalSource".to_owned(),
                    result_descriptor: TypedResultDescriptor::Raster(RasterResultDescriptor {
                        data_type: RasterDataType::F32,
                        spatial_reference: SpatialReference::new(
                            SpatialReferenceAuthority::Epsg,
                            25832
                        )
                        .into(),
                        measurement: Measurement::Unitless,
                        no_data_value: None
                    }),
                    symbology: None
                },
                DatasetListing {
                    id: DatasetId::External(ExternalDatasetId {
                        provider_id: DatasetProviderId::from_str(
                            "2cb964d5-b9fa-4f8f-ab6f-f6c7fb47d4cd"
                        )
                        .unwrap(),
                        dataset_id: "uas_orthomosaics_2020:2".to_owned()
                    }),
                    name: "UAS Orthomosaics Fall 2020".to_owned(),
                    description: "Band 2: green".to_owned(),
                    tags: vec!["UAV".to_owned(), "natur40".to_owned()],
                    source_operator: "GdalSource".to_owned(),
                    result_descriptor: TypedResultDescriptor::Raster(RasterResultDescriptor {
                        data_type: RasterDataType::F32,
                        spatial_reference: SpatialReference::new(
                            SpatialReferenceAuthority::Epsg,
                            25832
                        )
                        .into(),
                        measurement: Measurement::Unitless,
                        no_data_value: None
                    }),
                    symbology: None
                },
                DatasetListing {
                    id: DatasetId::External(ExternalDatasetId {
                        provider_id: DatasetProviderId::from_str(
                            "2cb964d5-b9fa-4f8f-ab6f-f6c7fb47d4cd"
                        )
                        .unwrap(),
                        dataset_id: "uas_orthomosaics_2020:3".to_owned()
                    }),
                    name: "UAS Orthomosaics Fall 2020".to_owned(),
                    description: "Band 3: blue".to_owned(),
                    tags: vec!["UAV".to_owned(), "natur40".to_owned()],
                    source_operator: "GdalSource".to_owned(),
                    result_descriptor: TypedResultDescriptor::Raster(RasterResultDescriptor {
                        data_type: RasterDataType::F32,
                        spatial_reference: SpatialReference::new(
                            SpatialReferenceAuthority::Epsg,
                            25832
                        )
                        .into(),
                        measurement: Measurement::Unitless,
                        no_data_value: None
                    }),
                    symbology: None
                }
            ]
        );
    }

    #[allow(clippy::eq_op)]
    #[tokio::test]
    async fn it_loads() {
        let mut server = Server::run();

        expect_lidar_requests(&mut server);

        let provider = Box::new(Nature40DataProviderDefinition {
            id: DatasetProviderId::from_str("2cb964d5-b9fa-4f8f-ab6f-f6c7fb47d4cd").unwrap(),
            name: "Nature40".to_owned(),
            base_url: Url::parse(&server.url_str("")).unwrap(),
            user: "geoengine".to_owned(),
            password: "pwd".to_owned(),
            request_retries: Default::default(),
        })
        .initialize()
        .await
        .unwrap();

        let meta: Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>> =
            provider
                .meta_data(&DatasetId::External(ExternalDatasetId {
                    provider_id: DatasetProviderId::from_str(
                        "2cb964d5-b9fa-4f8f-ab6f-f6c7fb47d4cd",
                    )
                    .unwrap(),
                    dataset_id: "lidar_2018_wetness_1m:1".to_owned(),
                }))
                .await
                .unwrap();

        assert_eq!(
            meta.result_descriptor().await.unwrap(),
            RasterResultDescriptor {
                data_type: RasterDataType::F32,
                spatial_reference: SpatialReference::new(SpatialReferenceAuthority::Epsg, 25832)
                    .into(),
                measurement: Measurement::Unitless,
                no_data_value: None
            }
        );

        let loading_info = meta
            .loading_info(QueryRectangle {
                spatial_bounds: SpatialPartition2D::new_unchecked(
                    (473_922.500, 5_634_057.500).into(),
                    (473_924.500, 5_634_055.50).into(),
                ),
                time_interval: TimeInterval::default(),
                spatial_resolution: SpatialResolution::new_unchecked(
                    (473_924.500 - 473_922.500) / 2.,
                    (5_634_057.500 - 5_634_055.50) / 2.,
                ),
            })
            .await
            .unwrap();

        if let GdalLoadingInfoTemporalSliceIterator::Static { mut parts } = loading_info.info {
            let params: GdalLoadingInfoTemporalSlice = parts.next().unwrap();

            assert_eq!(
                params,
                GdalLoadingInfoTemporalSlice {
                    time: TimeInterval::default(),
                    params: Some(GdalDatasetParameters {
                        file_path: PathBuf::from(format!("WCS:{}rasterdb/lidar_2018_wetness_1m/wcs?VERSION=1.0.0&COVERAGE=lidar_2018_wetness_1m", server.url_str(""))),
                        rasterband_channel: 1,
                        geo_transform: GdalDatasetGeoTransform {
                            origin_coordinate: (473_922.5, 5_634_057.5).into(),
                            x_pixel_size: 1.0,
                            y_pixel_size: -1.0,
                        },
                        width: 4295,
                        height: 3294,
                        file_not_found_handling: FileNotFoundHandling::Error,
                        no_data_value: None,
                        properties_mapping: None,
                        gdal_open_options: Some(vec!["UserPwd=geoengine:pwd".to_owned(), "HttpAuth=BASIC".to_owned()]),
                        gdal_config_options: None,
                    })
                }
            );

            assert_eq!(parts.next(), None);
        } else {
            panic!();
        }
    }

    #[allow(clippy::too_many_lines)]
    #[allow(clippy::eq_op)]
    #[tokio::test]
    async fn it_loads_time_layer() {
        let mut server = Server::run();

        expect_uas_requests(&mut server);

        let provider = Box::new(Nature40DataProviderDefinition {
            id: DatasetProviderId::from_str("2cb964d5-b9fa-4f8f-ab6f-f6c7fb47d4cd").unwrap(),
            name: "Nature40".to_owned(),
            base_url: Url::parse(&server.url_str("")).unwrap(),
            user: "geoengine".to_owned(),
            password: "pwd".to_owned(),
            request_retries: Default::default(),
        })
        .initialize()
        .await
        .unwrap();

        let meta: Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>> =
            provider
                .meta_data(&DatasetId::External(ExternalDatasetId {
                    provider_id: DatasetProviderId::from_str(
                        "2cb964d5-b9fa-4f8f-ab6f-f6c7fb47d4cd",
                    )
                    .unwrap(),
                    dataset_id: "uas_orthomosaics_2020:1".to_owned(),
                }))
                .await
                .unwrap();

        assert_eq!(
            meta.result_descriptor().await.unwrap(),
            RasterResultDescriptor {
                data_type: RasterDataType::F32,
                spatial_reference: SpatialReference::new(SpatialReferenceAuthority::Epsg, 25832)
                    .into(),
                measurement: Measurement::Unitless,
                no_data_value: None
            }
        );

        let loading_info = meta
            .loading_info(QueryRectangle {
                spatial_bounds: SpatialPartition2D::new_unchecked(
                    (473_922.500, 5_634_057.500).into(),
                    (473_924.500, 5_634_055.50).into(),
                ),
                time_interval: TimeInterval::new_unchecked(
                    TimeInstance::from(
                        NaiveDateTime::parse_from_str("2020-09-01T00:00", "%Y-%m-%dT%H:%M")
                            .unwrap(),
                    ),
                    TimeInstance::from(
                        NaiveDateTime::parse_from_str("2020-09-10T00:00", "%Y-%m-%dT%H:%M")
                            .unwrap(),
                    ),
                ),

                spatial_resolution: SpatialResolution::new_unchecked(
                    (473_924.500 - 473_922.500) / 2.,
                    (5_634_057.500 - 5_634_055.50) / 2.,
                ),
            })
            .await
            .unwrap();

        if let GdalLoadingInfoPartIterator::Static { mut parts } = loading_info.info {
            let mut params: GdalLoadingInfoPart = parts.next().unwrap();

            assert_eq!(
                params,
                GdalLoadingInfoPart {
                    time: TimeInterval::new_unchecked(
                        TimeInstance::from(
                            NaiveDateTime::parse_from_str("2020-09-01T00:00", "%Y-%m-%dT%H:%M")
                                .unwrap(),
                        ),
                        TimeInstance::from(
                            NaiveDateTime::parse_from_str("2020-09-02T00:00", "%Y-%m-%dT%H:%M")
                                .unwrap(),
                        ),
                    ),
                    params: GdalDatasetParameters {
                        file_path: PathBuf::from(format!("WCS:{}rasterdb/uas_orthomosaics_2020/wcs?VERSION=1.0.0&COVERAGE=uas_orthomosaics_2020&TIME=2020-09-01T00:00", server.url_str(""))),
                        rasterband_channel: 1,
                        geo_transform: GdalDatasetGeoTransform {
                            origin_coordinate: (477_626.465, 5_632_531.035).into(),
                            x_pixel_size: 0.07,
                            y_pixel_size: -0.07,
                        },
                        width: 7938,
                        height: 6680,
                        file_not_found_handling: FileNotFoundHandling::Error,
                        no_data_value: None,
                        properties_mapping: None,
                        gdal_open_options: Some(vec!["UserPwd=geoengine:pwd".to_owned(), "HttpAuth=BASIC".to_owned()]),
                        gdal_config_options: None,
                    }
                }
            );

            params = parts.next().unwrap();

            assert_eq!(
                params,
                GdalLoadingInfoPart {
                    time: TimeInterval::new_unchecked(
                        TimeInstance::from(
                            NaiveDateTime::parse_from_str("2020-09-02T00:00", "%Y-%m-%dT%H:%M")
                                .unwrap(),
                        ),
                        TimeInstance::from(
                            NaiveDateTime::parse_from_str("2020-09-02T00:00", "%Y-%m-%dT%H:%M")
                                .unwrap(),
                        ),
                    ),
                    params: GdalDatasetParameters {
                        file_path: PathBuf::from(format!("WCS:{}rasterdb/uas_orthomosaics_2020/wcs?VERSION=1.0.0&COVERAGE=uas_orthomosaics_2020&TIME=2020-09-02T00:00", server.url_str(""))),
                        rasterband_channel: 1,
                        geo_transform: GdalDatasetGeoTransform {
                            origin_coordinate: (477_626.465, 5_632_531.035).into(),
                            x_pixel_size: 0.07,
                            y_pixel_size: -0.07,
                        },
                        width: 7938,
                        height: 6680,
                        file_not_found_handling: FileNotFoundHandling::Error,
                        no_data_value: None,
                        properties_mapping: None,
                        gdal_open_options: Some(vec!["UserPwd=geoengine:pwd".to_owned(), "HttpAuth=BASIC".to_owned()]),
                        gdal_config_options: None,
                    }
                }
            );

            params = parts.next().unwrap();

            assert_eq!(
                params,
                GdalLoadingInfoPart {
                    time: TimeInterval::new_unchecked(
                        (TimeInstance::from(
                            NaiveDateTime::parse_from_str("2020-09-02T00:00", "%Y-%m-%dT%H:%M")
                                .unwrap(),
                        ) + TimeStep {
                            granularity: TimeGranularity::Millis,
                            step: 1,
                        }).unwrap(),
                        TimeInstance::from(
                            NaiveDateTime::parse_from_str("2020-09-10T00:00", "%Y-%m-%dT%H:%M")
                                .unwrap(),
                        ),
                    ),
                    params: GdalDatasetParameters {
                        file_path: PathBuf::from(format!("WCS:{}rasterdb/uas_orthomosaics_2020/wcs?VERSION=1.0.0&COVERAGE=uas_orthomosaics_2020&TIME=2020-09-02T00:00", server.url_str(""))),
                        rasterband_channel: 1,
                        geo_transform: GdalDatasetGeoTransform {
                            origin_coordinate: (477_626.465, 5_632_531.035).into(),
                            x_pixel_size: 0.07,
                            y_pixel_size: -0.07,
                        },
                        width: 7938,
                        height: 6680,
                        file_not_found_handling: FileNotFoundHandling::Error,
                        no_data_value: None,
                        properties_mapping: None,
                        gdal_open_options: Some(vec!["UserPwd=geoengine:pwd".to_owned(), "HttpAuth=BASIC".to_owned()]),
                        gdal_config_options: None,
                    }
                }
            );
        } else {
            panic!();
        }
    }
}
