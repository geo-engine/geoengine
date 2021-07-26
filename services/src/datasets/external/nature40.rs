use std::path::Path;

use crate::error::Error;
use crate::util::parsing::string_or_string_array;
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
use gdal::DatasetOptions;
use gdal::Metadata;
use geoengine_datatypes::dataset::{DatasetId, DatasetProviderId, ExternalDatasetId};
use geoengine_operators::engine::TypedResultDescriptor;
use geoengine_operators::source::GdalMetaDataStatic;
use geoengine_operators::util::gdal::{
    gdal_open_dataset_ex, gdal_parameters_from_dataset, raster_descriptor_from_dataset,
};
use geoengine_operators::{
    engine::{
        MetaData, MetaDataProvider, RasterQueryRectangle, RasterResultDescriptor,
        VectorQueryRectangle, VectorResultDescriptor,
    },
    mock::MockDatasetDataSourceLoadingInfo,
    source::{GdalLoadingInfo, OgrSourceDataset},
};
use log::info;
use quick_xml::events::Event;
use quick_xml::Reader;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Nature40DataProviderDefinition {
    id: DatasetProviderId,
    name: String,
    base_url: String,
    user: String,
    password: String,
}

#[typetag::serde]
#[async_trait]
impl DatasetProviderDefinition for Nature40DataProviderDefinition {
    async fn initialize(self: Box<Self>) -> crate::error::Result<Box<dyn DatasetProvider>> {
        Ok(Box::new(Nature40DataProvider {
            id: self.id,
            base_url: self.base_url,
            user: self.user,
            password: self.password,
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

pub struct Nature40DataProvider {
    id: DatasetProviderId,
    base_url: String,
    user: String,
    password: String,
}

#[derive(Deserialize, Debug)]
struct RasterDb {
    name: String,
    title: String,
    #[serde(deserialize_with = "string_or_string_array")]
    tags: Vec<String>,
}

impl RasterDb {
    fn url_from_name(base_url: &str, name: &str) -> String {
        format!(
            "WCS:{}/rasterdb/{name}/wcs?VERSION=1.0.0&COVERAGE={name}",
            base_url,
            name = name
        )
    }

    fn url(&self, base_url: &str) -> String {
        Self::url_from_name(base_url, &self.name)
    }
}

#[derive(Deserialize, Debug)]
struct RasterDbs {
    rasterdbs: Vec<RasterDb>,
    #[serde(deserialize_with = "string_or_string_array")]
    tags: Vec<String>,
    session: String,
}

#[async_trait]
impl DatasetProvider for Nature40DataProvider {
    async fn list(&self, _options: Validated<DatasetListOptions>) -> Result<Vec<DatasetListing>> {
        // TODO: query the other dbs as well
        let raster_dbs = self.load_raster_dbs().await?;

        let mut listing = vec![];

        let datasets = raster_dbs
            .rasterdbs
            .iter()
            .map(|db| self.load_dataset(db.url(&self.base_url)));
        let datasets: Vec<Result<gdal::Dataset>> = join_all(datasets).await;

        for (db, dataset) in raster_dbs.rasterdbs.iter().zip(datasets) {
            if let Ok(dataset) = dataset {
                let (dataset, band_labels) = self.get_band_labels(dataset).await?;

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

    async fn load(
        &self,
        _dataset: &geoengine_datatypes::dataset::DatasetId,
    ) -> crate::error::Result<crate::datasets::storage::Dataset> {
        Err(error::Error::NotYetImplemented)
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
        let auth = self.auth();
        tokio::task::spawn_blocking(move || {
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
            .get(format!("{}/rasterdbs.json", self.base_url))
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
    async fn get_band_labels(
        &self,
        dataset: gdal::Dataset,
    ) -> Result<(gdal::Dataset, Vec<String>)> {
        let labels = Self::parse_band_labels(&dataset)?;

        if labels.is_empty() {
            // no labels found during parsing, try to reopen the dataset to flush the cache and try again
            let name = dataset
                .metadata_item("label", "")
                .ok_or(Error::WcsDatasetMissingLabelInMetadata)?;

            drop(dataset);

            let dataset = self
                .load_dataset(RasterDb::url_from_name(&self.base_url, &name))
                .await?;

            let labels = Self::parse_band_labels(&dataset)?;
            Ok((dataset, labels))
        } else {
            Ok((dataset, labels))
        }
    }

    fn parse_band_labels(dataset: &gdal::Dataset) -> Result<Vec<String>> {
        let mut reader = Reader::from_file(&dataset.description()?)?;
        reader.trim_text(true);
        let mut txt = Vec::new();
        let mut buf = Vec::new();

        let mut first = true;

        loop {
            match reader.read_event(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    if e.name() == b"label" {
                        if first {
                            first = false; // skip first label which is the coverage label
                        } else {
                            txt.push(
                                reader
                                    .read_text(e.name(), &mut Vec::new())
                                    .unwrap_or_else(|_| "".to_owned()),
                            );
                        }
                    }
                }
                Ok(Event::Eof) => break,
                _ => (),
            }
            buf.clear();
        }
        Ok(txt)
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

        let db_url = RasterDb::url_from_name(&self.base_url, db_name);
        let dataset = self.load_dataset(db_url.clone()).await.map_err(|e| {
            geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            }
        })?;

        Ok(Box::new(GdalMetaDataStatic {
            time: None,
            params: gdal_parameters_from_dataset(&dataset, band_index, Path::new(&db_url), None)?,
            result_descriptor: raster_descriptor_from_dataset(&dataset, band_index as isize, None)?,
        }))
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
    use std::{fs::File, io::Read, str::FromStr};

    use geoengine_datatypes::{
        primitives::Measurement,
        raster::RasterDataType,
        spatial_reference::{SpatialReference, SpatialReferenceAuthority},
    };
    use httptest::{
        all_of,
        matchers::{contains, request, url_decoded},
        responders::{json_encoded, status_code},
        Expectation, Server,
    };
    use serde_json::json;

    use crate::{datasets::listing::OrderBy, util::user_input::UserInput};

    use super::*;

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn it_lists() {
        let server = Server::run();
        // TODO: auth
        server.expect(
            Expectation::matching(request::method_path("GET", "/rasterdbs.json")).respond_with(
                json_encoded(json!({
                    "rasterdbs": [{
                        "name": "geonode_ortho_muf_1m",
                        "title": "MOF Luftbild",
                        "tags": "natur40"
                    },
                    {
                        "name": "lidar_2018_wetness_1m",
                        "title": "Topografic Wetness index",
                        "tags": "natur40"
                    }],
                    "tags": ["UAV", "natur40"],
                    "session": "lhtdVm"
                })),
            ),
        );

        server.expect(
            Expectation::matching(all_of![
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

        server.expect(
            Expectation::matching(all_of![
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

        let mut geonode_ortho_muf_1m_bytes = vec![];
        File::open("test-data/nature40/geonode_ortho_muf_1m.tiff")
            .unwrap()
            .read_to_end(&mut geonode_ortho_muf_1m_bytes)
            .unwrap();

        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/rasterdb/geonode_ortho_muf_1m/wcs",),
                request::query(url_decoded(contains(("REQUEST", "GetCoverage"))))
            ])
            .respond_with(
                status_code(200)
                    .append_header("Content-Type", "image/tiff")
                    .body(geonode_ortho_muf_1m_bytes),
            ),
        );

        let mut lidar_2018_wetness_1m = vec![];
        File::open("test-data/nature40/lidar_2018_wetness_1m.tiff")
            .unwrap()
            .read_to_end(&mut lidar_2018_wetness_1m)
            .unwrap();

        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/rasterdb/lidar_2018_wetness_1m/wcs",),
                request::query(url_decoded(contains(("REQUEST", "GetCoverage"))))
            ])
            .respond_with(
                status_code(200)
                    .append_header("Content-Type", "image/tiff")
                    .body(lidar_2018_wetness_1m),
            ),
        );

        let provider = Box::new(Nature40DataProviderDefinition {
            id: DatasetProviderId::from_str("2cb964d5-b9fa-4f8f-ab6f-f6c7fb47d4cd").unwrap(),
            name: "Nature40".to_owned(),
            base_url: server.url_str("").strip_suffix('/').unwrap().to_owned(),
            user: "geoengine".to_owned(),
            password: "pwd".to_owned(),
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
                }
            ]
        );
    }
}
