use crate::contexts::Context;
use crate::error::Result;
use crate::util::config::{get_config_element, GFBio};
use actix_web::{web, FromRequest, Responder};
use chrono::Utc;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;
use geoengine_datatypes::dataset::{DatasetId, DatasetProviderId, ExternalDatasetId};
use geoengine_datatypes::primitives::VectorQueryRectangle;
use geoengine_operators::engine::{
    MetaDataProvider, TypedResultDescriptor, VectorResultDescriptor,
};
use geoengine_operators::source::{AttributeFilter, OgrSourceDataset};
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::datasets::external::gfbio::GfbioDataProvider;
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
        .dataset_provider(&session, config.gfbio_provider_id)
        .await
        .ok()
        .and_then(|b| b.downcast::<GfbioDataProvider>().ok());

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
        config.pangaea_provider_id,
        abcd_provider,
    )
    .await?;
    Ok(web::Json(basket))
}

#[derive(Serialize, Debug)]
struct Basket {
    basket_id: BasketId,
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
        abcd_provider: Option<Box<GfbioDataProvider>>,
    ) -> Result<Basket> {
        let abcd: Option<&GfbioDataProvider> = abcd_provider.as_ref().map(|b| b.as_ref());

        let mut entries = Vec::with_capacity(value.content.len());
        let mut futures = value
            .content
            .into_iter()
            .map(|x| x.to_basket_entry::<C>(&ec, pangaea_id, abcd))
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
    attribute_filters: Option<Vec<AttributeFilter>>,
}

const PANGAEA_PATTERN: &'static str = "doi.pangaea.de/";

#[derive(Debug, Deserialize)]
struct BasketInternal {
    #[serde(rename = "basketId")]
    basket_id: BasketId,
    content: Vec<BasketEntryInternal>,
    #[serde(rename = "userId")]
    user_id: Option<String>,
    #[serde(rename = "createdAt")]
    created: chrono::DateTime<Utc>,
    #[serde(rename = "updatedAt")]
    updated: chrono::DateTime<Utc>,
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

impl BasketEntryInternal {
    async fn to_basket_entry<C: Context>(
        self,
        ec: &<C as Context>::ExecutionContext,
        pangaea_id: DatasetProviderId,
        abcd_provider: Option<&GfbioDataProvider>,
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
                    let mdp = ec as &dyn MetaDataProvider<
                        OgrSourceDataset,
                        VectorResultDescriptor,
                        VectorQueryRectangle,
                    >;

                    Self::generate_loading_info(mdp, id, None).await
                }
                // Is an ABCD entry
                None => match abcd_provider {
                    Some(p) => self.handle_abcd_entry(p).await,
                    None => BasketEntryStatus::Unavailable,
                },
            }
        };
        BasketEntry {
            title: self.title,
            status,
        }
    }

    async fn handle_abcd_entry(&self, abcd_provider: &GfbioDataProvider) -> BasketEntryStatus {
        let dataset_id = if self.parent_id.is_some() {
            &self.parent_id
        } else {
            &self.data_link
        };

        let sg_id = match dataset_id {
            Some(id) => match abcd_provider.resolve_surrogate_key(id.as_str()).await {
                Ok(Some(s)) => s,
                Ok(None) => return BasketEntryStatus::Unavailable,
                Err(e) => {
                    return BasketEntryStatus::Error {
                        message: format!("Error looking up dataset \"{}\": {:?}", id.as_str(), e),
                    }
                }
            },
            None => {
                return BasketEntryStatus::Error {
                    message: format!("No dataset id in ABCD data."),
                }
            }
        };

        let id = DatasetId::External(ExternalDatasetId {
            provider_id: abcd_provider.id,
            dataset_id: sg_id.to_string(),
        });

        let mdp = abcd_provider
            as &dyn MetaDataProvider<
                OgrSourceDataset,
                VectorResultDescriptor,
                VectorQueryRectangle,
            >;

        let filter = self.unit_id.as_ref().map(|unit| {
            vec![AttributeFilter {
                // attribute: "adf8c075f2c6b97eaab5cee8f22e97abfdaf6b71".to_string(),
                attribute: "/DataSets/DataSet/Units/Unit/UnitID".to_string(),
                ranges: vec![StringOrNumberRange::String(
                    unit.to_string()..=unit.to_string(),
                )],
                keep_nulls: false,
            }]
        });

        Self::generate_loading_info(mdp, id, filter).await
    }

    async fn generate_loading_info(
        mdp: &dyn MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
        id: DatasetId,
        filter: Option<Vec<AttributeFilter>>,
    ) -> BasketEntryStatus {
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
                attribute_filters: filter,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::handlers::gfbio::{
        Basket, BasketEntry, BasketEntryLoadingDetails, BasketEntryStatus, BasketInternal,
    };
    use geoengine_datatypes::collections::VectorDataType;
    use geoengine_datatypes::dataset::{DatasetId, DatasetProviderId, ExternalDatasetId};
    use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceOption};
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_operators::engine::{
        MockExecutionContext, TypedResultDescriptor, VectorResultDescriptor,
    };
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
    fn pangaea_basket_deserialization() {
        // let ec = MockExecutionContext::test_default();

        let json = serde_json::json!({"content":[{"id":"PANGAEA.932375","titleUrl":"https://doi.pangaea.de/10.1594/PANGAEA.932375","title":"Vuilleumier, Laurent (2021): Other measurements at 10 m from station Payerne (2020-09)","upperLabels":[{"innerInfo":"2021","tooltip":"Publication year","colorClass":"bg-label-blue"},{"innerInfo":"PANGAEA","tooltip":"This dataset is provided by Data Publisher for Earth; Environmental Science  (PANGAEA).","colorClass":"bg-goldenrod"}],"citation":{"title":"Other measurements at 10 m from station Payerne (2020-09)","date":"2021-06-08","DOI":"https://doi.pangaea.de/10.1594/PANGAEA.932375","dataCenter":"PANGAEA","source":"Swiss Meteorological Agency, Payerne","creator":["Vuilleumier, Laurent"]},"licence":["Other"],"vat":false,"vatTooltip":"This dataset can be transfered to VAT.","xml":"<dataset xmlns=\"urn:pangaea.de:dataportals\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><dc:title>Other measurements at 10 m from station Payerne (2020-09)</dc:title><dc:creator>Vuilleumier, Laurent</dc:creator><principalInvestigator>Vuilleumier, Laurent</principalInvestigator><dc:source>Swiss Meteorological Agency, Payerne</dc:source><dc:publisher>PANGAEA</dc:publisher><dataCenter>PANGAEA: Data Publisher for Earth &amp; Environmental Science</dataCenter><dc:date>2021-06-08</dc:date><dc:type>Dataset</dc:type><dc:format>text/tab-separated-values, 430587 data points</dc:format><dc:identifier>https://doi.org/10.1594/PANGAEA.932375</dc:identifier><dc:language>en</dc:language><dc:rights>Access constraints: access rights needed</dc:rights><dc:relation>Vuilleumier, Laurent (2021): BSRN Station-to-archive file for station Payerne (2020-09). ftp://ftp.bsrn.awi.de/pay/pay0920.dat.gz</dc:relation><dc:subject type=\"parameter\" xsi:type=\"SubjectType\">DATE/TIME</dc:subject><dc:subject type=\"parameter\" xsi:type=\"SubjectType\">HEIGHT above ground</dc:subject><dc:subject type=\"parameter\" xsi:type=\"SubjectType\">Short-wave upward (REFLEX) radiation</dc:subject><dc:subject type=\"parameter\" xsi:type=\"SubjectType\">Short-wave upward (REFLEX) radiation, standard deviation</dc:subject><dc:subject type=\"parameter\" xsi:type=\"SubjectType\">Short-wave upward (REFLEX) radiation, minimum</dc:subject><dc:subject type=\"parameter\" xsi:type=\"SubjectType\">Short-wave upward (REFLEX) radiation, maximum</dc:subject><dc:subject type=\"parameter\" xsi:type=\"SubjectType\">Long-wave upward radiation</dc:subject><dc:subject type=\"parameter\" xsi:type=\"SubjectType\">Long-wave upward radiation, standard deviation</dc:subject><dc:subject type=\"parameter\" xsi:type=\"SubjectType\">Long-wave upward radiation, minimum</dc:subject><dc:subject type=\"parameter\" xsi:type=\"SubjectType\">Long-wave upward radiation, maximum</dc:subject><dc:subject type=\"parameter\" xsi:type=\"SubjectType\">Temperature, air</dc:subject><dc:subject type=\"parameter\" xsi:type=\"SubjectType\">Humidity, relative</dc:subject><dc:subject type=\"method\" xsi:type=\"SubjectType\">Pyranometer, Kipp &amp; Zonen, CM21, SN 041303, WRMC No. 21074</dc:subject><dc:subject type=\"method\" xsi:type=\"SubjectType\">Pyrgeometer, Eppley Laboratory Inc., PIR, SN 31964F3, WRMC No. 21089</dc:subject><dc:subject type=\"method\" xsi:type=\"SubjectType\">Thermometer</dc:subject><dc:subject type=\"method\" xsi:type=\"SubjectType\">Hygrometer</dc:subject><dc:subject type=\"project\" xsi:type=\"SubjectType\">BSRN: Baseline Surface Radiation Network</dc:subject><dc:subject type=\"sensor\" xsi:type=\"SubjectType\">Monitoring station</dc:subject><dc:subject type=\"sensor\" xsi:type=\"SubjectType\">MONS</dc:subject><dc:subject type=\"feature\" xsi:type=\"SubjectType\">PAY</dc:subject><dc:subject type=\"feature\" xsi:type=\"SubjectType\">Payerne</dc:subject><dc:subject type=\"feature\" xsi:type=\"SubjectType\">WCRP/GEWEX</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">citable</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">author31671</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">campaign32853</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">curlevel30</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">event2536888</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">geocode1599</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">geocode1600</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">geocode1601</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">geocode56349</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">geocode8128</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">inst32</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">method10271</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">method11312</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">method4722</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">method5039</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">method9835</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">param2219</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">param45299</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">param4610</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">param55911</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">param55912</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">param55913</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">param55914</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">param55915</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">param55916</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">param55917</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">pi31671</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">project4094</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">ref108695</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">sourceinst2878</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term1073288</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term21461</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term2663133</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term37764</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term38492</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term40453</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term41019</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term42421</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term43794</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term43863</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term43923</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term43972</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term43975</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">term44358</dc:subject><dc:subject type=\"pangaea-tech-keyword\" xsi:type=\"SubjectType\">topotype8</dc:subject><dc:coverage xsi:type=\"CoverageType\"><northBoundLatitude>46.815</northBoundLatitude><westBoundLongitude>6.944</westBoundLongitude><southBoundLatitude>46.815</southBoundLatitude><eastBoundLongitude>6.944</eastBoundLongitude><location>Switzerland</location><minElevation>10.0 m (HEIGHT above ground)</minElevation><maxElevation>10.0 m (HEIGHT above ground)</maxElevation><startDate>2020-09-01</startDate><endDate>2020-09-30</endDate></dc:coverage><linkage type=\"metadata\">https://doi.pangaea.de/10.1594/PANGAEA.932375</linkage><linkage accessRestricted=\"true\" type=\"data\">https://doi.pangaea.de/10.1594/PANGAEA.932375?format=textfile</linkage><additionalContent>BSRN station no: 21; Surface type: cultivated; Topography type: hilly, rural; Horizon: doi:10.1594/PANGAEA.669523; Station scientist: Laurent Vuilleumier (laurent.vuilleumier@meteoswiss.ch)</additionalContent></dataset>","longitude":6.944,"latitude":46.815,"titleTooltip":"min latitude: 46.815, max longitude: 6.944","metadatalink":"https://doi.pangaea.de/10.1594/PANGAEA.932375","datalink":"https://doi.pangaea.de/10.1594/PANGAEA.932375?format=textfile","dcIdentifier":"https://doi.org/10.1594/PANGAEA.932375","dcType":["Dataset"],"linkage":{"metadata":"https://doi.pangaea.de/10.1594/PANGAEA.932375","data":"https://doi.pangaea.de/10.1594/PANGAEA.932375?format=textfile"},"description":[{"title":"Parameters:","value":"DATE/TIME; HEIGHT above ground; Short-wave upward (REFLEX) radiation; Short-wave upward (REFLEX) radiation, standard deviation; Short-wave upward (REFLEX) radiation, minimum; Short-wave upward (REFLEX) radiation, maximum; Long-wave upward radiation; Long-wave upward radiation, standard deviation; Long-wave upward radiation, minimum; Long-wave upward radiation, maximum; Temperature, air; Humidity, relative"},{"title":"Relation:","value":"<ul><li>Vuilleumier, Laurent (2021): BSRN Station-to-archive file for station Payerne (2020-09). ftp://ftp.bsrn.awi.de/pay/pay0920.dat.gz</li></ul>"}],"multimediaObjs":[],"color":"#00fa9a","checkbox":true}],"query":null,"keywords":null,"filters":null,"basketId":"89957503-7c6c-4fd4-9a5a-ca8ddc64f190","userId":null,"createdAt":"2022-03-01T00:08:51.000Z","updatedAt":"2022-03-01T00:08:51.000Z"});

        let basket = serde_json::from_value::<BasketInternal>(json).unwrap();
        // let basket = Basket::new(
        //     basket, ec,
        //
        // )

        assert_eq!(1, basket.content.len())
    }
}
