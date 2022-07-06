use crate::error::Result;
use geoengine_datatypes::dataset::DatasetId;
use geoengine_operators::{
    engine::{OperatorName, RasterOperator, TypedOperator, VectorOperator},
    mock::{MockDatasetDataSource, MockDatasetDataSourceParams},
    source::{GdalSource, GdalSourceParameters, OgrSource, OgrSourceParameters},
};

pub fn source_operator_from_dataset(
    source_operator_name: &str,
    dataset: &DatasetId,
) -> Result<TypedOperator> {
    Ok(match source_operator_name {
        OgrSource::TYPE_NAME => TypedOperator::Vector(
            OgrSource {
                params: OgrSourceParameters {
                    dataset: dataset.clone(),
                    attribute_projection: None,
                    attribute_filters: None,
                },
            }
            .boxed(),
        ),
        GdalSource::TYPE_NAME => TypedOperator::Raster(
            GdalSource {
                params: GdalSourceParameters {
                    dataset: dataset.clone(),
                },
            }
            .boxed(),
        ),
        MockDatasetDataSource::TYPE_NAME => TypedOperator::Vector(
            MockDatasetDataSource {
                params: MockDatasetDataSourceParams {
                    dataset: dataset.clone(),
                },
            }
            .boxed(),
        ),
        s => {
            return Err(crate::error::Error::UnknownOperator {
                operator: s.to_owned(),
            })
        }
    })
}
