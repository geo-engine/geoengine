use crate::error::Result;
use geoengine_datatypes::dataset::DataId;
use geoengine_operators::{
    engine::{OperatorName, RasterOperator, TypedOperator, VectorOperator},
    mock::{MockDatasetDataSource, MockDatasetDataSourceParams},
    source::{GdalSource, GdalSourceParameters, OgrSource, OgrSourceParameters},
};

pub fn source_operator_from_dataset(
    source_operator_name: &str,
    id: &DataId,
) -> Result<TypedOperator> {
    Ok(match source_operator_name {
        OgrSource::TYPE_NAME => TypedOperator::Vector(
            OgrSource {
                params: OgrSourceParameters {
                    data: id.clone(),
                    attribute_projection: None,
                    attribute_filters: None,
                },
            }
            .boxed(),
        ),
        GdalSource::TYPE_NAME => TypedOperator::Raster(
            GdalSource {
                params: GdalSourceParameters { data: id.clone() },
            }
            .boxed(),
        ),
        MockDatasetDataSource::TYPE_NAME => TypedOperator::Vector(
            MockDatasetDataSource {
                params: MockDatasetDataSourceParams { data: id.clone() },
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
