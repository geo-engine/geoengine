use crate::error::Result;
use geoengine_datatypes::dataset::NamedData;
use geoengine_operators::{
    engine::{OperatorName, RasterOperator, TypedOperator, VectorOperator},
    mock::{MockDatasetDataSource, MockDatasetDataSourceParams},
    source::{
        GdalSource, GdalSourceParameters, MultiBandGdalSource, MultiBandGdalSourceParameters,
        OgrSource, OgrSourceParameters,
    },
};

pub fn source_operator_from_dataset(
    source_operator_name: &str,
    name: &NamedData,
) -> Result<TypedOperator> {
    Ok(match source_operator_name {
        OgrSource::TYPE_NAME => TypedOperator::Vector(
            OgrSource {
                params: OgrSourceParameters {
                    data: name.clone(),
                    attribute_projection: None,
                    attribute_filters: None,
                },
            }
            .boxed(),
        ),
        GdalSource::TYPE_NAME => TypedOperator::Raster(
            GdalSource {
                params: GdalSourceParameters::new(name.clone()),
            }
            .boxed(),
        ),
        MockDatasetDataSource::TYPE_NAME => TypedOperator::Vector(
            MockDatasetDataSource {
                params: MockDatasetDataSourceParams { data: name.clone() },
            }
            .boxed(),
        ),
        MultiBandGdalSource::TYPE_NAME => TypedOperator::Raster(
            MultiBandGdalSource {
                params: MultiBandGdalSourceParameters::new(name.clone()),
            }
            .boxed(),
        ),
        s => {
            return Err(crate::error::Error::UnknownOperator {
                operator: s.to_owned(),
            });
        }
    })
}
