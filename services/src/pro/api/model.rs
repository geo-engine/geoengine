use crate::identifier;

identifier!(MlModelId);

impl From<MlModelId> for geoengine_datatypes::pro::MlModelId {
    fn from(value: MlModelId) -> Self {
        Self(value.0)
    }
}

impl From<geoengine_datatypes::pro::MlModelId> for MlModelId {
    fn from(value: geoengine_datatypes::pro::MlModelId) -> Self {
        Self(value.0)
    }
}
