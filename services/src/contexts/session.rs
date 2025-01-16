use crate::identifier;
use crate::projects::ProjectId;
use crate::projects::STRectangle;
use geoengine_datatypes::primitives::DateTime;
use serde::Serialize;

identifier!(SessionId);

pub trait Session: Send + Sync + Serialize {
    fn id(&self) -> SessionId;
    fn created(&self) -> &DateTime;
    fn valid_until(&self) -> &DateTime;
    fn project(&self) -> Option<ProjectId>;
    fn view(&self) -> Option<&STRectangle>;
}

pub trait MockableSession: Session {
    fn mock() -> Self;
}
