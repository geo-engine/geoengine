use chrono::{DateTime, Utc, MAX_DATETIME, MIN_DATETIME};
use geoengine_datatypes::identifier;

use crate::projects::project::ProjectId;
use crate::projects::project::STRectangle;

identifier!(SessionId);

pub trait Session {
    fn id(&self) -> SessionId;
    fn created(&self) -> &DateTime<Utc>;
    fn valid_until(&self) -> &DateTime<Utc>;
    fn project(&self) -> Option<ProjectId>;
    fn view(&self) -> Option<&STRectangle>;
}

pub struct SimpleSession {
    id: SessionId,
    project: Option<ProjectId>,
    view: Option<STRectangle>,
}

impl Default for SimpleSession {
    fn default() -> Self {
        Self {
            id: SessionId::new(),
            project: None,
            view: None,
        }
    }
}

impl Session for SimpleSession {
    fn id(&self) -> SessionId {
        self.id
    }

    fn created(&self) -> &DateTime<Utc> {
        &MIN_DATETIME
    }

    fn valid_until(&self) -> &DateTime<Utc> {
        &MAX_DATETIME
    }

    fn project(&self) -> Option<ProjectId> {
        &self.project
    }

    fn view(&self) -> Option<&STRectangle> {
        &self.view
    }
}
