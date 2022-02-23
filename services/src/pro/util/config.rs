use serde::Deserialize;

use crate::util::config::ConfigElement;
use crate::util::parsing::deserialize_base_url;

#[derive(Debug, Deserialize)]
pub struct User {
    pub user_registration: bool,
}

impl ConfigElement for User {
    const KEY: &'static str = "user";
}

#[derive(Debug, Deserialize)]
pub struct Odm {
    #[serde(deserialize_with = "deserialize_base_url")]
    pub endpoint: url::Url,
}

impl ConfigElement for Odm {
    const KEY: &'static str = "odm";
}
