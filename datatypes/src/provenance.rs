use serde::{Deserialize, Serialize};

/// Information on how to properly cite a data entry
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct ProvenanceInformation {
    pub citation: String,
    pub license: String,
    pub uri: String,
}
