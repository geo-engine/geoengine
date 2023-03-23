use super::listing::LayerCollectionId;
use crate::api::model::datatypes::{DataProviderId, LayerId};
use crate::{error::Result, projects::Symbology, workflows::workflow::Workflow};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::openapi::{ArrayBuilder, ObjectBuilder, SchemaType};
use utoipa::{IntoParams, ToSchema};
use validator::Validate;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProviderLayerId {
    pub provider_id: DataProviderId,
    pub layer_id: LayerId,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProviderLayerCollectionId {
    pub provider_id: DataProviderId,
    pub collection_id: LayerCollectionId,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
pub struct Layer {
    pub id: ProviderLayerId,
    pub name: String,
    pub description: String,
    pub workflow: Workflow,
    pub symbology: Option<Symbology>,
    /// properties, for instance, to be rendered in the UI
    #[serde(default)]
    pub properties: Vec<Property>,
    /// metadata used for loading the data
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
pub struct LayerListing {
    pub id: ProviderLayerId,
    pub name: String,
    pub description: String,
    /// properties, for instance, to be rendered in the UI
    #[serde(default)]
    pub properties: Vec<Property>,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
// TODO: validate user input
pub struct AddLayer {
    #[schema(example = "Example Layer")]
    pub name: String,
    #[schema(example = "Example layer description")]
    pub description: String,
    pub workflow: Workflow,
    pub symbology: Option<Symbology>,
    /// properties, for instance, to be rendered in the UI
    #[serde(default)]
    pub properties: Vec<Property>,
    /// metadata used for loading the data
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LayerDefinition {
    pub id: LayerId,
    pub name: String,
    pub description: String,
    pub workflow: Workflow,
    pub symbology: Option<Symbology>,
    /// properties, for instance, to be rendered in the UI
    #[serde(default)]
    pub properties: Vec<Property>,
    /// metadata used for loading the data
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LayerCollection {
    pub id: ProviderLayerCollectionId,
    pub name: String,
    pub description: String,
    pub items: Vec<CollectionItem>,
    /// a common label for the collection's entries, if there is any
    // TODO: separate labels for collections and layers?
    pub entry_label: Option<String>,
    pub properties: Vec<Property>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Property((String, String));

impl From<(String, String)> for Property {
    fn from(v: (String, String)) -> Self {
        Self(v)
    }
}

impl From<[String; 2]> for Property {
    fn from(v: [String; 2]) -> Self {
        let [v1, v2] = v;
        Self((v1, v2))
    }
}

impl From<Property> for [String; 2] {
    fn from(v: Property) -> Self {
        let (k, v) = v.0;
        [k, v]
    }
}

impl From<&Property> for [String; 2] {
    fn from(v: &Property) -> Self {
        let (ref k, ref v) = v.0;
        [k.clone(), v.clone()]
    }
}

// manual implementation because utoipa doesn't support tuples for now
impl<'a> ToSchema<'a> for Property {
    fn schema() -> (&'a str, utoipa::openapi::RefOr<utoipa::openapi::Schema>) {
        (
            "Property",
            ArrayBuilder::new()
                .items(ObjectBuilder::new().schema_type(SchemaType::String))
                .min_items(Some(2))
                .max_items(Some(2))
                .into(),
        )
    }
}

#[cfg(feature = "postgres")]
mod psql {
    use super::*;
    use postgres_types::{FromSql, ToSql};

    #[derive(Debug, FromSql)]
    #[postgres(name = "PropertyType")]
    pub struct PropertyPsql {
        key: String,
        value: String,
    }

    #[derive(Debug, ToSql)]
    #[postgres(name = "PropertyType")]
    pub struct PropertyRefPsql<'s> {
        key: &'s str,
        value: &'s str,
    }

    impl From<Property> for PropertyPsql {
        fn from(v: Property) -> Self {
            let (key, value) = v.0;
            Self { key, value }
        }
    }

    impl From<PropertyPsql> for Property {
        fn from(v: PropertyPsql) -> Self {
            Self((v.key, v.value))
        }
    }

    impl<'s> From<&'s Property> for PropertyRefPsql<'s> {
        fn from(v: &'s Property) -> Self {
            let (ref key, ref value) = v.0;
            Self { key, value }
        }
    }

    impl<'a> postgres_types::FromSql<'a> for Property {
        fn from_sql(
            ty: &postgres_types::Type,
            raw: &'a [u8],
        ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
            <PropertyPsql as postgres_types::FromSql>::from_sql(ty, raw).map(Into::into)
        }

        fn accepts(ty: &postgres_types::Type) -> bool {
            <PropertyPsql as postgres_types::FromSql>::accepts(ty)
        }
    }

    impl postgres_types::ToSql for Property {
        fn to_sql(
            &self,
            ty: &postgres_types::Type,
            out: &mut bytes::BytesMut,
        ) -> std::result::Result<postgres_types::IsNull, Box<dyn snafu::Error + Sync + Send>>
        where
            Self: Sized,
        {
            let t: PropertyRefPsql = self.into();
            t.to_sql(ty, out)
        }

        fn accepts(ty: &postgres_types::Type) -> bool
        where
            Self: Sized,
        {
            <PropertyRefPsql as postgres_types::ToSql>::accepts(ty)
        }

        fn encode_format(&self, ty: &postgres_types::Type) -> postgres_types::Format {
            let t: PropertyRefPsql = self.into();
            t.encode_format(ty)
        }

        postgres_types::to_sql_checked!();
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LayerCollectionListing {
    pub id: ProviderLayerCollectionId,
    pub name: String,
    pub description: String,
    #[serde(default)]
    pub properties: Vec<Property>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum CollectionItem {
    Collection(LayerCollectionListing),
    Layer(LayerListing),
}

impl CollectionItem {
    pub fn name(&self) -> &str {
        match self {
            CollectionItem::Collection(c) => &c.name,
            CollectionItem::Layer(l) => &l.name,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
// TODO: validate user input
pub struct AddLayerCollection {
    #[schema(example = "Example Collection")]
    pub name: String,
    #[schema(example = "A description for an example collection")]
    pub description: String,
    #[serde(default)]
    pub properties: Vec<Property>,
}

#[derive(Debug, Serialize, Deserialize, Clone, IntoParams, Validate)]
// TODO: validate user input
pub struct LayerCollectionListOptions {
    #[param(example = 0)]
    pub offset: u32,
    #[param(example = 20)]
    #[validate(range(max = 20))]
    pub limit: u32,
}

impl Default for LayerCollectionListOptions {
    fn default() -> Self {
        Self {
            offset: 0,
            limit: 20,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LayerCollectionDefinition {
    pub id: LayerCollectionId,
    pub name: String,
    pub description: String,
    pub collections: Vec<LayerCollectionId>,
    pub layers: Vec<LayerId>,
    #[serde(default)]
    pub properties: Vec<Property>,
}
