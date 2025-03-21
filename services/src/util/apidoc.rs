use std::collections::BTreeMap;
use utoipa::{
    Modify,
    openapi::{RefOr, Schema},
};

pub struct OpenApiServerInfo;

impl Modify for OpenApiServerInfo {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let web_config: crate::config::Web =
            crate::config::get_config_element().expect("web config");

        let mut api_url = web_config.api_url().expect("external address").to_string();
        api_url.pop(); //remove trailing slash because codegen requires it

        openapi.servers = Some(vec![
            utoipa::openapi::ServerBuilder::new().url(api_url).build(),
        ]);
    }
}

/// Discriminator value to use, e.g., for the `compute_discriminator` macro.
pub trait DiscriminatorValue {
    const DISCRIMINATOR_VALUE: &'static str;
}

pub struct DeriveDiscriminatorMapping;

impl Modify for DeriveDiscriminatorMapping {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let Some(components) = openapi.components.as_mut() else {
            return; // no components -> nothing to do
        };

        let mut schemas_to_update = BTreeMap::new();

        for (name, schema) in &components.schemas {
            let RefOr::T(Schema::OneOf(schema)) = schema else {
                continue; // only interested in oneOf schemas
            };

            let mut schema = schema.clone();

            let Some(discriminator) = schema.discriminator.as_mut() else {
                continue; // no discriminator -> nothing to do
            };

            if !discriminator.mapping.is_empty() {
                continue; // values already present
            }

            for child in &schema.items {
                let Some((child_schema, ref_or_title)) =
                    Self::child_schema(&components.schemas, child, None)
                else {
                    continue; // unable to lookup child schema
                };

                let Some(tag_name) =
                    Self::discriminator_tag_of_child(child_schema, &discriminator.property_name)
                else {
                    continue; // no tag name found -> we have to skip this child
                };

                discriminator.mapping.insert(tag_name, ref_or_title);
            }

            schemas_to_update.insert(name.clone(), RefOr::T(Schema::OneOf(schema)));
        }

        components.schemas.extend(schemas_to_update);
    }
}

impl DeriveDiscriminatorMapping {
    fn child_schema<'s>(
        schemas: &'s BTreeMap<String, RefOr<Schema>>,
        child: &'s RefOr<Schema>,
        ref_string: Option<String>,
    ) -> Option<(&'s Schema, String)> {
        match child {
            RefOr::T(schema) => {
                if let Some(ref_string) = ref_string {
                    return Some((schema, ref_string));
                }

                let title = match schema {
                    Schema::Array(o) => o.title.as_ref(),
                    Schema::Object(o) => o.title.as_ref(),
                    Schema::OneOf(o) => o.title.as_ref(),
                    Schema::AllOf(o) => o.title.as_ref(),
                    Schema::AnyOf(_) | _ => return None,
                }?
                .clone();

                Some((schema, title))
            }
            RefOr::Ref(schema_ref) => {
                let (_, item_name) = schema_ref.ref_location.rsplit_once('/')?;

                let new_child = schemas.get(item_name)?;

                Self::child_schema(schemas, new_child, Some(schema_ref.ref_location.clone()))
            }
        }
    }

    fn discriminator_tag_of_child(item: &Schema, discriminator_field: &str) -> Option<String> {
        // get first object
        let child_properties = match item {
            Schema::Object(object) => Some(&object.properties),
            Schema::AllOf(allof) => {
                let mut result = None;
                for item in &allof.items {
                    let RefOr::T(Schema::Object(object)) = item else {
                        continue;
                    };
                    result = Some(&object.properties);
                    break;
                }
                result
            }
            Schema::AnyOf(_) | Schema::Array(_) | Schema::OneOf(_) | _ => None,
        }?;

        let property = child_properties.get(discriminator_field)?;

        let RefOr::T(Schema::Object(property)) = property else {
            return None; // tags are always inlined as objects
        };

        let Some(enum_values) = &property.enum_values else {
            return None; // no enum values -> we have to skip this child
        };

        let serde_json::Value::String(first_enum_value) = enum_values.first()? else {
            return None; // our expectation is to have exactly one enum value and it is a string
        };

        Some(first_enum_value.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_macros::type_tag;
    use pretty_assertions::assert_eq;
    use serde::Serialize;
    use utoipa::{
        ToSchema,
        openapi::{ComponentsBuilder, OpenApiBuilder},
    };

    #[test]
    #[allow(clippy::too_many_lines)]
    fn it_detects_discriminators() {
        #[allow(dead_code)]
        #[derive(Serialize, ToSchema)]
        #[serde(untagged)]
        #[schema(discriminator = "type")]
        enum MyEnum {
            A(A),
            B(B),
        }
        #[type_tag(tag = "aaa")]
        #[derive(Serialize, ToSchema)]
        struct A {
            text: String,
        }
        #[type_tag(tag = "bbb")]
        #[derive(Serialize, ToSchema)]
        struct B {
            #[serde(flatten)]
            inner: C,
        }
        #[derive(Serialize, ToSchema)]
        struct C {
            text: String,
        }

        let mut openapi = OpenApiBuilder::new()
            .components(Some(
                ComponentsBuilder::new()
                    .schema_from::<MyEnum>()
                    .schema_from::<A>()
                    .schema_from::<B>()
                    .schema_from::<C>()
                    .into(),
            ))
            .build();
        DeriveDiscriminatorMapping.modify(&mut openapi);

        assert_eq!(
            serde_json::to_value(openapi.components.unwrap().schemas).unwrap(),
            serde_json::json!({
                "MyEnum": {
                    "oneOf": [
                    {
                        "$ref": "#/components/schemas/A"
                    },
                    {
                        "$ref": "#/components/schemas/B"
                    }
                    ],
                    "discriminator": {
                        "propertyName": "type",
                        "mapping": {
                            "aaa": "#/components/schemas/A",
                            "bbb": "#/components/schemas/B"
                        }
                    }
                },
                "A": {
                    "type":"object",
                    "required": [
                       "type",
                       "text",
                    ],
                    "properties": {
                        "text": {
                            "type":"string",
                        },
                        "type": {
                            "type":"string",
                            "enum": [
                               "aaa",
                            ],
                        },
                    },
                },
                "B": {
                    "allOf": [
                        {
                            "$ref":"#/components/schemas/C",
                        },
                        {
                            "type":"object",
                            "required": [
                               "type",
                            ],
                            "properties": {
                                "type": {
                                    "type":"string",
                                    "enum": [
                                       "bbb",
                                    ],
                                },
                            },
                        },
                    ],
                },
                "C": {
                    "type":"object",
                    "required": [
                       "text",
                    ],
                    "properties": {
                        "text": {
                            "type":"string",
                        },
                    },
                },
            })
        );
    }
}
