use std::collections::BTreeMap;

use utoipa::{
    openapi::{Discriminator, OneOfBuilder, Ref, RefOr, Schema},
    Modify,
};

use super::openapi_visitors::get_schema_use_counts;

pub struct OpenApiServerInfo;

impl Modify for OpenApiServerInfo {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let web_config: crate::util::config::Web =
            crate::util::config::get_config_element().expect("web config");

        let mut api_url = web_config.api_url().expect("external address").to_string();
        api_url.pop(); //remove trailing slash because codegen requires it

        openapi.servers = Some(vec![utoipa::openapi::ServerBuilder::new()
            .url(api_url)
            .build()]);
    }
}

/// This implements the transformation proposed in <https://github.com/juhaku/utoipa/issues/617>.
/// At the time of writing utoipa inlines the enum variants when a serde tag is set. This is not
/// compliant with the OpenAPI spec. Because of that the Code Generator at
/// <https://openapi-generator.tech/docs/generators/python> can not be used.
/// This modifier solves the problem by creating separate schemas for each variant and linking
/// them into a `OneOf` using a discriminator with a custom mapping.
pub struct TransformSchemasWithTag;

impl TransformSchemasWithTag {
    fn get_variant_tag<'a>(schema: &'a Schema, discriminator: &String) -> Option<&'a str> {
        match schema {
            Schema::Object(obj) => {
                obj.properties
                    .get(discriminator)
                    .and_then(|ref_or| match ref_or {
                        RefOr::T(Schema::Object(prop)) => {
                            prop.enum_values.as_ref().and_then(|enum_values| {
                                enum_values.first().and_then(serde_json::Value::as_str)
                            })
                        }
                        _ => None,
                    })
            }
            Schema::AllOf(ao) => ao.items.iter().find_map(|item| match item {
                RefOr::Ref(_) => None,
                RefOr::T(concrete) => Self::get_variant_tag(concrete, discriminator),
            }),
            _ => None,
        }
    }

    fn trim_ref_location(reference: &Ref) -> &str {
        const SCHEMA_REF_PREFIX_LEN: usize = "#/components/schemas/".len();
        &reference.ref_location[SCHEMA_REF_PREFIX_LEN..]
    }

    fn get_base_type_name(schema: &Schema) -> Option<&str> {
        match schema {
            Schema::AllOf(ao) => ao.items.iter().find_map(|item| match item {
                RefOr::Ref(reference) => Some(Self::trim_ref_location(reference)),
                RefOr::T(_) => None,
            }),
            _ => None,
        }
    }

    fn get_title(schema: &Schema) -> Option<&String> {
        match schema {
            Schema::Array(arr) => arr.title.as_ref(),
            Schema::Object(obj) => obj.title.as_ref(),
            Schema::OneOf(oo) => oo.title.as_ref(),
            Schema::AllOf(ao) => ao.title.as_ref(),
            _ => None,
        }
    }

    fn uppercase_first_letter(s: &str) -> String {
        let mut chars = s.chars();
        match chars.next() {
            None => String::new(),
            Some(c1) => c1.to_uppercase().collect::<String>() + chars.as_str(),
        }
    }

    fn flatten_allof(
        schema: &Schema,
        all_schemas: &BTreeMap<String, RefOr<Schema>>,
    ) -> Option<Schema> {
        match schema {
            Schema::AllOf(ao) => {
                let Some(reference) = Self::get_base_type_name(schema) else {
                    return None;
                };
                let Some(RefOr::T(Schema::Object(referenced_object))) = all_schemas.get(reference)
                else {
                    return None;
                };
                let Some(mut obj_with_discrimator_prop) =
                    ao.items.iter().find_map(|item| match item {
                        RefOr::T(Schema::Object(concrete)) => Some(concrete.clone()),
                        _ => None,
                    })
                else {
                    return None;
                };
                let mut final_obj = referenced_object.clone();
                final_obj
                    .properties
                    .append(&mut obj_with_discrimator_prop.properties);
                final_obj
                    .required
                    .append(&mut obj_with_discrimator_prop.required);
                Some(Schema::Object(final_obj))
            }
            _ => None,
        }
    }
}

impl Modify for TransformSchemasWithTag {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let Some(old_components) = openapi.components.as_ref() else {
            debug_assert!(openapi.components.as_ref().is_some());
            return;
        };
        let schema_uses = get_schema_use_counts(openapi);
        let old_schemas = &old_components.schemas;
        let mut new_schemas = old_schemas.clone();

        'outer: for (schema_name, ref_or) in old_schemas {
            let RefOr::T(schema) = ref_or else {
                continue;
            };
            let Schema::OneOf(one_of) = schema else {
                continue;
            };
            let Some(Discriminator {
                property_name: discriminator,
                ..
            }) = &one_of.discriminator
            else {
                continue;
            };
            let mut items: Vec<&Schema> = Vec::new();

            for item in &one_of.items {
                match item {
                    RefOr::Ref(_) => continue 'outer,
                    RefOr::T(concrete) => items.push(concrete),
                }
            }
            let mut new_discriminator = Discriminator::new(discriminator.clone());
            let mut one_of_builder = OneOfBuilder::new();

            for item in items {
                let Some(variant_tag) = Self::get_variant_tag(item, discriminator) else {
                    continue 'outer;
                };

                let variant_schema_name = match Self::get_title(item) {
                    // Always prefer custom title as variant name.
                    Some(title) => title.to_owned(),
                    None => match Self::get_base_type_name(item) {
                        // If the variant has a single tuple payload, try using
                        // the name unchanged and just add a "type" field to it.
                        Some(base_type_name) => base_type_name.to_owned(),
                        // If the autogenerated name sounds bad in some cases, you can
                        // set #[schema(title = "XXX")] or create a new struct type and
                        // use it as tuple variant. Then the hardcoded name will be used.
                        None => format!(
                            "{}{}",
                            schema_name,
                            Self::uppercase_first_letter(variant_tag)
                        ),
                    },
                };

                if matches!(schema_uses.get(&variant_schema_name), Some(count) if count != &1usize)
                {
                    panic!("The type {variant_schema_name} is used in the enum {schema_name} as payload, but also in other places. \
                    You have to use a newly created struct, anonymous struct variant or set #[schema(title = \"XXX\")] on the variant.");
                }

                if let Some(flattened) = Self::flatten_allof(item, old_schemas) {
                    new_schemas.insert(variant_schema_name.clone(), flattened.into());
                } else {
                    new_schemas.insert(variant_schema_name.clone(), item.clone().into());
                }

                let reference = Ref::from_schema_name(variant_schema_name.clone());
                new_discriminator
                    .mapping
                    .insert(variant_tag.to_string(), reference.ref_location.clone());
                one_of_builder = one_of_builder.item(reference);
            }
            one_of_builder = one_of_builder.discriminator(Some(new_discriminator));

            new_schemas.insert(schema_name.clone(), one_of_builder.into());
        }
        let mut new_components = old_components.clone();
        new_components.schemas = new_schemas;
        openapi.components = Some(new_components);
    }
}

#[cfg(test)]
mod tests {
    use crate::util::apidoc::TransformSchemasWithTag;
    use assert_json_diff::assert_json_eq;
    use serde::Serialize;
    use serde_json::json;
    use utoipa::{openapi::*, Modify, ToSchema};

    #[test]
    #[allow(dead_code)]
    fn separate_inline_schema() {
        #[derive(Serialize, ToSchema)]
        #[serde(rename_all = "camelCase", tag = "type")]
        enum MyEnum {
            Inlined { v: usize },
            B(MyStruct),
        }
        #[derive(Serialize, ToSchema)]
        struct MyStruct {
            text: String,
        }
        let mut openapi = OpenApiBuilder::new()
            .components(Some(
                ComponentsBuilder::new()
                    .schema_from::<MyEnum>()
                    .schema_from::<MyStruct>()
                    .into(),
            ))
            .build();
        let transformer = TransformSchemasWithTag;
        transformer.modify(&mut openapi);

        assert_json_eq!(
            serde_json::to_value(openapi.components.unwrap().schemas).unwrap(),
            json!({
                "MyEnum": {
                    "oneOf": [
                        {
                            "$ref": "#/components/schemas/MyEnumInlined"
                        },
                        {
                            "$ref": "#/components/schemas/MyStruct"
                        }
                    ],
                    "discriminator": {
                        "propertyName": "type",
                        "mapping": {
                            "inlined": "#/components/schemas/MyEnumInlined",
                            "b": "#/components/schemas/MyStruct"
                        }
                    }
                },
                "MyStruct": {
                    "type": "object",
                    "required": [
                        "text",
                        "type"
                    ],
                    "properties": {
                        "type": {
                            "type": "string",
                            "enum": [
                                "b"
                            ]
                        },
                        "text": {
                            "type": "string"
                        }
                    }
                },
                "MyEnumInlined": {
                    "type": "object",
                    "required": [
                        "v",
                        "type"
                    ],
                    "properties": {
                        "type": {
                            "type": "string",
                            "enum": [
                                "inlined"
                            ]
                        },
                        "v": {
                            "type": "integer",
                            "minimum": 0
                        }
                    }
                }
            })
        );
    }

    #[test]
    #[allow(dead_code)]
    fn resolve_inner_allof() {
        #[derive(Serialize, ToSchema)]
        #[serde(tag = "type", rename_all = "camelCase")]
        enum MyEnum {
            X(MyStruct),
        }
        #[derive(Serialize, ToSchema)]
        struct MyStruct {
            name: String,
        }
        let mut openapi = OpenApiBuilder::new()
            .components(Some(
                ComponentsBuilder::new()
                    .schema_from::<MyEnum>()
                    .schema_from::<MyStruct>()
                    .into(),
            ))
            .build();
        let transformer = TransformSchemasWithTag;
        transformer.modify(&mut openapi);

        assert_json_eq!(
            serde_json::to_value(openapi.components.unwrap().schemas).unwrap(),
            json!({
                "MyEnum": {
                    "oneOf": [
                        {
                            "$ref": "#/components/schemas/MyStruct"
                        }
                    ],
                    "discriminator": {
                        "propertyName": "type",
                        "mapping": {
                            "x": "#/components/schemas/MyStruct",
                        }
                    }
                },
                "MyStruct": {
                    "type": "object",
                    "required": [
                        "name",
                        "type"
                    ],
                    "properties": {
                        "type": {
                            "type": "string",
                            "enum": [
                                "x"
                            ]
                        },
                        "name": {
                            "type": "string"
                        }
                    }
                },
            })
        );
    }

    #[test]
    #[allow(dead_code)]
    fn use_custom_title() {
        #[derive(Serialize, ToSchema)]
        #[serde(rename_all = "camelCase", tag = "type")]
        enum MyEnum {
            #[schema(title = "OVERRIDDEN")]
            X { val: String },
        }
        let mut openapi = OpenApiBuilder::new()
            .components(Some(
                ComponentsBuilder::new().schema_from::<MyEnum>().into(),
            ))
            .build();
        let transformer = TransformSchemasWithTag;
        transformer.modify(&mut openapi);

        assert_json_eq!(
            serde_json::to_value(openapi.components.unwrap().schemas).unwrap(),
            json!({
                "MyEnum": {
                    "oneOf": [
                        {
                            "$ref": "#/components/schemas/OVERRIDDEN"
                        }
                    ],
                    "discriminator": {
                        "propertyName": "type",
                        "mapping": {
                            "x": "#/components/schemas/OVERRIDDEN",
                        }
                    }
                },
                "OVERRIDDEN": {
                    "title": "OVERRIDDEN",
                    "type": "object",
                    "required": [
                        "val",
                        "type"
                    ],
                    "properties": {
                        "type": {
                            "type": "string",
                            "enum": [
                                "x"
                            ]
                        },
                        "val": {
                            "type": "string"
                        }
                    }
                }
            })
        );
    }
}
