use std::collections::BTreeMap;

use utoipa::{
    openapi::{Discriminator, OneOfBuilder, Ref, RefOr, Schema},
    Modify,
};

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
                let Some(reference) = Self::get_base_type_name(schema) else { return None; };
                let Some(RefOr::T(Schema::Object(referenced_object))) = all_schemas.get(reference) else { return None; };
                let Some(mut obj_with_discrimator_prop) = ao.items.iter().find_map(|item| match item {
                    RefOr::T(Schema::Object(concrete)) => Some(concrete.clone()),
                    _ => None,
                }) else { return None; };
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
        let old_components = openapi.components.as_ref().unwrap();
        let old_schemas = &old_components.schemas;
        let mut new_schemas = old_schemas.clone();

        'outer: for (schema_name, ref_or) in old_schemas {
            let RefOr::T(schema) = ref_or else {
                continue;
            };
            let Schema::OneOf(one_of) = schema else {
                continue;
            };
            let Some(Discriminator { property_name: discriminator, .. }) = &one_of.discriminator else {
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
                let Some(variant_tag) = Self::get_variant_tag(item, discriminator) else { continue 'outer; };
                /*let variant_schema_name = format!(
                    "{}With{}",
                    Self::get_base_type_name(item).map_or_else(
                        || format!(
                            "{}{}",
                            schema_name,
                            Self::uppercase_first_letter(variant_tag)
                        ),
                        std::borrow::ToOwned::to_owned
                    ),
                    Self::uppercase_first_letter(discriminator)
                );*/
                let variant_schema_name = match Self::get_base_type_name(item) {
                    Some(base_type) => format!(
                        "{}With{}",
                        base_type,
                        Self::uppercase_first_letter(discriminator),
                    ),
                    None => format!(
                        "{}{}",
                        schema_name,
                        Self::uppercase_first_letter(variant_tag)
                    ),
                };

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
