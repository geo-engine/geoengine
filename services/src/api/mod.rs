use std::collections::BTreeMap;
use utoipa::openapi::{OpenApi, Ref, RefOr, Response, Schema};

pub mod model;

fn throw_if_invalid_ref(reference: Ref, schemas: &BTreeMap<String, RefOr<Schema>>) {
    assert!(
        schemas.contains_key(&reference.ref_location[21..]),
        "Referenced the unknown schema `{}`",
        reference.ref_location
    );
}

fn contains_missing_schema(schema: RefOr<Schema>, schemas: &BTreeMap<String, RefOr<Schema>>) {
    match schema {
        RefOr::Ref(reference) => {
            throw_if_invalid_ref(reference, schemas);
        }
        RefOr::T(concrete) => match concrete {
            Schema::Array(arr) => {
                contains_missing_schema(*arr.items, &schemas);
            }
            Schema::Object(obj) => {
                for property in obj.properties.into_values() {
                    contains_missing_schema(property, schemas);
                }
                if let Some(additional_properties) = obj.additional_properties {
                    contains_missing_schema(*additional_properties, schemas);
                }
            }
            Schema::OneOf(oo) => {
                for item in oo.items {
                    contains_missing_schema(item, schemas);
                }
            }
            Schema::AllOf(ao) => {
                for item in ao.items {
                    contains_missing_schema(item, schemas);
                }
            }
            _ => panic!("Unknown schema type"),
        },
    }
}

pub fn find_missing_schemata(api: OpenApi) {
    let schemas = api
        .components
        .expect("api has at least one component")
        .schemas;

    for path_item in api.paths.paths.into_values() {
        for operation in path_item.operations.into_values() {
            if let Some(request_body) = operation.request_body {
                for content in request_body.content.into_values() {
                    contains_missing_schema(content.schema, &schemas);
                }
            }

            for response in operation.responses.responses.into_values() {
                match response {
                    RefOr::Ref(reference) => {
                        throw_if_invalid_ref(reference, &schemas);
                    }
                    RefOr::T(concrete) => {
                        for content in concrete.content.into_values() {
                            contains_missing_schema(content.schema, &schemas);
                        }
                    }
                }
            }
        }
    }
}
