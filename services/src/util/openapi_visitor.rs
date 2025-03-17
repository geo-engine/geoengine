use utoipa::openapi::{
    Components, OpenApi, Ref, RefOr, Response, Schema, schema::AdditionalProperties,
};

pub trait OpenapiVisitor {
    fn resolve_failed(&mut self, _ref_location: &str) {}

    fn visit_schema_component(
        &mut self,
        _name: &str,
        _schema: &RefOr<Schema>,
        _source_location: &str,
    ) {
    }
}

/// Recursively walks through the given schema object and calls corresponding events on the visitor.
///
/// # Panics
///
/// Panics if a schema has an unknown type.
pub fn visit_schema<T: OpenapiVisitor>(
    schema: &RefOr<Schema>,
    components: &Components,
    visitor: &mut T,
    source_location: &str,
) {
    match schema {
        RefOr::Ref(reference) => {
            visit_reference(reference, components, visitor, source_location);
        }
        RefOr::T(concrete) => match concrete {
            Schema::Array(arr) => {
                visit_schema(arr.items.as_ref(), components, visitor, source_location);
            }
            Schema::Object(obj) => {
                for property in obj.properties.values() {
                    visit_schema(property, components, visitor, source_location);
                }
                if let Some(additional_properties) = &obj.additional_properties {
                    if let AdditionalProperties::RefOr(properties_schema) =
                        additional_properties.as_ref()
                    {
                        visit_schema(properties_schema, components, visitor, source_location);
                    }
                }
            }
            Schema::OneOf(oo) => {
                for item in &oo.items {
                    visit_schema(item, components, visitor, source_location);
                }
            }
            Schema::AllOf(ao) => {
                for item in &ao.items {
                    visit_schema(item, components, visitor, source_location);
                }
            }
            _ => panic!("Unknown schema type"),
        },
    }
}

/// Recursively walks through the given response object and calls corresponding events on the visitor.
///
/// # Panics
///
/// Panics if a schema has an unknown type.
fn visit_response<T: OpenapiVisitor>(
    response: &RefOr<Response>,
    components: &Components,
    visitor: &mut T,
    source_location: &str,
) {
    match response {
        RefOr::Ref(reference) => {
            visit_reference(reference, components, visitor, source_location);
        }
        RefOr::T(concrete) => {
            for content in concrete.content.values() {
                visit_schema(&content.schema, components, visitor, source_location);
            }
        }
    }
}

/// Resolves the given reference, recursively walks through the target and
/// calls corresponding events on the visitor.
///
/// # Panics
///
/// Panics if a schema has an unknown type.
fn visit_reference<T: OpenapiVisitor>(
    reference: &Ref,
    components: &Components,
    visitor: &mut T,
    source_location: &str,
) {
    const SCHEMA_REF_PREFIX: &str = "#/components/schemas/";
    const RESPONSE_REF_PREFIX: &str = "#/components/responses/";

    let ref_location = reference.ref_location.as_str();

    if let Some(schema_name) = ref_location.strip_prefix(SCHEMA_REF_PREFIX) {
        match components.schemas.get(schema_name) {
            None => visitor.resolve_failed(ref_location),
            Some(resolved) => {
                visitor.visit_schema_component(schema_name, resolved, source_location);
                visit_schema(resolved, components, visitor, ref_location);
            }
        }
    } else if let Some(response_name) = ref_location.strip_prefix(RESPONSE_REF_PREFIX) {
        match components.responses.get(response_name) {
            None => visitor.resolve_failed(ref_location),
            Some(resolved) => visit_response(resolved, components, visitor, ref_location),
        }
    } else {
        visitor.resolve_failed(ref_location);
    }
}

/// Recursively walks through all registered HTTP handlers and the referenced schemas
/// (inside of request bodies, parameters or responses) and calls corresponding events
/// on the visitor.
///
/// # Panics
///
/// Panics if a schema has an unknown type.
pub fn visit_api<T: OpenapiVisitor>(api: &OpenApi, visitor: &mut T) {
    let Some(components) = api.components.as_ref() else {
        debug_assert!(api.components.as_ref().is_some());
        return;
    };

    for (source_location, path_item) in &api.paths.paths {
        if let Some(parameters) = &path_item.parameters {
            for parameter in parameters {
                if let Some(schema) = parameter.schema.as_ref() {
                    visit_schema(schema, components, visitor, source_location);
                }
            }
        }

        for operation in path_item.operations.values() {
            if let Some(request_body) = operation.request_body.as_ref() {
                for content in request_body.content.values() {
                    visit_schema(&content.schema, components, visitor, source_location);
                }
            }

            if let Some(parameters) = operation.parameters.as_ref() {
                for parameter in parameters {
                    if let Some(schema) = parameter.schema.as_ref() {
                        visit_schema(schema, components, visitor, source_location);
                    }
                }
            }

            for response in operation.responses.responses.values() {
                match response {
                    RefOr::Ref(reference) => {
                        visit_reference(reference, components, visitor, source_location);
                    }
                    RefOr::T(concrete) => {
                        for content in concrete.content.values() {
                            visit_schema(&content.schema, components, visitor, source_location);
                        }
                    }
                }
            }
        }
    }
}
