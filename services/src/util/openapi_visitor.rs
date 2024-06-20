use utoipa::openapi::{
    schema::AdditionalProperties, Components, OpenApi, Ref, RefOr, Response, Schema,
};

pub trait OpenapiVisitor {
    fn resolve_failed(&mut self, ref_location: &String);

    fn visit_schema_component(&mut self, name: &str, schema: &RefOr<Schema>);
}

/// Recursively checks that schemas referenced in the given schema object exist in the provided map.
fn visit_schema<T: OpenapiVisitor>(
    schema: &RefOr<Schema>,
    components: &Components,
    visitor: &mut T,
) {
    match schema {
        RefOr::Ref(reference) => {
            visit_reference(reference, components, visitor);
        }
        RefOr::T(concrete) =>        match concrete {
                Schema::Array(arr) => {
                    visit_schema(arr.items.as_ref(), components, visitor);
                }
                Schema::Object(obj) => {
                    for property in obj.properties.values() {
                        visit_schema(property, components, visitor);
                    }
                    if let Some(additional_properties) = &obj.additional_properties {
                        if let AdditionalProperties::RefOr(properties_schema) =
                            additional_properties.as_ref()
                        {
                            visit_schema(properties_schema, components, visitor);
                        }
                    }
                }
                Schema::OneOf(oo) => {
                    for item in &oo.items {
                        visit_schema(item, components, visitor);
                    }
                }
                Schema::AllOf(ao) => {
                    for item in &ao.items {
                        visit_schema(item, components, visitor);
                    }
                }
                _ => panic!("Unknown schema type"),
            }
    }
}

/// Recursively checks that schemas referenced in the given response object exist in the provided map.
fn visit_response<T: OpenapiVisitor>(
    response: &RefOr<Response>,
    components: &Components,
    visitor: &mut T,
) {
    match response {
        RefOr::Ref(reference) => {
            visit_reference(reference, components, visitor);
        }
        RefOr::T(concrete) => {
            for content in concrete.content.values() {
                visit_schema(&content.schema, components, visitor);
            }
        }
    }
}

/// Checks that the given reference can be resolved using the provided map.
fn visit_reference<T: OpenapiVisitor>(reference: &Ref, components: &Components, visitor: &mut T) {
    const SCHEMA_REF_PREFIX: &str = "#/components/schemas/";
    const RESPONSE_REF_PREFIX: &str = "#/components/responses/";

    let ref_location = &reference.ref_location;

    if ref_location.starts_with(SCHEMA_REF_PREFIX) {
        let schema_name = &ref_location[SCHEMA_REF_PREFIX.len()..];

        match components.schemas.get(schema_name) {
            None => visitor.resolve_failed(ref_location),
            Some(resolved) => {
                visitor.visit_schema_component(schema_name, resolved);
                visit_schema(resolved, components, visitor);
            },
        }
    } else if ref_location.starts_with(RESPONSE_REF_PREFIX) {
        let response_name = &ref_location[RESPONSE_REF_PREFIX.len()..];

        match components.responses.get(response_name) {
            None => visitor.resolve_failed(ref_location),
            Some(resolved) => visit_response(resolved, components, visitor),
        }
    } else {
        visitor.resolve_failed(ref_location);
    }
}

/// Loops through all registered HTTP handlers and ensures that the referenced schemas
/// (inside of request bodies, parameters or responses) exist and can be resolved.
pub fn visit_api<T: OpenapiVisitor>(api: OpenApi, visitor: &mut T) {
    let components = api.components.expect("api has at least one component");

    for path_item in api.paths.paths.into_values() {
        for operation in path_item.operations.into_values() {
            if let Some(request_body) = operation.request_body {
                for content in request_body.content.into_values() {
                    visit_schema(&content.schema, &components, visitor);
                }
            }

            if let Some(parameters) = operation.parameters {
                for parameter in parameters {
                    if let Some(schema) = parameter.schema {
                        visit_schema(&schema, &components, visitor);
                    }
                }
            }

            for response in operation.responses.responses.into_values() {
                match response {
                    RefOr::Ref(reference) => {
                        visit_reference(&reference, &components, visitor);
                    }
                    RefOr::T(concrete) => {
                        for content in concrete.content.into_values() {
                            visit_schema(&content.schema, &components, visitor);
                        }
                    }
                }
            }
        }
    }
}

pub struct CanResolveVisitor {
    pub unknown_ref: Option<String>,
}

impl OpenapiVisitor for CanResolveVisitor {
    fn resolve_failed(&mut self, ref_location: &String) {
        self.unknown_ref = Some(ref_location.to_owned());
    }

    fn visit_schema_component(&mut self, _name: &str, _schema: &RefOr<Schema>) {
        //noop
    }
}

#[cfg(test)]
mod tests {
    use utoipa::openapi::{
        AllOfBuilder, ArrayBuilder, ComponentsBuilder, ObjectBuilder, OneOfBuilder,
    };

    use super::*;

    fn try_resolve_schema(schema: &RefOr<Schema>, components: &Components) {
        let mut visitor = CanResolveVisitor { unknown_ref: None };
        visit_schema(schema, components, &mut visitor);

        if let Some(unknown_ref) = visitor.unknown_ref {
            panic!("Cannot resolve reference {}", unknown_ref);
        }
    }

    #[test]
    #[should_panic(expected = "MissingSchema")]
    fn detects_missing_array_ref() {
        try_resolve_schema(
            &RefOr::T(
                ArrayBuilder::new()
                    .items(Ref::from_schema_name("MissingSchema"))
                    .into(),
            ),
            &Components::new(),
        );
    }

    #[test]
    #[should_panic(expected = "MissingSchema")]
    fn detects_missing_object_ref() {
        try_resolve_schema(
            &RefOr::T(
                ObjectBuilder::new()
                    .property("Prop", Ref::from_schema_name("MissingSchema"))
                    .into(),
            ),
            &Components::new(),
        );
    }

    #[test]
    #[should_panic(expected = "MissingSchema")]
    fn detects_missing_oneof_ref() {
        try_resolve_schema(
            &RefOr::T(
                OneOfBuilder::new()
                    .item(Ref::from_schema_name("MissingSchema"))
                    .into(),
            ),
            &Components::new(),
        );
    }

    #[test]
    #[should_panic(expected = "MissingSchema")]
    fn detects_missing_allof_ref() {
        try_resolve_schema(
            &RefOr::T(
                AllOfBuilder::new()
                    .item(Ref::from_schema_name("MissingSchema"))
                    .into(),
            ),
            &Components::new(),
        );
    }

    #[test]
    #[should_panic(expected = "Inner")]
    fn detects_missing_nested_schema() {
        try_resolve_schema(
            &RefOr::Ref(Ref::from_schema_name("Outer")),
            &ComponentsBuilder::new()
                .schema("Outer", RefOr::Ref(Ref::from_schema_name("Inner")))
                .into(),
        );
    }
}
