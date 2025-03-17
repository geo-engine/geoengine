use std::collections::{HashMap, HashSet};

use utoipa::openapi::{OpenApi, RefOr, Schema};

use super::openapi_visitor::{OpenapiVisitor, visit_api};

struct CanResolveVisitor {
    pub unknown_ref: Option<String>,
}

impl OpenapiVisitor for CanResolveVisitor {
    fn resolve_failed(&mut self, ref_location: &str) {
        self.unknown_ref = Some(ref_location.to_owned());
    }
}

/// Loops through all registered HTTP handlers and ensures that the referenced schemas
/// (inside of request bodies, parameters or responses) exist and can be resolved.
///
/// # Panics
///
/// Panics if a referenced schema cannot be resolved.
///
pub fn can_resolve_api(api: &OpenApi) {
    let mut visitor = CanResolveVisitor { unknown_ref: None };
    visit_api(api, &mut visitor);

    if let Some(unknown_ref) = visitor.unknown_ref {
        panic!("Cannot resolve reference {unknown_ref}");
    }
}

struct SchemaUseCounter {
    parents: HashMap<String, HashSet<String>>,
}

impl SchemaUseCounter {
    fn get_schema_use_counts(self) -> HashMap<String, usize> {
        self.parents
            .into_iter()
            .map(|(key, parent_set)| (key, parent_set.len()))
            .collect()
    }
}

impl OpenapiVisitor for SchemaUseCounter {
    fn visit_schema_component(
        &mut self,
        name: &str,
        _schema: &RefOr<Schema>,
        source_location: &str,
    ) {
        let parent_set = self.parents.entry(name.to_owned()).or_default();
        parent_set.insert(source_location.to_owned());
    }
}

pub fn get_schema_use_counts(api: &OpenApi) -> HashMap<String, usize> {
    let mut visitor = SchemaUseCounter {
        parents: HashMap::new(),
    };
    visit_api(api, &mut visitor);
    visitor.get_schema_use_counts()
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::hashmap;
    use utoipa::openapi::{
        AllOfBuilder, ArrayBuilder, Components, ComponentsBuilder, ContentBuilder, HttpMethod,
        Object, ObjectBuilder, OneOfBuilder, OpenApiBuilder, PathsBuilder, Ref, ResponseBuilder,
        path::{OperationBuilder, ParameterBuilder, PathItemBuilder},
        request_body::RequestBodyBuilder,
    };

    use crate::util::openapi_visitor::visit_schema;

    use super::*;

    fn try_resolve_schema(schema: &RefOr<Schema>, components: &Components) {
        let mut visitor = CanResolveVisitor { unknown_ref: None };
        visit_schema(schema, components, &mut visitor, "root");

        if let Some(unknown_ref) = visitor.unknown_ref {
            panic!("Cannot resolve reference {unknown_ref}");
        }
    }

    fn count_schema_uses(
        schema: &RefOr<Schema>,
        components: &Components,
    ) -> HashMap<String, usize> {
        let mut visitor = SchemaUseCounter {
            parents: HashMap::new(),
        };
        visit_schema(schema, components, &mut visitor, "root");
        visitor.get_schema_use_counts()
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

    #[test]
    fn counts_schema_uses() {
        let uses = count_schema_uses(
            &RefOr::Ref(Ref::from_schema_name("Component")),
            &ComponentsBuilder::new()
                .schema(
                    "Component",
                    OneOfBuilder::new()
                        .item(Ref::from_schema_name("A"))
                        .item(Ref::from_schema_name("B")),
                )
                .schema("A", Object::new())
                .schema("B", Ref::from_schema_name("A"))
                .into(),
        );
        assert_eq!(
            uses,
            hashmap! {
                "Component".to_owned() => 1usize,
                "A".to_owned() => 2usize,
                "B".to_owned() => 1usize
            }
        );
    }

    #[test]
    fn successfull_api_validation() {
        let api: OpenApi = OpenApiBuilder::new()
            .paths(
                PathsBuilder::new().path(
                    "asdf",
                    PathItemBuilder::new()
                        .operation(
                            HttpMethod::Post,
                            OperationBuilder::new()
                                .parameter(
                                    ParameterBuilder::new()
                                        .schema(Some(Ref::from_schema_name("Schema1"))),
                                )
                                .request_body(Some(
                                    RequestBodyBuilder::new()
                                        .content(
                                            "application/json",
                                            ContentBuilder::new()
                                                .schema(Some(Ref::from_schema_name("Schema2")))
                                                .into(),
                                        )
                                        .into(),
                                ))
                                .response(
                                    "200",
                                    ResponseBuilder::new().content(
                                        "application/json",
                                        ContentBuilder::new()
                                            .schema(Some(Ref::from_schema_name("Schema3")))
                                            .into(),
                                    ),
                                ),
                        )
                        .into(),
                ),
            )
            .components(Some(
                ComponentsBuilder::new()
                    .schemas_from_iter([
                        ("Schema1", Schema::default()),
                        ("Schema2", Schema::default()),
                        ("Schema3", Schema::default()),
                    ])
                    .into(),
            ))
            .into();
        can_resolve_api(&api);
    }

    #[test]
    #[should_panic(expected = "MissingSchema")]
    fn detect_unresolvable_request_body() {
        let api: OpenApi = OpenApiBuilder::new()
            .paths(
                PathsBuilder::new().path(
                    "asdf",
                    PathItemBuilder::new()
                        .operation(
                            HttpMethod::Post,
                            OperationBuilder::new().request_body(Some(
                                RequestBodyBuilder::new()
                                    .content(
                                        "application/json",
                                        ContentBuilder::new()
                                            .schema(Some(Ref::from_schema_name("MissingSchema")))
                                            .into(),
                                    )
                                    .into(),
                            )),
                        )
                        .into(),
                ),
            )
            .components(Some(
                ComponentsBuilder::new()
                    .schemas_from_iter([
                        ("Schema1", Schema::default()),
                        ("Schema2", Schema::default()),
                        ("Schema3", Schema::default()),
                    ])
                    .into(),
            ))
            .into();
        can_resolve_api(&api);
    }

    #[test]
    #[should_panic(expected = "MissingSchema")]
    fn detect_unresolvable_parameter() {
        let api: OpenApi = OpenApiBuilder::new()
            .paths(
                PathsBuilder::new().path(
                    "asdf",
                    PathItemBuilder::new()
                        .operation(
                            HttpMethod::Post,
                            OperationBuilder::new().parameter(
                                ParameterBuilder::new()
                                    .schema(Some(Ref::from_schema_name("MissingSchema"))),
                            ),
                        )
                        .into(),
                ),
            )
            .components(Some(
                ComponentsBuilder::new()
                    .schemas_from_iter([
                        ("Schema1", Schema::default()),
                        ("Schema2", Schema::default()),
                        ("Schema3", Schema::default()),
                    ])
                    .into(),
            ))
            .into();
        can_resolve_api(&api);
    }

    #[test]
    #[should_panic(expected = "MissingSchema")]
    fn detect_unresolvable_response() {
        let api: OpenApi = OpenApiBuilder::new()
            .paths(
                PathsBuilder::new().path(
                    "asdf",
                    PathItemBuilder::new()
                        .operation(
                            HttpMethod::Post,
                            OperationBuilder::new().response(
                                "200",
                                ResponseBuilder::new().content(
                                    "application/json",
                                    ContentBuilder::new()
                                        .schema(Some(Ref::from_schema_name("MissingSchema")))
                                        .into(),
                                ),
                            ),
                        )
                        .into(),
                ),
            )
            .components(Some(
                ComponentsBuilder::new()
                    .schemas_from_iter([
                        ("Schema1", Schema::default()),
                        ("Schema2", Schema::default()),
                        ("Schema3", Schema::default()),
                    ])
                    .into(),
            ))
            .into();
        can_resolve_api(&api);
    }
}
