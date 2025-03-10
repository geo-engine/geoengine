/// This macro generates a type tag enum for a given type name.
/// It automatically implements `ToSchema`, `Deserialize`, and `Serialize` for the enum.
macro_rules! type_tag(
    ($name: ident) => { type_tag!($name, Tag); };

    ($name: ident, $suffix: ident) => {
        paste::paste! {

        #[derive(
            Debug, Default, Clone, Copy,
            PartialEq, Eq, Hash,
            utoipa::ToSchema,
            serde::Deserialize, serde::Serialize
        )]
        #[serde(rename_all = "camelCase")]
        pub enum [<$name Tag>] {
            #[default]
            $name,
        }

        impl std::fmt::Display for [<$name  Tag>] {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let value = serde_json::to_value(self).expect("failed to serialize type tag");
                let value_str = value.as_str().expect("failed to convert type tag to string");
                write!(f, "{value_str}")
            }
        }
    }

    };
);

#[cfg(test)]
mod tests {
    #[test]
    fn it_serializes() {
        type_tag!(Foo);
        let serialized = serde_json::to_string(&FooTag::default()).unwrap();
        assert_eq!(serialized, "\"foo\"");
        serde_json::from_str::<FooTag>(&serialized).unwrap();

        assert_eq!(FooTag::default().to_string(), "foo");
    }
}
