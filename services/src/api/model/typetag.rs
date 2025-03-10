/// This macro generates a type tag enum for a given type name.
/// It automatically implements `ToSchema`, `Deserialize`, and `Serialize` for the enum.
macro_rules! type_tag(
    ($name: ident) => {
        paste::paste! {

        #[derive(
            Debug, Default, Clone, Copy,
            PartialEq, Eq,
            utoipa::ToSchema,
            serde::Deserialize, serde::Serialize
        )]
        #[serde(rename_all = "camelCase")]
        pub enum [<TypeTag $name>] {
            #[default]
            $name,
        }

        impl std::fmt::Display for [<TypeTag $name>] {
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
        let serialized = serde_json::to_string(&TypeTagFoo::default()).unwrap();
        assert_eq!(serialized, "\"foo\"");
        serde_json::from_str::<TypeTagFoo>(&serialized).unwrap();

        assert_eq!(TypeTagFoo::default().to_string(), "foo");
    }
}
