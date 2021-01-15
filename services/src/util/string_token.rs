/// Generates a serializable string token
///
/// # Examples
///
/// `string_token!(Foo)` generates `Foo` that is serialized as `"Foo"`
///
/// `string_token!(Bar "bar")` generates `Bar` that is serialized as `"bar"`
///
#[macro_export]
macro_rules! string_token {
    ($struct:ident) => {
        string_token!($struct, stringify!($struct));
    };

    ($struct:ident, $string:expr) => {
        #[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
        pub struct $struct;

        impl serde::Serialize for $struct {
            paste::paste! {
                fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
                where
                    S: serde::Serializer,
                {
                    serializer.serialize_str($string)
                }
            }
        }

        impl<'de> serde::Deserialize<'de> for $struct {
            fn deserialize<D>(
                deserializer: D,
            ) -> Result<Self, <D as serde::Deserializer<'de>>::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct DeserializeVisitor;

                impl<'a> serde::de::Visitor<'a> for DeserializeVisitor {
                    type Value = $struct;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                        formatter.write_str($string)
                    }

                    fn visit_borrowed_str<E>(self, v: &'a str) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        use serde::de::Unexpected;

                        if v == $string {
                            Ok($struct)
                        } else {
                            Err(E::invalid_value(Unexpected::Str(v), &self))
                        }
                    }

                    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        self.visit_borrowed_str(v)
                    }
                }

                deserializer.deserialize_str(DeserializeVisitor)
            }
        }
    };
}

#[cfg(test)]
mod tests {

    #[test]
    fn serialize() {
        string_token!(Foo);
        string_token!(Bar, "bar");

        assert_eq!(
            serde_json::to_string(&Foo).unwrap(),
            serde_json::json! {"Foo"}.to_string()
        );
        assert_eq!(
            serde_json::to_string(&Bar).unwrap(),
            serde_json::json! {"bar"}.to_string()
        );

        serde_json::from_str::<Foo>(&serde_json::json! {"Foo"}.to_string()).unwrap();
        serde_json::from_str::<Bar>(&serde_json::json! {"bar"}.to_string()).unwrap();
    }

    #[test]
    fn binary() {
        string_token!(Foo);
        string_token!(Bar, "bar");

        let serialized_bytes: Vec<u8> = serde_json::to_string(&Foo).unwrap().into_bytes();
        let _: Foo = serde_json::from_reader(serialized_bytes.as_slice()).unwrap();
    }
}
