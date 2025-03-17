use crate::{Result, util::parse_config_args};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, Field, FieldMutability, Ident, Lit, parse_quote, parse2, spanned::Spanned};

const DEFAULT_DISCRIMINATOR: &str = "type";

pub fn type_tag(attr: TokenStream, item: &TokenStream) -> Result<TokenStream, syn::Error> {
    let mut ast = parse2::<DeriveInput>(item.clone())?;

    let syn::Data::Struct(struct_data) = &mut ast.data else {
        return Err(syn::Error::new_spanned(
            &ast,
            "type tag can only be derived for structs",
        ));
    };

    let syn::Fields::Named(fields) = &mut struct_data.fields else {
        return Err(syn::Error::new_spanned(
            &ast,
            "type tag can only be derived for named structs",
        ));
    };

    let inputs = parse_config_args(attr)?;

    let Some(literal) = inputs.get("tag") else {
        return Err(syn::Error::new_spanned(
            &ast,
            "type tag requires a `tag` argument",
        ));
    };

    let tag_field = match inputs.get("discriminator") {
        Some(Lit::Str(tag_field)) => tag_field.value(),
        Some(_) => {
            return Err(syn::Error::new_spanned(
                &ast,
                "the `discriminator` argument must be a string",
            ));
        }
        None => DEFAULT_DISCRIMINATOR.into(),
    };

    let type_name = &ast.ident;
    let newtype_name = Ident::new(&format!("{type_name}TypeTag"), type_name.span());

    let type_def = quote! {
        #[derive(
            Debug, Default, Clone, Copy,
            PartialEq, Eq, Hash,
            utoipa::ToSchema,
            serde::Deserialize, serde::Serialize
        )]
        #[serde(rename_all = "camelCase")]
        pub enum #newtype_name {
            #[default]
            #[serde(rename = #literal)]
            #newtype_name,
        }

        impl std::fmt::Display for #newtype_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, #literal)
            }
        }
    };

    let field = Field {
        attrs: vec![parse_quote! { #[schema(inline)] }],
        vis: parse_quote! { pub },
        mutability: FieldMutability::None,
        ident: Some(Ident::new_raw(&tag_field, fields.span())),
        colon_token: Default::default(),
        ty: parse_quote!(#newtype_name),
    };

    // prepend as first field
    fields.named.insert(0, field);

    Ok(quote! {
        #type_def

        #ast
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_eq_pretty;

    #[test]
    fn it_rewrites() {
        let input = quote! {
            #[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
            #[serde(rename_all = "camelCase")]
            struct Foo {
                bar: String,
            }
        };

        let output = quote! {
            #[derive(
                Debug, Default, Clone, Copy,
                PartialEq, Eq, Hash,
                utoipa::ToSchema,
                serde::Deserialize, serde::Serialize
            )]
            #[serde(rename_all = "camelCase")]
            pub enum FooTypeTag {
                #[default]
                #[serde(rename = "asd")]
                FooTypeTag,
            }

            impl std::fmt::Display for FooTypeTag {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "asd")
                }
            }

            #[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
            #[serde(rename_all = "camelCase")]
            struct Foo {
                #[schema(inline)]
                pub r#type: FooTypeTag,
                bar: String,
            }
        };

        assert_eq_pretty!(
            type_tag(quote! {tag = "asd"}, &input).unwrap().to_string(),
            output.to_string()
        );
    }
}
