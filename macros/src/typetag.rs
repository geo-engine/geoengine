use crate::{util::parse_config_args, Result};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{parse2, parse_quote, spanned::Spanned, DeriveInput, Field, FieldMutability, Ident};

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

    let newtype_name = Ident::new(&format!("{}TypeTag", ast.ident), ast.ident.span());

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
        ident: Some(Ident::new_raw("type", fields.span())),
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
