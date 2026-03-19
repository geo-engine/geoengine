use crate::Result;
use crate::testing::AttributeArgs;
use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::parse::Parser;
use syn::{DeriveInput, Ident, LitStr, Type, parse2};

pub fn api_operator(attr: TokenStream, item: &TokenStream) -> Result<TokenStream, syn::Error> {
    let span = proc_macro2::Span::call_site();
    let mut ast = parse2::<DeriveInput>(item.clone())?;

    let syn::Data::Struct(struct_data) = &mut ast.data else {
        return Err(syn::Error::new_spanned(
            &ast,
            "API operator can only be derived for structs",
        ));
    };

    let syn::Fields::Named(fields) = &mut struct_data.fields else {
        return Err(syn::Error::new_spanned(
            &ast,
            "API operator can only be derived for named structs",
        ));
    };

    let ExtractedInputs { title, examples } = parse_inputs(attr)?;

    // read struct name
    let struct_ident: Ident = ast.ident.clone();
    let struct_name = struct_ident.to_string();

    let title = title.unwrap_or_else(|| LitStr::new(&struct_name, span));

    let ExtractedFields {
        params_ty,
        sources_ty,
    } = extract_fields(fields)?;

    let type_tag = LitStr::new(&struct_name, span);

    let attrs = &ast.attrs;

    let source_field = sources_ty
        .map(|sources_ty| quote! { pub sources: #sources_ty, })
        .unwrap_or_default();

    Ok(quote_spanned! { span =>
        #(#attrs)*
        #[type_tag(value = #type_tag)]
        #[derive(
            Debug, Clone,
            PartialEq,
            utoipa::ToSchema,
            serde::Deserialize, serde::Serialize
        )]
        #[serde(rename_all = "camelCase")]
        #[schema(
            title = #title,
            examples(#(#examples),*),
        )]
        pub struct #struct_ident {
            pub params: #params_ty,
            #source_field
        }
    })
}

struct ExtractedFields {
    params_ty: Type,
    sources_ty: Option<Type>,
}

fn extract_fields(fields: &syn::FieldsNamed) -> Result<ExtractedFields, syn::Error> {
    let mut params_ty: Option<Type> = None;
    let mut sources_ty: Option<Type> = None;

    for f in &fields.named {
        let ident = f
            .ident
            .as_ref()
            .ok_or_else(|| syn::Error::new_spanned(f, "expected named field"))?;
        if ident == "params" {
            params_ty = Some(f.ty.clone());
        } else if ident == "sources" {
            sources_ty = Some(f.ty.clone());
        } else {
            return Err(syn::Error::new_spanned(
                ident,
                "unknown field; expected only `params` and `sources`",
            ));
        }
    }

    let params_ty =
        params_ty.ok_or_else(|| syn::Error::new_spanned(fields, "missing `params` field"))?;

    Ok(ExtractedFields {
        params_ty,
        sources_ty,
    })
}

struct ExtractedInputs {
    title: Option<LitStr>,
    examples: Vec<syn::Expr>,
}

fn parse_inputs(attr: TokenStream) -> Result<ExtractedInputs> {
    // AttributeArgs is Punctuated<Meta, Comma>
    let metas = AttributeArgs::parse_terminated.parse2(attr)?;

    let mut title: Option<LitStr> = None;
    let mut examples: Vec<syn::Expr> = Vec::new();

    for meta in &metas {
        let ident = match &meta {
            syn::Meta::NameValue(nv) => nv
                .path
                .get_ident()
                .ok_or_else(|| syn::Error::new_spanned(nv.clone(), "expected identifier"))?
                .to_string(),
            syn::Meta::List(list) => list
                .path
                .get_ident()
                .ok_or_else(|| {
                    syn::Error::new_spanned(list.clone(), "expected ident for meta list")
                })?
                .to_string(),
            syn::Meta::Path(path) => {
                return Err(syn::Error::new_spanned(
                    path,
                    "unexpected attribute argument",
                ));
            }
        };

        match ident.as_str() {
            "title" => {
                if let syn::Meta::NameValue(nv) = &meta
                    && let syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(s),
                        ..
                    }) = &nv.value
                {
                    title = Some(s.clone());
                } else {
                    return Err(syn::Error::new_spanned(
                        meta,
                        "`title` must be a string literal",
                    ));
                }
            }
            "examples" => {
                if let syn::Meta::List(list) = meta {
                    let exprs =
                        syn::punctuated::Punctuated::<syn::Expr, syn::Token![,]>::parse_terminated
                            .parse2(list.tokens.clone())?;
                    for e in exprs {
                        examples.push(e);
                    }
                } else {
                    return Err(syn::Error::new_spanned(meta, "`examples` must be a list"));
                }
            }
            _ => {
                return Err(syn::Error::new_spanned(
                    meta,
                    "unknown attribute for api_operator",
                ));
            }
        }
    }

    if examples.is_empty() {
        return Err(syn::Error::new_spanned(
            metas,
            "missing required `examples` argument",
        ));
    }

    Ok(ExtractedInputs { title, examples })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_eq_pretty;
    use quote::quote;

    #[test]
    fn it_rewrites() {
        let input = quote! {
            /// Really important comment.
            pub struct Histogram {
                pub params: HistogramParameters,
                pub sources: SingleRasterOrVectorSource,
            }
        };

        let output = quote! {
            /// Really important comment.
            #[type_tag(value = "Histogram")]
            #[derive(
                Debug, Clone,
                PartialEq,
                utoipa::ToSchema,
                serde::Deserialize, serde::Serialize
            )]
            #[serde(rename_all = "camelCase")]
            #[schema(
                title = "Cool Histogram",
                examples(serde_json::json!({"foo": "bar"})),
            )]
            pub struct Histogram {
                pub params: HistogramParameters,
                pub sources: SingleRasterOrVectorSource,
            }
        };

        assert_eq_pretty!(
            api_operator(
                quote! {title = "Cool Histogram", examples(serde_json::json!({"foo": "bar"}))},
                &input
            )
            .unwrap()
            .to_string(),
            output.to_string()
        );

        // Second test: only examples provided, title defaults to struct name

        let input = quote! {
            /// Really important comment.
            pub struct Statistics {
                 pub params: StatisticsParameters,
                 pub sources: SingleRasterOrVectorSource,
            }
        };

        let output = quote! {
            /// Really important comment.
            #[type_tag(value = "Statistics")]
            #[derive(
                Debug, Clone,
                PartialEq,
                utoipa::ToSchema,
                serde::Deserialize, serde::Serialize
            )]
            #[serde(rename_all = "camelCase")]
            #[schema(
                title = "Statistics",
                examples(serde_json::json!({"foo": "bar"})),
            )]
            pub struct Statistics {
                pub params: StatisticsParameters,
                pub sources: SingleRasterOrVectorSource,
            }
        };

        assert_eq_pretty!(
            api_operator(quote! {examples(serde_json::json!({"foo": "bar"}))}, &input)
                .unwrap()
                .to_string(),
            output.to_string()
        );

        // Third test: source operator

        let input = quote! {
            /// Really important comment.
            pub struct MySource {
                 pub params: MySourceParameters,
            }
        };

        let output = quote! {
            /// Really important comment.
            #[type_tag(value = "MySource")]
            #[derive(
                Debug, Clone,
                PartialEq,
                utoipa::ToSchema,
                serde::Deserialize, serde::Serialize
            )]
            #[serde(rename_all = "camelCase")]
            #[schema(
                title = "MySource",
                examples(serde_json::json!({"foo": "bar"})),
            )]
            pub struct MySource {
                pub params: MySourceParameters,
            }
        };

        assert_eq_pretty!(
            api_operator(quote! {examples(serde_json::json!({"foo": "bar"}))}, &input)
                .unwrap()
                .to_string(),
            output.to_string()
        );
    }

    #[test]
    fn it_fails() {
        assert!(api_operator(quote! {}, &quote! {}).is_err()); // no value
        assert!(
            api_operator(
                quote! {examples(json!({ "foo": "bar" }))},
                &quote! {
                    enum Foo {
                        Bar,
                    }
                }
            )
            .is_err()
        ); // no struct
        assert!(
            api_operator(
                quote! {examples(json!({ "foo": "bar" }))},
                &quote! {
                    struct Foo(String);
                }
            )
            .is_err()
        ); // no named struct
        assert!(
            api_operator(
                quote! {examples(json!({ "foo": "bar" }))},
                &quote! {
                    struct Foo {
                        bar: String,
                    }
                }
            )
            .is_err()
        ); // wrong fields
        assert!(
            api_operator(
                quote! {},
                &quote! {
                    struct Foo {
                        params: String,
                        sources: String,
                    }
                }
            )
            .is_err()
        ); // missing examples
    }
}
