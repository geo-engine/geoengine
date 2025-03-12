use crate::{testing::AttributeArgs, Result};
use proc_macro2::TokenStream;
use std::collections::HashMap;
use syn::{parse::Parser, Lit};

pub fn parse_config_args(attr: TokenStream) -> Result<HashMap<String, Lit>> {
    let inputs = AttributeArgs::parse_terminated.parse2(attr)?;

    let mut args = HashMap::new();

    for input in inputs {
        let syn::Meta::NameValue(name_value) = input else {
            return Err(syn::Error::new_spanned(input, "expected name-value pair"));
        };

        let ident = name_value
            .path
            .get_ident()
            .ok_or_else(|| {
                syn::Error::new_spanned(name_value.clone(), "Must have specified ident")
            })?
            .to_string()
            .to_lowercase();
        let lit = match &name_value.value {
            syn::Expr::Lit(syn::ExprLit { lit, .. }) => lit,
            expr => return Err(syn::Error::new_spanned(expr, "Must be a literal")),
        };

        args.insert(ident, lit.clone());
    }

    Ok(args)
}

#[cfg(test)]
#[macro_export]
// This macro is used to compare the pretty printed output of the expression parser.
// We will use a macro instead of a function to get errors in the places where they occur.
macro_rules! assert_eq_pretty {
    ( $left:expr, $right:expr ) => {{
        pretty_assertions::assert_str_eq!(
            prettyplease::unparse(&syn::parse_file(&$left).unwrap()),
            prettyplease::unparse(&syn::parse_file(&$right).unwrap()),
        );
    }};
}
