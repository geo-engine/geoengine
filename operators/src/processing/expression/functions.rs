use proc_macro2::{Ident, TokenStream};
use quote::quote;
use std::{collections::HashMap, ops::RangeInclusive, sync::OnceLock};

pub(super) struct Function {
    pub num_args: RangeInclusive<usize>,
    pub token_fn: fn(usize, Ident) -> TokenStream,
}

pub(super) static FUNCTIONS: OnceLock<HashMap<&'static str, Function>> = OnceLock::new();

// TODO: change to `LazyLock' once stable
#[allow(clippy::too_many_lines)]
pub(super) fn init_functions() -> HashMap<&'static str, Function> {
    let mut functions = HashMap::new();

    functions.insert(
        "min",
        Function {
            num_args: 2..=3,
            token_fn: |num_args, fn_name| match num_args {
                2 => quote! {
                    fn #fn_name(a: Option<f64>, b: Option<f64>) -> Option<f64> {
                        apply(a, b, f64::min)
                    }
                },
                3 => quote! {
                    fn #fn_name(a: Option<f64>, b: Option<f64>, c: Option<f64>) -> Option<f64> {
                        match (a, b, c) {
                            (Some(a), Some(b), Some(c)) => Some(f64::min(a, f64::min(b, c))),
                            _ => None,
                        }
                    }
                },
                _ => TokenStream::new(),
            },
        },
    );

    functions.insert(
        "max",
        Function {
            num_args: 2..=3,
            token_fn: |num_args, fn_name| match num_args {
                2 => quote! {
                    fn #fn_name(a: Option<f64>, b: Option<f64>) -> Option<f64> {
                        apply(a, b, f64::max)
                    }
                },
                3 => quote! {
                    fn #fn_name(a: Option<f64>, b: Option<f64>, c: Option<f64>) -> Option<f64> {
                        match (a, b, c) {
                            (Some(a), Some(b), Some(c)) => Some(f64::max(a, f64::max(b, c))),
                            _ => None,
                        }
                    }
                },
                _ => TokenStream::new(),
            },
        },
    );

    functions.insert(
        "abs",
        Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| {
                quote! {
                    fn #fn_name(a: Option<f64>) -> Option<f64> {
                        a.map(f64::abs)
                    }
                }
            },
        },
    );

    functions.insert(
        "pow",
        Function {
            num_args: 2..=2,
            token_fn: |_, fn_name| {
                quote! {
                    fn #fn_name(a: Option<f64>, b: Option<f64>) -> Option<f64> {
                        apply(a, b, f64::powf)
                    }
                }
            },
        },
    );

    functions.insert(
        "sqrt",
        Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| {
                quote! {
                    fn #fn_name(a: Option<f64>) -> Option<f64> {
                        a.map(f64::sqrt)
                    }
                }
            },
        },
    );

    functions.insert(
        "cos",
        Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| {
                quote! {
                    fn #fn_name(a: Option<f64>) -> Option<f64> {
                        a.map(f64::cos)
                    }
                }
            },
        },
    );

    functions.insert(
        "sin",
        Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| {
                quote! {
                    fn #fn_name(a: Option<f64>) -> Option<f64> {
                        a.map(f64::sin)
                    }
                }
            },
        },
    );

    functions.insert(
        "tan",
        Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| {
                quote! {
                    fn #fn_name(a: Option<f64>) -> Option<f64> {
                        a.map(f64::tan)
                    }
                }
            },
        },
    );

    functions.insert(
        "acos",
        Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| {
                quote! {
                    fn #fn_name(a: Option<f64>) -> Option<f64> {
                        a.map(f64::acos)
                    }
                }
            },
        },
    );

    functions.insert(
        "asin",
        Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| {
                quote! {
                    fn #fn_name(a: Option<f64>) -> Option<f64> {
                        a.map(f64::asin)
                    }
                }
            },
        },
    );

    functions.insert(
        "atan",
        Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| {
                quote! {
                    fn #fn_name(a: Option<f64>) -> Option<f64> {
                        a.map(f64::atan)
                    }
                }
            },
        },
    );

    functions.insert(
        "log10",
        Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| {
                quote! {
                    fn #fn_name(a: Option<f64>) -> Option<f64> {
                        a.map(f64::log10)
                    }
                }
            },
        },
    );

    functions.insert(
        "ln",
        Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| {
                quote! {
                    fn #fn_name(a: Option<f64>) -> Option<f64> {
                        a.map(f64::ln)
                    }
                }
            },
        },
    );

    functions.insert(
        "pi",
        Function {
            num_args: 0..=0,
            token_fn: |_, fn_name| {
                quote! {
                    fn #fn_name() -> Option<f64> {
                        Some(std::f64::consts::PI)
                    }
                }
            },
        },
    );

    functions.insert(
        "e",
        Function {
            num_args: 0..=0,
            token_fn: |_, fn_name| {
                quote! {
                    fn #fn_name() -> Option<f64> {
                        Some(std::f64::consts::E)
                    }
                }
            },
        },
    );

    functions.insert(
        "round",
        Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| {
                quote! {
                    fn #fn_name(a: Option<f64>) -> Option<f64> {
                        a.map(f64::round)
                    }
                }
            },
        },
    );

    functions.insert(
        "ceil",
        Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| {
                quote! {
                    fn #fn_name(a: Option<f64>) -> Option<f64> {
                        a.map(f64::ceil)
                    }
                }
            },
        },
    );

    functions.insert(
        "floor",
        Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| {
                quote! {
                    fn #fn_name(a: Option<f64>) -> Option<f64> {
                        a.map(f64::floor)
                    }
                }
            },
        },
    );

    functions.insert(
        "mod",
        Function {
            num_args: 2..=2,
            token_fn: |_, fn_name| {
                quote! {
                    fn #fn_name(a: Option<f64>, b: Option<f64>) -> Option<f64> {
                        apply(a, b, std::ops::Rem::rem)
                    }
                }
            },
        },
    );

    functions.insert(
        "to_radians",
        Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| {
                quote! {
                    fn #fn_name(a: Option<f64>) -> Option<f64> {
                        a.map(f64::to_radians)
                    }
                }
            },
        },
    );

    functions.insert(
        "to_degrees",
        Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| {
                quote! {
                    fn #fn_name(a: Option<f64>) -> Option<f64> {
                        a.map(f64::to_degrees)
                    }
                }
            },
        },
    );

    functions
}
