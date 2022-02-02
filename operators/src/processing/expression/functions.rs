use proc_macro2::{Ident, TokenStream};
use quote::quote;
use std::{collections::HashMap, ops::RangeInclusive};

pub(super) struct Function {
    pub num_args: RangeInclusive<usize>,
    pub token_fn: fn(usize, Ident) -> TokenStream,
}

lazy_static::lazy_static! {
    pub(super) static ref FUNCTIONS: HashMap<&'static str, Function> = {
        let mut functions = HashMap::new();

        functions.insert("min", Function {
            num_args: 2..=3,
            token_fn: |num_args, fn_name| {
                match num_args {
                    2 => quote! {
                        fn #fn_name(a: f64, b: f64) -> f64 {
                            f64::min(a, b)
                        }
                    },
                    3 => quote! {
                        fn #fn_name(a: f64, b: f64, c: f64) -> f64 {
                            f64::min(f64::min(a, b), c)
                        }
                    },
                    _ => TokenStream::new(),
                }
            }
        });

        functions.insert("max", Function {
            num_args: 2..=3,
            token_fn: |num_args, fn_name| {
                match num_args {
                    2 => quote! {
                        fn #fn_name(a: f64, b: f64) -> f64 {
                            f64::max(a, b)
                        }
                    },
                    3 => quote! {
                        fn #fn_name(a: f64, b: f64, c: f64) -> f64 {
                            f64::max(f64::max(a, b), c)
                        }
                    },
                    _ => TokenStream::new(),
                }
            }
        });

        functions.insert("abs", Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| quote! {
                fn #fn_name(a: f64) -> f64 {
                    f64::abs(a)
                }
            },
        });

        functions.insert("pow", Function {
            num_args: 2..=2,
            token_fn: |_, fn_name| quote! {
                fn #fn_name(a: f64, b: f64) -> f64 {
                    f64::powf(a, b)
                }
            },
        });

        functions.insert("sqrt", Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| quote! {
                fn #fn_name(a: f64) -> f64 {
                    f64::sqrt(a)
                }
            },
        });

        functions.insert("cos", Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| quote! {
                fn #fn_name(a: f64) -> f64 {
                    f64::cos(a)
                }
            },
        });

        functions.insert("sin", Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| quote! {
                fn #fn_name(a: f64) -> f64 {
                    f64::sin(a)
                }
            },
        });

        functions.insert("tan", Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| quote! {
                fn #fn_name(a: f64) -> f64 {
                    f64::tan(a)
                }
            },
        });

        functions.insert("acos", Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| quote! {
                fn #fn_name(a: f64) -> f64 {
                    f64::acos(a)
                }
            },
        });

        functions.insert("asin", Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| quote! {
                fn #fn_name(a: f64) -> f64 {
                    f64::asin(a)
                }
            },
        });

        functions.insert("atan", Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| quote! {
                fn #fn_name(a: f64) -> f64 {
                    f64::atan(a)
                }
            },
        });

        functions.insert("log10", Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| quote! {
                fn #fn_name(a: f64) -> f64 {
                    f64::log10(a)
                }
            },
        });

        functions.insert("ln", Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| quote! {
                fn #fn_name(a: f64) -> f64 {
                    f64::ln(a)
                }
            },
        });

        functions.insert("pi", Function {
            num_args: 0..=0,
            token_fn: |_, fn_name| quote! {
                fn #fn_name() -> f64 {
                    std::f64::consts::PI
                }
            },
        });

        functions.insert("e", Function {
            num_args: 0..=0,
            token_fn: |_, fn_name| quote! {
                fn #fn_name() -> f64 {
                    std::f64::consts::E
                }
            },
        });

        functions.insert("round", Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| quote! {
                fn #fn_name(a: f64) -> f64 {
                    f64::round(a)
                }
            },
        });

        functions.insert("ceil", Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| quote! {
                fn #fn_name(a: f64) -> f64 {
                    f64::ceil(a)
                }
            },
        });

        functions.insert("floor", Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| quote! {
                fn #fn_name(a: f64) -> f64 {
                    f64::floor(a)
                }
            },
        });

        functions.insert("mod", Function {
            num_args: 2..=2,
            token_fn: |_, fn_name| quote! {
                fn #fn_name(a: f64, b: f64) -> f64 {
                    a % b
                }
            },
        });

        functions.insert("to_radians", Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| quote! {
                fn #fn_name(a: f64) -> f64 {
                    f64::to_radians(a)
                }
            },
        });

        functions.insert("to_degrees", Function {
            num_args: 1..=1,
            token_fn: |_, fn_name| quote! {
                fn #fn_name(a: f64) -> f64 {
                    f64::to_degrees(a)
                }
            },
        });

        functions
    };
}
