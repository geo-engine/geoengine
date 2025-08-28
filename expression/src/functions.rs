use crate::{
    codegen::{DataType, Identifier},
    error::ExpressionSemanticError,
};
use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use std::{collections::HashMap, hash::Hash, sync::OnceLock};

type Result<T, E = ExpressionSemanticError> = std::result::Result<T, E>;

/// A function generator that can be used to generate a function
/// that can be used in an expression.
///
/// It checks whether the input arguments are valid.
///
pub struct FunctionGenerator {
    /// User-facing name of the function
    name: &'static str,

    /// Method to validate the input arguments.
    /// Return true if the arguments are valid.
    generate_fn: fn(&str, &[DataType]) -> Result<Function>,
}

impl FunctionGenerator {
    pub fn generate(&self, args: &[DataType]) -> Result<Function> {
        (self.generate_fn)(self.name, args)
    }
}

#[derive(Debug, Clone, Eq)]
pub struct Function {
    /// The call name of the function in the expression src code
    name: Identifier,

    /// Function signature
    signature: Vec<DataType>,

    /// Defines the output [`DataType`] of this function
    output_type: DataType,

    /// Write the function to a token stream
    token_fn: fn(&Self, &mut TokenStream) -> (),
}

impl Function {
    pub fn name(&self) -> &Identifier {
        &self.name
    }

    pub fn output_type(&self) -> DataType {
        self.output_type
    }
}

impl ToTokens for Function {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        (self.token_fn)(self, tokens);
    }
}

/// We generate a unique name, so we can compare [`Function`]s by name.
mod cmp_by_name {
    use super::*;

    impl PartialEq for Function {
        fn eq(&self, other: &Self) -> bool {
            self.name == other.name
        }
    }

    impl Ord for Function {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.name.cmp(&other.name)
        }
    }

    impl PartialOrd for Function {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.name.cmp(&other.name))
        }
    }

    impl Hash for Function {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.name.hash(state);
        }
    }
}

pub static FUNCTIONS: OnceLock<HashMap<&'static str, FunctionGenerator>> = OnceLock::new();

/// Add a function generator for a function that returns a [`DataType::Number`] constant.
macro_rules! add_const_num {
    ( $name:literal, $functions:expr, $fn:ty ) => {{
        let name = $name;
        $functions.insert(
            name,
            FunctionGenerator {
                name,
                generate_fn: |name, args| match args {
                    [] => Ok(Function {
                        name: unique_name(name, args),
                        signature: vec![DataType::Number, DataType::Number],
                        output_type: DataType::Number,
                        token_fn: |fn_, tokens| {
                            let name = &fn_.name;
                            let dtype = DataType::Number;
                            tokens.extend(quote! {
                                fn #name() -> Option<#dtype> {
                                    Some($fn)
                                }
                            });
                        },
                    }),
                    _ => Err(ExpressionSemanticError::InvalidFunctionArguments {
                        name: name.into(),
                        expected: vec![DataType::Number.group_name().to_string()],
                        actual: args.into(),
                    }),
                },
            },
        );
    }};
}

/// Add a function generator for a function with 1 [`DataType::Number`] that returns a [`DataType::Number`].
macro_rules! add_1_num {
    ( $name:literal, $functions:expr, $fn:ty ) => {{
        let name = $name;
        $functions.insert(
            name,
            FunctionGenerator {
                name,
                generate_fn: |name, args| match args {
                    [DataType::Number] => Ok(Function {
                        name: unique_name(name, args),
                        signature: vec![DataType::Number, DataType::Number],
                        output_type: DataType::Number,
                        token_fn: |fn_, tokens| {
                            let name = &fn_.name;
                            let dtype = DataType::Number;
                            tokens.extend(quote! {
                                fn #name(a: Option<#dtype>) -> Option<#dtype> {
                                    a.map($fn)
                                }
                            });
                        },
                    }),
                    _ => Err(ExpressionSemanticError::InvalidFunctionArguments {
                        name: name.into(),
                        expected: vec![DataType::Number.group_name().to_string()],
                        actual: args.into(),
                    }),
                },
            },
        );
    }};
}

/// Add a function generator for a function with 2 [`DataType::Number`]s that returns a [`DataType::Number`].
macro_rules! add_2_num {
    ( $name:literal, $functions:expr, $fn:ty ) => {{
        let name = $name;
        $functions.insert(
            name,
            FunctionGenerator {
                name,
                generate_fn: |name, args| match args {
                    [DataType::Number, DataType::Number] => Ok(Function {
                        name: unique_name(name, args),
                        signature: vec![DataType::Number, DataType::Number],
                        output_type: DataType::Number,
                        token_fn: |fn_, tokens| {
                            let name = &fn_.name;
                            let dtype = DataType::Number;
                            tokens.extend(quote! {
                                fn #name(a: Option<#dtype>, b: Option<#dtype>) -> Option<#dtype> {
                                    match (a, b) {
                                        (Some(a), Some(b)) => Some($fn(a, b)),
                                        _ => None,
                                    }
                                }
                            });
                        },
                    }),
                    _ => Err(ExpressionSemanticError::InvalidFunctionArguments {
                        name: name.into(),
                        expected: [DataType::Number, DataType::Number]
                            .iter()
                            .map(DataType::group_name)
                            .map(ToString::to_string)
                            .collect(),
                        actual: args.into(),
                    }),
                },
            },
        );
    }};
}

// TODO: change to [`std::sync::LazyLock'] once stable
#[allow(clippy::too_many_lines)]
pub fn init_functions() -> HashMap<&'static str, FunctionGenerator> {
    let mut functions = HashMap::new();

    add_2_num!("add", functions, std::ops::Add::add);
    add_2_num!("sub", functions, std::ops::Sub::sub);
    add_2_num!("mul", functions, std::ops::Mul::mul);
    add_2_num!("div", functions, std::ops::Div::div);
    add_2_num!("min", functions, f64::min);
    add_2_num!("max", functions, f64::max);
    add_2_num!("abs", functions, f64::abs);
    add_2_num!("pow", functions, f64::powf);
    add_2_num!("mod", functions, std::ops::Rem::rem);

    add_1_num!("sqrt", functions, f64::sqrt);
    add_1_num!("cos", functions, f64::cos);
    add_1_num!("sin", functions, f64::sin);
    add_1_num!("tan", functions, f64::tan);
    add_1_num!("acos", functions, f64::acos);
    add_1_num!("asin", functions, f64::asin);
    add_1_num!("atan", functions, f64::atan);
    add_1_num!("log10", functions, f64::log10);
    add_1_num!("ln", functions, f64::ln);
    add_1_num!("round", functions, f64::round);
    add_1_num!("ceil", functions, f64::ceil);
    add_1_num!("floor", functions, f64::floor);
    add_1_num!("to_radians", functions, f64::to_radians);
    add_1_num!("to_degrees", functions, f64::to_degrees);
    add_1_num!("tanh", functions, f64::tanh);

    add_const_num!("pi", functions, std::f64::consts::PI);
    add_const_num!("e", functions, std::f64::consts::E);

    // [`geo`] functions

    let name = "centroid";
    functions.insert(
        name,
        FunctionGenerator {
            name,
            generate_fn: |name, args| match args {
                [
                    dtype @ (DataType::MultiPoint
                    | DataType::MultiLineString
                    | DataType::MultiPolygon),
                ] => Ok(Function {
                    name: unique_name(name, args),
                    signature: vec![*dtype],
                    output_type: DataType::MultiPoint,
                    token_fn: move |fn_, tokens| {
                        let name = &fn_.name;
                        let dtype = fn_.signature[0];

                        let inner_operation = match dtype {
                            DataType::MultiPoint
                            | DataType::MultiLineString
                            | DataType::MultiPolygon => quote! {
                                geom.centroid()
                            },
                            // should never happen
                            DataType::Number => quote! {},
                        };

                        let output_type = &fn_.output_type;

                        tokens.extend(quote! {
                            fn #name(geom: Option<#dtype>) -> Option<#output_type> {
                                #inner_operation
                            }
                        });
                    },
                }),
                _ => Err(ExpressionSemanticError::InvalidFunctionArguments {
                    name: name.into(),
                    expected: [DataType::MultiPoint]
                        .iter()
                        .map(DataType::group_name)
                        .map(ToString::to_string)
                        .collect(),
                    actual: args.into(),
                }),
            },
        },
    );

    let name = "area";
    functions.insert(
        name,
        FunctionGenerator {
            name,
            generate_fn: |name, args| match args {
                [
                    dtype @ (DataType::MultiPoint
                    | DataType::MultiLineString
                    | DataType::MultiPolygon),
                ] => Ok(Function {
                    name: unique_name(name, args),
                    signature: vec![*dtype],
                    output_type: DataType::Number,
                    token_fn: move |fn_, tokens| {
                        let name = &fn_.name;
                        let dtype = fn_.signature[0];

                        let inner_operation = match dtype {
                            DataType::MultiPoint
                            | DataType::MultiLineString
                            | DataType::MultiPolygon => quote! {
                                geom.area()
                            },
                            // should never happen
                            DataType::Number => quote! {},
                        };

                        let output_type = &fn_.output_type;

                        tokens.extend(quote! {
                            fn #name(geom: Option<#dtype>) -> Option<#output_type> {
                                #inner_operation
                            }
                        });
                    },
                }),
                _ => Err(ExpressionSemanticError::InvalidFunctionArguments {
                    name: name.into(),
                    expected: [DataType::MultiPoint]
                        .iter()
                        .map(DataType::group_name)
                        .map(ToString::to_string)
                        .collect(),
                    actual: args.into(),
                }),
            },
        },
    );

    functions
}

pub const FUNCTION_PREFIX: &str = "expression_fn_";

/// Creates a unique name for a function to be used in the source code.
///
/// Format: `expression_fn__<name>__<dtype1>_<dtype2>_<dtype3>…`
///
fn unique_name(name: &str, signature: &[DataType]) -> Identifier {
    let mut fn_name = String::new();
    fn_name.push_str(FUNCTION_PREFIX);
    fn_name.push_str(name);
    fn_name.push('_');
    for dtype in signature {
        fn_name.push('_');
        fn_name.push(dtype.call_name_suffix());
    }
    fn_name.into()
}
