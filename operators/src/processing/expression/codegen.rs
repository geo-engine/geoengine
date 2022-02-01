use std::{collections::BTreeSet, fmt::Debug};

use proc_macro2::TokenStream;
use quote::{format_ident, quote, ToTokens};
use snafu::ensure;

use super::{
    error::{self, ExpressionError},
    functions::FUNCTIONS,
};

type Result<T, E = ExpressionError> = std::result::Result<T, E>;

// TODO: prefix for variables and functions

/// An expression as an abstract syntax tree.
/// Allows genering Rust code.
#[derive(Debug, Clone)]
pub struct ExpressionAst {
    /// This name is the generated function name after generating code.
    name: Identifier,
    root: AstNode,
    parameters: Vec<Parameter>,
    functions: BTreeSet<AstFunction>,
    // TODO: dtype Float or Int
}

impl ExpressionAst {
    pub fn new(
        name: Identifier,
        parameters: Vec<Parameter>,
        functions: BTreeSet<AstFunction>,
        root: AstNode,
    ) -> Result<ExpressionAst> {
        ensure!(!name.as_ref().is_empty(), error::EmptyExpressionName);

        Ok(Self {
            name,
            root,
            parameters,
            functions,
        })
    }

    pub fn code(&self) -> String {
        self.to_token_stream().to_string()
    }

    pub fn name(&self) -> &str {
        self.name.as_ref()
    }
}

impl ToTokens for ExpressionAst {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let dtype = format_ident!("{}", "f64");

        for function in &self.functions {
            function.to_tokens(tokens);
        }

        let fn_name = &self.name;
        let params: Vec<TokenStream> = self
            .parameters
            .iter()
            .map(|p| match p {
                Parameter::Number(param) => quote! { #param: #dtype },
                Parameter::Boolean(param) => quote! { #param: bool },
            })
            .collect();
        let content = &self.root;

        tokens.extend(quote! {
            #[no_mangle]
            pub extern "C" fn #fn_name (#(#params),*) -> #dtype {
                #content
            }
        });
    }
}

#[derive(Debug, Clone)]
pub enum AstNode {
    Constant(f64),
    Variable(Identifier),
    Operation {
        left: Box<AstNode>,
        op: AstOperator,
        right: Box<AstNode>,
    },
    Function {
        name: Identifier,
        args: Vec<AstNode>,
    },
    Branch {
        condition_branches: Vec<Branch>,
        else_branch: Box<AstNode>,
    },
    AssignmentsAndExpression {
        assignments: Vec<Assignment>,
        expression: Box<AstNode>,
    },
}

impl ToTokens for AstNode {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let new_tokens = match self {
            Self::Constant(n) => quote! { #n },
            Self::Variable(v) => quote! { #v },
            Self::Operation { left, op, right } => {
                quote! { ( #left #op #right ) }
            }
            Self::Function { name, args } => {
                let fn_name = format_ident!("import_{}", name.as_ref());
                quote! { #fn_name(#(#args),*) }
            }
            AstNode::Branch {
                condition_branches,
                else_branch: default_branch,
            } => {
                let mut new_tokens = TokenStream::new();
                for (i, branch) in condition_branches.iter().enumerate() {
                    let condition = &branch.condition;
                    let body = &branch.body;

                    new_tokens.extend(if i == 0 {
                        // first
                        quote! {
                            if #condition {
                                #body
                            }
                        }
                    } else {
                        // middle
                        quote! {
                            else if #condition {
                                #body
                            }
                        }
                    });
                }

                new_tokens.extend(quote! {
                    else {
                        #default_branch
                    }
                });

                new_tokens
            }
            Self::AssignmentsAndExpression {
                assignments,
                expression,
            } => {
                quote! {
                    #(#assignments)*
                    #expression
                }
            }
        };

        tokens.extend(new_tokens);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Identifier(String);

impl ToTokens for Identifier {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let identifier = format_ident!("{}", self.0);
        tokens.extend(quote! { #identifier });
    }
}

impl From<String> for Identifier {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for Identifier {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<&String> for Identifier {
    fn from(s: &String) -> Self {
        Self(s.to_string())
    }
}

impl AsRef<str> for Identifier {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Identifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        std::fmt::Display::fmt(&self.0, f)
    }
}

#[derive(Debug, Clone)]
pub enum AstOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
}

impl ToTokens for AstOperator {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let new_tokens = match self {
            Self::Add => quote! { + },
            Self::Subtract => quote! { - },
            Self::Multiply => quote! { * },
            Self::Divide => quote! { / },
        };

        tokens.extend(new_tokens);
    }
}

#[derive(Debug, Clone)]
pub struct Branch {
    pub condition: BooleanExpression,
    pub body: AstNode,
}

#[derive(Debug, Clone)]
pub enum BooleanExpression {
    Variable(Identifier),
    Constant(bool),
    Comparison {
        left: Box<AstNode>,
        op: BooleanComparator,
        right: Box<AstNode>,
    },
    Operation {
        left: Box<BooleanExpression>,
        op: BooleanOperator,
        right: Box<BooleanExpression>,
    },
}

impl ToTokens for BooleanExpression {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let new_tokens = match self {
            Self::Variable(v) => quote! { #v },
            Self::Constant(b) => quote! { #b },
            Self::Comparison { left, op, right } => quote! { ( (#left) #op (#right) ) },
            Self::Operation { left, op, right } => quote! { ( (#left) #op (#right) ) },
        };

        tokens.extend(new_tokens);
    }
}

#[derive(Debug, Clone)]
pub enum BooleanComparator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

impl ToTokens for BooleanComparator {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let new_tokens = match self {
            Self::Equal => quote! { == },
            Self::NotEqual => quote! { != },
            Self::LessThan => quote! { < },
            Self::LessThanOrEqual => quote! { <= },
            Self::GreaterThan => quote! { > },
            Self::GreaterThanOrEqual => quote! { >= },
        };

        tokens.extend(new_tokens);
    }
}

#[derive(Debug, Clone)]
pub enum BooleanOperator {
    And,
    Or,
}

impl ToTokens for BooleanOperator {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let new_tokens = match self {
            Self::And => quote! { && },
            Self::Or => quote! { || },
        };

        tokens.extend(new_tokens);
    }
}

#[derive(Debug, Clone)]
pub struct Assignment {
    pub identifier: Identifier,
    pub expression: AstNode,
}

impl ToTokens for Assignment {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let Self {
            identifier,
            expression,
        } = self;
        let new_tokens = quote! {
            let #identifier = #expression;
        };

        tokens.extend(new_tokens);
    }
}

#[derive(Debug, Clone)]
pub enum Parameter {
    Number(Identifier),
    Boolean(Identifier),
}

impl AsRef<str> for Parameter {
    fn as_ref(&self) -> &str {
        match self {
            Self::Number(identifier) | Self::Boolean(identifier) => identifier.as_ref(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AstFunction {
    pub name: Identifier,
    pub num_parameters: usize,
}

impl ToTokens for AstFunction {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let function = match FUNCTIONS.get(self.name.as_ref()) {
            Some(f) => f,
            None => return, // do nothing if, for some reason, the function doesn't exist
        };

        let prefixed_fn_name = format_ident!("import_{}", self.name.as_ref());

        tokens.extend(quote! {
            #[inline]
        });

        tokens.extend((function.token_fn)(self.num_parameters, prefixed_fn_name));
    }
}
