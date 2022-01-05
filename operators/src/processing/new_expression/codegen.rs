use std::{cell::RefCell, rc::Rc};

use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote, ToTokens};
use snafu::ensure;

use super::error::{self, ExpressionError};

type Result<T, E = ExpressionError> = std::result::Result<T, E>;

// TODO: prefix for variables and functions

#[derive(Debug)]
pub struct ExpressionAst {
    name: String,
    root: AstNode,
    parameters: Vec<Ident>,
    imports: Rc<RefCell<Vec<Ident>>>,
    // TODO: dtype Float or Int
}

impl ExpressionAst {
    pub fn new(
        name: String,
        parameters: Vec<Ident>,
        imports: Rc<RefCell<Vec<Ident>>>,
        root: AstNode,
    ) -> Result<ExpressionAst> {
        ensure!(!name.is_empty(), error::EmptyExpressionName);

        Ok(Self {
            name,
            root,
            parameters,
            imports,
        })
    }

    pub fn code(&self) -> String {
        self.to_token_stream().to_string()
    }

    pub fn formatted_code(&self) -> Result<String> {
        rustfmt_wrapper::rustfmt(self.to_token_stream()).map_err(|error| match error {
            rustfmt_wrapper::Error::NoRustfmt => ExpressionError::RustFmtMissing,
            rustfmt_wrapper::Error::Rustfmt(error) => ExpressionError::RustFmtError { error },
            rustfmt_wrapper::Error::IO(error) => ExpressionError::RustFmtIoError {
                error: error.to_string(),
            },
            rustfmt_wrapper::Error::Conversion(source) => {
                ExpressionError::RustFmtConversionError { source }
            }
        })
    }
}

impl ToTokens for ExpressionAst {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let dtype = format_ident!("{}", "f64");

        for fn_name in self.imports.borrow().iter().map(ToString::to_string) {
            let prefixed_fn_name = format_ident!("import_{}", fn_name);

            tokens.extend(quote! {
                #[inline]
            });

            let fn_tokens = match fn_name.as_str() {
                "max" => quote! {
                    fn #prefixed_fn_name (a: #dtype, b: #dtype) -> #dtype {
                        #dtype::max(a, b)
                    }
                },
                "pow" => quote! {
                    fn #prefixed_fn_name (a: #dtype, b: #dtype) -> #dtype {
                        #dtype::powf(a, b)
                    }
                },
                _ => todo!("{} is not yet supported", fn_name),
            };

            tokens.extend(fn_tokens);
        }

        let fn_name = format_ident!("{}", self.name);
        let params = &self.parameters;
        let content = &self.root;

        tokens.extend(quote! {
            #[no_mangle]
            pub extern "C" fn #fn_name (#(#params : #dtype),*) -> #dtype {
                #content
            }
        });
    }
}

#[derive(Debug)]
pub enum AstNode {
    Constant(f64),
    Variable(Ident),
    Operation {
        left: Box<AstNode>,
        op: AstOperator,
        right: Box<AstNode>,
    },
    Function {
        name: Ident,
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
                let fn_name = format_ident!("import_{}", name);
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

#[derive(Debug)]
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

#[derive(Debug)]
pub struct Branch {
    pub condition: BooleanExpression,
    pub body: AstNode,
}

#[derive(Debug)]
pub enum BooleanExpression {
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
            Self::Constant(b) => quote! { #b },
            Self::Comparison { left, op, right } => quote! { ( (#left) #op (#right) ) },
            Self::Operation { left, op, right } => quote! { ( (#left) #op (#right) ) },
        };

        tokens.extend(new_tokens);
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
pub struct Assignment {
    pub identifier: Ident,
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
