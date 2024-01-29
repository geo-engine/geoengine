use crate::{codegen::DataType, error::ExpressionParserError};

use super::{
    codegen::{
        Assignment, AstFunction, AstNode, BooleanComparator, BooleanExpression, BooleanOperator,
        Branch, ExpressionAst, Identifier, Parameter,
    },
    error::{self, ExpressionSemanticError},
    functions::{init_functions, FUNCTIONS},
    util::duplicate_or_empty_str_slice,
};
use pest::{
    iterators::{Pair, Pairs},
    pratt_parser::{Assoc, Op, PrattParser},
    Parser,
};
use pest_derive::Parser;
use snafu::{OptionExt, ResultExt};
use std::{
    cell::RefCell,
    collections::{hash_map, BTreeSet, HashMap, HashSet},
    rc::Rc,
    sync::OnceLock,
};

type Result<T, E = ExpressionParserError> = std::result::Result<T, E>;

pub type PestError = pest::error::Error<Rule>;

#[derive(Parser)]
#[grammar = "expression.pest"] // relative to src
struct _ExpressionParser;

/// A parser for user-defined expressions.
pub struct ExpressionParser {
    parameters: Vec<Parameter>,
    out_type: DataType,
    functions: Rc<RefCell<BTreeSet<AstFunction>>>,
}

static EXPRESSION_PARSER: OnceLock<PrattParser<Rule>> = OnceLock::new();
static BOOLEAN_EXPRESSION_PARSER: OnceLock<PrattParser<Rule>> = OnceLock::new();

// TODO: change to [`std::sync::LazyLock'] once stable
fn init_expression_parser() -> PrattParser<Rule> {
    PrattParser::new()
        .op(Op::infix(Rule::add, Assoc::Left) | Op::infix(Rule::subtract, Assoc::Left))
        .op(Op::infix(Rule::multiply, Assoc::Left) | Op::infix(Rule::divide, Assoc::Left))
        .op(Op::infix(Rule::power, Assoc::Right))
}

// TODO: change to [`std::sync::LazyLock'] once stable
fn init_boolean_expression_parser() -> PrattParser<Rule> {
    PrattParser::new()
        .op(Op::infix(Rule::or, Assoc::Left))
        .op(Op::infix(Rule::and, Assoc::Left))
}

impl ExpressionParser {
    pub fn new(parameters: &[Parameter], out_type: DataType) -> Result<Self> {
        match duplicate_or_empty_str_slice(parameters) {
            crate::util::DuplicateOrEmpty::Ok => (), // fine
            crate::util::DuplicateOrEmpty::Duplicate(parameter) => {
                return Err(
                    ExpressionSemanticError::DuplicateParameterName { parameter }
                        .into_definition_parser_error(),
                );
            }
            crate::util::DuplicateOrEmpty::Empty => {
                return Err(
                    ExpressionSemanticError::EmptyParameterName.into_definition_parser_error()
                );
            }
        };

        let mut numeric_parameters = HashSet::with_capacity(parameters.len());
        for parameter in parameters {
            match parameter {
                Parameter::Number(name) => numeric_parameters.insert(name.clone()),
                _ => false, // TODO: handle properly
            };
        }

        Ok(Self {
            parameters: parameters.to_vec(),
            out_type,
            functions: Rc::new(RefCell::new(Default::default())),
        })
    }

    pub fn parse(self, name: &str, input: &str) -> Result<ExpressionAst> {
        if name.is_empty() {
            return Err(ExpressionSemanticError::EmptyExpressionName.into_definition_parser_error());
        }

        let pairs = _ExpressionParser::parse(Rule::main, input)
            .map_err(ExpressionParserError::from_syntactic_error)?;

        // TODO: make variable case insensitive
        let variables = self
            .parameters
            .iter()
            .map(|param| (param.identifier().clone(), param.data_type()))
            .collect();

        let root = self.build_ast(pairs, &variables)?;

        if root.data_type() != self.out_type {
            return Err(ExpressionSemanticError::WrongOutputType {
                expected: self.out_type,
                actual: root.data_type(),
            }
            .into_definition_parser_error());
        }

        ExpressionAst::new(
            name.to_string().into(),
            self.parameters,
            self.out_type,
            self.functions.borrow_mut().clone(),
            root,
        )
    }

    fn build_ast(
        &self,
        pairs: Pairs<'_, Rule>,
        variables: &HashMap<Identifier, DataType>,
    ) -> Result<AstNode> {
        EXPRESSION_PARSER
            .get_or_init(init_expression_parser)
            .map_primary(|primary| self.resolve_expression_rule(primary, variables))
            .map_infix(|left, op, right| self.resolve_infix_operations(left, &op, right))
            // TODO: is there another way to remove EOIs?
            .parse(pairs.filter(|pair| pair.as_rule() != Rule::EOI))
    }

    fn resolve_expression_rule(
        &self,
        pair: Pair<Rule>,
        variables: &HashMap<Identifier, DataType>,
    ) -> Result<AstNode> {
        let span = pair.as_span();
        match pair.as_rule() {
            Rule::expression => self.build_ast(pair.into_inner(), variables),
            Rule::number => Ok(AstNode::Constant(
                pair.as_str()
                    .parse()
                    .context(error::ConstantIsNotAdNumber {
                        constant: pair.as_str(),
                    })
                    .map_err(|e| e.into_parser_error(span))?,
            )),
            Rule::identifier => {
                let identifier = pair.as_str().into();
                let data_type = *variables
                    .get(&identifier)
                    .context(error::UnknownVariable {
                        variable: identifier.to_string(),
                    })
                    .map_err(|e| e.into_parser_error(span))?;
                Ok(AstNode::Variable {
                    name: identifier,
                    data_type,
                })
            }
            Rule::nodata => Ok(AstNode::NoData),
            Rule::function => self.resolve_function(pair.into_inner(), span, variables),
            Rule::branch => self.resolve_branch(pair, span, variables),
            Rule::assignments_and_expression => {
                let mut assignments: Vec<Assignment> = vec![];

                let mut variables = variables.clone();

                for pair in pair.into_inner() {
                    if matches!(pair.as_rule(), Rule::assignment) {
                        let mut pairs = pair.into_inner();

                        let first_pair = pairs.next().ok_or(
                            ExpressionSemanticError::AssignmentNeedsTwoParts
                                .into_parser_error(span),
                        )?;
                        let second_pair = pairs.next().ok_or(
                            ExpressionSemanticError::AssignmentNeedsTwoParts
                                .into_parser_error(span),
                        )?;

                        let identifier: Identifier = first_pair.as_str().into();

                        let expression = self.build_ast(second_pair.into_inner(), &variables)?;
                        let expression_data_type = expression.data_type();

                        assignments.push(Assignment {
                            identifier: identifier.clone(),
                            expression,
                        });

                        // having an assignment allows more variables,
                        // but only in the next assignments or expression
                        match variables.entry(identifier) {
                            hash_map::Entry::Vacant(entry) => {
                                entry.insert(expression_data_type);
                            }
                            hash_map::Entry::Occupied(entry) => {
                                // we do not allow shadowing for now

                                let identifier: &Identifier = entry.key();
                                return Err(ExpressionSemanticError::VariableShadowing {
                                    variable: identifier.to_string(),
                                }
                                .into_parser_error(span));
                            }
                        };
                    } else {
                        let expression = self.build_ast(pair.into_inner(), &variables)?;

                        return Ok(AstNode::AssignmentsAndExpression {
                            assignments,
                            expression: Box::new(expression),
                        });
                    }
                }

                Err(ExpressionSemanticError::DoesNotEndWithExpression.into_parser_error(span))
            }
            _ => Err(ExpressionSemanticError::UnexpectedRule {
                rule: pair.as_str().to_string(),
            }
            .into_parser_error(span)),
        }
    }

    fn resolve_branch(
        &self,
        pair: Pair<Rule>,
        span: pest::Span<'_>,
        variables: &HashMap<Identifier, DataType>,
    ) -> Result<AstNode> {
        // pairs are boolean -> expression
        // and last one is just an expression
        let mut pairs = pair.into_inner();

        let mut condition_branches: Vec<Branch> = vec![];

        let mut data_type = None;

        while let Some(pair) = pairs.next() {
            if matches!(pair.as_rule(), Rule::boolean_expression) {
                let condition = self.build_boolean_expression(pair.into_inner(), variables)?;

                let next_pair = pairs
                    .next()
                    .ok_or(ExpressionSemanticError::MissingBranch.into_parser_error(span))?;
                let body = self.build_ast(next_pair.into_inner(), variables)?;

                if let Some(data_type) = data_type {
                    if data_type != body.data_type() {
                        return Err(ExpressionSemanticError::AllBranchesMustOutputSameType
                            .into_parser_error(span));
                    }
                } else {
                    data_type = Some(body.data_type());
                }

                condition_branches.push(Branch { condition, body });
            } else {
                let expression = self.build_ast(pair.into_inner(), variables)?;

                if let Some(data_type) = data_type {
                    if data_type != expression.data_type() {
                        return Err(ExpressionSemanticError::AllBranchesMustOutputSameType
                            .into_parser_error(span));
                    }
                }

                return Ok(AstNode::Branch {
                    condition_branches,
                    else_branch: Box::new(expression),
                });
            }
        }

        Err(ExpressionSemanticError::BranchStructureMalformed.into_parser_error(span))
    }

    fn resolve_function(
        &self,
        mut pairs: Pairs<Rule>,
        span: pest::Span<'_>,
        variables: &HashMap<Identifier, DataType>,
    ) -> Result<AstNode> {
        // first one is name
        let name: Identifier = pairs
            .next()
            .ok_or(ExpressionSemanticError::MalformedFunctionCall.into_parser_error(span))?
            .as_str()
            .into();

        let args = pairs
            .map(|pair| self.build_ast(pair.into_inner(), variables))
            .collect::<Result<Vec<_>, _>>()?;

        let function = FUNCTIONS
            .get_or_init(init_functions)
            .get(name.as_ref())
            .context(error::UnknownFunction {
                function: name.to_string(),
            })
            .map_err(|e| e.into_parser_error(span))?
            .generate(
                args.iter()
                    .map(AstNode::data_type)
                    .collect::<Vec<_>>()
                    .as_slice(),
            )
            .map_err(|e| e.into_parser_error(span))?;

        self.functions.borrow_mut().insert(AstFunction {
            function: function.clone(),
        });

        Ok(AstNode::Function { function, args })
    }

    fn resolve_infix_operations(
        &self,
        left: Result<AstNode>,
        op: &Pair<Rule>,
        right: Result<AstNode>,
    ) -> Result<AstNode> {
        let (left, right) = (left?, right?);

        if left.data_type() != DataType::Number || right.data_type() != DataType::Number {
            return Err(ExpressionSemanticError::OperatorsMustBeUsedWithNumbers
                .into_parser_error(op.as_span()));
        }

        let fn_name = match op.as_rule() {
            Rule::add => "add",
            Rule::subtract => "sub",
            Rule::multiply => "mul",
            Rule::divide => "div",
            Rule::power => "pow",
            _ => {
                return Err(ExpressionSemanticError::UnexpectedOperator {
                    found: op.as_str().to_string(),
                }
                .into_parser_error(op.as_span()))
            }
        };

        // we will change the operators to function calls
        let function = FUNCTIONS
            .get_or_init(init_functions)
            .get(fn_name)
            .ok_or_else(|| {
                ExpressionSemanticError::UnknownFunction {
                    function: fn_name.to_string(),
                }
                .into_parser_error(op.as_span())
            })?
            .generate(&[DataType::Number, DataType::Number])
            .map_err(|e| e.into_parser_error(op.as_span()))?;

        self.functions.borrow_mut().insert(AstFunction {
            function: function.clone(),
        });

        Ok(AstNode::Function {
            function,
            args: vec![left, right],
        })
    }

    fn build_boolean_expression(
        &self,
        pairs: Pairs<'_, Rule>,
        variables: &HashMap<Identifier, DataType>,
    ) -> Result<BooleanExpression> {
        BOOLEAN_EXPRESSION_PARSER
            .get_or_init(init_boolean_expression_parser)
            .map_primary(|primary| self.resolve_boolean_expression_rule(primary, variables))
            .map_infix(|left, op, right| Self::resolve_infix_boolean_operations(left, &op, right))
            .parse(pairs)
    }

    fn resolve_boolean_expression_rule(
        &self,
        pair: Pair<Rule>,
        variables: &HashMap<Identifier, DataType>,
    ) -> Result<BooleanExpression> {
        let span = pair.as_span();
        match pair.as_rule() {
            Rule::identifier_is_nodata => {
                // convert `A IS NODATA` to the check for `A.is_none()`

                let mut pairs = pair.into_inner();

                let identifier = pairs
                    .next()
                    .ok_or(
                        ExpressionSemanticError::MalformedIdentifierIsNodata
                            .into_parser_error(span),
                    )?
                    .as_str()
                    .into();

                let data_type = *variables
                    .get(&identifier)
                    .context(error::UnknownVariable {
                        variable: identifier.to_string(),
                    })
                    .map_err(|e| e.into_parser_error(span))?;

                if data_type != DataType::Number {
                    return Err(ExpressionSemanticError::ComparisonsMustBeUsedWithNumbers
                        .into_parser_error(span));
                }

                let left = AstNode::Variable {
                    name: identifier,
                    data_type,
                };

                Ok(BooleanExpression::Comparison {
                    left: Box::new(left),
                    op: BooleanComparator::Equal,
                    right: Box::new(AstNode::NoData),
                })
            }
            Rule::boolean_true => Ok(BooleanExpression::Constant(true)),
            Rule::boolean_false => Ok(BooleanExpression::Constant(false)),
            Rule::boolean_comparison => {
                let mut pairs = pair.into_inner();

                let first_pair = pairs.next().ok_or(
                    ExpressionSemanticError::ComparisonNeedsThreeParts.into_parser_error(span),
                )?;
                let second_pair = pairs.next().ok_or(
                    ExpressionSemanticError::ComparisonNeedsThreeParts.into_parser_error(span),
                )?;
                let third_pair = pairs.next().ok_or(
                    ExpressionSemanticError::ComparisonNeedsThreeParts.into_parser_error(span),
                )?;

                let left_expression = self.build_ast(first_pair.into_inner(), variables)?;
                let comparison = match second_pair.as_rule() {
                    Rule::equals => BooleanComparator::Equal,
                    Rule::not_equals => BooleanComparator::NotEqual,
                    Rule::smaller => BooleanComparator::LessThan,
                    Rule::smaller_equals => BooleanComparator::LessThanOrEqual,
                    Rule::larger => BooleanComparator::GreaterThan,
                    Rule::larger_equals => BooleanComparator::GreaterThanOrEqual,
                    _ => {
                        return Err(ExpressionSemanticError::UnexpectedComparator {
                            comparator: format!("{:?}", second_pair.as_rule()),
                        }
                        .into_parser_error(span))
                    }
                };
                let right_expression = self.build_ast(third_pair.into_inner(), variables)?;

                if left_expression.data_type() != DataType::Number
                    || right_expression.data_type() != DataType::Number
                {
                    return Err(ExpressionSemanticError::ComparisonsMustBeUsedWithNumbers
                        .into_parser_error(span));
                }

                Ok(BooleanExpression::Comparison {
                    left: Box::new(left_expression),
                    op: comparison,
                    right: Box::new(right_expression),
                })
            }
            Rule::boolean_expression => self.build_boolean_expression(pair.into_inner(), variables),
            _ => Err(ExpressionSemanticError::UnexpectedBooleanRule {
                rule: format!("{:?}", pair.as_rule()),
            }
            .into_parser_error(span)),
        }
    }

    fn resolve_infix_boolean_operations(
        left: Result<BooleanExpression>,
        op: &Pair<Rule>,
        right: Result<BooleanExpression>,
    ) -> Result<BooleanExpression> {
        let (left, right) = (left?, right?);

        let boolean_operator = match op.as_rule() {
            Rule::and => BooleanOperator::And,
            Rule::or => BooleanOperator::Or,
            _ => {
                return Err(ExpressionSemanticError::UnexpectedBooleanOperator {
                    operator: format!("{:?}", op.as_rule()),
                }
                .into_parser_error(op.as_span()));
            }
        };

        Ok(BooleanExpression::Operation {
            left: Box::new(left),
            op: boolean_operator,
            right: Box::new(right),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_str_eq;
    use proc_macro2::TokenStream;
    use quote::{quote, ToTokens};

    fn parse(name: &str, parameters: &[&str], input: &str) -> String {
        let parameters: Vec<Parameter> = parameters
            .iter()
            .map(|&p| Parameter::Number(Identifier::from(p)))
            .collect();

        try_parse(name, &parameters, DataType::Number, input).unwrap()
    }

    fn parse2(name: &str, parameters: &[Parameter], out_type: DataType, input: &str) -> String {
        try_parse(name, parameters, out_type, input).unwrap()
    }

    fn try_parse(
        name: &str,
        parameters: &[Parameter],
        out_type: DataType,
        input: &str,
    ) -> Result<String> {
        let parser = ExpressionParser::new(parameters, out_type)?;
        let ast = parser.parse(name, input)?;

        Ok(ast.into_token_stream().to_string())
    }

    // This macro is used to compare the pretty printed output of the expression parser.
    // We will use a macro instead of a function to get errors in the places where they occur.
    macro_rules! assert_eq_pretty {
        ( $left:expr, $right:expr ) => {{
            assert_str_eq!(
                prettyplease::unparse(&syn::parse_file(&$left).unwrap()),
                prettyplease::unparse(&syn::parse_file(&$right).unwrap()),
            );
        }};
    }

    enum Prelude {
        Add,
        Sub,
        Mul,
        Div,
        Pow,
    }

    impl ToTokens for Prelude {
        fn to_tokens(&self, tokens: &mut TokenStream) {
            tokens.extend(match self {
                Prelude::Add => quote! {
                    #[inline]
                    fn import_add__n_n(a: Option<f64>, b: Option<f64>) -> Option<f64> {
                        match (a, b) {
                            (Some(a), Some(b)) => Some(std::ops::Add::add(a, b)),
                            _ => None,
                        }
                    }
                },
                Prelude::Sub => quote! {
                    #[inline]
                    fn import_sub__n_n(a: Option<f64>, b: Option<f64>) -> Option<f64> {
                        match (a, b) {
                            (Some(a), Some(b)) => Some(std::ops::Sub::sub(a, b)),
                            _ => None,
                        }
                    }
                },
                Prelude::Mul => quote! {
                    #[inline]
                    fn import_mul__n_n(a: Option<f64>, b: Option<f64>) -> Option<f64> {
                        match (a, b) {
                            (Some(a), Some(b)) => Some(std::ops::Mul::mul(a, b)),
                            _ => None,
                        }
                    }
                },
                Prelude::Div => quote! {
                    #[inline]
                    fn import_div__n_n(a: Option<f64>, b: Option<f64>) -> Option<f64> {
                        match (a, b) {
                            (Some(a), Some(b)) => Some(std::ops::Div::div(a, b)),
                            _ => None,
                        }
                    }
                },
                Prelude::Pow => quote! {
                    #[inline]
                    fn import_pow__n_n(a: Option<f64>, b: Option<f64>) -> Option<f64> {
                        match (a, b) {
                            (Some(a), Some(b)) => Some(f64::powf(a, b)),
                            _ => None,
                        }
                    }
                },
            });
        }
    }

    const ADD_FN: Prelude = Prelude::Add;
    const SUB_FN: Prelude = Prelude::Sub;
    const DIV_FN: Prelude = Prelude::Div;
    const MUL_FN: Prelude = Prelude::Mul;
    const POW_FN: Prelude = Prelude::Pow;

    #[test]
    fn simple() {
        assert_eq_pretty!(
            parse("expression", &[], "1"),
            quote! {
                #[no_mangle]
                pub extern "Rust" fn expression() -> Option<f64> {
                    Some(1f64)
                }
            }
            .to_string()
        );

        assert_eq_pretty!(
            parse("foo", &[], "1 + 2"),
            quote! {
                #ADD_FN

                #[no_mangle]
                pub extern "Rust" fn foo() -> Option<f64> {
                    import_add__n_n(Some(1f64), Some(2f64))
                }
            }
            .to_string()
        );

        assert_eq_pretty!(
            parse("bar", &[], "-1 + 2"),
            quote! {
                #ADD_FN

                #[no_mangle]
                pub extern "Rust" fn bar() -> Option<f64> {
                    import_add__n_n(Some(-1f64), Some(2f64))
                }
            }
            .to_string()
        );

        assert_eq_pretty!(
            parse("baz", &[], "1 - -2"),
            quote! {
                #SUB_FN

                #[no_mangle]
                pub extern "Rust" fn baz() -> Option<f64> {
                    import_sub__n_n(Some(1f64), Some(-2f64))
                }
            }
            .to_string()
        );

        assert_eq_pretty!(
            parse("expression", &[], "1 + 2 / 3"),
            quote! {
                #ADD_FN

                #[inline]
                fn import_div__n_n(a: Option<f64>, b: Option<f64>) -> Option<f64> {
                    match (a, b) {
                        (Some(a), Some(b)) => Some(std::ops::Div::div(a, b)),
                        _ => None,
                    }
                }

                #[no_mangle]
                pub extern "Rust" fn expression() -> Option<f64> {
                    import_add__n_n(
                        Some(1f64),
                        import_div__n_n(Some(2f64), Some(3f64)),
                    )
                }
            }
            .to_string()
        );

        assert_eq_pretty!(
            parse("expression", &[], "2**4"),
            quote! {
                #POW_FN

                #[no_mangle]
                pub extern "Rust" fn expression() -> Option<f64> {
                    import_pow__n_n(Some(2f64) , Some(4f64))
                }
            }
            .to_string()
        );
    }

    #[test]
    fn params() {
        assert_eq_pretty!(
            parse("expression", &["a"], "a + 1"),
            quote! {
                #ADD_FN

                #[no_mangle]
                pub extern "Rust" fn expression(a: Option<f64>) -> Option<f64> {
                    import_add__n_n(a, Some(1f64))
                }
            }
            .to_string()
        );

        assert_eq_pretty!(
            parse("ndvi", &["a", "b"], "(a-b) / (a+b)"),
            quote! {
                #ADD_FN
                #DIV_FN
                #SUB_FN

                #[no_mangle]
                pub extern "Rust" fn ndvi(a: Option<f64>, b: Option<f64>) -> Option<f64> {
                    import_div__n_n(
                        import_sub__n_n(a, b),
                        import_add__n_n(a, b),
                    )
                }
            }
            .to_string()
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn functions() {
        assert_eq_pretty!(
            parse("expression", &["a"], "max(a, 0)"),
            quote! {
                #[inline]
                fn import_max__n_n(a: Option<f64>, b: Option<f64>) -> Option<f64> {
                    match (a, b) {
                        (Some(a), Some(b)) => Some(f64::max(a, b)),
                        _ => None,
                    }
                }

                #[no_mangle]
                pub extern "Rust" fn expression(a: Option<f64>) -> Option<f64> {
                    import_max__n_n(a, Some(0f64))
                }
            }
            .to_string()
        );

        assert_eq_pretty!(
            parse("expression", &["a"], "pow(sqrt(a), 2)"),
            quote! {
                #[inline]
                fn import_pow__n_n(a: Option<f64>, b: Option<f64>) -> Option<f64> {
                    match (a, b) {
                        (Some(a), Some(b)) => Some(f64::powf(a, b)),
                        _ => None,
                    }
                }
                #[inline]
                fn import_sqrt__n(a: Option<f64>) -> Option<f64> {
                    a.map(f64::sqrt)
                }

                #[no_mangle]
                pub extern "Rust" fn expression(a: Option<f64>) -> Option<f64> {
                    import_pow__n_n(import_sqrt__n(a), Some(2f64))
                }
            }
            .to_string()
        );

        assert_eq_pretty!(
            parse("waves", &[],  "cos(sin(tan(acos(asin(atan(1))))))"),
            quote! {
                #[inline]
                fn import_acos__n(a: Option<f64>) -> Option<f64> {
                    a.map(f64::acos)
                }
                #[inline]
                fn import_asin__n(a: Option<f64>) -> Option<f64> {
                    a.map(f64::asin)
                }
                #[inline]
                fn import_atan__n(a: Option<f64>) -> Option<f64> {
                    a.map(f64::atan)
                }
                #[inline]
                fn import_cos__n(a: Option<f64>) -> Option<f64> {
                    a.map(f64::cos)
                }
                #[inline]
                fn import_sin__n(a: Option<f64>) -> Option<f64> {
                    a.map(f64::sin)
                }
                #[inline]
                fn import_tan__n(a: Option<f64>) -> Option<f64> {
                    a.map(f64::tan)
                }

                #[no_mangle]
                pub extern "Rust" fn waves() -> Option<f64> {
                    import_cos__n(import_sin__n(import_tan__n(import_acos__n(import_asin__n(import_atan__n(Some(1f64)))))))
                }
            }
            .to_string()
        );

        assert_eq_pretty!(
            parse("non_linear", &[], "ln(log10(pi()))"),
            quote! {
                #[inline]
                fn import_ln__n(a: Option<f64>) -> Option<f64> {
                    a.map(f64::ln)
                }
                #[inline]
                fn import_log10__n(a: Option<f64>) -> Option<f64> {
                    a.map(f64::log10)
                }
                #[inline]
                fn import_pi_() -> Option<f64> {
                    Some(std::f64::consts::PI)
                }

                #[no_mangle]
                pub extern "Rust" fn non_linear() -> Option<f64> {
                    import_ln__n(import_log10__n(import_pi_()))
                }
            }
            .to_string()
        );

        assert_eq_pretty!(
            parse("rounding", &[], "round(1.3) + ceil(1.2) + floor(1.1)"),
            quote! {
                #ADD_FN
                #[inline]
                fn import_ceil__n(a: Option<f64>) -> Option<f64> {
                    a.map(f64::ceil)
                }
                #[inline]
                fn import_floor__n(a: Option<f64>) -> Option<f64> {
                    a.map(f64::floor)
                }
                #[inline]
                fn import_round__n(a: Option<f64>) -> Option<f64> {
                    a.map(f64::round)
                }

                #[no_mangle]
                pub extern "Rust" fn rounding() -> Option<f64> {
                    import_add__n_n(
                        import_add__n_n(
                            import_round__n(Some(1.3f64)),
                            import_ceil__n(Some(1.2f64)),
                        ),
                        import_floor__n(Some(1.1f64)),
                    )
                }
            }
            .to_string()
        );

        assert_eq_pretty!(
            parse("radians", &[], "to_radians(1.3) + to_degrees(1.3)"),
            quote! {
                #ADD_FN
                #[inline]
                fn import_to_degrees__n(a: Option<f64>) -> Option<f64> {
                    a.map(f64::to_degrees)
                }
                #[inline]
                fn import_to_radians__n(a: Option<f64>) -> Option<f64> {
                    a.map(f64::to_radians)
                }

                #[no_mangle]
                pub extern "Rust" fn radians() -> Option<f64> {
                    import_add__n_n(import_to_radians__n(Some(1.3f64)), import_to_degrees__n(Some(1.3f64)))
                }
            }
            .to_string()
        );

        assert_eq_pretty!(
            parse("mod_e", &[], "mod(5, e())"),
            quote! {
                #[inline]
                fn import_e_() -> Option<f64> {
                    Some(std::f64::consts::E)
                }
                #[inline]
                fn import_mod__n_n(a: Option<f64>, b: Option<f64>) -> Option<f64> {
                    match (a, b) {
                        (Some(a), Some(b)) => Some(std::ops::Rem::rem(a, b)),
                        _ => None,
                    }
                }

                #[no_mangle]
                pub extern "Rust" fn mod_e() -> Option<f64> {
                    import_mod__n_n(Some(5f64), import_e_())
                }
            }
            .to_string()
        );

        assert_eq!(
            try_parse("will_not_compile", &[], DataType::Number, "max(1, 2, 3)")
                .unwrap_err()
                .to_string(),
                " --> 1:1\n  |\n1 | max(1, 2, 3)\n  | ^----------^\n  |\n  = Invalid function arguments for function `max`: expected [number, number], got [number, number, number]"
        );
    }

    #[test]
    fn boolean_params() {
        assert_eq_pretty!(
            parse("expression", &["a"], "if a is nodata { 0 } else { a }"),
            quote! {
                #[no_mangle]
                pub extern "Rust" fn expression(a: Option<f64>) -> Option<f64> {
                    if ((a) == (None)) {
                        Some(0f64)
                    } else {
                        a
                    }
                }
            }
            .to_string()
        );

        assert_eq_pretty!(
            parse(
                "expression",
                &["A", "B"],
                "if A IS NODATA {
                    B * 2
                } else if A == 6 {
                    NODATA
                } else {
                    A
                }",
            ),
            quote! {
                #MUL_FN

                #[no_mangle]
                pub extern "Rust" fn expression(A: Option<f64>, B: Option<f64>) -> Option<f64> {
                    if ((A) == (None)) {
                        import_mul__n_n(B, Some(2f64))
                    } else if ((A) == (Some(6f64))) {
                        None
                    } else {
                        A
                    }
                }
            }
            .to_string()
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn branches() {
        assert_eq_pretty!(
            parse("expression", &[], "if true { 1 } else { 2 }"),
            quote! {
                #[no_mangle]
                pub extern "Rust" fn expression() -> Option<f64> {
                    if true {
                        Some(1f64)
                    } else {
                        Some(2f64)
                    }
                }
            }
            .to_string()
        );

        assert_eq_pretty!(
            parse(
                "expression",
                &[],
                "if TRUE { 1 } else if false { 2 } else { 1 + 2 }",
            ),
            quote! {
                #ADD_FN

                #[no_mangle]
                pub extern "Rust" fn expression() -> Option<f64> {
                    if true {
                        Some(1f64)
                    } else if false {
                        Some(2f64)
                    } else {
                        import_add__n_n(Some(1f64), Some(2f64))
                    }
                }
            }
            .to_string()
        );

        assert_eq_pretty!(
            parse(
                "expression",
                &[],
                "if 1 < 2 { 1 } else if 1 + 5 < 3 - 1 { 2 } else { 1 + 2 }"
            ),
            quote! {
                #ADD_FN
                #SUB_FN

                #[no_mangle]
                pub extern "Rust" fn expression() -> Option<f64> {
                    if ((Some(1f64)) < (Some(2f64))) {
                        Some(1f64)
                    } else if ((import_add__n_n(Some(1f64), Some(5f64))) < (import_sub__n_n(Some(3f64), Some(1f64)))) {
                        Some(2f64)
                    } else {
                        import_add__n_n(Some(1f64), Some(2f64))
                    }
                }
            }
            .to_string()
        );

        assert_eq_pretty!(
            parse(
                "expression",
                &[],
                "if true && false {
                    1
                } else if (1 < 2) && true {
                    2
                } else {
                    max(1, 2)
                }",
            ),
            quote! {
                #[inline]
                fn import_max__n_n(a: Option<f64>, b: Option<f64>) -> Option<f64> {
                    match (a, b) {
                        (Some(a), Some(b)) => Some(f64::max(a, b)),
                        _ => None,
                    }
                }

                #[no_mangle]
                pub extern "Rust" fn expression() -> Option<f64> {
                    if ((true) && (false)) {
                        Some(1f64)
                    } else if ( (( (Some(1f64)) < (Some(2f64)) )) && (true) ) {
                        Some(2f64)
                    } else {
                        import_max__n_n(Some(1f64), Some(2f64))
                    }
                }
            }
            .to_string()
        );
    }

    #[test]
    fn assignments() {
        assert_eq_pretty!(
            parse(
                "expression",
                &[],
                "let a = 1.2;
                let b = 2;
                a + b + 1",
            ),
            quote! {
                #ADD_FN

                #[no_mangle]
                pub extern "Rust" fn expression() -> Option<f64> {
                    let a = Some(1.2f64);
                    let b = Some(2f64);
                    import_add__n_n(
                        import_add__n_n(a, b),
                        Some(1f64),
                    )
                }
            }
            .to_string()
        );

        assert_eq!(
            try_parse(
                "expression",
                &[Parameter::Number("A".into())],
                DataType::Number,
                "let b = A;
                let b = C;
                let c = 2;
                a + b",
            )
                .unwrap_err()
                .to_string(),
                " --> 2:25\n  |\n2 |                 let b = C;\n  |                         ^\n  |\n  = The variable `C` was not defined",
                "no access before declaration"
        );

        assert_eq!(
            try_parse(
                "expression",
                &[Parameter::Number("A".into())],
                DataType::Number,
                "let A = 2;
                a",
            )
                .unwrap_err()
                .to_string(),
                " --> 1:1\n  |\n1 | let A = 2;\n2 |                 a\n  | ^---------------^\n  |\n  = The variable `A` was already defined",
                "no shadowing"
        );
    }

    #[test]
    fn it_fails_when_using_wrong_datatypes() {
        assert_eq!(
            try_parse(
                "expression",
                &[
                    Parameter::Number("A".into()),
                    Parameter::MultiPoint("B".into())
                ],
                DataType::Number,
                "if true { A } else { B }",
            )
            .unwrap_err()
            .to_string(),
            " --> 1:1\n  |\n1 | if true { A } else { B }\n  | ^----------------------^\n  |\n  = All branches of an if-then-else expression must output the same type",
            "cannot use branches of different data types"
        );

        assert_eq!(
            try_parse(
                "expression",
                &[
                    Parameter::Number("A".into()),
                    Parameter::MultiPoint("B".into())
                ],
                DataType::Number,
                "if B IS NODATA { A } else { A }",
            )
            .unwrap_err()
            .to_string(),
            " --> 1:4\n  |\n1 | if B IS NODATA { A } else { A }\n  |    ^---------^\n  |\n  = Comparisons can only be used with numbers",
            "cannot use non-numeric comparison"
        );

        assert_eq!(
            try_parse(
                "expression",
                &[Parameter::MultiPoint("A".into())],
                DataType::Number,
                "A + 1",
            )
            .unwrap_err()
            .to_string(),
            " --> 1:3\n  |\n1 | A + 1\n  |   ^\n  |\n  = Operators can only be used with numbers",
            "cannot use non-numeric operators"
        );

        assert_eq!(
            try_parse(
                "expression",
                &[Parameter::MultiPoint("A".into())],
                DataType::Number,
                "sqrt(A)",
            )
            .unwrap_err()
            .to_string(),
            " --> 1:1\n  |\n1 | sqrt(A)\n  | ^-----^\n  |\n  = Invalid function arguments for function `sqrt`: expected [number], got [geometry (multipoint)]",
            "cannot call numeric fn with geom"
        );

        assert_eq!(
            try_parse("expression", &[], DataType::MultiPoint, "1",)
                .unwrap_err()
                .to_string(),
                " --> 1:1\n  |\n1 | \n  | ^---\n  |\n  = The expression was expected to output `geometry (multipoint)`, but it outputs `number`",
            "cannot call with wrong output"
        );
    }

    #[test]
    fn it_works_with_geoms() {
        assert_eq_pretty!(
            parse2(
                "make_centroid",
                &[Parameter::MultiPolygon("geom".into())],
                DataType::MultiPoint,
                "centroid(geom)",
            ),
            quote! {
                #[inline]
                fn import_centroid__q(
                    geom: Option<geo::geometry::MultiPolygon<geo::Polygon<f64>>>
                ) -> Option<geo::geometry::MultiPoint<geo::Point<f64>>> {
                    use geo::Centroid;
                    geom.centroid().map(Into::into)
                }

                #[no_mangle]
                pub extern "Rust" fn make_centroid(
                    geom: Option<geo::geometry::MultiPolygon<geo::Polygon<f64>>>
                ) -> Option<geo::geometry::MultiPoint<geo::Point<f64>>> {
                    import_centroid__q(geom)
                }
            }
            .to_string()
        );
    }
}
