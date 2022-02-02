use std::{cell::RefCell, collections::HashSet, rc::Rc};

use pest::{
    iterators::{Pair, Pairs},
    prec_climber::{Assoc, Operator, PrecClimber},
    Parser,
};
use pest_derive::Parser;
use snafu::{ensure, ResultExt};

use crate::{processing::expression::codegen::Parameter, util::duplicate_or_empty_str_slice};

use super::{
    codegen::{
        Assignment, AstFunction, AstNode, AstOperator, BooleanComparator, BooleanExpression,
        BooleanOperator, Branch, ExpressionAst, Identifier,
    },
    error::{self, ExpressionError},
    functions::FUNCTIONS,
};

type Result<T, E = ExpressionError> = std::result::Result<T, E>;

pub type PestError = pest::error::Error<Rule>;

#[derive(Parser)]
#[grammar = "processing/expression/expression.pest"] // relative to src
struct _ExpressionParser;

/// A parser for user-defined expressions.
pub struct ExpressionParser {
    parameters: Vec<Parameter>,
    numeric_parameters: HashSet<Identifier>,
    boolean_parameters: HashSet<Identifier>,
    variables: Rc<RefCell<Vec<Identifier>>>,
    functions: Rc<RefCell<Vec<AstFunction>>>,
}

lazy_static::lazy_static! {
    static ref EXPRESSION_CLIMBER: PrecClimber<Rule> = PrecClimber::new(vec![
        Operator::new(Rule::add, Assoc::Left) | Operator::new(Rule::subtract, Assoc::Left),
        Operator::new(Rule::multiply, Assoc::Left) | Operator::new(Rule::divide, Assoc::Left),
        Operator::new(Rule::power, Assoc::Right),
    ]);

    static ref BOOLEAN_EXPRESSION_CLIMBER: PrecClimber<Rule> = PrecClimber::new(vec![
        Operator::new(Rule::or, Assoc::Left),
        Operator::new(Rule::and, Assoc::Left),
    ]);
}

impl ExpressionParser {
    pub fn new(parameters: &[Parameter]) -> Result<Self> {
        match duplicate_or_empty_str_slice(parameters) {
            crate::util::DuplicateOrEmpty::Ok => (), // fine
            crate::util::DuplicateOrEmpty::Duplicate(parameter) => {
                return Err(ExpressionError::DuplicateParameterName { parameter });
            }
            crate::util::DuplicateOrEmpty::Empty => {
                return Err(ExpressionError::EmptyParameterName);
            }
        };

        let mut numeric_parameters = HashSet::with_capacity(parameters.len() / 2);
        let mut boolean_parameters = HashSet::with_capacity(parameters.len() / 2);
        for parameter in parameters {
            match parameter {
                Parameter::Number(name) => numeric_parameters.insert(name.clone()),
                Parameter::Boolean(name) => boolean_parameters.insert(name.clone()),
            };
        }

        Ok(Self {
            parameters: parameters.to_vec(),
            numeric_parameters,
            boolean_parameters,
            variables: Rc::new(RefCell::new(Vec::new())),
            functions: Rc::new(RefCell::new(vec![])),
        })
    }

    pub fn parse(self, name: &str, input: &str) -> Result<ExpressionAst> {
        ensure!(!name.is_empty(), error::EmptyExpressionName);

        let pairs = _ExpressionParser::parse(Rule::main, input).context(error::Parser)?;

        let root = self.build_ast(pairs)?;

        ExpressionAst::new(
            name.to_string().into(),
            self.parameters,
            self.functions.borrow_mut().drain(..).collect(),
            root,
        )
    }

    fn build_ast(&self, pairs: Pairs<'_, Rule>) -> Result<AstNode> {
        EXPRESSION_CLIMBER.climb(
            pairs,
            |pair| self.resolve_expression_rule(pair),
            |left, op, right| self.resolve_infix_operations(left, &op, right),
        )
    }

    fn resolve_expression_rule(&self, pair: Pair<Rule>) -> Result<AstNode> {
        match pair.as_rule() {
            Rule::expression => self.build_ast(pair.into_inner()),
            Rule::number => Ok(AstNode::Constant(
                pair.as_str().parse().context(error::InvalidNumber)?,
            )),
            Rule::identifier => {
                let identifier = pair.as_str().into();
                if self.numeric_parameters.contains(&identifier)
                    || self.variables.borrow().contains(&identifier)
                {
                    Ok(AstNode::Variable(identifier))
                } else {
                    Err(ExpressionError::UnknownVariable {
                        variable: identifier.to_string(),
                    })
                }
            }
            Rule::function => self.resolve_function(pair.into_inner()),
            Rule::branch => {
                // pairs are boolean -> expression
                // and last one is just an expression
                let mut pairs = pair.into_inner();

                let mut condition_branches: Vec<Branch> = vec![];

                while let Some(pair) = pairs.next() {
                    if matches!(pair.as_rule(), Rule::boolean_expression) {
                        let boolean = self.build_boolean_expression(pair.into_inner())?;

                        let next_pair = pairs
                            .next()
                            .ok_or(ExpressionError::BranchStructureMalformed)?;
                        let expression = self.build_ast(next_pair.into_inner())?;

                        condition_branches.push(Branch {
                            condition: boolean,
                            body: expression,
                        });
                    } else {
                        let expression = self.build_ast(pair.into_inner())?;

                        return Ok(AstNode::Branch {
                            condition_branches,
                            else_branch: Box::new(expression),
                        });
                    }
                }

                Err(ExpressionError::UnexpectedBranchStructure)
            }
            Rule::assignments_and_expression => {
                let mut assignments: Vec<Assignment> = vec![];

                for pair in pair.into_inner() {
                    if matches!(pair.as_rule(), Rule::assignment) {
                        let mut pairs = pair.into_inner();

                        let first_pair = pairs
                            .next()
                            .ok_or(ExpressionError::AssignmentNeedsTwoParts)?;
                        let second_pair = pairs
                            .next()
                            .ok_or(ExpressionError::AssignmentNeedsTwoParts)?;

                        let identifier = first_pair.as_str().into();

                        if self.numeric_parameters.contains(&identifier) {
                            return Err(ExpressionError::CannotAssignToParameter {
                                parameter: identifier.to_string(),
                            });
                        }

                        // having an assignment allows more variables
                        self.variables.borrow_mut().push(identifier.clone());

                        let expression = self.build_ast(second_pair.into_inner())?;

                        assignments.push(Assignment {
                            identifier,
                            expression,
                        });
                    } else {
                        let expression = self.build_ast(pair.into_inner())?;

                        return Ok(AstNode::AssignmentsAndExpression {
                            assignments,
                            expression: Box::new(expression),
                        });
                    }
                }

                Err(ExpressionError::DoesNotEndWithExpression)
            }
            _ => Err(ExpressionError::UnexpectedRule {
                rule: format!("{:?}", pair.as_rule()),
            }),
        }
    }

    fn resolve_function(&self, mut pairs: Pairs<Rule>) -> Result<AstNode> {
        // first one is name
        let name: Identifier = pairs
            .next()
            .ok_or(ExpressionError::MissingFunctionName)?
            .as_str()
            .into();

        let args = pairs
            .map(|pair| self.build_ast(pair.into_inner()))
            .collect::<Result<Vec<_>, _>>()?;

        match FUNCTIONS.get(name.as_ref()) {
            Some(function) if function.num_args.contains(&args.len()) => {
                self.functions.borrow_mut().push(AstFunction {
                    name: name.clone(),
                    num_parameters: args.len(),
                });

                Ok(AstNode::Function { name, args })
            }
            Some(function) => Err(ExpressionError::InvalidFunctionArgumentCount {
                function: name.to_string(),
                expected_min: *function.num_args.start(),
                expected_max: *function.num_args.end(),
                actual: args.len(),
            }),
            None => Err(ExpressionError::UnknownFunction {
                function: name.to_string(),
            }),
        }
    }

    fn resolve_infix_operations(
        &self,
        left: Result<AstNode>,
        op: &Pair<Rule>,
        right: Result<AstNode>,
    ) -> Result<AstNode> {
        let (left, right) = (left?, right?);

        // change some operators to functions
        if matches!(op.as_rule(), Rule::power) {
            self.functions.borrow_mut().push(AstFunction {
                name: "pow".into(),
                num_parameters: 2,
            });

            return Ok(AstNode::Function {
                name: "pow".into(),
                args: vec![left, right],
            });
        }

        let ast_operator = match op.as_rule() {
            Rule::add => AstOperator::Add,
            Rule::subtract => AstOperator::Subtract,
            Rule::multiply => AstOperator::Multiply,
            Rule::divide => AstOperator::Divide,
            _ => {
                return Err(ExpressionError::UnexpectedOperator {
                    operator: format!("{:?}", op.as_rule()),
                })
            }
        };

        Ok(AstNode::Operation {
            left: Box::new(left),
            op: ast_operator,
            right: Box::new(right),
        })
    }

    fn build_boolean_expression(&self, pairs: Pairs<'_, Rule>) -> Result<BooleanExpression> {
        BOOLEAN_EXPRESSION_CLIMBER.climb(
            pairs,
            |pair| self.resolve_boolean_expression_rule(pair),
            |left, op, right| Self::resolve_infix_boolean_operations(left, &op, right),
        )
    }

    fn resolve_boolean_expression_rule(&self, pair: Pair<Rule>) -> Result<BooleanExpression> {
        match pair.as_rule() {
            Rule::identifier => {
                let identifier = pair.as_str().into();
                if self.boolean_parameters.contains(&identifier) {
                    Ok(BooleanExpression::Variable(identifier))
                } else {
                    Err(ExpressionError::UnknownBooleanVariable {
                        variable: identifier.to_string(),
                    })
                }
            }
            Rule::identifier_is_nodata => {
                // convert `A IS NODATA` to the variable `A_is_nodata`

                let mut pairs = pair.into_inner();

                let identifier = pairs
                    .next()
                    .ok_or(ExpressionError::MissingIdentifier)?
                    .as_str();

                let new_identifier = format!("{}_is_nodata", identifier).into();

                if self.boolean_parameters.contains(&new_identifier) {
                    Ok(BooleanExpression::Variable(new_identifier))
                } else {
                    Err(ExpressionError::UnknownBooleanVariable {
                        variable: identifier.to_string(),
                    })
                }
            }
            Rule::boolean_true => Ok(BooleanExpression::Constant(true)),
            Rule::boolean_false => Ok(BooleanExpression::Constant(false)),
            Rule::boolean_comparison => {
                let mut pairs = pair.into_inner();

                let first_pair = pairs
                    .next()
                    .ok_or(ExpressionError::ComparisonNeedsThreeParts)?;
                let second_pair = pairs
                    .next()
                    .ok_or(ExpressionError::ComparisonNeedsThreeParts)?;
                let third_pair = pairs
                    .next()
                    .ok_or(ExpressionError::ComparisonNeedsThreeParts)?;

                let left_expression = self.build_ast(first_pair.into_inner())?;
                let comparison = match second_pair.as_rule() {
                    Rule::equals => BooleanComparator::Equal,
                    Rule::not_equals => BooleanComparator::NotEqual,
                    Rule::smaller => BooleanComparator::LessThan,
                    Rule::smaller_equals => BooleanComparator::LessThanOrEqual,
                    Rule::larger => BooleanComparator::GreaterThan,
                    Rule::larger_equals => BooleanComparator::GreaterThanOrEqual,
                    _ => {
                        return Err(ExpressionError::UnexpectedComparator {
                            comparator: format!("{:?}", second_pair.as_rule()),
                        })
                    }
                };
                let right_expression = self.build_ast(third_pair.into_inner())?;

                Ok(BooleanExpression::Comparison {
                    left: Box::new(left_expression),
                    op: comparison,
                    right: Box::new(right_expression),
                })
            }
            Rule::boolean_expression => self.build_boolean_expression(pair.into_inner()),
            _ => Err(ExpressionError::UnexpectedBooleanRule {
                rule: format!("{:?}", pair.as_rule()),
            }),
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
                return Err(ExpressionError::UnexpectedBooleanOperator {
                    operator: format!("{:?}", op.as_rule()),
                });
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
    use quote::{quote, ToTokens};

    use super::*;

    fn parse(name: &str, parameters: &[&str], boolean_parameters: &[&str], input: &str) -> String {
        let parameters: Vec<Parameter> = parameters
            .iter()
            .map(|&p| Parameter::Number(Identifier::from(p)))
            .chain(
                boolean_parameters
                    .iter()
                    .map(|&p| Parameter::Boolean(Identifier::from(p))),
            )
            .collect();

        let parser = ExpressionParser::new(&parameters).unwrap();
        let ast = parser.parse(name, input).unwrap();

        ast.into_token_stream().to_string()
    }

    #[test]
    fn simple() {
        assert_eq!(
            parse("expression", &[], &[], "1"),
            quote! {
                #[no_mangle]
                pub extern "C" fn expression() -> f64 {
                    1f64
                }
            }
            .to_string()
        );

        assert_eq!(
            parse("foo", &[], &[], "1 + 2"),
            quote! {
                #[no_mangle]
                pub extern "C" fn foo() -> f64 {
                    (1f64 + 2f64)
                }
            }
            .to_string()
        );

        assert_eq!(
            parse("bar", &[], &[], "-1 + 2"),
            quote! {
                #[no_mangle]
                pub extern "C" fn bar() -> f64 {
                    (-1f64 + 2f64)
                }
            }
            .to_string()
        );

        assert_eq!(
            parse("baz", &[], &[], "1 - -2"),
            quote! {
                #[no_mangle]
                pub extern "C" fn baz() -> f64 {
                    (1f64 - -2f64)
                }
            }
            .to_string()
        );

        assert_eq!(
            parse("expression", &[], &[], "1 + 2 / 3"),
            quote! {
                #[no_mangle]
                pub extern "C" fn expression() -> f64 {
                    (1f64 + (2f64 / 3f64))
                }
            }
            .to_string()
        );

        assert_eq!(
            parse("expression", &[], &[], "2**4"),
            quote! {
                #[inline]
                fn import_pow(a: f64, b: f64) -> f64 {
                    f64::powf(a, b)
                }
                #[no_mangle]
                pub extern "C" fn expression() -> f64 {
                    import_pow(2f64 , 4f64)
                }
            }
            .to_string()
        );
    }

    #[test]
    fn params() {
        assert_eq!(
            parse("expression", &["a"], &[], "a + 1"),
            quote! {
                #[no_mangle]
                pub extern "C" fn expression(a: f64) -> f64 {
                    (a + 1f64)
                }
            }
            .to_string()
        );

        assert_eq!(
            parse("ndvi", &["a", "b"], &[], "(a-b) / (a+b)"),
            quote! {
                #[no_mangle]
                pub extern "C" fn ndvi(a: f64, b: f64) -> f64 {
                    ((a - b) / (a + b))
                }
            }
            .to_string()
        );
    }

    #[test]
    fn functions() {
        assert_eq!(
            parse("expression", &["a"], &[], "max(a, 0)"),
            quote! {
                #[inline]
                fn import_max(a: f64, b: f64) -> f64 {
                    f64::max(a, b)
                }
                #[no_mangle]
                pub extern "C" fn expression(a: f64) -> f64 {
                    import_max(a, 0f64)
                }
            }
            .to_string()
        );

        assert_eq!(
            parse("expression", &["a"], &[], "pow(sqrt(a), 2)"),
            quote! {
                #[inline]
                fn import_pow(a: f64, b: f64) -> f64 {
                    f64::powf(a, b)
                }
                #[inline]
                fn import_sqrt(a: f64) -> f64 {
                    f64::sqrt(a)
                }
                #[no_mangle]
                pub extern "C" fn expression(a: f64) -> f64 {
                    import_pow(import_sqrt(a), 2f64)
                }
            }
            .to_string()
        );

        assert_eq!(
            parse("waves", &[], &[], "cos(sin(tan(acos(asin(atan(1))))))"),
            quote! {
                #[inline]
                fn import_acos(a: f64) -> f64 {
                    f64::acos(a)
                }
                #[inline]
                fn import_asin(a: f64) -> f64 {
                    f64::asin(a)
                }
                #[inline]
                fn import_atan(a: f64) -> f64 {
                    f64::atan(a)
                }
                #[inline]
                fn import_cos(a: f64) -> f64 {
                    f64::cos(a)
                }
                #[inline]
                fn import_sin(a: f64) -> f64 {
                    f64::sin(a)
                }
                #[inline]
                fn import_tan(a: f64) -> f64 {
                    f64::tan(a)
                }
                #[no_mangle]
                pub extern "C" fn waves() -> f64 {
                    import_cos(import_sin(import_tan(import_acos(import_asin(import_atan(1f64))))))
                }
            }
            .to_string()
        );

        assert_eq!(
            parse("non_linear", &[], &[], "ln(log10(pi()))"),
            quote! {
                #[inline]
                fn import_ln(a: f64) -> f64 {
                    f64::ln(a)
                }
                #[inline]
                fn import_log10(a: f64) -> f64 {
                    f64::log10(a)
                }
                #[inline]
                fn import_pi() -> f64 {
                    std::f64::consts::PI
                }
                #[no_mangle]
                pub extern "C" fn non_linear() -> f64 {
                    import_ln(import_log10(import_pi()))
                }
            }
            .to_string()
        );
    }

    #[test]
    fn boolean_params() {
        assert_eq!(
            parse(
                "expression",
                &["a"],
                &["a_is_nodata"],
                "if a is nodata { 0 } else { a }"
            ),
            quote! {
                #[no_mangle]
                pub extern "C" fn expression(a: f64, a_is_nodata: bool) -> f64 {
                    if a_is_nodata {
                        0f64
                    } else {
                        a
                    }
                }
            }
            .to_string()
        );

        assert_eq!(
            parse(
                "expression",
                &["A", "B", "out_nodata"],
                &["A_is_nodata", "B_is_nodata"],
                "if A IS NODATA {
                    B * 2
                } else if A == 6 {
                    out_nodata
                } else {
                    A
                }"
            ),
            quote! {
                #[no_mangle]
                pub extern "C" fn expression(A: f64, B: f64, out_nodata: f64, A_is_nodata: bool, B_is_nodata: bool) -> f64 {
                    if A_is_nodata {
                        (B * 2f64)
                    } else if ((A) == (6f64)) {
                        out_nodata
                    } else {
                        A
                    }
                }
            }
            .to_string()
        );
    }

    #[test]
    fn branches() {
        assert_eq!(
            parse("expression", &[], &[], "if true { 1 } else { 2 }"),
            quote! {
                #[no_mangle]
                pub extern "C" fn expression() -> f64 {
                    if true {
                        1f64
                    } else {
                        2f64
                    }
                }
            }
            .to_string()
        );

        assert_eq!(
            parse(
                "expression",
                &[],
                &[],
                "if TRUE { 1 } else if false { 2 } else { 1 + 2 }"
            ),
            quote! {
                #[no_mangle]
                pub extern "C" fn expression() -> f64 {
                    if true {
                        1f64
                    } else if false {
                        2f64
                    } else {
                        (1f64 + 2f64)
                    }
                }
            }
            .to_string()
        );

        assert_eq!(
            parse(
                "expression",
                &[],
                &[],
                "if 1 < 2 { 1 } else if 1 + 5 < 3 - 1 { 2 } else { 1 + 2 }"
            ),
            quote! {
                #[no_mangle]
                pub extern "C" fn expression() -> f64 {
                    if ((1f64) < (2f64)) {
                        1f64
                    } else if (((1f64 + 5f64)) < ((3f64 - 1f64))) {
                        2f64
                    } else {
                        (1f64 + 2f64)
                    }
                }
            }
            .to_string()
        );

        assert_eq!(
            parse(
                "expression",
                &[],
                &[],
                "if true && false {
                    1
                } else if (1 < 2) && true {
                    2
                } else {
                    max(1, 2)
                }"
            ),
            quote! {
                #[inline]
                fn import_max(a: f64, b: f64) -> f64 {
                    f64::max(a, b)
                }
                #[no_mangle]
                pub extern "C" fn expression() -> f64 {
                    if ((true) && (false)) {
                        1f64
                    } else if ((((1f64) < (2f64))) && (true)) {
                        2f64
                    } else {
                        import_max(1f64, 2f64)
                    }
                }
            }
            .to_string()
        );
    }

    #[test]
    fn assignments() {
        assert_eq!(
            parse(
                "expression",
                &[],
                &[],
                "let a = 1.2;
                let b = 2;
                a + b + 1"
            ),
            quote! {
                #[no_mangle]
                pub extern "C" fn expression() -> f64 {
                    let a = 1.2f64;
                    let b = 2f64;
                    ((a + b) + 1f64)
                }
            }
            .to_string()
        );
    }
}
