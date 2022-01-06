use std::{cell::RefCell, rc::Rc};

use pest::{
    iterators::{Pair, Pairs},
    prec_climber::{Assoc, Operator, PrecClimber},
    Parser,
};
use pest_derive::Parser;
use snafu::{ensure, ResultExt};

use crate::util::duplicate_or_empty_str_slice;

use super::{
    codegen::{
        Assignment, AstNode, AstOperator, BooleanComparator, BooleanExpression, BooleanOperator,
        Branch, ExpressionAst, Identifier,
    },
    error::{self, ExpressionError},
};

type Result<T, E = ExpressionError> = std::result::Result<T, E>;

pub type PestError = pest::error::Error<Rule>;

#[derive(Parser)]
#[grammar = "processing/new_expression/expression.pest"] // relative to src
struct _ExpressionParser;

/// A parser for user-defined expressions.
pub struct ExpressionParser {
    parameters: Vec<Identifier>,
    variables: Rc<RefCell<Vec<Identifier>>>,
    imports: Rc<RefCell<Vec<Identifier>>>,
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
    pub fn new(parameters: &[String]) -> Result<Self> {
        match duplicate_or_empty_str_slice(parameters) {
            crate::util::DuplicateOrEmpty::Ok => (), // fine
            crate::util::DuplicateOrEmpty::Duplicate(parameter) => {
                return Err(ExpressionError::DuplicateParameterName { parameter });
            }
            crate::util::DuplicateOrEmpty::Empty => {
                return Err(ExpressionError::EmptyParameterName);
            }
        };

        for parameter in parameters {
            ensure!(!parameter.is_empty(), error::EmptyParameterName);
        }

        Ok(Self {
            parameters: parameters.iter().map(Into::into).collect(),
            variables: Rc::new(RefCell::new(Vec::new())),
            imports: Rc::new(RefCell::new(vec![])),
        })
    }

    pub fn parse(self, name: &str, input: &str) -> Result<ExpressionAst> {
        ensure!(!name.is_empty(), error::EmptyExpressionName);

        let pairs = _ExpressionParser::parse(Rule::main, input).context(error::ParserError)?;

        let root = self.build_ast(pairs)?;

        ExpressionAst::new(
            name.to_string().into(),
            self.parameters,
            self.imports.borrow().iter().cloned().collect(),
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
        // dbg!(&pair);
        match pair.as_rule() {
            Rule::expression => self.build_ast(pair.into_inner()),
            Rule::number => Ok(AstNode::Constant(
                pair.as_str().parse().context(error::InvalidNumber)?,
            )),
            Rule::identifier => {
                let identifier = pair.as_str().into();
                if self.parameters.contains(&identifier)
                    || self.variables.borrow().contains(&identifier)
                {
                    Ok(AstNode::Variable(identifier))
                } else {
                    Err(ExpressionError::UnknownVariable {
                        variable: identifier.to_string(),
                    })
                }
            }
            Rule::function => {
                let mut pairs = pair.into_inner();

                // first one is name
                let name: Identifier = pairs
                    .next()
                    .ok_or(ExpressionError::MissingFunctionName)?
                    .as_str()
                    .into();

                let args = pairs
                    .map(|pair| self.build_ast(pair.into_inner()))
                    .collect::<Result<Vec<_>, _>>()?;

                self.imports.borrow_mut().push(name.clone());

                Ok(AstNode::Function { name, args })
            }
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

                        if self.parameters.contains(&identifier) {
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

    fn resolve_infix_operations(
        &self,
        left: Result<AstNode>,
        op: &Pair<Rule>,
        right: Result<AstNode>,
    ) -> Result<AstNode> {
        let (left, right) = (left?, right?);

        // change some operators to functions
        if matches!(op.as_rule(), Rule::power) {
            self.imports.borrow_mut().push("pow".into());

            return Ok(AstNode::Function {
                name: "pow".into(),
                args: vec![left, right],
            });
        }

        // dbg!("merge", &left, &op, &right);
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

        // dbg!("merge", &left, &op, &right);
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

    fn parse(name: &str, parameters: &[&str], input: &str) -> String {
        let parameters = parameters
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>();

        let parser = ExpressionParser::new(&parameters).unwrap();
        let ast = parser.parse(name, input).unwrap();

        ast.into_token_stream().to_string()
    }

    #[test]
    fn simple() {
        assert_eq!(
            parse("expression", &[], "1"),
            quote! {
                #[no_mangle]
                pub extern "C" fn expression() -> f64 {
                    1f64
                }
            }
            .to_string()
        );

        assert_eq!(
            parse("foo", &[], "1 + 2"),
            quote! {
                #[no_mangle]
                pub extern "C" fn foo() -> f64 {
                    (1f64 + 2f64)
                }
            }
            .to_string()
        );

        assert_eq!(
            parse("bar", &[], "-1 + 2"),
            quote! {
                #[no_mangle]
                pub extern "C" fn bar() -> f64 {
                    (-1f64 + 2f64)
                }
            }
            .to_string()
        );

        assert_eq!(
            parse("baz", &[], "1 - -2"),
            quote! {
                #[no_mangle]
                pub extern "C" fn baz() -> f64 {
                    (1f64 - -2f64)
                }
            }
            .to_string()
        );

        assert_eq!(
            parse("expression", &[], "1 + 2 / 3"),
            quote! {
                #[no_mangle]
                pub extern "C" fn expression() -> f64 {
                    (1f64 + (2f64 / 3f64))
                }
            }
            .to_string()
        );

        assert_eq!(
            parse("expression", &[], "2**4"),
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
            parse("expression", &["a"], "a + 1"),
            quote! {
                #[no_mangle]
                pub extern "C" fn expression(a: f64) -> f64 {
                    (a + 1f64)
                }
            }
            .to_string()
        );

        assert_eq!(
            parse("ndvi", &["a", "b"], "(a-b) / (a+b)"),
            quote! {
                #[no_mangle]
                pub extern "C" fn ndvi(a: f64, b: f64) -> f64 {
                    ((a - b) / (a + b))
                }
            }
            .to_string()
        );

        assert_eq!(
            parse("expression", &["a"], "max(a, 0)"),
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
    }

    #[test]
    fn branches() {
        assert_eq!(
            parse("expression", &[], "if true { 1 } else { 2 }"),
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
