use std::{cmp::Ordering, io};

use crate::query::{Identifier, Literal};

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Comparison {
    Equals,
    NotEquals,
    LessThanEquals,
    LessThan,
    MoreThanEquals,
    MoreThan,
}

impl Comparison {
    fn pass_filter(&self, ord: Ordering) -> bool {
        match self {
            Self::Equals => matches!(ord, Ordering::Equal),
            Self::NotEquals => !matches!(ord, Ordering::Equal),
            Self::LessThanEquals => matches!(ord, Ordering::Equal | Ordering::Less),
            Self::LessThan => matches!(ord, Ordering::Less),
            Self::MoreThanEquals => matches!(ord, Ordering::Equal | Ordering::Greater),
            Self::MoreThan => matches!(ord, Ordering::Greater),
        }
    }
    pub fn eval(&self, left: &Literal, right: &Literal) -> io::Result<bool> {
        match (left, right) {
            (Literal::Uint(l), Literal::Uint(r)) => {
                let ordering = l.cmp(r);
                Ok(self.pass_filter(ordering))
            }
            (Literal::String(l), Literal::String(r)) => {
                let ordering = l.cmp(r);
                Ok(self.pass_filter(ordering))
            }
            (Literal::Int(l), Literal::Int(r)) => {
                let ordering = l.cmp(r);
                Ok(self.pass_filter(ordering))
            }
            (Literal::Float(l), Literal::Float(r)) => {
                let ordering = l.total_cmp(r);
                Ok(self.pass_filter(ordering))
            }
            _ => Err(io::Error::other("Both fields have to be of the same type")),
        }
    }
}

pub type BoxedExpression<'a> = Box<Expression<'a>>;
#[derive(Clone, PartialEq, Debug)]
pub enum Expression<'a> {
    And(BoxedExpression<'a>, BoxedExpression<'a>),
    Or(BoxedExpression<'a>, BoxedExpression<'a>),
    Binary {
        left: &'a Identifier,
        right: Literal<'a>,
        sym: Comparison,
    },
}

impl<'a> Expression<'a> {
    pub fn binary(
        left: impl Into<&'a Identifier>,
        right: impl Into<Literal<'a>>,
        sym: Comparison,
    ) -> Self {
        Self::Binary {
            left: left.into(),
            right: right.into(),
            sym,
        }
    }

    fn field_recursive(&'a self, v: &mut Vec<&'a str>) {
        match self {
            Self::And(l, r) => {
                l.field_recursive(v);
                r.field_recursive(v);
            }
            Self::Or(l, r) => {
                l.field_recursive(v);
                r.field_recursive(v);
            }
            &Self::Binary { left, .. } => v.push(left),
        }
    }

    /// This function returns the fields that need to be passed to the eval function.
    /// It iterates over all the identifiers of expressions recursively and returns them
    pub fn fields(&self) -> Vec<&str> {
        let mut v = vec![];
        self.field_recursive(&mut v);
        v
    }

    /// This function uses an iterator of Literals that should come from the fields in self.fields
    /// to evaluate an expression
    /// Self::extract_index should be used before to get index constraints instead of filtering
    pub fn eval(&self, iter: &mut impl Iterator<Item = &'a Literal<'a>>) -> io::Result<bool> {
        match self {
            Self::And(l, r) => Ok(l.eval(iter)? && r.eval(iter)?),
            Self::Or(l, r) => Ok(l.eval(iter)? || r.eval(iter)?),
            Self::Binary { right, sym, .. } => {
                let left = iter.next().ok_or(io::Error::other("Ran out of fields"))?;
                sym.eval(left, right)
            }
        }
    }

    /// Strips all of the index comparisons into constraints
    /// This removes all references to the index from the expression
    pub fn extract_index(&mut self, index_name: &str) -> Constraint<'a> {
        match self {
            Expression::And(left, right) => {
                let l = left.extract_index(index_name);
                let r = right.extract_index(index_name);
                match (l, r) {
                    (Constraint::Empty, Constraint::Empty) => Constraint::Empty,
                    (c @ Constraint::SimpleConstraint(..), Constraint::Empty) => {
                        *self = *right.clone();
                        c
                    }
                    (Constraint::Empty, c @ Constraint::SimpleConstraint(..)) => {
                        *self = *left.clone();
                        c
                    }
                    (l, r) => Constraint::And(l.into(), r.into()),
                }
            }
            Expression::Or(left, right) => {
                let l = left.extract_index(index_name);
                let r = right.extract_index(index_name);
                match (l, r) {
                    (Constraint::Empty, Constraint::Empty) => Constraint::Empty,
                    (c @ Constraint::SimpleConstraint(..), Constraint::Empty) => {
                        *self = *right.clone();
                        c
                    }
                    (Constraint::Empty, c @ Constraint::SimpleConstraint(..)) => {
                        *self = *left.clone();
                        c
                    }
                    (l, r) => Constraint::Or(l.into(), r.into()),
                }
            }
            Expression::Binary { left, right, sym } => {
                if &(***left) == index_name {
                    Constraint::SimpleConstraint(*sym, *right)
                } else {
                    Constraint::Empty
                }
            }
        }
    }
}

#[macro_export]
macro_rules! expr_and {
    ($x:expr, $y: expr) => {
        Expression::And(Box::from($x), Box::from($y))
    };
    ($head: expr, $($tail:expr),*) => {
        Expression::And(Box::from($head), Box::from(expr_and!($($tail),*)))
    };
}

#[macro_export]
macro_rules! expr_or {
    ($x:expr, $y: expr) => {
        Expression::Or(Box::from($x), Box::from($y))
    };
    ($head: expr, $($tail:expr),*) => {
        Expression::Or(Box::from($head), Box::from(expr_or!($($tail),*)))
    };
}

#[derive(Debug, PartialEq)]
pub enum Constraint<'a> {
    And(Box<Constraint<'a>>, Box<Constraint<'a>>),
    Or(Box<Constraint<'a>>, Box<Constraint<'a>>),
    SimpleConstraint(Comparison, Literal<'a>),
    Empty,
}

impl<'a> Constraint<'a> {
    pub fn simple(comp: Comparison, lit: impl Into<Literal<'a>>) -> Self {
        Self::SimpleConstraint(comp, lit.into())
    }
}

#[macro_export]
macro_rules! constr_and {
    ($x:expr, $y: expr) => {
        Constraint::And(Box::from($x), Box::from($y))
    };
    ($head: expr, $($tail:expr),*) => {
        Constraint::And(Box::from($head), Box::from(constr_and!($($tail),*)))
    };
}

#[macro_export]
macro_rules! constr_or {
    ($x:expr, $y: expr) => {
        Constraint::Or(Box::from($x), Box::from($y))
    };
    ($head: expr, $($tail:expr),*) => {
        Constraint::Or(Box::from($head), Box::from(constr_or!($($tail),*)))
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fields() {
        let expr = expr_and!(
            Expression::binary("id", 5usize, Comparison::LessThan),
            Expression::binary("test", 10usize, Comparison::MoreThan)
        );
        let fields = expr.fields();
        assert_eq!(vec!["id", "test"], fields);
    }

    #[test]
    fn test_true_expression() {
        let expr = expr_and!(
            Expression::binary("id", 5usize, Comparison::LessThan),
            Expression::binary("test", 10usize, Comparison::MoreThan)
        );
        let iter = [Literal::Uint(1), Literal::Uint(20)];
        let res = expr.eval(&mut iter.iter()).unwrap();
        assert!(res, "This expression should return true")
    }

    #[test]
    fn test_false_expression() {
        let expr = expr_or!(
            Expression::binary("id", 5usize, Comparison::LessThan),
            Expression::binary("test", 10usize, Comparison::MoreThan)
        );
        let iter = [Literal::Uint(9), Literal::Uint(10)];
        let res = expr.eval(&mut iter.iter()).unwrap();
        assert!(!res, "This expression should return false")
    }

    #[test]
    fn test_extracting_index_constraint() {
        let index = "id";
        let sub1 = expr_and!(
            Expression::binary(index, 20usize, Comparison::MoreThan),
            Expression::binary("test", 5usize, Comparison::Equals)
        );
        let sub2 = expr_and!(
            Expression::binary(index, 5usize, Comparison::LessThan),
            Expression::binary("test", 10usize, Comparison::LessThan)
        );

        let mut expr = expr_or!(sub1, sub2);
        let constraint = expr.extract_index(index);
        assert_eq!(
            constr_or!(
                Constraint::simple(Comparison::MoreThan, 20usize),
                Constraint::simple(Comparison::LessThan, 5usize)
            ),
            constraint
        );
    }

    #[test]
    fn test_extracting_index_remaining_expression() {
        let index = "id";
        let first_expr = Expression::binary("test", 5usize, Comparison::Equals);
        let sub1 = expr_and!(
            Expression::binary(index, 20usize, Comparison::MoreThan),
            first_expr.clone()
        );
        let second_expr = Expression::binary("test", 10usize, Comparison::LessThan);
        let sub2 = expr_and!(
            Expression::binary(index, 5usize, Comparison::LessThan),
            second_expr.clone()
        );

        let mut expr = expr_or!(sub1, sub2);
        let _constraint = expr.extract_index(index);
        assert_eq!(expr_or!(first_expr, second_expr), expr);
    }
}
