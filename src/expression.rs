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

    pub fn fields(&self) -> Vec<&str> {
        let mut v = vec![];
        self.field_recursive(&mut v);
        v
    }

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
}

#[macro_export]
macro_rules! and {
    ($x:expr, $y: expr) => {
        Expression::And(Box::from($x), Box::from($y))
    };
    ($head: expr, $($tail:expr),*) => {
        Expression::And(Box::from($head), Box::from(and!($($tail),*)))
    };
}

#[macro_export]
macro_rules! or {
    ($x:expr, $y: expr) => {
        Expression::Or(Box::from($x), Box::from($y))
    };
    ($head: expr, $($tail:expr),*) => {
        Expression::Or(Box::from($head), Box::from(or!($($tail),*)))
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fields() {
        let expr = and!(
            Expression::binary("id", 5usize, Comparison::LessThan),
            Expression::binary("test", 10usize, Comparison::MoreThan)
        );
        let fields = expr.fields();
        assert_eq!(vec!["id", "test"], fields);
    }

    #[test]
    fn test_true_expression() {
        let expr = and!(
            Expression::binary("id", 5usize, Comparison::LessThan),
            Expression::binary("test", 10usize, Comparison::MoreThan)
        );
        let iter = [Literal::Uint(1), Literal::Uint(20)];
        let res = expr.eval(&mut iter.iter()).unwrap();
        assert!(res, "This expression should return true")
    }

    #[test]
    fn test_false_expression() {
        let expr = or!(
            Expression::binary("id", 5usize, Comparison::LessThan),
            Expression::binary("test", 10usize, Comparison::MoreThan)
        );
        let iter = [Literal::Uint(9), Literal::Uint(10)];
        let res = expr.eval(&mut iter.iter()).unwrap();
        assert!(!res, "This expression should return false")
    }
}
