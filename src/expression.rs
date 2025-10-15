use crate::utils::range::Range;
use crate::utils::range::SimpleRange;
use crate::{range, simple_range};
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
    pub fn eval(&self, left: &Literal, right: &Literal) -> bool {
        let ordering = left
            .partial_cmp(right)
            .expect("The two expressions should have the same type");
        self.pass_filter(ordering)
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

    // TODO: Optimize this to not have to read the same fields a lot of times
    /// This function uses an iterator of Literals that should come from the fields in self.fields
    /// to evaluate an expression
    /// Self::extract_index should be used before to get index constraints instead of filtering
    pub fn eval(&self, iter: &mut impl Iterator<Item = &'a Literal<'a>>) -> io::Result<bool> {
        match self {
            Self::And(l, r) => Ok(l.eval(iter)? && r.eval(iter)?),
            Self::Or(l, r) => Ok(l.eval(iter)? || r.eval(iter)?),
            Self::Binary { right, sym, .. } => {
                let left = iter.next().ok_or(io::Error::other("Ran out of fields"))?;
                Ok(sym.eval(left, right))
            }
        }
    }

    /// Strips all of the index comparisons into constraints
    /// This removes all references to the index from the expression
    /// Returns (Range, bool), where the bool represents if the expression is empty
    pub fn extract_index(&mut self, index_name: &str) -> (Range<Literal<'a>>, bool) {
        match self {
            Expression::And(left, right) => {
                let (l, l_remove) = left.extract_index(index_name);
                let (r, r_remove) = right.extract_index(index_name);
                let mut intersection = l;
                intersection.intersection(r);

                if l_remove && r_remove {
                    (intersection, true)
                } else if l_remove {
                    *self = *right.clone();
                    (intersection, false)
                } else if r_remove {
                    *self = *left.clone();
                    (intersection, false)
                } else {
                    (intersection, false)
                }
            }
            Expression::Or(left, right) => {
                let (l, l_remove) = left.extract_index(index_name);
                let (r, r_remove) = right.extract_index(index_name);
                let mut union = l;
                union.union(r);

                if l_remove && r_remove {
                    (union, true)
                } else if l_remove {
                    *self = *right.clone();
                    (union, false)
                } else if r_remove {
                    *self = *left.clone();
                    (union, false)
                } else {
                    (union, false)
                }
            }
            Expression::Binary { left, right, sym } => {
                if &(***left) == index_name {
                    let r = Range::from_comparison(*sym, *right);
                    (r, true)
                } else {
                    (range!({,}), false)
                }
            }
        }
    }
}

#[macro_export]
macro_rules! expression {
    ($x:ident) => {
        $x
    };
    (($($x:tt)+)) => {
        expression!($($x)*)
    };
    ($x:tt & $($y:tt)+) => {
        Expression::And(Box::from(expression!($x)), Box::from(expression!($($y)*)))
    };
    ($x:tt | $($y:tt)+) => {
        Expression::Or(Box::from(expression!($x)), Box::from(expression!($($y)*)))
    };
    ($x:tt = $y:tt) => {
        Expression::Binary { left: $x.into(), right: $y.into(), sym: Comparison::Equals}
    };
    ($x:tt != $y:tt) => {
        Expression::Binary { left: $x.into(), right: $y.into(), sym: Comparison::NotEquals}
    };
    ($x:tt >= $y:tt) => {
        Expression::Binary { left: $x.into(), right: $y.into(), sym: Comparison::MoreThanEquals}
    };
    ($x:tt > $y:tt) => {
        Expression::Binary { left: $x.into(), right: $y.into(), sym: Comparison::MoreThan}
    };
    ($x:tt <= $y:tt) => {
        Expression::Binary { left: $x.into(), right: $y.into(), sym: Comparison::LessThanEquals}
    };
    ($x:tt < $y:tt) => {
        Expression::Binary { left: $x.into(), right: $y.into(), sym: Comparison::LessThan}
    };
}

#[cfg(test)]
mod tests {
    use crate::expression;
    use crate::utils::range::IntervalEnd;
    use crate::utils::range::IntervalStart;

    use super::*;

    #[test]
    fn test_fields() {
        let expr = expression!(("id" < 5usize) & ("test" > 10usize));
        let fields = expr.fields();
        assert_eq!(vec!["id", "test"], fields);
    }

    #[test]
    fn test_true_expression() {
        let expr = expression!(("id" < 5usize) & ("test" > 10usize));
        let iter = [Literal::Uint(1), Literal::Uint(20)];
        let res = expr.eval(&mut iter.iter()).unwrap();
        assert!(res, "This expression should return true")
    }

    #[test]
    fn test_false_expression() {
        let expr = expression!(("id" < 5usize) & ("test" > 10usize));
        let iter = [Literal::Uint(9), Literal::Uint(10)];
        let res = expr.eval(&mut iter.iter()).unwrap();
        assert!(!res, "This expression should return false")
    }

    #[test]
    fn test_extracting_index() {
        let index = "id";
        let field = "test";

        let mut expr = expression!(
            ((index < 10usize) & (field = 5usize)) | ((index > 20usize) & (field = 10usize))
        );

        let (range, empty) = expr.extract_index(index);
        assert!(!empty, "Expression shouldn't be empty");

        assert_eq!(
            range.buf,
            vec![simple_range!({,(10usize)}), simple_range!({(20usize),})]
        );

        assert_eq!(expr, expression!((field = 5usize) | (field = 10usize)));
    }
}
