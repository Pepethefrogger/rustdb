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

pub trait IntervalElement: Ord + Clone + Copy {}
impl<T: Ord + Clone + Copy> IntervalElement for T {}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum IntervalStart<T: IntervalElement> {
    Open(T),
    Closed(T),
}

impl<T: IntervalElement> IntervalStart<T> {
    pub fn past(&self, v: &T) -> bool {
        match self {
            Self::Open(o) => {
                let cmp = v.cmp(o);
                matches!(cmp, Ordering::Greater)
            }
            Self::Closed(c) => {
                let cmp = v.cmp(c);
                matches!(cmp, Ordering::Greater | Ordering::Equal)
            }
        }
    }

    pub fn value(&self) -> &T {
        match self {
            Self::Open(o) => o,
            Self::Closed(c) => c,
        }
    }
}

impl<T: IntervalElement> Eq for IntervalStart<T> {}

impl<T: IntervalElement> PartialOrd for IntervalStart<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: IntervalElement> Ord for IntervalStart<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Open(o1), Self::Open(o2)) => o1.cmp(o2),
            (Self::Closed(c1), Self::Closed(c2)) => c1.cmp(c2),
            (Self::Open(o), Self::Closed(c)) => {
                let cmp = o.cmp(c);
                if let Ordering::Equal = cmp {
                    Ordering::Greater
                } else {
                    cmp
                }
            }
            (Self::Closed(c), Self::Open(o)) => {
                let cmp = c.cmp(o);
                if let Ordering::Equal = cmp {
                    Ordering::Less
                } else {
                    cmp
                }
            }
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum IntervalEnd<T: IntervalElement> {
    Open(T),
    Closed(T),
}

impl<T: IntervalElement> IntervalEnd<T> {
    pub fn before(&self, v: &T) -> bool {
        match self {
            Self::Open(o) => {
                let cmp = v.cmp(o);
                matches!(cmp, Ordering::Less)
            }
            Self::Closed(c) => {
                let cmp = v.cmp(c);
                matches!(cmp, Ordering::Less | Ordering::Equal)
            }
        }
    }

    pub fn value(&self) -> &T {
        match self {
            Self::Open(o) => o,
            Self::Closed(c) => c,
        }
    }
}

impl<T: IntervalElement> Eq for IntervalEnd<T> {}

impl<T: IntervalElement> PartialOrd for IntervalEnd<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: IntervalElement> Ord for IntervalEnd<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Open(o1), Self::Open(o2)) => o1.cmp(o2),
            (Self::Closed(c1), Self::Closed(c2)) => c1.cmp(c2),
            (Self::Open(o), Self::Closed(c)) => {
                let cmp = o.cmp(c);
                if let Ordering::Equal = cmp {
                    Ordering::Less
                } else {
                    cmp
                }
            }
            (Self::Closed(c), Self::Open(o)) => {
                let cmp = c.cmp(o);
                if let Ordering::Equal = cmp {
                    Ordering::Greater
                } else {
                    cmp
                }
            }
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum SimpleRange<T: IntervalElement> {
    Values(IntervalStart<T>, IntervalEnd<T>),
    Value(T),
    Start(IntervalStart<T>),
    End(IntervalEnd<T>),
    Empty,
    Full,
}

impl<T: IntervalElement> SimpleRange<T> {
    fn value_past_start(&self, v: &T) -> bool {
        match self {
            Self::Values(s, _) => s.past(v),
            Self::Value(v) => matches!(v.cmp(v), Ordering::Equal | Ordering::Greater),
            Self::Start(s) => s.past(v),
            Self::End(_) => true,
            Self::Empty => true,
            Self::Full => true,
        }
    }

    fn value_before_end(&self, v: &T) -> bool {
        match self {
            Self::Values(_, e) => e.before(v),
            Self::Value(v) => matches!(v.cmp(v), Ordering::Equal | Ordering::Less),
            Self::Start(_) => true,
            Self::End(e) => e.before(v),
            Self::Empty => true,
            Self::Full => true,
        }
    }

    fn contains(&self, v: &T) -> bool {
        self.value_past_start(v) && self.value_before_end(v)
    }

    /// Returns true if this range overlaps with the other one
    pub fn overlaps(&self, other: &Self) -> bool {
        match self {
            Self::Values(s, e) => other.contains(s.value()) || other.contains(e.value()),
            Self::Value(v) => other.contains(v),
            Self::Start(s) => other.value_before_end(s.value()),
            Self::End(e) => other.value_past_start(e.value()),
            Self::Empty => true,
            Self::Full => true,
        }
    }

    /// Returns the union range
    /// # Requirements
    /// self and other have to overlap
    pub fn union(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::Values(s1, e1), Self::Values(s2, e2)) => {
                let min_start = std::cmp::min(s1, s2);
                let max_end = std::cmp::max(e1, e2);
                Self::Values(*min_start, *max_end)
            }
            (Self::Values(s1, _), Self::Start(s2)) | (Self::Start(s2), Self::Values(s1, _)) => {
                let min_start = std::cmp::min(s1, s2);
                Self::Start(*min_start)
            }
            (Self::Values(_, e1), Self::End(e2)) | (Self::End(e2), Self::Values(_, e1)) => {
                let max_end = std::cmp::max(e1, e2);
                Self::End(*max_end)
            }
            (Self::Values(s1, e1), Self::Value(v)) | (Self::Value(v), Self::Values(s1, e1)) => {
                let s2 = &IntervalStart::Closed(*v);
                let min_start = std::cmp::min(s1, s2);
                let e2 = &IntervalEnd::Closed(*v);
                let max_end = std::cmp::max(e1, e2);
                Self::Values(*min_start, *max_end)
            }
            (Self::Value(v), Self::Start(s)) | (Self::Start(s), Self::Value(v)) => {
                let s2 = &IntervalStart::Closed(*v);
                let min_start = std::cmp::min(s, s2);
                Self::Start(*min_start)
            }
            (Self::Value(v), Self::End(e)) | (Self::End(e), Self::Value(v)) => {
                let e2 = &IntervalEnd::Closed(*v);
                let max_end = std::cmp::max(e, e2);
                Self::End(*max_end)
            }
            (v @ Self::Value(_), Self::Value(_)) => *v,
            (Self::Start(s1), Self::Start(s2)) => {
                let min_start = std::cmp::min(s1, s2);
                Self::Start(*min_start)
            }
            (Self::End(e1), Self::End(e2)) => {
                let max_end = std::cmp::max(e1, e2);
                Self::End(*max_end)
            }
            (Self::Start(_), Self::End(_)) | (Self::End(_), Self::Start(_)) => Self::Full,
            (Self::Full, _) | (_, Self::Full) => Self::Full,
            (Self::Empty, o) | (o, Self::Empty) => *o,
        }
    }

    /// Returns the intesection range
    /// # Requirements
    /// self and other have to overlap
    pub fn intersection(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::Values(s1, e1), Self::Values(s2, e2)) => {
                let max_start = std::cmp::max(s1, s2);
                let min_end = std::cmp::min(e1, e2);
                Self::Values(*max_start, *min_end)
            }
            (Self::Values(s1, e1), Self::Start(s2)) | (Self::Start(s2), Self::Values(s1, e1)) => {
                let max_start = std::cmp::max(s1, s2);
                Self::Values(*max_start, *e1)
            }
            (Self::Values(s1, e1), Self::End(e2)) | (Self::End(e2), Self::Values(s1, e1)) => {
                let min_end = std::cmp::min(e1, e2);
                Self::Values(*s1, *min_end)
            }
            (Self::Start(s1), Self::Start(s2)) => {
                let max_start = std::cmp::max(s1, s2);
                Self::Start(*max_start)
            }
            (Self::End(e1), Self::End(e2)) => {
                let min_end = std::cmp::min(e1, e2);
                Self::End(*min_end)
            }
            (Self::Start(s), Self::End(e)) | (Self::End(e), Self::Start(s)) => Self::Values(*s, *e),
            (Self::Full, o) | (o, Self::Full) => *o,
            (Self::Empty, _) | (_, Self::Empty) => Self::Empty,
            (v @ Self::Value(_), _) | (_, v @ Self::Value(_)) => *v,
        }
    }
}

#[macro_export]
macro_rules! range {
    ({[$x:expr] , [$y:expr]}) => {
        SimpleRange::Values(
            IntervalStart::Closed($x.into()),
            IntervalEnd::Closed($y.into()),
        )
    };
    ({[$x:expr] , ($y:expr)}) => {
        SimpleRange::Values(
            IntervalStart::Closed($x.into()),
            IntervalEnd::Open($y.into()),
        )
    };
    ({($x:expr) , [$y:expr]}) => {
        SimpleRange::Values(
            IntervalStart::Open($x.into()),
            IntervalEnd::Closed($y.into()),
        )
    };
    ({($x:expr) , ($y:expr)}) => {
        SimpleRange::Values(IntervalStart::Open($x.into()), IntervalEnd::Open($y.into()))
    };
    ({[$x: expr] ,}) => {
        SimpleRange::Start(IntervalStart::Closed($x.into()))
    };
    ({($x: expr) ,}) => {
        SimpleRange::Start(IntervalStart::Open($x.into()))
    };
    ({, [$x: expr]}) => {
        SimpleRange::End(IntervalEnd::Closed($x.into()))
    };
    ({, ($x: expr)}) => {
        SimpleRange::End(IntervalEnd::Open($x.into()))
    };
    ({,}) => {
        SimpleRange::Full
    };
    ({}) => {
        SimpleRange::Empty
    };
    ({$x: expr}) => {
        SimpleRange::Value($x.into())
    };
    ($x:tt & $y:tt) => {
        range!($x).union(&range!($y))
    };
    ($x:tt | $y:tt) => {
        range!($x).intersection(&range!($y))
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

    #[test]
    fn test_interval_start() {
        let a: IntervalStart<Literal> = IntervalStart::Open(5usize.into());
        let b = IntervalStart::Open(10usize.into());
        let c = a.partial_cmp(&b).unwrap();
        assert!(matches!(c, Ordering::Less));

        let a: IntervalStart<Literal> = IntervalStart::Closed(5usize.into());
        let b = IntervalStart::Closed(10usize.into());
        let c = a.partial_cmp(&b).unwrap();
        assert!(matches!(c, Ordering::Less));

        let a: IntervalStart<Literal> = IntervalStart::Closed(10usize.into());
        let b = IntervalStart::Open(10usize.into());
        let c = a.partial_cmp(&b).unwrap();
        assert!(
            matches!(c, Ordering::Less),
            "A closed interval with the same value should be ordered less"
        )
    }

    #[test]
    fn test_interval_end() {
        let a: IntervalEnd<Literal> = IntervalEnd::Open(5usize.into());
        let b = IntervalEnd::Open(10usize.into());
        let c = a.partial_cmp(&b).unwrap();
        assert!(matches!(c, Ordering::Less));

        let a: IntervalEnd<Literal> = IntervalEnd::Closed(5usize.into());
        let b = IntervalEnd::Closed(10usize.into());
        let c = a.partial_cmp(&b).unwrap();
        assert!(matches!(c, Ordering::Less));

        let a: IntervalEnd<Literal> = IntervalEnd::Closed(10usize.into());
        let b = IntervalEnd::Open(10usize.into());
        let c = a.partial_cmp(&b).unwrap();
        assert!(
            matches!(c, Ordering::Greater),
            "A closed interval with the same value should be ordered more"
        )
    }

    #[test]
    fn test_simple_range_union() {
        let r: SimpleRange<Literal> = range!(
            {(3usize), (10usize)}
            &
            {(4usize), [10usize]}
        );
        assert_eq!(range!({(3usize), [10usize]}), r);

        let r: SimpleRange<Literal> = range!(
            {(3usize), }
            &
            {(2usize), [10usize]}
        );
        assert_eq!(range!({(2usize), }), r);

        let r: SimpleRange<Literal> = range!(
            {,(5usize)}
            &
            {(2usize), [10usize]}
        );
        assert_eq!(range!({,[10usize]}), r);

        let r: SimpleRange<Literal> = range!(
            {,(5usize)}
            &
            {(2usize),}
        );
        assert_eq!(range!({,}), r);

        let r: SimpleRange<Literal> = range!({,} & {(10usize), [15usize]});
        assert_eq!(range!({,}), r);

        let r: SimpleRange<Literal> = range!({} & {(4usize), [10usize]});
        assert_eq!(range!({(4usize), [10usize]}), r);

        let r: SimpleRange<Literal> = range!({5usize} & {(5usize), [10usize]});
        assert_eq!(range!({[5usize], [10usize]}), r);
    }

    #[test]
    fn test_simple_range_intersection() {
        let r: SimpleRange<Literal> = range!(
            {(3usize), (10usize)}
            |
            {(4usize), [10usize]}
        );
        assert_eq!(range!({(4usize), (10usize)}), r);

        let r: SimpleRange<Literal> = range!(
            {(4usize), }
            |
            {(2usize), [10usize]}
        );
        assert_eq!(range!({(4usize), [10usize]}), r);

        let r: SimpleRange<Literal> = range!(
            {,(5usize)}
            |
            {(2usize), [10usize]}
        );
        assert_eq!(range!({(2usize),(5usize)}), r);

        let r: SimpleRange<Literal> = range!(
            {,(5usize)}
            |
            {(2usize),}
        );
        assert_eq!(range!({(2usize), (5usize)}), r);

        let r: SimpleRange<Literal> = range!({,} | {(10usize), [15usize]});
        assert_eq!(range!({(10usize), [15usize]}), r);

        let r: SimpleRange<Literal> = range!({} | {(4usize), [10usize]});
        assert_eq!(range!({}), r);

        let r: SimpleRange<Literal> = range!({6usize} | {(5usize), [10usize]});
        assert_eq!(range!({ 6usize }), r);
    }
}
