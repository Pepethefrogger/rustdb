pub use crate::range;
use std::cmp::Ordering;

use crate::expression::Comparison;

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

    pub fn open(&self) -> bool {
        match self {
            Self::Open(_) => true,
            Self::Closed(_) => false,
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

    pub fn open(&self) -> bool {
        match self {
            Self::Open(_) => true,
            Self::Closed(_) => false,
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
    pub fn value_past_start(&self, v: &T) -> bool {
        match self {
            Self::Values(s, _) => s.past(v),
            Self::Value(v) => matches!(v.cmp(v), Ordering::Equal | Ordering::Greater),
            Self::Start(s) => s.past(v),
            Self::End(_) => true,
            Self::Empty => true,
            Self::Full => true,
        }
    }

    pub fn value_before_end(&self, v: &T) -> bool {
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

    /// Returns the value at the start None if there isn't a start
    pub fn start(&self) -> Option<T> {
        match self {
            Self::Value(v) => Some(*v),
            Self::Values(s, _) => Some(*s.value()),
            Self::Start(s) => Some(*s.value()),
            Self::End(_) => None,
            Self::Empty => None,
            Self::Full => None,
        }
    }

    /// Returns the value at the end None if there isn't an end
    pub fn end(&self) -> Option<T> {
        match self {
            Self::Value(v) => Some(*v),
            Self::Values(_, e) => Some(*e.value()),
            Self::Start(_) => None,
            Self::End(e) => Some(*e.value()),
            Self::Empty => None,
            Self::Full => None,
        }
    }
}

#[macro_export]
macro_rules! simple_range {
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
    ($x:tt | $($y:tt)+) => {
        simple_range!($x).union(&simple_range!($($y)*))
    };
    ($x:tt & $($y:tt)+) => {
        simple_range!($x).intersection(&simple_range!($($y)*))
    };
    (($($x:tt)+)) => {
        simple_range!($($x)*)
    }
}

#[derive(Debug)]
pub struct Range<T: IntervalElement> {
    pub buf: Vec<SimpleRange<T>>,
}

impl<T: IntervalElement> Range<T> {
    pub fn new(range: SimpleRange<T>) -> Self {
        Self { buf: vec![range] }
    }

    pub fn from_comparison(comp: Comparison, v: T) -> Self {
        match comp {
            Comparison::Equals => range!({ v }),
            Comparison::NotEquals => range!({,(v)} | {(v),}),
            Comparison::MoreThanEquals => range!({[v],}),
            Comparison::MoreThan => range!({(v),}),
            Comparison::LessThanEquals => range!({,[v]}),
            Comparison::LessThan => range!({,(v)}),
        }
    }

    fn push_union(&mut self, range: SimpleRange<T>) {
        let mut new_buf = vec![];

        let mut union = range;
        for r in &self.buf {
            if union.overlaps(r) {
                union = union.union(r);
            } else {
                new_buf.push(*r);
            }
        }
        new_buf.push(union);
        self.buf = new_buf;
    }

    pub fn union(&mut self, other: Self) {
        for r in &other.buf {
            self.push_union(*r);
        }
    }

    fn push_intersection(&mut self, range: SimpleRange<T>) {
        for r in &mut self.buf {
            if range.overlaps(r) {
                *r = range.intersection(r);
            }
        }
    }

    pub fn intersection(&mut self, other: Self) {
        for r in other.buf {
            self.push_intersection(r);
        }
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &SimpleRange<T>> {
        self.buf.iter()
    }
}

#[macro_export]
macro_rules! range {
    ($x:tt & $($y:tt)+) => {
        {
            let mut r = range!($x);
            r.intersection(range!($($y)*));
            r
        }
    };
    ($x:tt | $($y:tt)+) => {
        {
            let mut r = range!($x);
            r.union(range!($($y)*));
            r
        }
    };
    (($($x:tt)+)) => {
        range!($($x)*)
    };
    ($x:tt) => {
        Range::new(simple_range!($x))
    };
}

#[cfg(test)]
mod tests {
    use crate::query::Literal;

    use super::*;

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
        let r: SimpleRange<Literal> = simple_range!(
            {(3usize), (10usize)}
            |
            {(4usize), [10usize]}
        );
        assert_eq!(simple_range!({(3usize), [10usize]}), r);

        let r: SimpleRange<Literal> = simple_range!(
            {(3usize), }
            |
            {(2usize), [10usize]}
        );
        assert_eq!(simple_range!({(2usize), }), r);

        let r: SimpleRange<Literal> = simple_range!(
            {,(5usize)}
            |
            {(2usize), [10usize]}
        );
        assert_eq!(simple_range!({,[10usize]}), r);

        let r: SimpleRange<Literal> = simple_range!(
            {,(5usize)}
            |
            {(2usize),}
        );
        assert_eq!(simple_range!({,}), r);

        let r: SimpleRange<Literal> = simple_range!({,} | {(10usize), [15usize]});
        assert_eq!(simple_range!({,}), r);

        let r: SimpleRange<Literal> = simple_range!({} | {(4usize), [10usize]});
        assert_eq!(simple_range!({(4usize), [10usize]}), r);

        let r: SimpleRange<Literal> = simple_range!({5usize} | {(5usize), [10usize]});
        assert_eq!(simple_range!({[5usize], [10usize]}), r);
    }

    #[test]
    fn test_simple_range_intersection() {
        let r: SimpleRange<Literal> = simple_range!(
            {(3usize), (10usize)}
            &
            {(4usize), [10usize]}
        );
        assert_eq!(simple_range!({(4usize), (10usize)}), r);

        let r: SimpleRange<Literal> = simple_range!(
            {(4usize), }
            &
            {(2usize), [10usize]}
        );
        assert_eq!(simple_range!({(4usize), [10usize]}), r);

        let r: SimpleRange<Literal> = simple_range!(
            {,(5usize)}
            &
            {(2usize), [10usize]}
        );
        assert_eq!(simple_range!({(2usize),(5usize)}), r);

        let r: SimpleRange<Literal> = simple_range!(
            {,(5usize)}
            &
            {(2usize),}
        );
        assert_eq!(simple_range!({(2usize), (5usize)}), r);

        let r: SimpleRange<Literal> = simple_range!({,} & {(10usize), [15usize]});
        assert_eq!(simple_range!({(10usize), [15usize]}), r);

        let r: SimpleRange<Literal> = simple_range!({} & {(4usize), [10usize]});
        assert_eq!(simple_range!({}), r);

        let r: SimpleRange<Literal> = simple_range!({6usize} & {(5usize), [10usize]});
        assert_eq!(simple_range!({ 6usize }), r);
    }

    #[test]
    fn test_range_union() {
        let r: Range<Literal> = range!(
            ({(4usize), (10usize)} |
            {(5usize), [8usize]}) |
            {(9usize), [20usize]}
        );

        assert_eq!(r.buf, vec![simple_range!({(4usize), [20usize]})]);
    }

    #[test]
    fn test_range_intersection() {
        let r: Range<Literal> = range!(
            ({(4usize), (10usize)}
            |
            {(14usize), (20usize)})
            & {(5usize), (16usize)}
        );

        assert_eq!(
            r.buf,
            vec![
                simple_range!({(5usize), (10usize)}),
                simple_range!({(14usize),(16usize)})
            ]
        );
    }
}
