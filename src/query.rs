use std::ops::Deref;

use chumsky::{prelude::*, text::digits};

use crate::{
    expr_and, expr_or,
    expression::{BoxedExpression, Comparison, Expression},
};

#[repr(transparent)]
#[derive(Debug, PartialEq)]
pub struct Identifier(str);

impl Identifier {
    fn new(str: &str) -> &Self {
        str.into()
    }
}

impl<'a> From<&'a str> for &'a Identifier {
    fn from(value: &'a str) -> Self {
        let ptr = value as *const str as *const Identifier;
        unsafe { &*ptr }
    }
}

impl Deref for Identifier {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> From<&'a Identifier> for &'a str {
    fn from(value: &'a Identifier) -> Self {
        &value.0
    }
}

type ParsingError<'a> = extra::Err<Simple<'a, char>>;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Literal<'a> {
    String(&'a str),
    Int(isize),
    Uint(usize),
    Float(f64),
}

impl<'a> Literal<'a> {
    pub fn write_to(&self, buf: &mut [u8]) {
        match self {
            Self::String(str) => {
                let data = str.as_bytes();
                let len = data.len();
                const USIZE_FIELD: usize = std::mem::size_of::<usize>();

                buf[0..USIZE_FIELD].copy_from_slice(&len.to_ne_bytes());
                buf[USIZE_FIELD..(USIZE_FIELD + len)].copy_from_slice(data);
            }
            Self::Int(i) => {
                let data = &i.to_ne_bytes();
                buf.copy_from_slice(data);
            }
            Self::Uint(i) => {
                let data = &i.to_ne_bytes();
                buf.copy_from_slice(data);
            }
            Self::Float(f) => {
                let data = &f.to_ne_bytes();
                buf.copy_from_slice(data);
            }
        }
    }
}

impl From<usize> for Literal<'_> {
    fn from(value: usize) -> Self {
        Self::Uint(value)
    }
}

impl From<isize> for Literal<'_> {
    fn from(value: isize) -> Self {
        Self::Int(value)
    }
}

impl From<f64> for Literal<'_> {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl<'a> From<&'a str> for Literal<'a> {
    fn from(value: &'a str) -> Self {
        Self::String(value)
    }
}

fn string<'a>() -> impl Parser<'a, &'a str, Literal<'a>, ParsingError<'a>> + Clone {
    none_of("\"")
        .repeated()
        .to_slice()
        .delimited_by(just("\""), just("\""))
        .map(Literal::String)
}

fn num<'a>() -> impl Parser<'a, &'a str, usize, ParsingError<'a>> + Clone {
    digits(10).to_slice().try_map(|v: &str, span| {
        let digit: Result<usize, _> = v.parse();
        digit.map_err(|_e| Simple::new(Some('a'.into()), span))
    })
}

fn unsigned_integer<'a>() -> impl Parser<'a, &'a str, Literal<'a>, ParsingError<'a>> + Clone {
    num().map(Literal::Uint)
}

fn integer<'a>() -> impl Parser<'a, &'a str, Literal<'a>, ParsingError<'a>> + Clone {
    choice((
        just("+").ignore_then(num()).map(|n| n as isize),
        just("-").ignore_then(num()).map(|n| -(n as isize)),
    ))
    .map(Literal::Int)
}

fn float<'a>() -> impl Parser<'a, &'a str, Literal<'a>, ParsingError<'a>> + Clone {
    digits(10)
        .to_slice()
        .then_ignore(just("."))
        .then(digits(10).to_slice())
        .try_map(|(f, s), span| {
            let mut string = String::from(f);
            string.push('.');
            string.push_str(s);
            let digit: Result<f64, _> = string.parse();
            digit
                .map(Literal::Float)
                .map_err(|_e| Simple::new(Some('a'.into()), span))
        })
}

fn value<'a>() -> impl Parser<'a, &'a str, Literal<'a>, ParsingError<'a>> + Clone {
    chumsky::primitive::choice((string(), unsigned_integer(), integer(), float()))
}

fn ident<'a>() -> impl Parser<'a, &'a str, &'a Identifier, ParsingError<'a>> + Clone {
    text::ident().map(Identifier::new)
}

fn parentheses<'a, T>(
    parser: impl Parser<'a, &'a str, T, ParsingError<'a>> + Clone,
) -> impl Parser<'a, &'a str, Vec<T>, ParsingError<'a>> + Clone {
    parser
        .separated_by(just(",").padded())
        .collect::<Vec<_>>()
        .delimited_by(just("("), just(")"))
}

fn binary_operation<'a, L, S, R>(
    left: impl Parser<'a, &'a str, L, ParsingError<'a>> + Clone,
    right: impl Parser<'a, &'a str, R, ParsingError<'a>> + Clone,
    sym: impl Parser<'a, &'a str, S, ParsingError<'a>> + Clone,
) -> impl Parser<'a, &'a str, (L, R, S), ParsingError<'a>> + Clone {
    left.then(sym.padded())
        .then(right)
        .map(|((l, s), r)| (l, r, s))
}

#[derive(Debug, PartialEq)]
pub enum Operation<'a> {
    Select {
        table: &'a Identifier,
        columns: Vec<&'a Identifier>,
    },
    Insert {
        table: &'a Identifier,
        values: Vec<(&'a Identifier, Literal<'a>)>,
    },
    Update {
        table: &'a Identifier,
        values: Vec<(&'a Identifier, Literal<'a>)>,
    },
    Delete {
        table: &'a Identifier,
    },
}

impl<'a> Operation<'a> {
    pub fn table(&self) -> &Identifier {
        match self {
            Self::Select { table, .. } => table,
            Self::Insert { table, .. } => table,
            Self::Update { table, .. } => table,
            Self::Delete { table } => table,
        }
    }
}

/// SELECT a, b, c FROM table
fn select<'a>() -> impl Parser<'a, &'a str, Operation<'a>, ParsingError<'a>> + Clone {
    let columns = ident()
        .separated_by(just(",").padded())
        .at_least(1)
        .collect::<Vec<_>>();

    just("SELECT")
        .padded()
        .ignore_then(columns)
        .then_ignore(just("FROM").padded())
        .then(ident())
        .map(|(columns, table)| Operation::Select { columns, table })
}

/// INSERT INTO table (col1, col2) VALUES (1, 2)
fn insert<'a>() -> impl Parser<'a, &'a str, Operation<'a>, ParsingError<'a>> + Clone {
    just("INSERT")
        .padded()
        .then(just("INTO").padded())
        .ignore_then(ident())
        .then(parentheses(ident()).padded())
        .then_ignore(just("VALUES").padded())
        .then(parentheses(value()).padded())
        .try_map(|((table, columns), parentheses), span| {
            if columns.len() != parentheses.len() {
                Err(Simple::new(Some('a'.into()), span))
            } else {
                let values = columns.into_iter().zip(parentheses).collect();
                Ok(Operation::Insert { table, values })
            }
        })
}

/// UPDATE table SET col1 = 1
fn update<'a>() -> impl Parser<'a, &'a str, Operation<'a>, ParsingError<'a>> + Clone {
    let values = binary_operation(ident(), value(), just("=").ignored())
        .map(|(l, r, _)| (l, r))
        .padded()
        .separated_by(just(","))
        .collect::<Vec<_>>();
    just("UPDATE")
        .padded()
        .ignore_then(ident())
        .then_ignore(just("SET").padded())
        .then(values)
        .map(|(table, values)| Operation::Update { table, values })
}

/// DELETE FROM table
fn delete<'a>() -> impl Parser<'a, &'a str, Operation<'a>, ParsingError<'a>> + Clone {
    just("DELETE")
        .padded()
        .ignore_then(just("FROM").padded())
        .ignore_then(ident())
        .map(|table| Operation::Delete { table })
}

#[derive(Debug, PartialEq)]
pub struct Statement<'a> {
    pub operation: Operation<'a>,
    pub wher: Option<BoxedExpression<'a>>,
    pub limit: Option<usize>,
    pub skip: Option<usize>,
}

impl<'a> Statement<'a> {
    fn new(operation: Operation<'a>) -> Self {
        Self {
            operation,
            wher: None,
            limit: None,
            skip: None,
        }
    }
}

fn comparison<'a>() -> impl Parser<'a, &'a str, Comparison, ParsingError<'a>> + Clone {
    choice((
        just("=").to(Comparison::Equals),
        just("!=").to(Comparison::NotEquals),
        just("<=").to(Comparison::LessThanEquals),
        just("<").to(Comparison::LessThan),
        just(">=").to(Comparison::MoreThanEquals),
        just(">").to(Comparison::MoreThan),
    ))
}

fn binary_expression<'a>() -> impl Parser<'a, &'a str, Expression<'a>, ParsingError<'a>> + Clone {
    binary_operation(ident(), value(), comparison()).map(|(left, right, sym)| Expression::Binary {
        left,
        right,
        sym,
    })
}

fn expression<'a>() -> impl Parser<'a, &'a str, BoxedExpression<'a>, ParsingError<'a>> + Clone {
    recursive::<_, BoxedExpression<'a>, _, _, _>(|expr| {
        let and_expr = expr
            .clone()
            .then_ignore(just("AND").padded())
            .then(expr.clone())
            .delimited_by(just("(").padded(), just(")").padded())
            .map(|(l, r)| Box::new(expr_and!(l, r)));
        let or_expr = expr
            .clone()
            .then_ignore(just("OR").padded())
            .then(expr)
            .delimited_by(just("(").padded(), just(")").padded())
            .map(|(l, r)| Box::new(expr_or!(l, r)));
        let binary = binary_expression().map(Box::new);

        choice((and_expr, or_expr, binary)).padded()
    })
}

enum Clause<'a> {
    Limit(usize),
    Skip(usize),
    Where(BoxedExpression<'a>),
}

fn parse_limit<'a>() -> impl Parser<'a, &'a str, Clause<'a>, ParsingError<'a>> + Clone {
    just("LIMIT")
        .padded()
        .ignore_then(num().padded())
        .map(Clause::Limit)
}

fn parse_skip<'a>() -> impl Parser<'a, &'a str, Clause<'a>, ParsingError<'a>> + Clone {
    just("SKIP")
        .padded()
        .ignore_then(num().padded())
        .map(Clause::Skip)
}

fn parse_where<'a>() -> impl Parser<'a, &'a str, Clause<'a>, ParsingError<'a>> + Clone {
    just("WHERE")
        .padded()
        .ignore_then(expression())
        .map(Clause::Where)
}

fn parse_clause<'a>() -> impl Parser<'a, &'a str, Clause<'a>, ParsingError<'a>> + Clone {
    chumsky::primitive::choice((parse_limit(), parse_skip(), parse_where()))
}

pub fn parser<'a>() -> impl Parser<'a, &'a str, Statement<'a>, ParsingError<'a>> + Clone {
    let operation_parser = chumsky::primitive::choice((select(), insert(), update(), delete()));
    operation_parser.map(Statement::new).foldl(
        parse_clause().repeated(),
        |mut statement, clause| {
            match clause {
                Clause::Skip(s) => statement.skip = Some(s),
                Clause::Limit(l) => statement.limit = Some(l),
                Clause::Where(w) => statement.wher = Some(w),
            }
            statement
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! assert_parse {
        ($parser: expr, $str: expr, $res: expr) => {{
            let result = ($parser).parse($str).unwrap();
            assert_eq!(result, $res)
        }};
    }

    macro_rules! assert_parse_operation {
        ($parser: expr, $str: expr, $res: expr) => {{ assert_parse!($parser, $str, Statement::new($res)) }};
    }

    #[test]
    fn test_parse_parentheses() {
        let str = "(a, b, c)";
        assert_parse!(
            parentheses(ident()),
            str,
            ["a", "b", "c"].map(Identifier::new)
        )
    }

    #[test]
    fn test_parse_uint() {
        let str = "5";
        assert_parse!(unsigned_integer(), str, Literal::Uint(5))
    }

    #[test]
    fn test_parse_pos_int() {
        let str = "+5";
        assert_parse!(integer(), str, Literal::Int(5))
    }

    #[test]
    fn test_parse_neg_int() {
        let str = "-5";
        assert_parse!(integer(), str, Literal::Int(-5))
    }

    #[test]
    fn test_parse_float() {
        let str = "4.5";
        assert_parse!(float(), str, Literal::Float(4.5))
    }

    #[test]
    fn test_parse_string() {
        let str = "\"string\"";
        assert_parse!(string(), str, Literal::String("string"))
    }

    #[test]
    fn test_parse_comparison() {
        let str = ">";
        assert_parse!(comparison(), str, Comparison::MoreThan)
    }

    #[test]
    fn parse_binary_expression() {
        let str = "id < 5";
        assert_parse!(
            binary_expression(),
            str,
            Expression::binary("id", 5usize, Comparison::LessThan)
        );
    }

    #[test]
    fn parse_and_expression() {
        let str = "(id < 5 AND (size > 10 AND field = 5))";
        assert_parse!(
            expression(),
            str,
            expr_and!(
                Expression::binary("id", 5usize, Comparison::LessThan),
                Expression::binary("size", 10usize, Comparison::MoreThan),
                Expression::binary("field", 5usize, Comparison::Equals)
            )
            .into()
        );
    }

    #[test]
    fn parse_or_expression() {
        let str = "(id < 5 OR (size > 10 OR field = 5))";
        assert_parse!(
            expression(),
            str,
            expr_or!(
                Expression::binary("id", 5usize, Comparison::LessThan),
                Expression::binary("size", 10usize, Comparison::MoreThan),
                Expression::binary("field", 5usize, Comparison::Equals)
            )
            .into()
        );
    }

    #[test]
    fn parse_complex_expression() {
        let str = "(id < 5 OR (size > 10 AND field = 5))";
        assert_parse!(
            expression(),
            str,
            expr_or!(
                Expression::binary("id", 5usize, Comparison::LessThan),
                expr_and!(
                    Expression::binary("size", 10usize, Comparison::MoreThan),
                    Expression::binary("field", 5usize, Comparison::Equals)
                )
            )
            .into()
        );
    }

    #[test]
    fn test_parse_select() {
        let str = "SELECT col1, col2 FROM table";
        assert_parse_operation!(
            parser(),
            str,
            Operation::Select {
                table: "table".into(),
                columns: vec!["col1".into(), "col2".into()],
            }
        )
    }

    #[test]
    fn test_parse_insert() {
        let str = "INSERT INTO table (col1, col2) VALUES (3, 5)";
        assert_parse_operation!(
            parser(),
            str,
            Operation::Insert {
                table: "table".into(),
                values: vec![
                    ("col1".into(), Literal::Uint(3)),
                    ("col2".into(), Literal::Uint(5))
                ],
            }
        );
    }

    #[test]
    fn test_parse_update() {
        let str = "UPDATE table SET col1 = 0, col2 = 3";
        assert_parse_operation!(
            parser(),
            str,
            Operation::Update {
                table: "table".into(),
                values: vec![
                    ("col1".into(), Literal::Uint(0)),
                    ("col2".into(), Literal::Uint(3))
                ]
            }
        );
    }

    #[test]
    fn test_parse_delete() {
        let str = "DELETE FROM table";
        assert_parse_operation!(
            parser(),
            str,
            Operation::Delete {
                table: "table".into()
            }
        );
    }

    #[test]
    fn test_clauses() {
        let str = "SELECT id FROM table LIMIT 10 SKIP 5";
        let operation = Operation::Select {
            table: "table".into(),
            columns: vec!["id".into()],
        };
        assert_parse!(
            parser(),
            str,
            Statement {
                operation,
                wher: None,
                skip: Some(5),
                limit: Some(10)
            }
        )
    }
}
