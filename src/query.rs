use chumsky::prelude::*;

#[derive(Debug, PartialEq, Eq)]
pub enum Expr<'a> {
    Select {
        columns: Vec<&'a str>,
        table: &'a str,
    },
}

fn select<'a>() -> impl Parser<'a, &'a str, Expr<'a>> {
    let ident = text::ident().padded();
    let columns = ident
        .separated_by(just(",").padded())
        .at_least(1)
        .collect::<Vec<_>>();

    just("SELECT")
        .padded()
        .ignore_then(columns)
        .then_ignore(just("FROM").padded())
        .then(ident)
        .map(|(columns, table)| Expr::Select { columns, table })
}

pub fn parser<'a>() -> impl Parser<'a, &'a str, Expr<'a>> {
    select()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select() {
        let str = "SELECT col1, col2 FROM table";
        let result = parser().parse(str).unwrap();
        assert_eq!(
            result,
            Expr::Select {
                columns: vec!["col1", "col2"],
                table: "table"
            }
        );
    }
}

