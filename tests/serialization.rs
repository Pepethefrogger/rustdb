use rustdb::{query::Literal, table::metadata::Type};

macro_rules! test_serialize {
    ($literal:expr, $type:expr) => {
        let size = $type.size().size;
        let mut buf = vec![0u8; size];
        $literal.write_to(&mut buf);

        let result = $type.read(&buf);

        assert_eq!($literal, result);
    };
}

#[test]
fn test_int() {
    let value = -29;
    test_serialize!(Literal::Int(value), Type::Int);
}

#[test]
fn test_uint() {
    let value = 29;
    test_serialize!(Literal::Uint(value), Type::Uint);
}

#[test]
fn test_float() {
    let value = 4.5;
    test_serialize!(Literal::Float(value), Type::Float);
}

#[test]
fn test_string() {
    let value = "testing";
    test_serialize!(Literal::String(value), Type::String(255));
}

