use rustdb::{
    pager::PageNum,
    query::Literal,
    table::{
        data::Data,
        metadata::{Metadata, Type},
    },
};

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

#[test]
fn test_multiple_values() {
    let test_data = &[
        ("uint", Type::Uint, Literal::Uint(25)),
        ("int", Type::Int, Literal::Int(-5)),
        ("float", Type::Float, Literal::Float(4.5)),
        ("string", Type::String(255), Literal::String("testing")),
    ];
    let types: Vec<_> = test_data
        .iter()
        .copied()
        .map(|(name, typ, _)| (name, typ))
        .collect();
    let metadata = Metadata::new(PageNum(0), ("id", Type::Uint), &types);

    let entry_size = metadata.entry_size();
    println!("Entry size: {:?}", entry_size);

    let mut buf = vec![0u8; entry_size.aligned];
    let data = Data::new_mut(&mut buf);

    let iter = metadata.iter().skip(1).zip(test_data.map(|(_, _, l)| l));
    println!("Writing fields");
    for (f, l) in iter.clone() {
        if !f.primary {
            println!("Field: {:?}", f);
            println!("Literal: {:?}", l);
            let field_buf = data.get_mut(f.layout);
            l.write_to(field_buf);
            println!("Buf: {:?}", field_buf);
        }
    }

    println!("Reading fields");
    for (f, l) in iter {
        if !f.primary {
            println!("Field: {:?}", f);
            let field_buf = data.read(f.layout);
            println!("Buf: {:?}", field_buf);
            let value = f.typ.read(field_buf);
            println!("Literal: {:?}", value);
            assert_eq!(value, l);
        }
    }
}
