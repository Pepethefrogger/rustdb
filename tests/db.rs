use rustdb::{
    db::{DB, OperationResult},
    query::{Identifier, Literal, Statement},
    table::metadata::Type,
};
use tempfile::tempdir;

macro_rules! array_into {
    ($t:ident, $($x:expr),*) => {
        [$($t::from($x)),*]
    };
}

#[test]
fn test_database() {
    let dir = tempdir().unwrap();
    let mut db = DB::new(dir.path());
    let table_name = "test";
    db.create_table(table_name, ("id", Type::Uint), &[("name", Type::Int)])
        .unwrap();
    let table = db.table(table_name).unwrap();

    let entry = &10usize.to_ne_bytes();
    table.insert(0, entry).unwrap();
    let value = table.find(0).unwrap();
    assert_eq!(value.read_all(), entry);
    drop(dir);
}

#[test]
fn test_database_persistence() {
    let dir = tempdir().unwrap();
    let mut db = DB::new(dir.path());
    let table_name = "test";
    db.create_table(table_name, ("id", Type::Uint), &[("name", Type::Int)])
        .unwrap();
    let table = db.table(table_name).unwrap();

    let entry = &10usize.to_ne_bytes();
    table.insert(0, entry).unwrap();
    drop(db);

    let mut db = DB::new(dir.path());
    let table = db.table(table_name).unwrap();
    let value = table.find(0).unwrap();
    assert_eq!(value.read_all(), entry);
    drop(dir);
}

#[test]
fn test_insert() {
    let dir = tempdir().unwrap();
    let mut db = DB::new(dir.path());
    let table_name = "test";
    let id_field = "id";
    let fields = [
        ("uint", Type::Uint),
        ("int", Type::Int),
        ("string", Type::String(255)),
    ];
    db.create_table(table_name, (id_field, Type::Uint), &fields)
        .unwrap();

    let table = db.table(table_name).unwrap();
    println!(
        "Table fields: {:?}",
        table.metadata.metadata.fields().collect::<Vec<_>>()
    );

    let test_data: [[Literal; 3]; _] = [
        array_into!(Literal, 5usize, 10isize, "hello"),
        array_into!(Literal, 9usize, 5isize, "bye"),
        array_into!(Literal, 50usize, 1000isize, "test"),
    ];

    let field_names: Vec<&Identifier> = fields
        .iter()
        .copied()
        .map(|(name, _)| name.into())
        .collect();

    for (id, data) in test_data.iter().copied().enumerate() {
        let mut values: Vec<_> = field_names.iter().copied().zip(data).collect();
        values.push((id_field.into(), id.into()));

        let insert_statement = Statement {
            operation: rustdb::query::Operation::Insert {
                table: table_name.into(),
                values,
            },
            limit: None,
            skip: None,
        };
        let result = db.execute(insert_statement).unwrap();
        assert!(matches!(result, OperationResult::Empty));
    }

    let table = db.table(table_name).unwrap();
    for (id, literals) in test_data.iter().copied().enumerate() {
        let data = table.find(id).unwrap();
        table
            .metadata
            .metadata
            .data_fields()
            .zip(literals)
            .for_each(|(f, l)| {
                let value = f.read(data);
                assert_eq!(l, value);
            });
    }
}
