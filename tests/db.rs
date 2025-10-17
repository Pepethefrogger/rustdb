use rustdb::expression;
use rustdb::{
    db::{DB, OperationResult},
    expression::{Comparison, Expression},
    query::{Identifier, Literal, Statement},
    table::{data::Data, metadata::Type},
};
use tempfile::tempdir;

// macro_rules! array_into {
//     ($t:ident, $($x:expr),*) => {
//         [$($t::from($x)),*]
//     };
// }

macro_rules! array_into {
    ($t:ident; [$($x:tt),*]) => {
        [$(array_into!($t; $x)),*]
    };
    ($t:ident; $x:expr) => {
        $t::from($x)
    }
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

    let test_data = array_into!(Literal;
        [
            [5usize, 10isize, "hello"],
            [9usize, 5isize, "bye"],
            [50usize, 1000isize, "test"]
        ]
    );

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
            wher: None,
            limit: None,
            skip: None,
        };
        let result = db.execute(insert_statement).unwrap();
        assert!(matches!(result, OperationResult::Ok));
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

#[test]
fn test_select() {
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

    let test_data = array_into!(Literal;
        [
            [5usize, 10isize, "hello"],
            [9usize, 5isize, "bye"],
            [50usize, 1000isize, "test"]
        ]
    );

    let entry_size = table.metadata.metadata.entry_size();
    let mut buffer = vec![0u8; entry_size.size];
    let data_buffer = Data::new_mut(&mut buffer);
    for (id, data) in test_data.iter().copied().enumerate() {
        table
            .metadata
            .metadata
            .data_fields()
            .zip(data)
            .for_each(|(f, l)| f.write(&l, data_buffer));
        table.insert(id, data_buffer.read_all()).unwrap();
    }

    let columns: Vec<_> = fields
        .iter()
        .copied()
        .map(|(name, _)| name.into())
        .collect();
    let select_statement = Statement {
        operation: rustdb::query::Operation::Select {
            table: table_name.into(),
            columns,
        },
        wher: None,
        limit: None,
        skip: None,
    };

    let entries = match db.execute(select_statement).unwrap() {
        OperationResult::Entries(entries) => entries,
        _ => panic!("Should return entries"),
    };

    entries
        .iter()
        .zip(test_data)
        .for_each(|(value, expected)| assert_eq!(expected, value));
}

#[test]
fn test_update() {
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

    let test_data = array_into!(Literal;
        [
            [5usize, 10isize, "hello"],
            [9usize, 5isize, "bye"],
            [50usize, 1000isize, "test"]
        ]
    );

    let entry_size = table.metadata.metadata.entry_size();
    let mut buffer = vec![0u8; entry_size.size];
    let data_buffer = Data::new_mut(&mut buffer);
    for (id, data) in test_data.iter().copied().enumerate() {
        table
            .metadata
            .metadata
            .data_fields()
            .zip(data)
            .for_each(|(f, l)| f.write(&l, data_buffer));
        table.insert(id, data_buffer.read_all()).unwrap();
    }

    let modified_uint = 10usize;
    let modified_int = -5isize;
    let values = vec![
        (fields[0].0.into(), modified_uint.into()),
        (fields[1].0.into(), modified_int.into()),
    ];

    let update_statement = Statement {
        operation: rustdb::query::Operation::Update {
            table: table_name.into(),
            values,
        },
        wher: None,
        limit: None,
        skip: None,
    };
    match db.execute(update_statement).unwrap() {
        OperationResult::Count(c) => {
            assert_eq!(c, test_data.len(), "All elements should have been updated")
        }
        _ => panic!("Update has to return count"),
    }

    let modified_values = test_data
        .iter()
        .copied()
        .map(|[_, _, s]| [modified_uint.into(), modified_int.into(), s]);

    let table = db.table(table_name).unwrap();
    for (id, literals) in modified_values.enumerate() {
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

#[test]
fn test_select_clause() {
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

    let test_data = array_into!(Literal;
        [
            [5usize, 10isize, "hello"],
            [9usize, 5isize, "bye"],
            [50usize, 1000isize, "test"],
            [30usize, 100isize, "ok"],
            [90usize, (-10isize), "nice"],
            [50usize, (1500isize), "testing"],
            [15usize, (400isize), "hello"],
            [1000usize, (110isize), "ko"],
            [20usize, (-1000isize), "name"],
            [10000usize, (10500isize), "testing"]
        ]
    );

    let entry_size = table.metadata.metadata.entry_size();
    let mut buffer = vec![0u8; entry_size.size];
    let data_buffer = Data::new_mut(&mut buffer);
    for (id, data) in test_data.iter().copied().enumerate() {
        table
            .metadata
            .metadata
            .data_fields()
            .zip(data)
            .for_each(|(f, l)| f.write(&l, data_buffer));
        table.insert(id, data_buffer.read_all()).unwrap();
    }

    let columns: Vec<_> = fields
        .iter()
        .copied()
        .map(|(name, _)| name.into())
        .collect();
    let select_statement = Statement {
        operation: rustdb::query::Operation::Select {
            table: table_name.into(),
            columns,
        },
        wher: Some(Box::new(expression!(
            (id_field > 3usize) & ("int" >= 10isize)
        ))),
        limit: Some(2),
        skip: Some(2),
    };

    let entries = match db.execute(select_statement).unwrap() {
        OperationResult::Entries(entries) => entries,
        _ => panic!("Should return entries"),
    };

    let limit = 2;
    let skip = 2;
    let filtered_data = test_data
        .iter()
        .enumerate()
        .filter(|(id, [_, int, _])| {
            let int = match int {
                Literal::Int(i) => i,
                _ => unreachable!(),
            };
            (*id > 3) && (*int >= 10)
        })
        .skip(skip)
        .take(limit)
        .map(|(_, d)| d);

    entries
        .iter()
        .zip(filtered_data)
        .for_each(|(value, expected)| assert_eq!(expected, value));
}

#[test]
fn test_update_clause() {
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

    let test_data = array_into!(Literal;
        [
            [5usize, 10isize, "hello"],
            [9usize, 5isize, "bye"],
            [50usize, 1000isize, "testing"],
            [30usize, 100isize, "ok"],
            [90usize, (-10isize), "nice"],
            [50usize, (1500isize), "testing"],
            [15usize, (400isize), "hello"],
            [1000usize, (110isize), "testing"],
            [20usize, (-1000isize), "name"],
            [1000usize, (110isize), "testing"],
            [10000usize, (10500isize), "testing"]
        ]
    );

    let entry_size = table.metadata.metadata.entry_size();
    let mut buffer = vec![0u8; entry_size.size];
    let data_buffer = Data::new_mut(&mut buffer);
    for (id, data) in test_data.iter().copied().enumerate() {
        table
            .metadata
            .metadata
            .data_fields()
            .zip(data)
            .for_each(|(f, l)| f.write(&l, data_buffer));
        table.insert(id, data_buffer.read_all()).unwrap();
    }

    let modified_uint = 67usize;
    let modified_int = -51isize;
    let values = vec![
        (fields[0].0.into(), modified_uint.into()),
        (fields[1].0.into(), modified_int.into()),
    ];

    let limit = 2;
    let skip = 1;
    let update_statement = Statement {
        operation: rustdb::query::Operation::Update {
            table: table_name.into(),
            values,
        },
        wher: Some(Box::new(expression!(
            ((id_field > 2usize) & (id_field <= 9usize) & ("string" = "testing"))
        ))),
        limit: Some(limit),
        skip: Some(skip),
    };

    let mut count = 0usize;
    let mut modified_values = test_data;
    modified_values
        .iter_mut()
        .enumerate()
        .filter(|(id, [_, _, string])| {
            let string = match string {
                Literal::String(s) => s,
                _ => unreachable!(),
            };
            (*id > 2) && (*id <= 9) && (*string == "testing")
        })
        .map(|(_, v)| v)
        .skip(skip)
        .take(limit)
        .for_each(|[u, i, _]| {
            count += 1;
            *u = modified_uint.into();
            *i = modified_int.into();
        });

    match db.execute(update_statement).unwrap() {
        OperationResult::Count(c) => {
            assert_eq!(c, count, "Elements updated doesn't match")
        }
        _ => panic!("Update has to return count"),
    }

    let table = db.table(table_name).unwrap();
    for (id, literals) in modified_values.iter().enumerate() {
        let data = table.find(id).unwrap();
        table
            .metadata
            .metadata
            .data_fields()
            .zip(literals)
            .for_each(|(f, l)| {
                let value = f.read(data);
                assert_eq!(*l, value);
            });
    }
}
