use rustdb::{db::DB, table::metadata::Type};
use tempfile::tempdir;

#[test]
fn test_database() {
    let dir = tempdir().unwrap();
    let mut db = DB::new(dir.path());
    let table_name = "test";
    db.create_table(table_name, &[("name", Type::Int)]).unwrap();
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
    db.create_table(table_name, &[("name", Type::Int)]).unwrap();
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
