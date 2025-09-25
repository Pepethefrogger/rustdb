use std::slice;

use rustdb::tree::Table;
use tempfile::tempfile;

#[test]
fn test_insert_once() {
    let entry = [1,2,3,4];
    let entry_size = std::mem::size_of_val(&entry);

    let file = tempfile().unwrap();
    let mut table = Table::from_file(file, entry_size).unwrap();
    table.insert(0, &entry).unwrap();
    let data = table.find(0).unwrap();
    assert_eq!(data, entry);
}

#[test]
fn test_persistence() {
    let entry = [1,2,3,4];
    let entry_size = std::mem::size_of_val(&entry);

    let file = tempfile().unwrap();
    let mut table = Table::from_file(file.try_clone().unwrap(), entry_size).unwrap();
    table.insert(0, &entry).unwrap();

    drop(table);

    let mut table = Table::from_file(file, entry_size).unwrap();
    let data = table.find(0).unwrap();
    assert_eq!(data, entry);
}

#[test]
fn test_insert_some() {
    let entries = 0u8..20;
    let entry_size = std::mem::size_of::<u8>();

    let file = tempfile().unwrap();
    let mut table = Table::from_file(file, entry_size).unwrap();
    entries.clone().for_each(|e| {
        // println!("Inserting {} at index {}", e, e);
        table.insert(e as usize, slice::from_ref(&e)).unwrap()
    });
    entries.for_each(|e| {
        let data = table.find(e as usize).unwrap();
        // println!("Retrieved {} from index {}, should be {}", data[0], e, e);
        assert_eq!(data[0], e);
    });
}
