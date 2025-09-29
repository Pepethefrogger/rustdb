use std::{ops::Range, slice};

use rustdb::{pager::INTERNAL_NODE_CELL_COUNT, tree::Table};
use tempfile::tempfile;

fn insert_range(table: &mut Table, range: Range<usize>) {
    range.clone().for_each(|e| {
        println!("Inserting {} at index {}", e, e);
        table.insert(e, &e.to_ne_bytes()).unwrap()
    });
}

fn check_range(table: &mut Table, range: Range<usize>) {
    range.for_each(|e| {
        let data = table.find(e).unwrap();
        // println!("Retrieved {} from index {}, should be {}", data[0], e, e);
        assert_eq!(
            usize::from_ne_bytes(data.try_into().expect("Data didn't fit")),
            e
        );
    });
}

#[test]
fn test_persistence() {
    let entry = [1, 2, 3, 4];
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
fn test_duplicate_key() {
    let entry = 8;
    let entry_size = std::mem::size_of_val(&entry);

    let file = tempfile().unwrap();
    let mut table = Table::from_file(file.try_clone().unwrap(), entry_size).unwrap();
    table.insert(0, slice::from_ref(&entry)).unwrap();
    table
        .insert(0, slice::from_ref(&entry))
        .expect_err("Should return duplicate key");
}

#[test]
fn test_fill_leaf() {
    let entry_size = std::mem::size_of::<usize>();
    let file = tempfile().unwrap();
    let mut table = Table::from_file(file, entry_size).unwrap();

    let max_entries_per_leaf = table.max_leaf_cells;
    println!("max entries per leaf {}", max_entries_per_leaf);
    let entries = 0usize..max_entries_per_leaf;
    insert_range(&mut table, entries.clone());
    check_range(&mut table, entries);
}

#[test]
fn test_split_leaf_node() {
    let entry_size = std::mem::size_of::<usize>();
    let file = tempfile().unwrap();
    let mut table = Table::from_file(file, entry_size).unwrap();

    let max_entries_per_leaf = table.max_leaf_cells;
    println!("max entries per leaf {}", max_entries_per_leaf);
    let entries = 0usize..(max_entries_per_leaf + max_entries_per_leaf / 2);
    insert_range(&mut table, entries.clone());
    check_range(&mut table, entries);
}

#[test]
fn test_fill_internal_node() {
    let entry_size: usize = std::mem::size_of::<usize>();
    let file = tempfile().unwrap();
    let mut table = Table::from_file(file, entry_size).unwrap();

    let max_entries_per_leaf: usize = table.max_leaf_cells;
    let half_entries = INTERNAL_NODE_CELL_COUNT - 1;
    let max_entries = max_entries_per_leaf + half_entries * (max_entries_per_leaf / 2);
    println!(
        "max entries -> leaf {}, internal {}, total {}",
        max_entries_per_leaf, INTERNAL_NODE_CELL_COUNT, max_entries
    );

    let entries = 0usize..(max_entries);
    insert_range(&mut table, entries.clone());
    check_range(&mut table, entries);
}
