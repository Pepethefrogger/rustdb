use std::ops::Range;

use rustdb::table::{
    Table, debug::debug_table, internal::INTERNAL_NODE_CELL_COUNT, metadata::Type,
};
use tempfile::tempfile;

fn insert_range(table: &mut Table, range: Range<usize>) {
    range.clone().for_each(|e| {
        // println!("Inserting {} at index {}", e, e);
        table.insert(e, &e.to_ne_bytes()).unwrap()
    });
}

fn check_range(table: &mut Table, range: Range<usize>) {
    range.for_each(|e| {
        let bytes = table.find(e).unwrap();
        let data = usize::from_ne_bytes(bytes.read_all().try_into().expect("Data didn't fit"));
        // println!("Retrieved {} from index {}, should be {}", data, e, e);
        assert_eq!(data, e);
    });
}

#[test]
fn test_persistence() {
    let entry = 10usize.to_ne_bytes();

    let data_file = tempfile().unwrap();
    let metadata_file = tempfile().unwrap();
    let mut table = Table::create(
        data_file.try_clone().unwrap(),
        metadata_file.try_clone().unwrap(),
        ("id", Type::Uint),
        &[("num", Type::Int)],
    )
    .unwrap();
    table.insert(0, &entry).unwrap();

    drop(table);

    let table = Table::open(data_file, metadata_file).unwrap();
    let data = table.find(0).unwrap();
    assert_eq!(data.read_all(), entry);
}

#[test]
fn test_duplicate_key() {
    let entry = 20usize.to_ne_bytes();

    let data_file = tempfile().unwrap();
    let metadata_file = tempfile().unwrap();
    let mut table = Table::create(
        data_file,
        metadata_file,
        ("id", Type::Uint),
        &[("name", Type::Uint)],
    )
    .unwrap();
    table.insert(0, &entry).unwrap();
    table
        .insert(0, &entry)
        .expect_err("Should return duplicate key");
}

#[test]
fn test_fill_leaf() {
    let data_file = tempfile().unwrap();
    let metadata_file = tempfile().unwrap();
    let mut table = Table::create(
        data_file,
        metadata_file,
        ("id", Type::Uint),
        &[("name", Type::Uint)],
    )
    .unwrap();

    let max_entries_per_leaf = table.max_leaf_cells;
    println!("max entries per leaf {}", max_entries_per_leaf);
    let entries = 0usize..max_entries_per_leaf;
    insert_range(&mut table, entries.clone());
    debug_table(&table);
    check_range(&mut table, entries);
}

#[test]
fn test_split_leaf_node() {
    let data_file = tempfile().unwrap();
    let metadata_file = tempfile().unwrap();
    let mut table = Table::create(
        data_file,
        metadata_file,
        ("id", Type::Uint),
        &[("name", Type::Uint)],
    )
    .unwrap();

    let max_entries_per_leaf = table.max_leaf_cells;
    println!("max entries per leaf {}", max_entries_per_leaf);
    let entries = 0usize..(max_entries_per_leaf + max_entries_per_leaf / 2);
    insert_range(&mut table, entries.clone());
    debug_table(&table);
    check_range(&mut table, entries);
}

#[test]
fn test_fill_internal_node() {
    let data_file = tempfile().unwrap();
    let metadata_file = tempfile().unwrap();
    let mut table = Table::create(
        data_file,
        metadata_file,
        ("id", Type::Uint),
        &[("name", Type::Uint)],
    )
    .unwrap();

    let max_entries_per_leaf: usize = table.max_leaf_cells;
    let half_entries = INTERNAL_NODE_CELL_COUNT;
    let max_entries = max_entries_per_leaf + half_entries * (max_entries_per_leaf / 2) + 1;
    println!(
        "max entries -> leaf {}, internal {}, total {}",
        max_entries_per_leaf, INTERNAL_NODE_CELL_COUNT, max_entries
    );

    let entries = 0usize..max_entries;
    insert_range(&mut table, entries.clone());
    debug_table(&table);
    check_range(&mut table, entries);
}

#[test]
fn test_split_internal_node() {
    let data_file = tempfile().unwrap();
    let metadata_file = tempfile().unwrap();
    let mut table = Table::create(
        data_file,
        metadata_file,
        ("id", Type::Uint),
        &[("name", Type::Uint)],
    )
    .unwrap();

    let max_entries_per_leaf: usize = table.max_leaf_cells;
    let half_entries = INTERNAL_NODE_CELL_COUNT - 1;
    let max_entries_per_internal = max_entries_per_leaf + half_entries * (max_entries_per_leaf / 2);
    let max_entries = max_entries_per_internal + max_entries_per_internal / 2;
    println!(
        "max entries -> leaf {}, internal {}, total {}",
        max_entries_per_leaf, INTERNAL_NODE_CELL_COUNT, max_entries
    );

    let entries = 0usize..max_entries;
    insert_range(&mut table, entries.clone());
    debug_table(&table);
    check_range(&mut table, entries);
}

#[test]
fn test_advancing_cursor() {
    let data_file = tempfile().unwrap();
    let metadata_file = tempfile().unwrap();
    let mut table = Table::create(
        data_file,
        metadata_file,
        ("id", Type::Uint),
        &[("name", Type::Uint)],
    )
    .unwrap();

    let max_entries_per_leaf: usize = table.max_leaf_cells;
    let half_entries = INTERNAL_NODE_CELL_COUNT - 1;
    let max_entries_per_internal = max_entries_per_leaf + half_entries * (max_entries_per_leaf / 2);
    let max_entries = max_entries_per_internal + max_entries_per_internal / 2;
    println!(
        "max entries -> leaf {}, internal {}, total {}",
        max_entries_per_leaf, INTERNAL_NODE_CELL_COUNT, max_entries
    );

    let mut entries = 0usize..max_entries;
    insert_range(&mut table, entries.clone());
    debug_table(&table);
    let mut cursor = table.find_cursor(0).unwrap();
    let e = entries.next().unwrap();
    let bytes = cursor.value(&table);
    let data = usize::from_ne_bytes(bytes.read_all().try_into().expect("Data didn't fit"));
    assert_eq!(data, e);

    while cursor.advance(&table) {
        println!("Cursor -> {:?}: {:?}", cursor.page_num, cursor.cell_num);
        let e = entries.next().unwrap();
        println!("Entry: {}", e);
        let bytes = cursor.value(&table);
        let data = usize::from_ne_bytes(bytes.read_all().try_into().expect("Data didn't fit"));
        assert_eq!(data, e);
    }
    assert_eq!(entries.next(), None);
    println!(
        "Cursor: {{ page_num: {:?}, cell_num: {:?} }}",
        cursor.page_num, cursor.cell_num
    );
}
