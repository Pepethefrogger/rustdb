use crate::{
    pager::PageNum,
    table::{Table, node::Node},
};

fn print_with_indent(str: &str, indentation: usize) {
    println!("{:indent$}{}", "", str, indent = indentation * 4)
}

fn debug_node(table: &Table, page_num: PageNum, indentation: usize) {
    let node = table.pager.get_page(page_num).page_header().node();
    match node {
        Node::InternalNode(internal) => {
            print_with_indent(
                &format!(
                    "Internal {:?}: {{num_keys: {}, parent: {:?}}}",
                    page_num, internal.num_keys, internal.parent_ptr
                ),
                indentation,
            );
            for i in 0..internal.num_keys {
                let cell = internal.cell_unchecked(i);
                let key = cell.key;
                let child = cell.ptr;
                debug_node(table, child, indentation + 2);
                print_with_indent(&format!("Key: {}", key), indentation + 1);
            }
            debug_node(table, internal.right_child, indentation + 2);
        }
        Node::LeafNode(leaf) => {
            print_with_indent(
                &format!(
                    "Leaf {:?}: {{num_cells: {}, parent: {:?}}}",
                    page_num, leaf.num_cells, leaf.parent_ptr
                ),
                indentation,
            );
            for i in 0..leaf.num_cells {
                let cell = leaf.cell_unchecked(i, table.entry_size);
                let key = cell.key;
                let data = cell.data(table.entry_size);
                let value = usize::from_ne_bytes(data.read_all().try_into().unwrap());
                print_with_indent(&format!("Key: {}, Value: {}", key, value), indentation + 1);
            }
        }
    }
}

pub fn debug_table(table: &Table) {
    let root = table.get_root();
    debug_node(table, root, 0);
}

pub fn debug_find(table: &Table, key: usize) {
    let mut page_num = table.get_root();
    let mut node = table.pager.get_page(page_num).page_header().node();
    println!("Searching for key {}", key);
    while let Node::InternalNode(internal) = node {
        let index = internal.find_index(key);
        println!("Internal: {:?}, found next at index {}", page_num, index);
        page_num = internal.find(key);
        let page = table.pager.get_page(page_num);
        node = page.page_header().node();
    }
    let leaf = node.leaf().unwrap();
    let index = leaf.find(key, table.entry_size);
    println!("Leaf: {:?}, found next at index {}", page_num, index);
}
