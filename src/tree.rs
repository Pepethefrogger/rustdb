use std::{fs, io, slice};

use derivative::Derivative;

use crate::pager::{leaf_cells_max, NodeMut, PageNum, Pager};

pub struct InternalNodeCell {
    pub key: usize,
    pub ptr: PageNum,
}
pub const INTERNAL_NODE_CELL_SIZE: usize = std::mem::size_of::<InternalNodeCell>();


pub const LEAF_NODE_CELL_KEY_SIZE: usize = std::mem::size_of::<LeafNodeCell>();
pub struct LeafNodeCell {
    pub key: usize,
}

impl LeafNodeCell {
    pub fn data(&self, size: usize) -> &[u8] {
        let ptr = unsafe { (self as *const Self).add(1) };
        unsafe { slice::from_raw_parts(ptr as *const u8, size) }
    }

    pub fn data_mut(&mut self, size: usize) -> &mut [u8] {
        let ptr = unsafe { (self as *mut Self).add(1) };
        unsafe { slice::from_raw_parts_mut(ptr as *mut u8, size) }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Cursor<'a> {
    #[derivative(Debug="ignore")]
    pub table: &'a mut Table,
    pub page_num: PageNum,
    pub cell_num: usize,
}

impl<'a> Cursor<'a> {
    /// Return the node that this cursor points to
    fn node(&mut self) -> io::Result<NodeMut<'a>> {
        let page = self.table.pager.get_page(self.page_num)?;
        let node = page.page_header_mut().node_mut();
        Ok(node)
    }

    /// Returns the entry that this cursor points to
    pub fn cell(&mut self) -> io::Result<&'a mut LeafNodeCell> {
        let node = self.node()?;
        if let NodeMut::LeafNode(leaf) = node {
            Ok(leaf.cell_mut(self.cell_num, self.table.aligned_entry_size))
        } else {
            unreachable!("A cursor shouldn't point to an internal node")
        }
    }

    /// Moves the cursor to the entry with the desired key
    pub fn find(&mut self, key: usize) -> io::Result<()> {
        let node = self.node()?;
        match node {
            NodeMut::InternalNode(_) => unimplemented!("Searching internal nodes is not implemented"),
            NodeMut::LeafNode(leaf) => {
                let mut min_index = 0;
                let mut max_index_past_one = leaf.num_cells;
                while min_index != max_index_past_one {
                    let index = (min_index + max_index_past_one) / 2;
                    let key_at_index = leaf.cell(index, self.table.aligned_entry_size).key;
                    if key_at_index == key {
                        self.cell_num = index;
                        return Ok(())
                    }
                    if key < key_at_index {
                        max_index_past_one = index;
                    } else {
                        min_index = index + 1;
                    }
                }
                self.cell_num = min_index;
                Ok(())
            }
        }
    }

    // Resets the cursor to point at the root node
    pub fn reset(&mut self) {
        self.page_num = self.table.get_root();
    }

    /// Creates an entry with the key, you can use the cursor to replace the value
    pub fn insert(&mut self, key: usize) -> io::Result<()> {
        self.find(key)?;
        let node = self.node()?;
        match node {
            NodeMut::InternalNode(_) => unreachable!(),
            NodeMut::LeafNode(leaf) => {
                if leaf.num_cells >= self.table.max_leaf_cells {
                    unimplemented!("Splitting leaf nodes isn't implemented");
                }
                if self.cell_num < leaf.num_cells {
                    let key_at_index = self.cell()?.key;
                    if key_at_index == key {
                        return Err(io::Error::new(io::ErrorKind::Other, "Duplicate key"));
                    }
                    // Make space for new cell, shift elements to the right: next = prev
                    let entry_size = self.table.aligned_entry_size;
                    for i in (self.cell_num..leaf.num_cells).rev() {
                        leaf.move_cell(i-1, i, entry_size);
                    }
                }
                leaf.num_cells += 1;
                Ok(())
            },
        }
    }
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct AlignedSize(pub usize);

const fn size_aligned(size: usize, align: usize) -> AlignedSize {
    AlignedSize((size + align - 1) & !(align - 1))
}

pub struct Table {
    pub pager: Pager,
    root: PageNum,
    entry_size: usize,
    aligned_entry_size: AlignedSize,
    max_leaf_cells: usize,
}

impl<'a> Table {
    pub fn new(mut pager: Pager, entry_size: usize) -> io::Result<Self> {
        let aligned_entry_size = size_aligned(entry_size, 8);
        let max_leaf_cells = leaf_cells_max(aligned_entry_size.0);
        // let total_size = PAGE_HEADER_SIZE + LEAF_NODE_HEADER_SIZE + max_leaf_cells * (LEAF_NODE_CELL_KEY_SIZE + aligned_entry_size.0);
        // println!("Aligned entry size {}, max leaf cells {}, total size {}", aligned_entry_size.0, max_leaf_cells, total_size);
        let root = pager.get_metadata()?.root;
        Ok(Self{pager: pager, root: root, entry_size, aligned_entry_size, max_leaf_cells})
    }

    pub fn get_root(&self) -> PageNum {
        self.root
    }

    pub fn set_root(&mut self, page: PageNum) -> io::Result<()> {
        self.pager.get_metadata()?.root = page;
        self.root = page;
        Ok(())
    }

    pub fn from_file(file: fs::File, entry_size: usize) -> io::Result<Self> {
        let pager = Pager::new(file)?;
        Ok(Table::new(pager, entry_size)?)
    }

    pub fn cursor(&'a mut self) -> Cursor<'a> {
        let root = self.root;
        Cursor{ table: self, page_num: root, cell_num: 0 }
    }

    pub fn insert(&'a mut self, key: usize, value: &[u8]) -> io::Result<()> {
        let entry_size = self.entry_size;
        let mut cursor = self.cursor();
        cursor.insert(key)?;
        let cell = cursor.cell()?;
        cell.key = key;
        cell.data_mut(entry_size).copy_from_slice(value);
        Ok(())
    }

    pub fn find(&'a mut self, key: usize) -> io::Result<&'a [u8]> {
        let entry_size = self.entry_size;
        let mut cursor = self.cursor();
        cursor.find(key)?;
        let cell = cursor.cell()?;
        Ok(cell.data(entry_size))
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        self.pager.flush().expect("Failed to flush pager");
    }
}
