use core::slice;
use std::{fmt::Debug, marker::PhantomData};

use crate::{
    pager::{PAGE_HEADER_SIZE, PAGE_SIZE, Page, PageNum},
    table::{data::Data, metadata::Size, node::NodeType},
};

pub const LEAF_NODE_CELL_KEY_SIZE: usize = std::mem::size_of::<LeafNodeCell>();
pub struct LeafNodeCell<'page> {
    pub key: usize,
    phantom: PhantomData<&'page mut Page>,
}

impl<'page> LeafNodeCell<'page> {
    #[inline]
    pub const fn max_cells(data_size: usize) -> usize {
        let headers_size = PAGE_HEADER_SIZE + LEAF_NODE_HEADER_SIZE;
        let free_size = PAGE_SIZE - headers_size;
        let leaf_cell_size = LEAF_NODE_CELL_KEY_SIZE + data_size;
        free_size / leaf_cell_size
    }
    #[inline]
    pub fn initialize(&mut self, key: usize, value: &[u8], size: Size) {
        self.key = key;
        self.data_mut(size).write_all(value);
    }

    #[inline]
    pub fn data(&self, size: Size) -> &'page Data {
        let ptr = unsafe { (self as *const Self).add(1) };
        let slice = unsafe { slice::from_raw_parts(ptr as *const u8, size.size) };
        Data::new_ref(slice)
    }

    #[inline]
    pub fn data_mut(&mut self, size: Size) -> &'page mut Data {
        let ptr = unsafe { (self as *mut Self).add(1) };
        let slice = unsafe { slice::from_raw_parts_mut(ptr as *mut u8, size.size) };
        Data::new_mut(slice)
    }

    #[inline]
    pub fn clone_from(&mut self, other: &Self, size: Size) {
        self.initialize(other.key, other.data(size).read_all(), size);
    }
}

pub const LEAF_NODE_HEADER_SIZE: usize = std::mem::size_of::<LeafNodeHeader>();
pub struct LeafNodeHeader<'page> {
    pub parent_ptr: PageNum,
    pub num_cells: usize,
    phantom: PhantomData<&'page mut Page>,
}

pub struct DebugLeaf<'a> {
    leaf: &'a LeafNodeHeader<'a>,
    size: Size,
}

impl Debug for DebugLeaf<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeafNode")
            .field("parent", &self.leaf.parent_ptr)
            .field("num_cells", &self.leaf.num_cells)
            .field("is_root", &self.leaf.is_root())
            .finish()?;
        for i in 0..self.leaf.num_cells {
            let cell = self.leaf.cell_unchecked(i, self.size);
            let key = cell.key;
            let value = cell.data(self.size);
            write!(f, "\n\t")?;
            f.debug_struct("LeafCell")
                .field("key", &key)
                .field("value", &value)
                .finish()?
        }
        Ok(())
    }
}

impl LeafNodeHeader<'_> {
    pub fn debug(&self, size: Size) -> DebugLeaf<'_> {
        DebugLeaf { leaf: self, size }
    }
}

impl<'page> LeafNodeHeader<'page> {
    pub fn initialize(page: &'page mut Page, parent: PageNum) -> &'page mut Self {
        let header = page.page_header_mut();
        header.node_type = NodeType::LeafNode;
        let leaf = header.node_mut().leaf().expect("Just initialized as leaf");
        leaf.num_cells = 0;
        leaf.parent_ptr = parent;
        leaf
    }

    const fn cell_size(entry_size: Size) -> usize {
        LEAF_NODE_CELL_KEY_SIZE + entry_size.aligned
    }

    unsafe fn cell_raw<'a>(&self, i: usize, entry_size: Size) -> *mut LeafNodeCell<'a> {
        unsafe {
            let first_cell = (self as *const Self).add(1) as *mut LeafNodeCell;
            first_cell.byte_add(i * Self::cell_size(entry_size))
        }
    }

    pub fn cell_unchecked(&self, i: usize, entry_size: Size) -> &'page LeafNodeCell<'page> {
        #[allow(clippy::transmute_ptr_to_ref)]
        unsafe {
            std::mem::transmute(self.cell_raw(i, entry_size))
        }
    }
    pub fn cell_mut_unchecked(
        &mut self,
        i: usize,
        entry_size: Size,
    ) -> &'page mut LeafNodeCell<'page> {
        #[allow(clippy::transmute_ptr_to_ref)]
        unsafe {
            std::mem::transmute(self.cell_raw(i, entry_size))
        }
    }
    pub fn move_cell(&mut self, src: usize, dst: usize, entry_size: Size) {
        debug_assert!(src != dst, "Can't move a cell to itself");
        unsafe {
            let src_cell = self.cell_raw(src, entry_size) as *const LeafNodeCell;
            let dst_cell = self.cell_raw(dst, entry_size);
            (*dst_cell).clone_from(&*src_cell, entry_size);
        }
    }
    pub fn is_root(&self) -> bool {
        self.parent_ptr.is_null()
    }

    fn find_index(&self, key: usize, entry_size: Size) -> usize {
        let mut min_index = 0;
        let mut max_index_past_one = self.num_cells;
        while min_index != max_index_past_one {
            let index = (min_index + max_index_past_one) / 2;
            let key_at_index = self.cell_unchecked(index, entry_size).key;
            if key_at_index == key {
                return index;
            }
            if key < key_at_index {
                max_index_past_one = index;
            } else {
                min_index = index + 1;
            }
        }
        min_index
    }

    /// Finds the cell index for the given key
    /// Can be used for retrieving as well as inserting
    pub fn find(&self, key: usize, entry_size: Size) -> usize {
        self.find_index(key, entry_size)
    }

    /// Makes a space in index and inserts the new cell
    pub fn insert_at_index(&mut self, index: usize, key: usize, value: &[u8], entry_size: Size) {
        if index < self.num_cells {
            // make space for new cell, shift elements to the right: next = prev
            for i in (index..self.num_cells).rev() {
                self.move_cell(i, i + 1, entry_size);
            }
        }
        self.cell_mut_unchecked(index, entry_size)
            .initialize(key, value, entry_size);
        self.num_cells += 1;
    }

    /// Inserts the cell into the leaf node and returns the cell num it was inserted at
    pub fn insert(&mut self, key: usize, value: &[u8], entry_size: Size) -> usize {
        let index = self.find_index(key, entry_size);
        self.insert_at_index(index, key, value, entry_size);
        index
    }

    pub const fn split_count(max_leaf_cells: usize) -> usize {
        max_leaf_cells.div_ceil(2)
    }
}
