use std::{fmt::Debug, marker::PhantomData};

use crate::{
    pager::{PAGE_HEADER_SIZE, PAGE_SIZE, Page, PageNum},
    table::node::NodeType,
};

pub struct InternalNodeCell<'page> {
    pub key: usize,
    pub ptr: PageNum,
    phantom: PhantomData<&'page mut Page>,
}

impl<'page> InternalNodeCell<'page> {
    #[inline]
    pub fn initialize(&mut self, key: usize, ptr: PageNum) {
        self.key = key;
        self.ptr = ptr;
    }

    #[inline]
    pub fn clone_from(&mut self, other: &Self) {
        self.initialize(other.key, other.ptr);
    }
}
pub const INTERNAL_NODE_CELL_SIZE: usize = std::mem::size_of::<InternalNodeCell>();

pub struct InternalNodeHeader<'page> {
    pub parent_ptr: PageNum,
    pub num_keys: usize,
    pub right_child: PageNum,
    phantom: PhantomData<&'page mut Page>,
}

impl Debug for InternalNodeHeader<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InternalNode")
            .field("parent", &self.parent_ptr)
            .field("num_keys", &self.num_keys)
            .field("right_child", &self.right_child)
            .field("is_root", &self.is_root())
            .finish()?;
        for i in 0..self.num_keys {
            let cell = self.cell_unchecked(i);
            write!(f, "\n\t")?;
            f.debug_struct("InternalCell")
                .field("key", &cell.key)
                .field("ptr", &cell.ptr)
                .finish()?
        }
        Ok(())
    }
}

const INTERNAL_NODE_HEADER_SIZE: usize = std::mem::size_of::<InternalNodeHeader>();
impl<'page> InternalNodeHeader<'page> {
    pub fn initialize(
        page: &'page mut Page,
        parent: PageNum,
        key: usize,
        left_child: PageNum,
        right_child: PageNum,
    ) -> &'page mut Self {
        let header = page.page_header_mut();
        header.node_type = NodeType::InternalNode;
        let internal = header
            .node_mut()
            .internal()
            .expect("Just initialized as internal");
        internal.num_keys = 1;
        internal.parent_ptr = parent;
        internal.right_child = right_child;
        let cell = internal.cell_mut_unchecked(0);
        cell.key = key;
        cell.ptr = left_child;
        internal
    }

    pub fn initialize_empty(page: &'page mut Page, parent: PageNum) -> &'page mut Self {
        let header = page.page_header_mut();
        header.node_type = NodeType::InternalNode;
        let internal = header
            .node_mut()
            .internal()
            .expect("Just initialized as internal");
        internal.parent_ptr = parent;
        internal.num_keys = 0;
        internal
    }

    unsafe fn cell_raw<'a>(&self, i: usize) -> *mut InternalNodeCell<'a> {
        unsafe {
            let first_cell = (self as *const Self).add(1) as *mut InternalNodeCell;
            first_cell.add(i)
        }
    }

    pub fn cell_unchecked(&self, i: usize) -> &'page InternalNodeCell<'page> {
        // assert!(
        //     i < INTERNAL_NODE_CELL_COUNT,
        //     "Tried to access out of bounds cell"
        // );
        #[allow(clippy::transmute_ptr_to_ref)]
        unsafe {
            std::mem::transmute(self.cell_raw(i))
        }
    }
    pub fn cell_mut_unchecked(&mut self, i: usize) -> &'page mut InternalNodeCell<'page> {
        // assert!(
        //     i < INTERNAL_NODE_CELL_COUNT,
        //     "Tried to access out of bounds cell"
        // );
        #[allow(clippy::transmute_ptr_to_ref)]
        unsafe {
            std::mem::transmute(self.cell_raw(i))
        }
    }
    pub fn move_cell(&mut self, src: usize, dst: usize) {
        debug_assert!(src != dst, "Can't move a cell to itself");
        unsafe {
            let src_cell = self.cell_raw(src) as *const InternalNodeCell;
            let dst_cell = self.cell_raw(dst);
            (*dst_cell).clone_from(&*src_cell);
        }
    }
    pub fn is_root(&self) -> bool {
        self.parent_ptr.is_null()
    }

    /// Finds the index that this key needs to be inserted in
    pub fn find_index(&self, key: usize) -> usize {
        let mut min_index = 0;
        let mut max_index_past_one = self.num_keys;
        while min_index != max_index_past_one {
            let index = (min_index + max_index_past_one) / 2;
            let key_at_index = self.cell_unchecked(index).key;
            if key_at_index == key {
                min_index = index + 1;
                break;
            }
            if key < key_at_index {
                max_index_past_one = index;
            } else {
                min_index = index + 1;
            }
        }

        min_index
    }

    /// Find the page that contains the given key
    pub fn find(&self, key: usize) -> PageNum {
        let index = self.find_index(key);
        if index == self.num_keys {
            self.right_child
        } else {
            self.cell_unchecked(index).ptr
        }
    }

    /// Inserts a key and value in the correct place
    pub fn insert(&mut self, key: usize, ptr: PageNum) {
        let index = self.find_index(key);
        if index < self.num_keys {
            // Make space for new cell, shift elements to the right: next = prev
            for i in (index..self.num_keys).rev() {
                self.move_cell(i, i + 1);
            }
            self.cell_mut_unchecked(index).initialize(key, ptr);
        } else {
            self.cell_mut_unchecked(index)
                .initialize(key, self.right_child);
            self.right_child = ptr;
        }
        self.num_keys += 1;
    }
}

const FREE_INTERNAL_NODE_SIZE: usize = PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE - PAGE_HEADER_SIZE;
pub const INTERNAL_NODE_CELL_COUNT: usize = FREE_INTERNAL_NODE_SIZE / INTERNAL_NODE_CELL_SIZE;
