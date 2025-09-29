use std::cell::{RefCell, UnsafeCell};
use std::fmt::Debug;
use std::fs;
use std::io::Seek;
use std::marker::PhantomData;
use std::os::unix::fs::FileExt;
use std::{io, iter, ptr};

use crate::tree::{
    INTERNAL_NODE_CELL_SIZE, InternalNodeCell, LEAF_NODE_CELL_KEY_SIZE, LeafNodeCell, Size,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct PageNum(pub usize);

impl PageNum {
    pub const NULL: Self = Self(0);
    pub fn is_null(&self) -> bool {
        self.0 == 0
    }
}

const PAGE_SIZE: usize = 1024;
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
            let cell = self.cell(i);
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
    unsafe fn cell_raw<'a>(&self, i: usize) -> *mut InternalNodeCell<'a> {
        unsafe {
            let first_cell = (self as *const Self).add(1) as *mut InternalNodeCell;
            first_cell.add(i)
        }
    }

    pub fn cell(&self, i: usize) -> &'page InternalNodeCell<'page> {
        assert!(
            i < INTERNAL_NODE_CELL_COUNT,
            "Tried to access out of bounds cell"
        );
        #[allow(clippy::transmute_ptr_to_ref)]
        unsafe {
            std::mem::transmute(self.cell_raw(i))
        }
    }
    pub fn cell_mut(&mut self, i: usize) -> &'page mut InternalNodeCell<'page> {
        assert!(
            i < INTERNAL_NODE_CELL_COUNT,
            "Tried to access out of bounds cell"
        );
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
            let key_at_index = self.cell(index).key;
            if key_at_index == key {
                min_index = index;
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
            self.cell(index).ptr
        }
    }

    /// Inserts a key and value in the correct place
    pub fn insert(&mut self, key: usize, ptr: PageNum) {
        let index = self.find_index(key);
        if index < self.num_keys {
            // Make space for new cell, shift elements to the right: next = prev
            for i in (index..self.num_keys).rev() {
                self.move_cell(i - 1, i);
            }
            self.cell_mut(index).initialize(key, ptr);
        } else {
            self.cell_mut(index).initialize(key, self.right_child);
            self.right_child = ptr;
        }
        self.num_keys += 1;
    }
}

const FREE_INTERNAL_NODE_SIZE: usize = PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE - PAGE_HEADER_SIZE;
pub const INTERNAL_NODE_CELL_COUNT: usize = FREE_INTERNAL_NODE_SIZE / INTERNAL_NODE_CELL_SIZE;

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
            let cell = self.leaf.cell(i, self.size);
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
    const fn cell_size(entry_size: Size) -> usize {
        LEAF_NODE_CELL_KEY_SIZE + entry_size.aligned
    }

    unsafe fn cell_raw<'a>(&self, i: usize, entry_size: Size) -> *mut LeafNodeCell<'a> {
        unsafe {
            let first_cell = (self as *const Self).add(1) as *mut LeafNodeCell;
            first_cell.byte_add(i * Self::cell_size(entry_size))
        }
    }

    pub fn cell(&self, i: usize, entry_size: Size) -> &'page LeafNodeCell<'page> {
        #[allow(clippy::transmute_ptr_to_ref)]
        unsafe {
            std::mem::transmute(self.cell_raw(i, entry_size))
        }
    }
    pub fn cell_mut(&mut self, i: usize, entry_size: Size) -> &'page mut LeafNodeCell<'page> {
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
            let key_at_index = self.cell(index, entry_size).key;
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

    pub fn insert_at_index(&mut self, index: usize, key: usize, value: &[u8], entry_size: Size) {
        if index < self.num_cells {
            // make space for new cell, shift elements to the right: next = prev
            for i in (index..self.num_cells).rev() {
                self.move_cell(i - 1, i, entry_size);
            }
        }
        self.cell_mut(index, entry_size)
            .initialize(key, value, entry_size);
        self.num_cells += 1;
    }

    pub fn insert(&mut self, key: usize, value: &[u8], entry_size: Size) {
        let index = self.find_index(key, entry_size);
        self.insert_at_index(index, key, value, entry_size);
    }

    pub const fn split_count(max_leaf_cells: usize) -> usize {
        max_leaf_cells.div_ceil(2)
    }
}

#[repr(u8)]
enum NodeType {
    InternalNode = 0,
    LeafNode = 1,
}

pub enum Node<'page> {
    InternalNode(&'page InternalNodeHeader<'page>),
    LeafNode(&'page LeafNodeHeader<'page>),
}

pub enum NodeMut<'page> {
    InternalNode(&'page mut InternalNodeHeader<'page>),
    LeafNode(&'page mut LeafNodeHeader<'page>),
}

impl<'page> NodeMut<'page> {
    // pub fn covariant<'b>(self) -> NodeMut<'b, 'page> where 'b: 'reference {
    //     unsafe { std::mem::transmute(self) }
    // }
    pub fn internal(self) -> Option<&'page mut InternalNodeHeader<'page>> {
        match self {
            Self::InternalNode(internal) => Some(internal),
            _ => None,
        }
    }

    pub fn leaf(self) -> Option<&'page mut LeafNodeHeader<'page>> {
        match self {
            Self::LeafNode(leaf) => Some(leaf),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
#[repr(align(8))]
pub struct Page([u8; PAGE_SIZE]);

impl Page {
    pub fn page_header(&self) -> &PageHeader<'_> {
        #[allow(clippy::transmute_ptr_to_ref)]
        unsafe {
            std::mem::transmute(ptr::from_ref(self))
        }
    }

    pub fn page_header_mut(&mut self) -> &mut PageHeader<'_> {
        #[allow(clippy::transmute_ptr_to_ref)]
        unsafe {
            std::mem::transmute(ptr::from_ref(self))
        }
    }

    fn metadata(&mut self) -> &mut MetadataPage {
        #[allow(clippy::transmute_ptr_to_ref)]
        unsafe {
            std::mem::transmute(ptr::from_ref(self))
        }
    }

    pub fn initialize_leaf_node(page: &mut Self, parent: PageNum) -> &LeafNodeHeader<'_> {
        let header = page.page_header_mut();
        header.node_type = NodeType::LeafNode;
        let node = header.node_mut();
        if let NodeMut::LeafNode(leaf) = node {
            leaf.num_cells = 0;
            leaf.parent_ptr = parent;
            leaf
        } else {
            unreachable!()
        }
    }

    pub fn initialize_internal_node(
        page: &mut Self,
        parent: PageNum,
        key: usize,
        left_child: PageNum,
        right_child: PageNum,
    ) -> &InternalNodeHeader<'_> {
        let header = page.page_header_mut();
        header.node_type = NodeType::InternalNode;
        let node = header.node_mut();
        if let NodeMut::InternalNode(internal) = node {
            internal.num_keys = 1;
            internal.parent_ptr = parent;
            internal.right_child = right_child;
            let cell = internal.cell_mut(0);
            cell.key = key;
            cell.ptr = left_child;
            internal
        } else {
            unreachable!()
        }
    }

    pub fn initialize_metadata_page(page: &mut Self, root: PageNum) {
        let metadata = page.metadata();
        metadata.root = root;
    }
}

pub const PAGE_HEADER_SIZE: usize = std::mem::size_of::<PageHeader>();
#[repr(align(8))]
pub struct PageHeader<'page> {
    node_type: NodeType,
    phantom: PhantomData<&'page mut Page>,
}

impl<'page> PageHeader<'page> {
    pub fn node(&self) -> Node<'page> {
        let header_ptr = self as *const Self;
        // println!("header ptr: {:?}", header_ptr);
        let node_ptr = unsafe { header_ptr.add(1) };
        // println!("node ptr: {:?}", node_ptr);
        match self.node_type {
            NodeType::InternalNode => {
                let ptr = unsafe { &*(node_ptr as *const InternalNodeHeader) };
                Node::InternalNode(ptr)
            }
            NodeType::LeafNode => {
                let ptr = unsafe { &*(node_ptr as *const LeafNodeHeader) };
                Node::LeafNode(ptr)
            }
        }
    }

    pub fn node_mut(&mut self) -> NodeMut<'page> {
        let header_ptr = self as *mut Self;
        // println!("header ptr: {:?}", header_ptr);
        let node_ptr = unsafe { header_ptr.add(1) };
        // println!("node ptr: {:?}", node_ptr);
        match self.node_type {
            NodeType::InternalNode => {
                let ptr = unsafe { &mut *(node_ptr as *mut InternalNodeHeader) };
                NodeMut::InternalNode(ptr)
            }
            NodeType::LeafNode => {
                let ptr = unsafe { &mut *(node_ptr as *mut LeafNodeHeader) };
                NodeMut::LeafNode(ptr)
            }
        }
    }
}

pub struct MetadataPage {
    pub root: PageNum,
}

pub struct Pager {
    file: fs::File,
    num_pages: usize,
    pub pages: RefCell<Vec<UnsafeCell<Option<Page>>>>,
}

impl Pager {
    pub fn new(mut file: fs::File) -> io::Result<Self> {
        let length = file.seek(io::SeekFrom::End(0))? as usize;
        let num_pages = length / PAGE_SIZE;
        let pager = Self {
            file,
            num_pages,
            pages: vec![].into(),
        };
        if num_pages == 0 {
            let root_page = PageNum(1);
            let metadata_page = pager.get_page(PageNum(0))?;
            Page::initialize_metadata_page(metadata_page, root_page);
            let root_page = pager.get_page(root_page)?;
            Page::initialize_leaf_node(root_page, PageNum::NULL);
        }
        Ok(pager)
    }

    pub fn get_metadata(&mut self) -> io::Result<&mut MetadataPage> {
        Ok(self.get_page(PageNum(0))?.metadata())
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get_page(&self, page_num: PageNum) -> io::Result<&mut Page> {
        let len = self.pages.borrow().len();
        if page_num.0 >= len {
            self.pages
                .borrow_mut()
                .extend(iter::repeat_with(|| UnsafeCell::new(None)).take(page_num.0 - len + 1));
        }

        let page_slot = unsafe { &mut *self.pages.borrow()[page_num.0].get() };
        match page_slot {
            Some(page) => Ok(page),
            None => {
                let page = page_slot.insert(Page([0; 1024]));
                if page_num.0 < self.num_pages {
                    let page_offset = page_num.0 * PAGE_SIZE;
                    self.file.read_exact_at(&mut page.0, page_offset as u64)?;
                }
                Ok(page_slot.as_mut().unwrap())
            }
        }
    }

    pub fn get_free_page(&self) -> io::Result<PageNum> {
        let page_num = PageNum(self.pages.borrow().len().max(self.num_pages));
        self.get_page(page_num)?;
        Ok(page_num)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        let biggest_page_index = self
            .pages
            .borrow()
            .iter()
            .enumerate()
            .rev()
            .find(|(_, p)| unsafe { p.get().as_ref() }.is_some())
            .map(|(i, _)| i)
            .expect("At least one page shouldn't be empty");
        if biggest_page_index >= self.num_pages {
            let file_size = (biggest_page_index - self.num_pages + 1) * PAGE_SIZE;
            self.file.set_len(file_size as u64)?;
        }
        for i in 0..=biggest_page_index {
            let page = unsafe { &*self.pages.borrow()[i].get() };
            if let Some(page) = page {
                let page_location = i * PAGE_SIZE;
                self.file.write_all_at(&page.0, page_location as u64)?;
            }
        }
        self.file.sync_data()?;
        Ok(())
    }
}

pub const fn leaf_cells_max(data_size: usize) -> usize {
    let headers_size = PAGE_HEADER_SIZE + LEAF_NODE_HEADER_SIZE;
    let free_size = PAGE_SIZE - headers_size;
    let leaf_cell_size = LEAF_NODE_CELL_KEY_SIZE + data_size;
    free_size / leaf_cell_size
}
