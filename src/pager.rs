use std::io::Seek;
use std::{io, iter, ptr};
use std::fs;
use std::os::unix::fs::FileExt;

use crate::tree::{AlignedSize, InternalNodeCell, LeafNodeCell, INTERNAL_NODE_CELL_SIZE, LEAF_NODE_CELL_KEY_SIZE};

#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct PageNum(pub usize);

const PAGE_SIZE: usize = 1024;
pub struct InternalNodeHeader {
    pub parent_ptr: PageNum,
    pub num_keys: usize,
    pub right_child: PageNum,
}

const INTERNAL_NODE_HEADER_SIZE: usize = std::mem::size_of::<InternalNodeHeader>();
impl InternalNodeHeader {
    pub fn cell(&self, i: usize) -> &InternalNodeCell {
        assert!(i < INTERNAL_NODE_CELL_COUNT, "Tried to access out of bounds cell");
        let first_cell = unsafe { (self as *const Self).add(1) } as *const InternalNodeCell;
        unsafe { &*first_cell.add(i) }
    }
    pub fn cell_mut(&mut self, i: usize) -> &mut InternalNodeCell {
        assert!(i < INTERNAL_NODE_CELL_COUNT, "Tried to access out of bounds cell");
        let first_cell = unsafe { (self as *const Self).add(1) } as *mut InternalNodeCell;
        unsafe { &mut *first_cell.add(i) }
    }

    pub fn find(&mut self, key: usize) -> PageNum {
        let mut min_index = 0;
        let mut max_index = self.num_keys;
        while min_index != max_index {
            let index = (min_index + max_index) / 2;
            let key_to_right = self.cell(index).key;
            if key_to_right >= key {
                max_index = index;
            } else {
                min_index = index + 1;
            }
        }
        if min_index == INTERNAL_NODE_CELL_COUNT {
            self.right_child
        } else {
            self.cell(min_index).ptr
        }
    }
}

const FREE_INTERNAL_NODE_SIZE: usize = PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE - PAGE_HEADER_SIZE;
pub const INTERNAL_NODE_CELL_COUNT: usize = FREE_INTERNAL_NODE_SIZE / INTERNAL_NODE_CELL_SIZE;


pub const LEAF_NODE_HEADER_SIZE: usize = std::mem::size_of::<LeafNodeHeader>();
pub struct LeafNodeHeader {
    pub parent_ptr: PageNum,
    pub num_cells: usize,
    pub right_child: PageNum,
}

impl LeafNodeHeader {
    const fn cell_size(entry_size: AlignedSize) -> usize {
        LEAF_NODE_CELL_KEY_SIZE + entry_size.0
    }

    pub fn cell(&self, i: usize, entry_size: AlignedSize) -> &LeafNodeCell {
        let first_cell = unsafe { (self as *const Self).add(1) } as *const LeafNodeCell;
        unsafe { &*first_cell.byte_add(i * Self::cell_size(entry_size)) }
    }
    pub fn cell_mut(&mut self, i: usize, entry_size: AlignedSize) -> &mut LeafNodeCell {
        let first_cell = unsafe { (self as *const Self).add(1) } as *mut LeafNodeCell;
        unsafe { &mut *first_cell.byte_add(i * Self::cell_size(entry_size)) }
    }
    pub fn move_cell(&mut self, src: usize, dst: usize, entry_size: AlignedSize) {
        let src_cell = self.cell(src, entry_size) as *const LeafNodeCell;
        let dst_cell = self.cell_mut(dst, entry_size) as *mut LeafNodeCell;
        unsafe { 
            (*dst_cell).key = (*src_cell).key;
            (*dst_cell).data_mut(entry_size.0).copy_from_slice((*src_cell).data(entry_size.0));
        }
    }
}

#[repr(u8)]
enum NodeType {
    InternalNode = 0,
    LeafNode = 1,
}

pub enum Node<'a> {
    InternalNode(&'a InternalNodeHeader),
    LeafNode(&'a LeafNodeHeader),
}

pub enum NodeMut<'a> {
    InternalNode(&'a mut InternalNodeHeader),
    LeafNode(&'a mut LeafNodeHeader),
}

#[derive(Clone, Debug)]
#[repr(align(8))]
pub struct Page([u8; PAGE_SIZE]);

impl Page {
    pub fn page_header(&self) -> &PageHeader {
        unsafe { std::mem::transmute(ptr::from_ref(self)) }
    }

    pub fn page_header_mut(&mut self) -> &mut PageHeader {
        unsafe { std::mem::transmute(ptr::from_ref(self)) }
    }

    fn metadata(&mut self) -> &mut MetadataPage {
        unsafe { std::mem::transmute(ptr::from_ref(self)) }
    }

    pub fn initialize_leaf_node(page: &mut Self, parent: PageNum) {
        let header = page.page_header_mut();
        header.node_type = NodeType::LeafNode;
        let node = header.node_mut();
        if let NodeMut::LeafNode(leaf) = node {
            leaf.num_cells = 0;
            leaf.right_child = PageNum(0);
            leaf.parent_ptr = parent;
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
pub struct PageHeader {
    node_type: NodeType
}

impl<'a> PageHeader {
    pub fn node(&self) -> Node<'a> {
        let header_ptr = self as *const Self;
        // println!("header ptr: {:?}", header_ptr);
        let node_ptr = unsafe { header_ptr.add(1)};
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

    pub fn node_mut(&mut self) -> NodeMut<'a> {
        let header_ptr = self as *mut Self;
        // println!("header ptr: {:?}", header_ptr);
        let node_ptr = unsafe { header_ptr.add(1)};
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
    pub pages: Vec<Option<Page>>,
}

impl Pager {
    pub fn new(mut file: fs::File) -> io::Result<Self> {
        let length = file.seek(io::SeekFrom::End(0))? as usize;
        let num_pages = length / PAGE_SIZE;
        let mut pager = Self{file: file, num_pages, pages: vec![]};
        if num_pages == 0 {
            let root_page = PageNum(1);
            let metadata_page = pager.get_page(PageNum(0))?;
            Page::initialize_metadata_page(metadata_page, root_page);
            let root_page = pager.get_page(root_page)?;
            Page::initialize_leaf_node(root_page, PageNum(0));
        }
        Ok(pager)
    }

    pub fn get_metadata(&mut self) -> io::Result<&mut MetadataPage> {
        Ok(self.get_page(PageNum(0))?.metadata())
    }

    pub fn get_page(&mut self, page_num: PageNum) -> io::Result<&mut Page> {
        let len = self.pages.len();
        if page_num.0 >= len {
            self.pages.extend(iter::repeat_n(None, page_num.0 - len + 1));
        }

        let page_slot = &mut self.pages[page_num.0];
        match page_slot {
            Some(slot) => Ok(slot),
            None => {
                page_slot.replace(Page([0; 1024]));
                if page_num.0 >= self.num_pages {
                    return Ok(page_slot.as_mut().unwrap())
                }
                let page_offset = page_num.0 * PAGE_SIZE;
                self.file.read_exact_at(&mut page_slot.as_mut().unwrap().0, page_offset as u64)?;
                Ok(page_slot.as_mut().unwrap())
            }
        }
    }

    pub fn flush(&mut self) -> io::Result<()> {
        let biggest_page_index = self.pages
            .iter()
            .enumerate()
            .rev()
            .find(|(_, p)| p.is_some())
            .map(|(i, _)| i)
            .expect("At least one page shouldn't be empty");
        if biggest_page_index >= self.num_pages {
            let file_size = (biggest_page_index - self.num_pages + 1) * PAGE_SIZE;
            self.file.set_len(file_size as u64)?;
        }
        for i in 0..=biggest_page_index {
            let page = &self.pages[i];
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
