use std::cell::{RefCell, UnsafeCell};
use std::fmt::Debug;
use std::fs;
use std::io::Seek;
use std::marker::PhantomData;
use std::os::unix::fs::FileExt;
use std::{io, iter, ptr};

use crate::table::internal::InternalNodeHeader;
use crate::table::leaf::LeafNodeHeader;
use crate::table::node::{Node, NodeMut, NodeType};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct PageNum(pub usize);

impl PageNum {
    pub const NULL: Self = Self(0);
    pub fn is_null(&self) -> bool {
        self.0 == 0
    }
}

pub const PAGE_SIZE: usize = 1024;

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

    pub fn initialize_metadata_page(_page: &mut Self, _root: PageNum) {
        // NOOP
        // let metadata = page.metadata();
        // metadata.root = root;
    }
}

pub const PAGE_HEADER_SIZE: usize = std::mem::size_of::<PageHeader>();
#[repr(align(8))]
pub struct PageHeader<'page> {
    pub node_type: NodeType,
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

// TODO: Use this page for collecting free pages and something else
pub struct MetadataPage {}

// TODO: Change pager from using a vec to something else
const MAX_PAGES: usize = 256;
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
            pages: Vec::with_capacity(MAX_PAGES).into(),
        };
        if num_pages == 0 {
            let root_page = PageNum(1);
            let metadata_page = pager.get_page(PageNum(0))?;
            Page::initialize_metadata_page(metadata_page, root_page);
            let root_page = pager.get_page(root_page)?;
            LeafNodeHeader::initialize(root_page, PageNum::NULL);
        }
        Ok(pager)
    }

    pub fn get_metadata(&mut self) -> io::Result<&mut MetadataPage> {
        Ok(self.get_page(PageNum(0))?.metadata())
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get_page(&self, page_num: PageNum) -> io::Result<&mut Page> {
        assert!(page_num.0 < MAX_PAGES, "Can't request more than MAX_PAGES");
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

    pub fn get_node(&self, page_num: PageNum) -> io::Result<NodeMut<'_>> {
        self.get_page(page_num)
            .map(|p| p.page_header_mut().node_mut())
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
