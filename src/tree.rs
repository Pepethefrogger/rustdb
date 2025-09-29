use std::{fs, io, marker::PhantomData, slice};

use derivative::Derivative;

use crate::pager::{
    INTERNAL_NODE_CELL_COUNT, LeafNodeHeader, NodeMut, Page, PageHeader, PageNum, Pager,
    leaf_cells_max,
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

pub const LEAF_NODE_CELL_KEY_SIZE: usize = std::mem::size_of::<LeafNodeCell>();
pub struct LeafNodeCell<'page> {
    pub key: usize,
    phantom: PhantomData<&'page mut Page>,
}

impl<'page> LeafNodeCell<'page> {
    #[inline]
    pub fn initialize(&mut self, key: usize, value: &[u8], size: Size) {
        self.key = key;
        self.data_mut(size).copy_from_slice(value);
    }

    #[inline]
    pub fn data(&self, size: Size) -> &'page [u8] {
        let ptr = unsafe { (self as *const Self).add(1) };
        unsafe { slice::from_raw_parts(ptr as *const u8, size.size) }
    }

    #[inline]
    pub fn data_mut(&mut self, size: Size) -> &'page mut [u8] {
        let ptr = unsafe { (self as *mut Self).add(1) };
        unsafe { slice::from_raw_parts_mut(ptr as *mut u8, size.size) }
    }

    #[inline]
    pub fn clone_from(&mut self, other: &Self, size: Size) {
        self.initialize(other.key, other.data(size), size);
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Cursor<'table> {
    #[derivative(Debug = "ignore")]
    pub table: &'table mut Table,
    pub page_num: PageNum,
    pub cell_num: usize,
}

impl<'table> Cursor<'table> {
    /// Return the node that this cursor points to
    fn node(&self) -> io::Result<NodeMut<'table>> {
        let page = self.table.pager.get_page(self.page_num)?;
        let node = page.page_header_mut().node_mut();
        #[allow(clippy::missing_transmute_annotations)]
        Ok(unsafe { std::mem::transmute(node) })
    }

    /// Return the header that this cursor points to
    fn header(&self) -> io::Result<&'table mut PageHeader<'table>> {
        let header = self.table.pager.get_page(self.page_num)?.page_header_mut();
        #[allow(clippy::missing_transmute_annotations)]
        Ok(unsafe { std::mem::transmute(header) })
    }

    /// Returns the entry that this cursor points to
    pub fn cell(&self) -> io::Result<&'table mut LeafNodeCell<'table>> {
        let cell_num = self.cell_num;
        let aligned_entry_size = self.table.entry_size;
        let node = self.node()?;
        if let NodeMut::LeafNode(leaf) = node {
            Ok(leaf.cell_mut(cell_num, aligned_entry_size))
        } else {
            unreachable!("A cursor shouldn't point to an internal node")
        }
    }

    /// Moves the cursor to the entry with the desired key
    pub fn find(&mut self, key: usize) -> io::Result<()> {
        let mut node = self.node()?;
        while let NodeMut::InternalNode(ref mut internal) = node {
            let page_num = internal.find(key);
            let page = self.table.pager.get_page(page_num)?;
            node = page.page_header_mut().node_mut();
            self.page_num = page_num;
        }
        if let NodeMut::LeafNode(leaf) = node {
            self.cell_num = leaf.find(key, self.table.entry_size);
            Ok(())
        } else {
            unreachable!();
        }
    }

    /// Resets the cursor to point at the root node
    pub fn reset(&mut self) {
        self.page_num = self.table.get_root();
    }

    /// Creates an entry with the key, you can use the cursor to replace the value
    pub fn insert(&mut self, key: usize, value: &[u8]) -> io::Result<()> {
        let entry_size = self.table.entry_size;
        let max_leaf_cells = self.table.max_leaf_cells;
        self.find(key)?;
        let header = self.header()?;
        let node = header.node_mut();
        match node {
            NodeMut::InternalNode(_) => unreachable!("A find can't end in an internal node"),
            NodeMut::LeafNode(leaf) => {
                if self.cell_num < leaf.num_cells && leaf.cell(self.cell_num, entry_size).key == key
                {
                    return Err(io::Error::other("Duplicate key"));
                }

                if leaf.num_cells == max_leaf_cells {
                    if leaf.is_root() {
                        self.split_root_leaf_and_insert(leaf, key, value)?;
                    } else {
                        self.split_nonroot_leaf_and_insert(leaf, key, value)?;
                    }
                } else {
                    leaf.insert_at_index(self.cell_num, key, value, entry_size);
                }
                Ok(())
            }
        }
    }

    /// Creates a new leaf node, copies cells from self to other until self has split_count cells
    /// Also it creates a new entry in the correct leaf, and mutates the cursor to point at it
    /// Returns the newly created page, as well as the first key in the right node
    fn split_leaf_and_insert(
        &self,
        leaf: &mut LeafNodeHeader,
        key: usize,
        value: &[u8],
        parent: PageNum,
        max_leaf_cells: usize,
    ) -> io::Result<(PageNum, usize)> {
        let new_leaf_page_num = self.table.pager.get_free_page()?;
        let new_leaf_page = self.table.pager.get_page(new_leaf_page_num)?;
        Page::initialize_leaf_node(new_leaf_page, parent);
        let new_leaf = new_leaf_page
            .page_header_mut()
            .node_mut()
            .leaf()
            .expect("Just initialized this as a leaf node");

        let entry_size = self.table.entry_size;
        // Copy half of the cells from old leaf, if new cell has to go into new leaf
        // copy it there and point the cursor
        let split_count = LeafNodeHeader::split_count(max_leaf_cells);
        for i in split_count..=max_leaf_cells {
            // Include max cell count because we have that many + 1
            let new_node_index = i - split_count;
            if i == self.cell_num {
                new_leaf.cell_mut(new_node_index, entry_size).key = key;
            } else if i < self.cell_num {
                let cell = new_leaf.cell_mut(new_node_index, entry_size);
                let old_cell = leaf.cell(i, entry_size);
                cell.clone_from(old_cell, entry_size);
            } else {
                let cell = new_leaf.cell_mut(new_node_index, entry_size);
                let old_cell = leaf.cell(i - 1, entry_size);
                cell.clone_from(old_cell, entry_size);
            }
        }
        // Set correct metadata before possible inserting into the old leaf
        leaf.parent_ptr = parent;
        leaf.num_cells = split_count;
        new_leaf.parent_ptr = parent;
        new_leaf.num_cells = max_leaf_cells - split_count;
        if self.cell_num < split_count {
            leaf.insert_at_index(self.cell_num, key, value, entry_size);
        } else {
            new_leaf.num_cells += 1;
            new_leaf
                .cell_mut(self.cell_num - split_count, entry_size)
                .initialize(key, value, entry_size);
        }
        let split_key = new_leaf.cell(0, entry_size).key;
        Ok((new_leaf_page_num, split_key))
    }

    fn split_root_leaf_and_insert(
        &mut self,
        root: &'table mut LeafNodeHeader,
        key: usize,
        value: &[u8],
    ) -> io::Result<()> {
        let old_leaf_page_num = self.page_num;
        let new_internal_page_num = self.table.pager.get_free_page()?;
        self.table.set_root(new_internal_page_num)?; // Set new root node

        let max_leaf_cells = self.table.max_leaf_cells;
        // Split children into two new leaf nodes
        let (new_leaf_page_num, split_key) =
            self.split_leaf_and_insert(root, key, value, new_internal_page_num, max_leaf_cells)?;

        let new_internal_page = self.table.pager.get_page(new_internal_page_num)?;
        Page::initialize_internal_node(
            new_internal_page,
            PageNum::NULL,
            split_key,
            old_leaf_page_num,
            new_leaf_page_num,
        );
        // println!("Internal {:?}: \n{:?}", new_internal_page_num, new_internal);
        // println!("Leaf {:?}: \n{:?}", old_leaf_page_num, leaf.debug(entry_size));
        // println!("Leaf {:?}: \n{:?}", new_leaf_page_num, new_leaf.debug(entry_size));
        Ok(())
    }

    fn split_nonroot_leaf_and_insert(
        &mut self,
        leaf: &'table mut LeafNodeHeader,
        key: usize,
        value: &[u8],
    ) -> io::Result<()> {
        let entry_size = self.table.entry_size;
        let max_leaf_cells = self.table.max_leaf_cells;
        let parent_page_num = leaf.parent_ptr;
        // We do this because it returns an "owned pointer"
        let old_page_num = self.page_num;
        self.page_num = parent_page_num;
        let node = self.node()?;
        // Restore the page num in case we use the same node
        self.page_num = old_page_num;
        if let NodeMut::InternalNode(internal) = node {
            if internal.num_keys == INTERNAL_NODE_CELL_COUNT {
                unimplemented!("Don't know how to split an internal node")
            } else {
                let (new_leaf_page_num, split_key) =
                    self.split_leaf_and_insert(leaf, key, value, parent_page_num, max_leaf_cells)?;
                internal.insert(split_key, new_leaf_page_num);

                println!("Inserting key: {}", key);
                println!("Internal: {:?}\n{:?}", parent_page_num, internal);
                println!("Left: {:?}\n{:?}", old_page_num, leaf.debug(entry_size));
                let right_leaf = self
                    .table
                    .pager
                    .get_page(new_leaf_page_num)?
                    .page_header_mut()
                    .node_mut()
                    .leaf()
                    .unwrap();
                println!(
                    "Right: {:?}\n{:?}",
                    new_leaf_page_num,
                    right_leaf.debug(entry_size)
                );
                Ok(())
            }
        } else {
            unreachable!("A parent can't be a leaf node");
        }
    }
}

#[derive(Clone, Copy)]
pub struct Size {
    pub size: usize,
    pub aligned: usize,
}

impl Size {
    const fn new(size: usize, align: usize) -> Size {
        let aligned = (size + align - 1) & !(align - 1);
        Self { size, aligned }
    }
}

pub struct Table {
    pub pager: Pager,
    root: PageNum,
    pub entry_size: Size,
    pub max_leaf_cells: usize,
}

impl Table {
    pub fn new(mut pager: Pager, entry_size: usize) -> io::Result<Self> {
        let entry_size = Size::new(entry_size, 8);
        let max_leaf_cells = leaf_cells_max(entry_size.aligned);
        // let total_size = PAGE_HEADER_SIZE + LEAF_NODE_HEADER_SIZE + max_leaf_cells * (LEAF_NODE_CELL_KEY_SIZE + aligned_entry_size.0);
        // println!("Aligned entry size {}, max leaf cells {}, total size {}", aligned_entry_size.0, max_leaf_cells, total_size);
        let root = pager.get_metadata()?.root;
        Ok(Self {
            pager,
            root,
            entry_size,
            max_leaf_cells,
        })
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
        Table::new(pager, entry_size)
    }

    pub fn cursor(&mut self) -> Cursor<'_> {
        let root = self.root;
        Cursor {
            table: self,
            page_num: root,
            cell_num: 0,
        }
    }

    pub fn insert(&mut self, key: usize, value: &[u8]) -> io::Result<()> {
        let mut cursor = self.cursor();
        cursor.insert(key, value)?;
        Ok(())
    }

    pub fn find<'a>(&'a mut self, key: usize) -> io::Result<&'a [u8]> {
        let entry_size = self.entry_size;
        let mut cursor: Cursor<'a> = self.cursor();
        cursor.find(key)?;
        let cell: &'a LeafNodeCell<'a> = cursor.cell()?;
        Ok(cell.data(entry_size))
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        self.pager.flush().expect("Failed to flush pager");
    }
}
