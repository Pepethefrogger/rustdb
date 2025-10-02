use std::{fs, io, marker::PhantomData, slice};

use crate::{
    debug::debug_table,
    pager::{
        INTERNAL_NODE_CELL_COUNT, InternalNodeHeader, LeafNodeHeader, NodeMut, Page, PageNum,
        Pager, leaf_cells_max,
    },
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

pub struct Cursor {
    pub page_num: PageNum,
    pub cell_num: usize,
}

impl Cursor {
    /// Returns the value that this cursor points to
    pub fn value<'table>(&self, table: &'table Table) -> io::Result<&'table mut [u8]> {
        let cell = self.cell(table)?;
        Ok(cell.data_mut(table.entry_size))
    }
    /// Returns the entry that this cursor points to
    pub fn cell<'table>(
        &self,
        table: &'table Table,
    ) -> io::Result<&'table mut LeafNodeCell<'table>> {
        let cell_num = self.cell_num;
        let leaf = self.leaf(table)?;
        Ok(leaf.cell_mut_unchecked(cell_num, table.entry_size))
    }
    /// Returns the leaf node that this cursor points to
    #[allow(clippy::mut_from_ref)]
    pub fn leaf<'table>(
        &self,
        table: &'table Table,
    ) -> io::Result<&'table mut LeafNodeHeader<'table>> {
        let leaf = table
            .pager
            .get_node(self.page_num)?
            .leaf()
            .expect("A cursor has to point to a leaf");
        Ok(unsafe {
            std::mem::transmute::<&mut LeafNodeHeader<'_>, &'table mut LeafNodeHeader<'table>>(leaf)
        })
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
        let root = pager.get_metadata()?.root;
        Ok(Self {
            pager,
            root,
            entry_size,
            max_leaf_cells,
        })
    }

    #[inline]
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

    fn cursor(&self, page_num: PageNum, cell_num: usize) -> Cursor {
        Cursor { page_num, cell_num }
    }

    /// Returns the value for the specified key
    pub fn find(&self, key: usize) -> io::Result<&[u8]> {
        let cursor = self.find_cursor(key)?;
        let leaf = cursor.leaf(self)?;
        if cursor.cell_num < leaf.num_cells && cursor.cell(self)?.key == key {
            cursor.value(self).map(|v| v as &[u8])
        } else {
            Err(io::Error::other("Key not found"))
        }
    }

    /// Returns a cursor pointing to the specified value.
    /// Can be used for inserting, so it doesn't always point to a cell with cell.key == key
    fn find_cursor(&self, key: usize) -> io::Result<Cursor> {
        let mut page_num = self.root;
        let mut node = self.pager.get_node(page_num)?;
        while let NodeMut::InternalNode(ref mut internal) = node {
            page_num = internal.find(key);
            let page = self.pager.get_page(page_num)?;
            node = page.page_header_mut().node_mut();
        }
        let leaf = node.leaf().unwrap();
        let cell_num = leaf.find(key, self.entry_size);
        Ok(self.cursor(page_num, cell_num))
    }

    pub fn insert(&mut self, key: usize, value: &[u8]) -> io::Result<()> {
        let entry_size = self.entry_size;
        let max_leaf_cells = self.max_leaf_cells;
        let cursor = self.find_cursor(key)?;
        let leaf = cursor.leaf(self)?;
        if cursor.cell_num < leaf.num_cells
            && leaf.cell_unchecked(cursor.cell_num, entry_size).key == key
        {
            return Err(io::Error::other("Duplicate key"));
        }

        if leaf.num_cells == max_leaf_cells {
            if leaf.is_root() {
                self.split_root_leaf_and_insert(&cursor, key, value)?;
            } else {
                self.split_nonroot_leaf_and_insert(&cursor, key, value)?;
            }
        } else {
            leaf.insert_at_index(cursor.cell_num, key, value, entry_size);
        }
        Ok(())
    }

    /// Creates a new leaf node, copies cells from self to other until self has split_count cells
    /// Also it creates a new entry in the correct leaf
    /// Returns the newly created page, as well as the first key in the right node
    fn split_leaf_and_insert(
        &self,
        cursor: &Cursor,
        // leaf: &mut LeafNodeHeader,
        key: usize,
        value: &[u8],
        parent: PageNum,
        max_leaf_cells: usize,
    ) -> io::Result<(PageNum, usize)> {
        let leaf = cursor.leaf(self)?;

        let new_leaf_page_num = self.pager.get_free_page()?;
        let new_leaf_page = self.pager.get_page(new_leaf_page_num)?;
        let new_leaf = Page::initialize_leaf_node(new_leaf_page, parent);

        let entry_size = self.entry_size;
        // Copy half of the cells from old leaf, if new cell has to go into new leaf
        // copy it there and point the cursor
        let split_count = LeafNodeHeader::split_count(max_leaf_cells);
        for i in split_count..max_leaf_cells {
            let new_node_index = i - split_count;
            let cell = new_leaf.cell_mut_unchecked(new_node_index, entry_size);
            let old_cell = leaf.cell_unchecked(i, entry_size);
            cell.clone_from(old_cell, entry_size);
        }
        // Set correct metadata before possible inserting into the old leaf
        leaf.parent_ptr = parent;
        leaf.num_cells = split_count;
        new_leaf.parent_ptr = parent;
        new_leaf.num_cells = max_leaf_cells - split_count;
        if cursor.cell_num < split_count {
            leaf.insert_at_index(cursor.cell_num, key, value, entry_size);
        } else {
            new_leaf.insert(key, value, entry_size);
        }
        let split_key = new_leaf.cell_unchecked(0, entry_size).key;
        Ok((new_leaf_page_num, split_key))
    }

    fn split_root_leaf_and_insert(
        &mut self,
        cursor: &Cursor,
        key: usize,
        value: &[u8],
    ) -> io::Result<()> {
        let new_internal_page_num = self.pager.get_free_page()?;
        self.set_root(new_internal_page_num)?;

        let max_leaf_cells = self.max_leaf_cells;
        // Split children into two new leaf nodes
        let (new_leaf_page_num, split_key) =
            self.split_leaf_and_insert(cursor, key, value, new_internal_page_num, max_leaf_cells)?;

        let new_internal_page = self.pager.get_page(new_internal_page_num)?;
        Page::initialize_internal_node(
            new_internal_page,
            PageNum::NULL,
            split_key,
            cursor.page_num,
            new_leaf_page_num,
        );
        // println!("Internal {:?}: \n{:?}", new_internal_page_num, new_internal);
        // println!("Leaf {:?}: \n{:?}", old_leaf_page_num, leaf.debug(entry_size));
        // println!("Leaf {:?}: \n{:?}", new_leaf_page_num, new_leaf.debug(entry_size));
        Ok(())
    }

    fn split_nonroot_leaf_and_insert(
        &mut self,
        cursor: &Cursor,
        key: usize,
        value: &[u8],
    ) -> io::Result<()> {
        let leaf = cursor.leaf(self)?;
        let max_leaf_cells = self.max_leaf_cells;
        let parent_page_num = leaf.parent_ptr;
        let parent = self
            .pager
            .get_node(parent_page_num)?
            .internal()
            .expect("Parent can't be leaf node");
        if parent.num_keys == INTERNAL_NODE_CELL_COUNT {
            if parent.is_root() {
                let (new_leaf_page_num, leaf_split_key) = self.split_leaf_and_insert(
                    cursor,
                    key,
                    value,
                    parent_page_num,
                    max_leaf_cells,
                )?;
                let new_root_page_num = self.pager.get_free_page()?;
                let (new_internal_page_num, internal_split_key) = self.split_internal_and_insert(
                    parent,
                    leaf_split_key,
                    new_leaf_page_num,
                    new_root_page_num,
                )?;
                let new_root_page = self.pager.get_page(new_root_page_num)?;
                let _new_root = Page::initialize_internal_node(
                    new_root_page,
                    PageNum::NULL,
                    internal_split_key,
                    parent_page_num,
                    new_internal_page_num,
                );

                // println!("Inserting {}: {:?}", leaf_split_key, new_leaf_page_num);
                // println!("New root {:?}:\n{:?}", new_root_page_num, _new_root);
                // println!("Left {:?}:\n{:?}", parent_page_num, parent);
                // let right_internal_page = self.pager.get_page(new_internal_page_num)?;
                // let right_internal = right_internal_page
                //     .page_header_mut()
                //     .node_mut()
                //     .internal()
                //     .unwrap();
                // println!("Right {:?}:\n{:?}", new_internal_page_num, right_internal);
                println!("Table after split");
                self.set_root(new_root_page_num)?;
                debug_table(self)?;
                Ok(())
            } else {
                unimplemented!("Don't know how to recursively insert to internal");
            }
        } else {
            let (new_leaf_page_num, split_key) =
                self.split_leaf_and_insert(cursor, key, value, parent_page_num, max_leaf_cells)?;
            parent.insert(split_key, new_leaf_page_num);
            Ok(())
        }
    }

    /// Creates a new internal node, copies cells from self to other until self has split_count cells
    /// Also it creates a new entry in the correct internal node and initializes the parent_ptr
    /// field on the child
    /// Returns the newly created page, as well as the first key in the right node
    fn split_internal_and_insert(
        &self,
        internal: &mut InternalNodeHeader,
        key: usize,
        ptr: PageNum,
        parent: PageNum,
    ) -> io::Result<(PageNum, usize)> {
        // println!("Old internal\n{:?}", internal);
        let new_internal_page_num = self.pager.get_free_page()?;
        let new_internal_page = self.pager.get_page(new_internal_page_num)?;
        let new_internal = Page::initialize_empty_internal_node(new_internal_page, parent);

        let index = internal.find_index(key);
        // Copy half of the cells from old internal
        const SPLIT_COUNT: usize = INTERNAL_NODE_CELL_COUNT.div_ceil(2);
        const REST: usize = INTERNAL_NODE_CELL_COUNT - SPLIT_COUNT;
        for i in SPLIT_COUNT..INTERNAL_NODE_CELL_COUNT {
            let new_node_index = i - SPLIT_COUNT;
            let cell = new_internal.cell_mut_unchecked(new_node_index);
            let old_cell = internal.cell_unchecked(i);
            cell.clone_from(old_cell);
            let ptr = old_cell.ptr;
            let node = self.pager.get_node(ptr)?;
            match node {
                NodeMut::InternalNode(internal) => internal.parent_ptr = new_internal_page_num,
                NodeMut::LeafNode(leaf) => leaf.parent_ptr = new_internal_page_num,
            }
        }
        new_internal.num_keys = REST;
        new_internal.parent_ptr = parent;
        new_internal.right_child = internal.right_child;
        let node = self.pager.get_node(internal.right_child)?;
        match node {
            NodeMut::InternalNode(internal) => internal.parent_ptr = new_internal_page_num,
            NodeMut::LeafNode(leaf) => leaf.parent_ptr = new_internal_page_num,
        }

        internal.num_keys = SPLIT_COUNT;
        internal.parent_ptr = parent;
        let last_child = internal.cell_unchecked(SPLIT_COUNT - 1);
        let split_key = last_child.key;
        internal.right_child = last_child.ptr;
        internal.num_keys -= 1;

        if index < SPLIT_COUNT {
            internal.insert(key, ptr);
        } else {
            new_internal.insert(key, ptr);
            // Change the parent if we insert into the new one
            let node = self.pager.get_node(ptr)?;
            match node {
                NodeMut::LeafNode(leaf) => leaf.parent_ptr = new_internal_page_num,
                NodeMut::InternalNode(internal) => internal.parent_ptr = new_internal_page_num,
            }
        }

        Ok((new_internal_page_num, split_key))
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        self.pager.flush().expect("Failed to flush pager");
    }
}
