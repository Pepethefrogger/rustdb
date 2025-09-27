use std::{fs, io, marker::PhantomData, slice};

use derivative::Derivative;

use crate::pager::{leaf_cells_max, NodeMut, Page, PageHeader, PageNum, Pager};

pub struct InternalNodeCell<'page> {
    pub key: usize,
    pub ptr: PageNum,
    phantom: PhantomData<&'page mut Page>,
}
pub const INTERNAL_NODE_CELL_SIZE: usize = std::mem::size_of::<InternalNodeCell>();


pub const LEAF_NODE_CELL_KEY_SIZE: usize = std::mem::size_of::<LeafNodeCell>();
pub struct LeafNodeCell<'page> {
    pub key: usize,
    phantom: PhantomData<&'page mut Page>,
}

impl<'page> LeafNodeCell<'page> {
    pub fn initialize(&mut self, key: usize, value: &[u8], size: Size) {
        self.key = key;
        self.data_mut(size).copy_from_slice(value);
    }

    pub fn data(&self, size: Size) -> &'page [u8] {
        let ptr = unsafe { (self as *const Self).add(1) };
        unsafe { slice::from_raw_parts(ptr as *const u8, size.size) }
    }

    pub fn data_mut(&mut self, size: Size) -> &'page mut [u8] {
        let ptr = unsafe { (self as *mut Self).add(1) };
        unsafe { slice::from_raw_parts_mut(ptr as *mut u8, size.size) }
    }

    pub fn clone_from(&mut self, other: &Self, size: Size) {
        self.initialize(other.key, other.data(size), size);
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Cursor<'table> {
    #[derivative(Debug="ignore")]
    pub table: &'table mut Table,
    pub page_num: PageNum,
    pub cell_num: usize,
}

impl<'table> Cursor<'table> {
    /// Return the node that this cursor points to
    fn node(&self) -> io::Result<NodeMut<'table>> {
        let page = self.table.pager.get_page(self.page_num)?;
        let node = page.page_header_mut().node_mut();
        Ok(unsafe { std::mem::transmute(node) })
    }

    /// Return the header that this cursor points to
    fn header(&self) -> io::Result<&'table mut PageHeader<'table>> {
        let header = self.table.pager.get_page(self.page_num)?.page_header_mut();
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

    // Resets the cursor to point at the root node
    pub fn reset(&mut self) {
        self.page_num = self.table.get_root();
    }

    /// Creates an entry with the key, you can use the cursor to replace the value
    pub fn make_entry(&mut self, key: usize) -> io::Result<()> {
        let entry_size = self.table.entry_size;
        let max_leaf_cells = self.table.max_leaf_cells;
        self.find(key)?;
        let header = self.header()?;
        let node = header.node_mut();
        match node {
            NodeMut::InternalNode(_) => unreachable!("A find can't end in an internal node"),
            NodeMut::LeafNode(leaf) => {
                if self.cell_num < leaf.num_cells && leaf.cell(self.cell_num, entry_size).key == key {
                    return Err(io::Error::new(io::ErrorKind::Other, "Duplicate key"));
                }

                if leaf.num_cells == max_leaf_cells {
                    if leaf.is_root() {
                        let new_internal_page_num = self.table.pager.get_free_page()?;
                        self.table.set_root(new_internal_page_num)?; // Set new root node

                        // Split children into two new leaf nodes
                        let split_count = (max_leaf_cells + 1) / 2;
                        // println!("Splitting at {} and inserting key {}", split_count, key);

                        // Create new leaf children and reuse previous leaf
                        let new_leaf_page_num = self.table.pager.get_free_page()?;
                        let new_leaf_page = self.table.pager.get_page(new_leaf_page_num)?;
                        Page::initialize_leaf_node(new_leaf_page, new_internal_page_num);
                        let new_leaf = new_leaf_page.page_header_mut().node_mut().leaf().expect("Just initialized this as a leaf node");

                        let entry_size = self.table.entry_size;
                        // Copy half of the cells from old leaf, if new cell has to go into new leaf
                        // copy it there and point the cursor
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
                                let old_cell = leaf.cell(i-1, entry_size);
                                cell.clone_from(old_cell, entry_size);
                            }
                        }
                        // Set correct metadata before possible inserting into the old leaf
                        let old_leaf_page_num = self.page_num;
                        leaf.parent_ptr = new_internal_page_num;
                        leaf.num_cells = split_count;
                        leaf.parent_ptr = new_internal_page_num;
                        new_leaf.num_cells = max_leaf_cells - split_count;
                        if self.cell_num < split_count {
                            // Cursor is already pointing to leaf and cell
                            leaf.make_space_for_key(self.cell_num, entry_size, max_leaf_cells);
                        } else {
                            new_leaf.num_cells += 1;
                            self.page_num = new_leaf_page_num;
                            self.cell_num = self.cell_num - split_count;
                        }

                        let new_internal_page = self.table.pager.get_page(new_internal_page_num)?;
                        Page::initialize_internal_node(new_internal_page, PageNum::NULL, split_count, old_leaf_page_num, new_leaf_page_num);
                        // println!("Internal {:?}: \n{:?}", new_internal_page_num, new_internal);
                        // println!("Leaf {:?}: \n{:?}", old_leaf_page_num, leaf.debug(entry_size));
                        // println!("Leaf {:?}: \n{:?}", new_leaf_page_num, new_leaf.debug(entry_size));
                    } else {
                        unimplemented!("Don't know how to add keys to internal")
                    }
                } else {
                    leaf.make_space_for_key(self.cell_num, entry_size, self.table.max_leaf_cells);
                    let cell = leaf.cell_mut(self.cell_num, entry_size);
                    cell.key = key;
                }
                Ok(())
            },
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
        Self{size, aligned}
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
        Ok(Self{pager: pager, root: root, entry_size, max_leaf_cells})
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

    pub fn cursor(&mut self) -> Cursor<'_> {
        let root = self.root;
        Cursor{ table: self, page_num: root, cell_num: 0 }
    }

    pub fn insert(&mut self, key: usize, value: &[u8]) -> io::Result<()> {
        let entry_size = self.entry_size;
        let mut cursor = self.cursor();
        cursor.make_entry(key)?;
        let cell = cursor.cell()?;
        cell.key = key;
        cell.data_mut(entry_size).copy_from_slice(value);
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
