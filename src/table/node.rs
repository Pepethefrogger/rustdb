use crate::table::{internal::InternalNodeHeader, leaf::LeafNodeHeader};

#[repr(u8)]
pub enum NodeType {
    InternalNode = 0,
    LeafNode = 1,
}

pub enum Node<'page> {
    InternalNode(&'page InternalNodeHeader<'page>),
    LeafNode(&'page LeafNodeHeader<'page>),
}

impl<'page> Node<'page> {
    pub fn internal(self) -> Option<&'page InternalNodeHeader<'page>> {
        match self {
            Self::InternalNode(internal) => Some(internal),
            _ => None,
        }
    }

    pub fn leaf(self) -> Option<&'page LeafNodeHeader<'page>> {
        match self {
            Self::LeafNode(leaf) => Some(leaf),
            _ => None,
        }
    }
}

pub enum NodeMut<'page> {
    InternalNode(&'page mut InternalNodeHeader<'page>),
    LeafNode(&'page mut LeafNodeHeader<'page>),
}

impl<'page> NodeMut<'page> {
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
