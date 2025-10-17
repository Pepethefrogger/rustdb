use std::{
    fmt::Debug,
    fs,
    io::{self, Read, Seek, Write},
    ops::Add,
};

use crate::{pager::PageNum, query::Literal, table::data::Data};

#[derive(Clone, Copy, Default, Debug)]
pub struct Size {
    pub size: usize,
    pub aligned: usize,
}

impl Add for Size {
    type Output = Self;
    fn add(self, rhs: Self) -> Self::Output {
        Self {
            size: self.aligned + rhs.size,
            aligned: self.aligned + rhs.aligned,
        }
    }
}

impl Size {
    const ALIGN: usize = 8;
    const fn new(size: usize) -> Size {
        let aligned = (size + Self::ALIGN - 1) & !(Self::ALIGN - 1);
        Self { size, aligned }
    }
}

#[derive(Clone, Copy, Default, Debug)]
pub struct Layout {
    pub offset: usize,
    pub size: Size,
}

#[derive(Clone, Copy, Default, Debug, PartialEq)]
pub enum Type {
    String(usize),
    #[default]
    Int,
    Uint,
    Float,
}

impl Type {
    pub fn size(&self) -> Size {
        match self {
            Type::String(length) => Type::Uint.size() + Size::new(*length),
            Type::Int => Size::new(std::mem::size_of::<i64>()),
            Type::Uint => Size::new(std::mem::size_of::<u64>()),
            Type::Float => Size::new(std::mem::size_of::<f64>()),
        }
    }

    pub fn read<'a>(&self, buf: &'a [u8]) -> Literal<'a> {
        match self {
            Type::String(_) => {
                const USIZE_FIELD: usize = std::mem::size_of::<usize>();
                let length = usize::from_ne_bytes(buf[0..USIZE_FIELD].try_into().unwrap());
                let str = &buf[USIZE_FIELD..(USIZE_FIELD + length)];
                Literal::String(unsafe { str::from_utf8_unchecked(str) })
            }
            Type::Int => Literal::Int(isize::from_ne_bytes(
                buf.try_into().expect("Invalid size for parsing int"),
            )),
            Type::Uint => Literal::Uint(usize::from_ne_bytes(
                buf.try_into().expect("Invalid size for parsing uint"),
            )),
            Type::Float => Literal::Float(f64::from_ne_bytes(
                buf.try_into().expect("Invalid size for parsing float"),
            )),
        }
    }
}

const MAX_NAME_LENGTH: usize = 32;
#[derive(Clone, Copy, Default)]
pub struct Name {
    name_len: u8,
    name: [u8; MAX_NAME_LENGTH],
}

impl Name {
    pub fn new(str: &str) -> Self {
        let mut name = Name::default();
        name.write(str);
        name
    }

    pub fn str(&self) -> &str {
        let bytes = &self.name[..self.name_len as usize];
        unsafe { str::from_utf8_unchecked(bytes) }
    }

    pub fn write(&mut self, name: &str) {
        let len = name.len();
        self.name[..len].copy_from_slice(name.as_bytes());
        self.name_len = len as u8;
    }
}

impl Debug for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.str())
    }
}

#[derive(Clone, Copy, Default, Debug)]
pub struct Field {
    pub primary: bool,
    pub layout: Layout,
    pub name: Name,
    pub typ: Type,
}

impl Field {
    pub fn read<'a>(&self, buf: &'a Data) -> Literal<'a> {
        let field_buf = buf.read(self.layout);
        self.typ.read(field_buf)
    }

    pub fn write(&self, value: &Literal, buf: &mut Data) {
        let field_buf = buf.get_mut(self.layout);
        value.write_to(field_buf);
    }
}

const MAX_FIELDS: usize = 64;
pub struct Metadata {
    pub root: PageNum,
    pub num_fields: usize,
    pub fields: [Field; MAX_FIELDS],
}

impl Metadata {
    /// Create a new metadata struct with the corresponding fields.
    pub fn new(root: PageNum, primary_field: (&str, Type), fields: &[(&str, Type)]) -> Self {
        let mut metadata = Self {
            root,
            num_fields: fields.len() + 1,
            fields: [Field::default(); MAX_FIELDS],
        };
        let (name, typ) = primary_field;
        let primary = &mut metadata.fields[0];
        primary.primary = true;
        primary.name.write(name);
        primary.typ = typ;

        let mut offset = 0;
        fields
            .iter()
            .copied()
            .zip(metadata.fields[1..].iter_mut())
            .for_each(|((name, typ), f)| {
                f.name.write(name);
                f.typ = typ;
                let size = typ.size();
                f.layout = Layout { offset, size };
                offset += size.aligned;
            });
        metadata.fields[0].primary = true;
        metadata
    }
    #[inline]
    pub fn field(&self, name: &str) -> Option<&Field> {
        self.fields().find(|&field| field.name.str() == name)
    }
    #[inline]
    pub fn fields(&self) -> impl Iterator<Item = &Field> + Clone {
        self.fields.iter().take(self.num_fields)
    }
    #[inline]
    pub fn data_fields(&self) -> impl Iterator<Item = &Field> + Clone {
        self.fields().filter(|f| !f.primary)
    }
    #[inline]
    pub fn entry_size(&self) -> Size {
        self.fields().fold(Size::default(), |acc, field| {
            if field.primary {
                acc
            } else {
                acc + field.layout.size
            }
        })
    }
}

pub struct MetadataHandler {
    file: fs::File,
    pub metadata: Metadata,
}

impl MetadataHandler {
    const LENGTH: usize = std::mem::size_of::<Metadata>();
    pub fn new(file: fs::File, metadata: Metadata) -> Self {
        Self { file, metadata }
    }

    pub fn open(mut file: fs::File) -> io::Result<Self> {
        let mut buf = [0; Self::LENGTH];
        file.rewind()?;
        file.read_exact(&mut buf)?;
        let metadata = unsafe { std::mem::transmute::<[u8; Self::LENGTH], Metadata>(buf) };
        Ok(Self { file, metadata })
    }

    pub fn flush(&mut self) {
        let data = unsafe { std::mem::transmute::<&Metadata, &[u8; Self::LENGTH]>(&self.metadata) };
        self.file
            .set_len(data.len() as u64)
            .expect("Failed to set metadata length");
        self.file.rewind().expect("Failed to reset metadata fd");
        self.file.write_all(data).expect("Failed to write metadata");
        self.file.sync_data().expect("Failed to sync metadata");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field() {
        let mut field = Field::default();
        field.name.write("test");
        field.typ = Type::String(10);

        assert_eq!(field.name.str(), "test");
        assert_eq!(field.typ, Type::String(10));
    }

    #[test]
    fn test_id_field() {
        let data_name = "test";
        let metadata = Metadata::new(PageNum(0), ("id", Type::Uint), &[(data_name, Type::Uint)]);
        let data_field = metadata.field(data_name).unwrap();
        assert_eq!(data_field.layout.offset, 0);
    }
}
