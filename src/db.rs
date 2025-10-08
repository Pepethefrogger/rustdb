use std::{
    collections::HashMap,
    fs::OpenOptions,
    io,
    path::{Path, PathBuf},
};

use crate::table::{Table, metadata::Type};

pub struct DB<'a> {
    dir: &'a Path,
    tables: HashMap<String, Table>,
}

#[inline]
fn table_data_path(name: &str) -> PathBuf {
    Path::new(name).with_extension("tbl")
}

#[inline]
fn table_metadata_path(name: &str) -> PathBuf {
    Path::new(name).with_extension("mt")
}

#[inline]
fn table_paths(name: &str) -> (PathBuf, PathBuf) {
    (table_data_path(name), table_metadata_path(name))
}

impl<'a> DB<'a> {
    pub fn new(dir: &'a Path) -> Self {
        Self {
            dir,
            tables: HashMap::new(),
        }
    }

    pub fn table(&mut self, name: &str) -> io::Result<&mut Table> {
        if !self.tables.contains_key(name) {
            let (data, metadata) = table_paths(name);
            let data_path = self.dir.join(data);
            let metadata_path = self.dir.join(metadata);

            let mut open_options = OpenOptions::new();
            open_options.read(true).write(true).create(false);

            let data_file = open_options.clone().open(data_path)?;
            let metadata_file = open_options.open(metadata_path)?;
            let new_table = Table::open(data_file, metadata_file)?;
            self.tables.insert(name.to_owned(), new_table);
        }
        Ok(self.tables.get_mut(name).unwrap())
    }

    pub fn create_table(&mut self, name: &str, fields: &[(&str, Type)]) -> io::Result<()> {
        if self.tables.contains_key(name) {
            return Err(io::Error::other("Table already exists"));
        }

        let (data, metadata) = table_paths(name);
        let data_path = self.dir.join(data);
        let metadata_path = self.dir.join(metadata);
        let mut open_options = OpenOptions::new();
        open_options.read(true).write(true).create_new(true);

        let data_file = open_options.clone().open(data_path)?;
        let metadata_file = open_options.open(metadata_path)?;
        let table = Table::create(data_file, metadata_file, fields)?;
        self.tables.insert(name.to_owned(), table);
        Ok(())
    }
}
