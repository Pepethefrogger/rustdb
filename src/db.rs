use std::{
    collections::HashMap,
    fs::OpenOptions,
    io,
    path::{Path, PathBuf},
};

use crate::{
    query::{Literal, Operation, Statement},
    table::{Table, data::Data, metadata::Type},
    utils::EntryVector,
};

pub enum OperationResult<'a> {
    Empty,
    Entries(EntryVector<Literal<'a>>),
    Count(usize),
}

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

    pub fn create_table(
        &mut self,
        name: &str,
        primary_field: (&str, Type),
        fields: &[(&str, Type)],
    ) -> io::Result<()> {
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
        let table = Table::create(data_file, metadata_file, primary_field, fields)?;
        self.tables.insert(name.to_owned(), table);
        Ok(())
    }

    pub fn execute(&mut self, statement: Statement) -> io::Result<OperationResult<'_>> {
        let operation = statement.operation;
        let table_id = operation.table();
        let table = self.table(table_id)?;
        match operation {
            Operation::Select { columns, .. } => {
                let fields: Option<Vec<_>> = columns
                    .iter()
                    .map(|i| table.metadata.metadata.field(i))
                    .collect();
                let fields = fields.ok_or(io::Error::other("Field not found"))?;

                let mut entries = EntryVector::<Literal>::new(fields.len());

                let mut cursor = table.find_cursor(0)?;

                // TODO: Implement a better way to check if db is empty
                if cursor.leaf(table)?.num_cells == 0 {
                    return Ok(OperationResult::Entries(entries));
                }
                if let Some(skip) = statement.skip {
                    for _ in 0..skip {
                        if !cursor.advance(table)? {
                            return Ok(OperationResult::Entries(entries));
                        }
                    }
                }

                let limit = statement.limit.unwrap_or(usize::MAX);
                let mut selected = 0usize;
                loop {
                    if selected >= limit {
                        break;
                    }
                    let data = cursor.value(table)?;
                    let literals = fields.iter().map(|f| {
                        if f.primary {
                            let id = cursor.cell(table).unwrap().key;
                            Literal::Uint(id)
                        } else {
                            f.read(data)
                        }
                    });
                    selected += 1;
                    entries.push(literals);
                    if !cursor.advance(table)? {
                        break;
                    }
                }
                Ok(OperationResult::Entries(entries))
            }
            Operation::Insert { values, .. } => {
                let fields: Option<Vec<_>> = values
                    .iter()
                    .map(|(i, l)| {
                        let field = table.metadata.metadata.field(i);
                        field.map(|f| (f, l))
                    })
                    .collect();
                let fields = fields.ok_or(io::Error::other("Field not found"))?;
                let mut value = vec![0u8; table.entry_size.size];
                let data = Data::new_mut(&mut value);

                let mut id: Option<usize> = None;
                for (f, l) in fields {
                    if f.primary {
                        if let Literal::Uint(n) = l {
                            id.replace(*n);
                        } else {
                            unimplemented!("Primary field has to be uint")
                        }
                    } else {
                        f.write(l, data);
                    }
                }

                let id = id.ok_or(io::Error::other(
                    "You have to provide a value for the primary field",
                ))?;
                table.insert(id, &value)?;
                Ok(OperationResult::Empty)
            }
            Operation::Update { values, .. } => {
                let fields: Option<Vec<_>> = values
                    .iter()
                    .map(|(i, l)| {
                        let field = table.metadata.metadata.field(i);
                        field.map(|f| (f, l))
                    })
                    .collect();
                let fields = fields.ok_or(io::Error::other("Field not found"))?;

                let mut cursor = table.find_cursor(0)?;
                // TODO: Implement a better way to check if db is empty
                if cursor.leaf(table)?.num_cells == 0 {
                    return Ok(OperationResult::Count(0));
                }
                if let Some(skip) = statement.skip {
                    for _ in 0..skip {
                        if !cursor.advance(table)? {
                            return Ok(OperationResult::Count(0));
                        }
                    }
                }

                let limit = statement.limit.unwrap_or(usize::MAX);
                let mut updated = 0usize;
                loop {
                    if updated >= limit {
                        break;
                    }
                    let data = cursor.value(table)?;
                    for (field, literal) in fields.iter() {
                        field.write(literal, data);
                    }
                    updated += 1;
                    if !cursor.advance(table)? {
                        break;
                    }
                }
                Ok(OperationResult::Count(updated))
            }
            Operation::Delete { .. } => {
                unimplemented!("Don't know how to delete entries")
            }
        }
    }
}
