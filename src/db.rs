use crate::{
    expression::Expression,
    query::{Literal, Operation, Statement},
    table::{
        Table, TableError,
        data::Data,
        metadata::{Field, Type},
    },
    utils::{entry_vec::EntryVector, range::Range},
};
use std::{
    collections::HashMap,
    fs::OpenOptions,
    io,
    mem::MaybeUninit,
    path::{Path, PathBuf},
};

pub enum OperationResult<'a> {
    Ok,
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

#[derive(Debug)]
pub enum DBError {
    FailedToOpenTable,
    TableNotExists,
    TableAlreadyExists,
    TableError(TableError),
}

impl From<TableError> for DBError {
    fn from(value: TableError) -> Self {
        Self::TableError(value)
    }
}

pub type DBResult<T> = Result<T, DBError>;

impl<'a> DB<'a> {
    pub fn new(dir: &'a Path) -> Self {
        Self {
            dir,
            tables: HashMap::new(),
        }
    }

    pub fn table(&mut self, name: &str) -> DBResult<&mut Table> {
        if !self.tables.contains_key(name) {
            let (data, metadata) = table_paths(name);
            let data_path = self.dir.join(data);
            let metadata_path = self.dir.join(metadata);

            if !data_path.exists() || !metadata_path.exists() {
                return Err(DBError::TableNotExists);
            }

            let mut open_options = OpenOptions::new();
            open_options.read(true).write(true).create(false);

            let data_file = open_options
                .clone()
                .open(data_path)
                .expect("Failed to open table's data");
            let metadata_file = open_options
                .open(metadata_path)
                .expect("Failed to open table's metadata");
            let new_table = Table::open(data_file, metadata_file).expect("Failed to create table");
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

    pub fn execute<'b>(&'b mut self, statement: Statement<'b>) -> DBResult<OperationResult<'b>> {
        let operation = statement.operation;
        let table_id = operation.table();
        let table = self.table(table_id)?;
        match operation {
            Operation::Select { columns, .. } => {
                // TODO: Make sure that these fields exists when parsing
                let fields: Vec<_> = columns
                    .iter()
                    .map(|i| table.metadata.metadata.field(i).unwrap())
                    .collect();

                let mut entries = EntryVector::<Literal>::new(fields.len());

                let cursor = FilteringCursor::from_options(
                    table,
                    statement.limit,
                    statement.skip,
                    statement.wher.map(|x| *x),
                );

                cursor.iter().for_each(|(id, data)| {
                    let literals = fields.iter().map(|f| {
                        if f.primary {
                            Literal::Uint(id)
                        } else {
                            f.read(data)
                        }
                    });
                    entries.push(literals);
                });
                Ok(OperationResult::Entries(entries))
            }
            Operation::Insert { values, .. } => {
                let fields: Vec<_> = values
                    .iter()
                    .map(|(i, l)| {
                        let f = table.metadata.metadata.field(i).unwrap();
                        (f, l)
                    })
                    .collect();
                let mut value = vec![0u8; table.entry_size.size];
                let data = Data::new_mut(&mut value);

                let mut id: MaybeUninit<usize> = MaybeUninit::uninit();
                for (f, l) in fields {
                    if f.primary {
                        if let Literal::Uint(n) = l {
                            id.write(*n);
                        } else {
                            unimplemented!("Only uint ids are supported")
                        }
                    } else {
                        f.write(l, data);
                    }
                }
                let id = unsafe { id.assume_init() };

                table.insert(id, &value)?;
                Ok(OperationResult::Ok)
            }
            Operation::Update { values, .. } => {
                let fields: Vec<_> = values
                    .iter()
                    .map(|(i, l)| {
                        let f = table.metadata.metadata.field(i).unwrap();
                        (f, l)
                    })
                    .collect();

                let cursor = FilteringCursor::from_options(
                    table,
                    statement.limit,
                    statement.skip,
                    statement.wher.map(|x| *x),
                );

                let mut count = 0usize;
                cursor.iter().for_each(|(_, data)| {
                    for (field, literal) in fields.iter() {
                        field.write(literal, data);
                    }
                    count += 1;
                });
                Ok(OperationResult::Count(count))
            }
            Operation::Delete { .. } => {
                unimplemented!("Don't know how to delete entries")
            }
        }
    }
}

pub struct FilteringCursor<'a> {
    table: &'a Table,
    limit: usize,
    skip: usize,
    fields: Vec<Field>,
    expression: Expression<'a>,
    range: Range<Literal<'a>>,
}

impl<'a> FilteringCursor<'a> {
    pub fn new(
        table: &'a Table,
        limit: usize,
        skip: usize,
        mut expression: Expression<'a>,
    ) -> Self {
        let index = table
            .metadata
            .metadata
            .fields()
            .find(|f| f.primary)
            .expect("Primary field not found");
        let index_name = index.name.str();
        let range = expression.extract_index(index_name);
        let field_names = expression.fields();
        let fields: Vec<_> = field_names
            .iter()
            .map(|f| *table.metadata.metadata.field(f).unwrap())
            .collect();
        Self {
            table,
            limit,
            skip,
            fields,
            expression,
            range,
        }
    }

    pub fn from_options(
        table: &'a Table,
        limit: Option<usize>,
        skip: Option<usize>,
        expression: Option<Expression<'a>>,
    ) -> Self {
        Self::new(
            table,
            limit.unwrap_or(usize::MAX),
            skip.unwrap_or(0),
            expression.unwrap_or(Expression::Empty),
        )
    }

    fn evaluate_entry(&self, index: usize, data: &Data) -> bool {
        let mut iter = self.fields.iter().map(|f| {
            if f.primary {
                Literal::Uint(index)
            } else {
                f.read(data)
            }
        });
        self.expression.eval(&mut iter)
    }

    pub fn iter(&self) -> impl Iterator<Item = (usize, &'a mut Data)> {
        self.range
            .iter()
            .flat_map(|r| {
                let cursor = match r.start() {
                    Some(Literal::Uint(id)) => self.table.find_cursor(id),
                    None => self.table.min_cursor(),
                    _ => unimplemented!("Only uint can be used as id"),
                };
                cursor
                    .into_iter(self.table)
                    .skip_while(|&(index, _)| !r.value_past_start(&index.into()))
                    .take_while(|&(index, _)| r.value_before_end(&index.into()))
                    .filter(|&(index, ref data)| self.evaluate_entry(index, data))
            })
            .skip(self.skip)
            .take(self.limit)
    }
}
