use std::{fs, sync::Arc};

use anyhow::Result;
use arrow::{
    csv,
    datatypes::{DataType, Field, Schema},
    util::pretty::print_batches,
};

const FILE: &str = "assets/person.csv";

fn main() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("Name", DataType::Utf8, false),
        Field::new("Age", DataType::Utf8, false),
        Field::new("Address", DataType::Utf8, false),
    ]);

    let file = fs::File::open(FILE)?;

    let mut csv = csv::ReaderBuilder::new(Arc::new(schema)).build(file)?;

    let batch = csv.next().unwrap()?;
    print_batches(&[batch])?;

    Ok(())
}
