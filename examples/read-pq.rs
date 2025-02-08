use std::fs::File;

use anyhow::Result;
use arrow::util::pretty::print_batches;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const PATH: &str = "assets/sample.parquet";

fn main() -> Result<()> {
    let file = File::open(PATH)?;
    let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(10)
        .build()?;

    let mut batches = Vec::new();

    for batch in parquet_reader {
        batches.push(batch?);
    }

    print_batches(&batches)?;

    Ok(())
}
