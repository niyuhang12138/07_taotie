use std::{fs::File, io::Seek, sync::Arc};

use arrow::{
    csv::{self, reader::Format},
    util::pretty::print_batches,
};

const PATH: &str = "assets/person.csv";

fn main() {
    let mut file = File::open(PATH).unwrap();
    let format = Format::default().with_header(true);
    let (schema, _) = format.infer_schema(&mut file, Some(100)).unwrap();
    file.rewind().unwrap();

    let builder = csv::ReaderBuilder::new(Arc::new(schema)).with_format(format);
    let mut csv = builder.build(file).unwrap();
    let batch = csv.next().unwrap().unwrap();
    print_batches(&[batch]).unwrap();
}
