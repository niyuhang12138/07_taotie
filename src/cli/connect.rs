use clap::{ArgMatches, Parser};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;

use crate::{CmdExecutor, ReplContext, ReplMsg};

use super::ReplResult;

#[derive(Debug, Clone)]
pub enum DatasetConn {
    Postgres(String),
    CSv(FileOps),
    Parquet(String),
    NdJson(FileOps),
}

#[derive(Debug, Clone)]
pub struct FileOps {
    pub filename: String,
    pub ext: String,
    pub compression: FileCompressionType,
}

#[derive(Parser, Debug)]
pub struct ConnectOps {
    #[arg( value_parser = verify_conn, help = "Connection string to the dataset, could be postgres of local file (support: csv, parquet, json)")]
    pub conn: DatasetConn,

    #[arg(short, help = "If database, the name of the table")]
    pub table: Option<String>,

    #[arg(help = "The name of the dataset")]
    pub name: String,
}

fn verify_conn(s: &str) -> Result<DatasetConn, String> {
    let conn_str = s.to_string();

    if conn_str.starts_with("postgres://") {
        return Ok(DatasetConn::Postgres(conn_str));
    }

    if conn_str.ends_with(".parquet") {
        return Ok(DatasetConn::Parquet(conn_str));
    }

    let exts = conn_str.rsplit('.');
    let count = exts.clone().count() - 1;
    let mut exts = exts.take(count);
    let ext1 = exts.next();
    let ext2 = exts.next();
    match (ext1, ext2) {
        (Some(ext1), Some(ext2)) => {
            let compression = match ext1 {
                "gz" => FileCompressionType::GZIP,
                "bz2" => FileCompressionType::BZIP2,
                "xz" => FileCompressionType::XZ,
                "zstd" => FileCompressionType::ZSTD,
                v => return Err(format!("invalid compression type: {}", v)),
            };

            let opts = FileOps {
                filename: s.to_string(),
                ext: ext2.to_string(),
                compression,
            };

            match ext1 {
                "csv" => Ok(DatasetConn::CSv(opts)),
                "json" | "jsonl" | "ndjson" => Ok(DatasetConn::NdJson(opts)),
                v => Err(format!("Invalid file extension: {v}")),
            }
        }
        (Some(ext1), None) => {
            let opts = FileOps {
                filename: s.to_string(),
                ext: ext1.to_string(),
                compression: FileCompressionType::UNCOMPRESSED,
            };

            match ext1 {
                "csv" => Ok(DatasetConn::CSv(opts)),
                "json" | "jsonl" | "ndjson" => Ok(DatasetConn::NdJson(opts)),
                v => Err(format!("Invalid file extension: {v}")),
            }
        }
        _ => Err(format!("Invalid Connection string: {s}")),
    }
}

pub fn connect(args: ArgMatches, context: &mut ReplContext) -> ReplResult {
    let conn = args
        .get_one::<DatasetConn>("conn")
        .expect("Connection string is required")
        .to_owned();

    let table = args.get_one::<String>("table").map(|s| s.to_owned());
    let name = args
        .get_one::<String>("name")
        .expect("Name is required")
        .to_owned();

    let ret = ReplMsg::new(ConnectOps::new(conn, table, name));

    Ok(context.send(ret.0, ret.1))
}

impl ConnectOps {
    pub fn new(conn: DatasetConn, table: Option<String>, name: String) -> Self {
        Self { conn, table, name }
    }
}

impl CmdExecutor for ConnectOps {
    async fn execute<T: crate::Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        backend.connect(&self).await?;
        Ok(format!("Connected to dataset: {}", self.name))
    }
}
