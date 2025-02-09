use clap::{ArgMatches, Parser};

use crate::{CmdExecutor, ReplContext, ReplMsg};

use super::ReplResult;

#[derive(Debug, Clone)]
pub enum DatasetConn {
    Postgres(String),
    CSv(String),
    Parquet(String),
    NdJson(String),
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
    if s.starts_with("postgres://") {
        Ok(DatasetConn::Postgres(s.to_string()))
    } else if s.ends_with(".csv") {
        Ok(DatasetConn::CSv(s.to_string()))
    } else if s.ends_with(".parquet") {
        Ok(DatasetConn::Parquet(s.to_string()))
    } else if s.ends_with(".ndjson") {
        Ok(DatasetConn::NdJson(s.to_string()))
    } else {
        Err(format!("Invalid connection string: {}", s))
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
