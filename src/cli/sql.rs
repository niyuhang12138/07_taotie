use clap::{ArgMatches, Parser};

use crate::{CmdExecutor, ReplContext, ReplDisplay, ReplMsg};

use super::ReplResult;

#[derive(Debug, Parser)]
pub struct SqlOps {
    #[arg(help = "SQL query to execute")]
    pub query: String,
}

pub fn head(args: ArgMatches, context: &mut ReplContext) -> ReplResult {
    let query = args
        .get_one::<String>("query")
        .expect("Query is required")
        .to_owned();

    let ret = ReplMsg::new(SqlOps::new(query));

    Ok(context.send(ret.0, ret.1))
}

impl SqlOps {
    pub fn new(query: String) -> Self {
        Self { query }
    }
}

impl CmdExecutor for SqlOps {
    async fn execute<T: crate::Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.sql(&self.query).await?;
        df.display().await
    }
}
