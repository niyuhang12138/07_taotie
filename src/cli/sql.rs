use clap::{ArgMatches, Parser};

use crate::ReplContext;

use super::{ReplCommand, ReplResult};

#[derive(Debug, Parser)]
pub struct SqlOps {
    #[arg(short, long, help = "SQL query to execute")]
    pub query: String,
}

pub fn head(args: ArgMatches, context: &mut ReplContext) -> ReplResult {
    let query = args
        .get_one::<String>("query")
        .expect("Query is required")
        .to_owned();

    let cmd: ReplCommand = SqlOps::new(query).into();

    context.send(cmd);

    Ok(None)
}

impl SqlOps {
    pub fn new(query: String) -> Self {
        Self { query }
    }
}

impl From<SqlOps> for ReplCommand {
    fn from(value: SqlOps) -> Self {
        ReplCommand::Sql(value)
    }
}
