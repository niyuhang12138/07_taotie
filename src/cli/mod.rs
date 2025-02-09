pub mod connect;
pub mod describe;
pub mod head;
pub mod list;
pub mod schema;
pub mod sql;

use clap::Parser;
use enum_dispatch::enum_dispatch;

pub type ReplResult = Result<Option<String>, reedline_repl_rs::Error>;

#[derive(Parser, Debug)]
#[enum_dispatch(CmdExecutor)]
pub enum ReplCommand {
    #[command(
        name = "connect",
        about = "Connect to a dataset and register it to Taotie"
    )]
    Connect(connect::ConnectOps),

    #[command(name = "list", about = "List all registered datasets")]
    List(list::ListOps),

    #[command(name = "schema", about = "Show schema of a dataset")]
    Schema(schema::SchemaOps),

    #[command(name = "describe", about = "Describe a dataset")]
    Describe(describe::DescribeOps),

    #[command(name = "head", about = "Show first few rows of a dataset")]
    Head(head::HeadOps),

    #[command(name = "sql", about = "Query a dataset using given SQL")]
    Sql(sql::SqlOps),
}
