pub mod connect;
pub mod describe;
pub mod head;
pub mod list;
pub mod sql;

use clap::Parser;

pub type ReplResult = Result<Option<String>, reedline_repl_rs::Error>;

#[derive(Parser, Debug)]
pub enum ReplCommand {
    #[command(
        name = "connect",
        about = "Connect to a dataset and register it to Taotie"
    )]
    Connect(connect::ConnectOps),

    #[command(name = "list", about = "List all registered datasets")]
    List,

    #[command(name = "describe", about = "Describe a dataset")]
    Describe(describe::DescribeOps),

    #[command(name = "head", about = "Show first few rows of a dataset")]
    Head(head::HeadOps),

    #[command(name = "sql", about = "Query a dataset using given SQL")]
    Sql(sql::SqlOps),
}
