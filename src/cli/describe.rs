use clap::{ArgMatches, Parser};

use crate::ReplContext;

use super::{ReplCommand, ReplResult};

#[derive(Parser, Debug)]
pub struct DescribeOps {
    #[arg(short, long, help = "The name of the dataset")]
    pub name: String,
}

pub fn describe(args: ArgMatches, context: &mut ReplContext) -> ReplResult {
    let name = args
        .get_one::<String>("name")
        .expect("Name is required")
        .to_owned();

    let cmd: ReplCommand = DescribeOps::new(name).into();

    context.send(cmd);

    Ok(None)
}

impl DescribeOps {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl From<DescribeOps> for ReplCommand {
    fn from(value: DescribeOps) -> Self {
        ReplCommand::Describe(value)
    }
}
