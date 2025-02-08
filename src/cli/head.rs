use clap::{ArgMatches, Parser};

use crate::ReplContext;

use super::{ReplCommand, ReplResult};

#[derive(Parser, Debug)]
pub struct HeadOps {
    #[arg(short, long, help = "The name of the dataset")]
    pub name: String,

    #[arg(short, long, help = "Number of rows to show")]
    pub n: Option<usize>,
}

pub fn head(args: ArgMatches, context: &mut ReplContext) -> ReplResult {
    let name = args
        .get_one::<String>("name")
        .expect("Name is required")
        .to_owned();
    let n = args.get_one::<usize>("n").copied();

    let cmd: ReplCommand = HeadOps::new(name, n).into();

    context.send(cmd);

    Ok(None)
}

impl HeadOps {
    pub fn new(name: String, n: Option<usize>) -> Self {
        Self { name, n }
    }
}

impl From<HeadOps> for ReplCommand {
    fn from(value: HeadOps) -> Self {
        ReplCommand::Head(value)
    }
}
