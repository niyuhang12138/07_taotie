use super::ReplResult;
use crate::{CmdExecutor, ReplContext, ReplDisplay, ReplMsg};
use clap::{ArgMatches, Parser};

#[derive(Parser, Debug)]
pub struct HeadOps {
    #[arg(help = "The name of the dataset")]
    pub name: String,

    #[arg(short, help = "Number of rows to show")]
    pub n: Option<usize>,
}

pub fn head(args: ArgMatches, context: &mut ReplContext) -> ReplResult {
    let name = args
        .get_one::<String>("name")
        .expect("Name is required")
        .to_owned();
    let n = args.get_one::<usize>("n").copied();

    let ret = ReplMsg::new(HeadOps::new(name, n));

    Ok(context.send(ret.0, ret.1))
}

impl HeadOps {
    pub fn new(name: String, n: Option<usize>) -> Self {
        Self { name, n }
    }
}

impl CmdExecutor for HeadOps {
    async fn execute<T: crate::Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.head(&self.name, self.n.unwrap_or(5)).await?;
        df.display().await
    }
}
