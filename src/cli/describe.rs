use clap::{ArgMatches, Parser};

use crate::{CmdExecutor, ReplContext, ReplDisplay, ReplMsg};

use super::ReplResult;

#[derive(Parser, Debug)]
pub struct DescribeOps {
    #[arg(help = "The name of the dataset")]
    pub name: String,
}

pub fn describe(args: ArgMatches, context: &mut ReplContext) -> ReplResult {
    let name = args
        .get_one::<String>("name")
        .expect("Name is required")
        .to_owned();

    let ret = ReplMsg::new(DescribeOps::new(name));

    Ok(context.send(ret.0, ret.1))
}

impl DescribeOps {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl CmdExecutor for DescribeOps {
    async fn execute<T: crate::Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.describe(&self.name).await?;
        df.display().await
    }
}
