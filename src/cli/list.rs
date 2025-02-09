use super::ReplResult;
use crate::{CmdExecutor, ReplContext, ReplDisplay, ReplMsg};
use clap::ArgMatches;
use clap::Parser;

#[derive(Debug, Parser)]
pub struct ListOps;

pub fn list(_args: ArgMatches, context: &mut ReplContext) -> ReplResult {
    let ret = ReplMsg::new(ListOps::new());

    Ok(context.send(ret.0, ret.1))
}

impl ListOps {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ListOps {
    fn default() -> Self {
        Self::new()
    }
}

impl CmdExecutor for ListOps {
    async fn execute<T: crate::Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.list().await?;
        df.display().await
    }
}
