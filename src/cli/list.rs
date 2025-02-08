use clap::ArgMatches;

use crate::ReplContext;

use super::{ReplCommand, ReplResult};

pub fn list(_args: ArgMatches, context: &mut ReplContext) -> ReplResult {
    let cmd: ReplCommand = ReplCommand::List;

    context.send(cmd);

    Ok(None)
}
