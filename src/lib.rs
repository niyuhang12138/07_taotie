mod backend;
mod cli;

use backend::DataFusionBackend;
use cli::{connect, describe, head, list, schema, sql};
use crossbeam_channel as mpsc;
use enum_dispatch::enum_dispatch;
use reedline_repl_rs::CallBackMap;
use std::{
    ops::{Deref, DerefMut},
    thread,
};
use tokio::runtime::Runtime;

pub use cli::ReplCommand;

#[enum_dispatch]
trait CmdExecutor {
    async fn execute<T: Backend>(self, backend: &mut T) -> anyhow::Result<String>;
}

trait Backend {
    type DataFrame: ReplDisplay;
    async fn connect(&mut self, opts: &cli::connect::ConnectOps) -> anyhow::Result<()>;
    async fn list(&self) -> anyhow::Result<Self::DataFrame>;
    async fn schema(&self, name: &str) -> anyhow::Result<Self::DataFrame>;
    async fn describe(&self, name: &str) -> anyhow::Result<Self::DataFrame>;
    async fn head(&self, name: &str, size: usize) -> anyhow::Result<Self::DataFrame>;
    async fn sql(&self, sql: &str) -> anyhow::Result<Self::DataFrame>;
}

trait ReplDisplay {
    async fn display(self) -> anyhow::Result<String>;
}

pub struct ReplContext {
    pub tx: mpsc::Sender<ReplMsg>,
}

pub struct ReplMsg {
    pub cmd: ReplCommand,
    pub tx: oneshot::Sender<String>,
}

pub type ReplCallbacks = CallBackMap<ReplContext, reedline_repl_rs::Error>;

pub fn get_callbacks() -> ReplCallbacks {
    let mut callbacks: ReplCallbacks = CallBackMap::new();
    callbacks.insert("connect".to_string(), cli::connect::connect);
    callbacks.insert("list".to_string(), cli::list::list);
    callbacks.insert("schema".to_string(), cli::schema::schema);
    callbacks.insert("describe".to_string(), cli::describe::describe);
    callbacks.insert("head".to_string(), cli::head::head);
    callbacks.insert("sql".to_string(), cli::sql::head);
    callbacks
}

impl ReplContext {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded::<ReplMsg>();

        let rt = Runtime::new().expect("Failed to create runtime");

        let mut backend = DataFusionBackend::new();

        thread::Builder::new()
            .name("ReplBackend".to_string())
            .spawn(move || {
                while let Ok(msg) = rx.recv() {
                    if let Err(e) = rt.block_on(async {
                        let ret = msg.cmd.execute(&mut backend).await?;
                        msg.tx.send(ret)?;
                        Ok::<_, anyhow::Error>(())
                    }) {
                        eprintln!("Failed to process command: {}", e);
                    }
                }
            })
            .unwrap();

        Self { tx }
    }

    pub fn send(&self, msg: ReplMsg, rx: oneshot::Receiver<String>) -> Option<String> {
        if let Err(e) = self.tx.send(msg) {
            eprintln!("REPL Send Error: {}", e);
            std::process::exit(1);
        }

        // if the oneshot receiver is dropped, return None, because the sender has been dropped
        rx.recv().ok()
    }
}

impl ReplMsg {
    pub fn new(cmd: impl Into<ReplCommand>) -> (Self, oneshot::Receiver<String>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                cmd: cmd.into(),
                tx,
            },
            rx,
        )
    }
}

impl Default for ReplContext {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for ReplContext {
    type Target = mpsc::Sender<ReplMsg>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl DerefMut for ReplContext {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn taotie_test() {}
}
