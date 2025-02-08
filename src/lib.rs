mod cli;

use crossbeam_channel as mpsc;
use reedline_repl_rs::CallBackMap;
use std::{
    ops::{Deref, DerefMut},
    thread,
};

pub use cli::ReplCommand;

pub struct ReplContext {
    pub tx: mpsc::Sender<cli::ReplCommand>,
}

pub type ReplCallbacks = CallBackMap<ReplContext, reedline_repl_rs::Error>;

pub fn get_callbacks() -> ReplCallbacks {
    let mut callbacks: ReplCallbacks = CallBackMap::new();
    callbacks.insert("connect".to_string(), cli::connect::connect);
    callbacks.insert("list".to_string(), cli::list::list);
    callbacks.insert("describe".to_string(), cli::describe::describe);
    callbacks.insert("head".to_string(), cli::head::head);
    callbacks.insert("sql".to_string(), cli::sql::head);
    callbacks
}

impl ReplContext {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded();

        thread::Builder::new()
            .name("ReplBackend".to_string())
            .spawn(move || {
                while let Ok(cmd) = rx.recv() {
                    println!("{:?}", cmd);
                }
            })
            .unwrap();

        Self { tx }
    }

    pub fn send(&self, cmd: ReplCommand) {
        if let Err(e) = self.tx.send(cmd) {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }
}

impl Default for ReplContext {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for ReplContext {
    type Target = mpsc::Sender<cli::ReplCommand>;

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
