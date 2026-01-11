//! commands
pub mod migration;

use clap::Parser;

///
#[derive(Debug, Parser)]
#[command(infer_subcommands = true)]
#[allow(clippy::large_enum_variant)]
pub enum MainCmd {
    /// Migrate mdbx to rocksdb for PlainAccountState and PlainStorageState tables.
    Migration(migration::Cmd),
}

impl MainCmd {
    ///
    pub fn run(&self) {
        match self {
            Self::Migration(cmd) => cmd.run(),
        }
    }
}
