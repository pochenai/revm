//! commands
pub mod cache_killer;
pub mod migration;

use clap::Parser;

///
#[derive(Debug, Parser)]
#[command(infer_subcommands = true)]
#[allow(clippy::large_enum_variant)]
pub enum MainCmd {
    /// Migrate mdbx to rocksdb for PlainAccountState and PlainStorageState tables.
    Migration(migration::Cmd),
    /// Allocate a large amount of memory and hold it to evict OS file system caches for other I/O bench program.
    CacheKiller(cache_killer::Cmd),
}

impl MainCmd {
    ///
    pub fn run(&self) {
        match self {
            Self::Migration(cmd) => cmd.run::<false>(),
            Self::CacheKiller(cmd) => cmd.run(),
        }
    }
}
