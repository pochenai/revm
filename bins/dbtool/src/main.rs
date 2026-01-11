//! rocksdb migration
use clap::Parser;
use dbtool::cmd::MainCmd;

fn main() {
    MainCmd::parse().run();
}
