use std::{iter::zip, thread};

use clap::Parser;
use revm::{
    context::block_states::{import_struct, AccountStates, PreBlockState},
    state::bal::Bal,
};

/// `balsize` subcommand
#[derive(Parser, Debug, Default)]
pub struct Cmd {
    /// Number of blocks (n) for file blocks_n.json, bals_n.json, blockHashes_n.json, prestates_n.json.
    #[arg(short = 'n', default_value_t = 1)]
    nblocks: u64,
}

impl Cmd {
    /// Run the `balsize` command
    pub fn run(&self) -> Result<(), super::Error> {
        let nblocks = self.nblocks;

        let bals_f = thread::spawn(move || import_struct(format!("./data/bals_{nblocks}.json")));

        let prestates_f =
            thread::spawn(move || import_struct(format!("./data/prestates_{nblocks}.json")));

        let bals: Vec<Bal> = bals_f.join().unwrap();
        let prestates: Vec<PreBlockState> = prestates_f.join().unwrap();

        // with read keys and values
        let mut total_size_read_kvs = 0;
        let mut max_size_read_kvs = 0;
        // with read keys
        let mut total_size_read_keys = 0;
        let mut max_size_read_keys = 0;
        // without read
        let mut total_size_no_read = 0;
        let mut max_size_no_read = 0;

        let nblocks = bals.len();

        for (bal, prestate) in zip(bals, prestates) {
            let prestate_rlp = prestate.into_encodable_state(&bal);

            let alloy_bal = bal.into_alloy_bal();
            let mut buf = Vec::new();
            alloy_rlp::encode_list(&alloy_bal, &mut buf);

            let bal_read_len = buf.len();
            total_size_read_keys += bal_read_len;
            max_size_read_keys = max_size_read_keys.max(buf.len());

            buf.clear();
            alloy_rlp::encode_list(&prestate_rlp, &mut buf);
            total_size_read_kvs += bal_read_len + buf.len();
            max_size_read_kvs = max_size_read_kvs.max(buf.len());

            let alloy_bal_no_read: Vec<_> = alloy_bal
                .into_iter()
                .filter_map(|mut bal| {
                    if bal.storage_changes.is_empty()
                        && bal.balance_changes.is_empty()
                        && bal.nonce_changes.is_empty()
                        && bal.code_changes.is_empty()
                    {
                        None
                    } else {
                        bal.storage_reads = vec![];
                        Some(bal)
                    }
                })
                .collect();
            buf.clear();
            alloy_rlp::encode_list(&alloy_bal_no_read, &mut buf);

            total_size_no_read += buf.len();
            max_size_no_read = max_size_no_read.max(buf.len());
        }

        println!(
            "{:<40} {:>12} bytes",
            "BAL with read kvs, avg rlp-enc size:",
            format_with_commas((total_size_read_kvs / nblocks) as u64),
        );

        println!(
            "{:<40} {:>12} bytes",
            "BAL with read keys, avg rlp-enc size:",
            format_with_commas((total_size_read_keys / nblocks) as u64),
        );
        println!(
            "{:<40} {:>12} bytes",
            "BAL without read, avg rlp-enc size:",
            format_with_commas((total_size_no_read / nblocks) as u64),
        );

        println!(
            "Size reduced without read, relative to with read kvs:{:.2}, only keys:{:.2}",
            100.0 * (total_size_read_kvs as f64 - total_size_no_read as f64)
                / total_size_read_kvs as f64,
            100.0 * (total_size_read_keys as f64 - total_size_no_read as f64)
                / total_size_read_keys as f64
        );

        Ok(())
    }
}

fn format_with_commas(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::new();

    for (i, ch) in s.chars().rev().enumerate() {
        if i != 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }

    out.chars().rev().collect()
}
