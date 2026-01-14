use clap::Parser;
use revm::{context::block_states::import_struct, state::bal::Bal};

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
        let bals: Vec<Bal> = import_struct(format!("./data/bals_{nblocks}.json"));
        // with read
        let mut total_size = 0;
        let mut max_size = 0;
        // without read
        let mut total_size_no_read = 0;
        let mut max_size_no_read = 0;

        let nblocks = bals.len();

        for bal in bals {
            let alloy_bal = bal.into_alloy_bal();
            let mut buf = Vec::new();
            alloy_rlp::encode_list(&alloy_bal, &mut buf);

            total_size += buf.len();
            max_size = max_size.max(buf.len());

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
            "BAL with read, avg rlp-enc size:",
            format_with_commas((total_size / nblocks) as u64),
        );
        println!(
            "{:<40} {:>12} bytes",
            "BAL without read, avg rlp-enc size:",
            format_with_commas((total_size_no_read / nblocks) as u64),
        );

        println!(
            "Size reduced without read: {:.2}%",
            100.0 * (total_size as f64 - total_size_no_read as f64) / total_size as f64
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
