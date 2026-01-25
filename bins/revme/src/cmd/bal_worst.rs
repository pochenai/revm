//
// each address contains address: 8000 keys
//  batched I/O：time
// parallel I/O time:
// 4 address simulate block with 1 batch
// 16 address simulate block with 2 batch
// 64 address simulate block with 8 batch

use alloy_primitives::{address, hex, keccak256};
use librocksdb::store::Store;
use rayon::prelude::*;
use std::{collections::HashSet, iter::zip, thread, time::Instant};

use clap::Parser;
use flatdb::{preblock_db_provider::PreBlockStateCache, ProviderRW};
use revm::{
    context::block_states::{import_struct, AccountStates, PreBlockState},
    database::bal::{self, BalReadsTy},
    primitives::StorageKey,
    state::bal::Bal,
};

use crate::cmd::bal_test::IOPattern;

const CA_IN_BLOCK: u64 = 4;
const MAX_TX_GAS: u64 = 16_000_000;

/// `balworst` subcommand
#[derive(Parser, Debug, Default)]
pub struct Cmd {
    /// Number of blocks (n) for file blocks_n.json, bals_n.json, blockHashes_n.json, prestates_n.json.
    #[arg(short = 'b', default_value_t = 1)]
    batch_blocks: u64,
    #[arg(long, value_enum, default_value = "par")]
    io: IOPattern,
    #[arg(long, default_value_t = 16)]
    io_threads: usize,
    #[arg(short = 't', default_value_t = 16)]
    threads: usize,
    #[arg(long)]
    datadir: String,
}

impl Cmd {
    /// Run the `balsize` command
    pub fn run(&self) -> Result<(), super::Error> {
        let bal_read = self.gen_sim_blocks();
        let total_reads = bal_read.iter().map(|(_, keys)| keys.len()).sum::<usize>();
        let batch = self.batch_blocks;

        // initialize db
        let db = Store::new_rocksdb_backend(&self.datadir);
        println!("RocksDB {} loaded", self.datadir);

        let start = Instant::now();

        match self.io {
            IOPattern::Batched => {
                let mut p = PreBlockStateCache::new(&db);
                let bal_read = &bal_read;
                let (acct_time, storage_time) = p.batch_preblock_state(bal_read, self.io_threads);
                // print incase compiler optimize away the I/O
                println!(
                    "Batched I/O: account time: {:?}, storage time: {:?}",
                    acct_time, storage_time
                );
            }
            IOPattern::Parallel => {
                rayon::ThreadPoolBuilder::new()
                    .num_threads(self.threads)
                    .thread_name(|i| format!("rayon-{}", i))
                    .build_global()
                    .unwrap();

                let res: Vec<_> = bal_read
                    .par_iter()
                    .map_init(
                        || db.lastest_provider_ro(),
                        |provider_ro, (address, storage)| {
                            // simulate read account and storage
                            let mut vals = Vec::with_capacity(storage.len());
                            for key in storage.iter() {
                                let val = provider_ro.storage_ref(*address, *key).unwrap();
                                vals.push(1);
                            }

                            vals
                        },
                    )
                    .collect();
                // print incase compiler optimize away the I/O
                println!("Parallel I/O: total accounts processed: {}", res.len());
            }
        }

        let elasped = start.elapsed();

        println!(
            "I/O pattern {:?}: gas per second:{} MGas/s, total time:{:?}, avg I/O time:{:.2} µs",
            self.io,
            format_with_commas(
                (CA_IN_BLOCK * batch * MAX_TX_GAS) as u64 / elasped.as_millis() as u64 / 1000
            ),
            elasped,
            elasped.as_micros() as f64 / total_reads as f64
        );

        Ok(())
    }

    fn gen_sim_blocks(&self) -> BalReadsTy {
        let mut bal_read = BalReadsTy::default();
        let max_reads_in_tx = max_reads_in_one_tx();
        println!("max_reads_in_tx:{}", max_reads_in_tx);
        let addrs = vec![
            address!("0xdAC17F958D2ee523a2206206994597C13D831ec7"),
            address!("0x04a5b8C32f9c38092B008A4939f1F91D550C4345"),
            address!("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
            address!("0xdAC17F958D2ee523a2206206994597C13D831ec7"),
            address!("0xB5571E76693ba60110B5811DD650FFefce1C955f"),
            address!("0x603bb2c05D474794ea97805e8De69bCcFb3bCA12"),
            address!("0x8755b31f47C0b67721BADD6BCdE0dC641Ff62c6F"),
            address!("0xECC2d7C17bfCA3F7ce08f3646D92651Ad5Ef0e2e"),
            address!("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"),
            address!("0xfBd4cdB413E45a52E2C8312f670e9cE67E794C37"),
            //
            address!("0x1f2F10D1C40777AE1Da742455c65828FF36Df387"),
            address!("0xC727eb69Ccf89d5911042f21bE25A193D67e2c23"),
            address!("0xE07a16358aA878CBDa2D49A88E5106871E0db307"),
            address!("0x66a9893cC07D91D95644AEDD05D03f95e1dBA8Af"),
            address!("0x0c2a47d52efb0bfbDda1F51cd4Eb97857441fEf6"),
            address!("0x6aba0315493b7e6989041C91181337b662fB1b90"),
            address!("0xAe0207C757Aa2B4019Ad96edD0092ddc63EF0c50"),
            address!("0xdD468A1DDc392dcdbEf6db6e34E89AA338F9F186"),
            address!("0x881D40237659C251811CEC9c364ef91dC08D300C"),
            address!("0xb300000b72DEAEb607a12d5f54773D1C19c7028d"),
            //
            address!("0x45312ea0eFf7E09C83CBE249fa1d7598c4C8cd4e"),
            address!("0xE6Bfd33F52d82Ccb5b37E16D3dD81f9FFDAbB195"),
            address!("0x111111125421cA6dc452d289314280a0f8842A65"),
            address!("0x1231DEB6f5749EF6cE6943a275A1D3E7486F4EaE"),
            address!("0xD7e42D9502Fbd66d90750E544e05C2B3CA7CBD22"),
            address!("0x0000000071727De22E5E9d8BAf0edAc6f37da032"),
            address!("0x0000000000001fF3684f28c67538d4D072C22734"),
            address!("0x0000000000000068F116a894984e2DB1123eB395"),
            address!("0xc6fD8084fB9b6a0768CF943c341049eDD1085B82"),
            address!("0x0dE8bf93dA2f7eecb3d9169422413A9bef4ef628"),
            //
            address!("0x3328F7f4A1D1C57c35df56bBf0c9dCAFCA309C49"),
            address!("0x663DC15D3C1aC63ff12E45Ab68FeA3F0a883C251"),
            address!("0xCcC88a9d1B4ED6b0EABA998850414b24f1c315bE"),
            address!("0x1231DEB6f5749EF6cE6943a275A1D3E7486F4EaE"),
            address!("0x51C72848c68a965f66FA7a88855F9f7784502a7F"),
            address!("0xb92fe925DC43a0ECdE6c8b1a2709c170Ec4fFf4f"),
            address!("0x6131B5fae19EA4f9D964eAc0408E4408b66337b5"),
            address!("0xBBbfD134E9b44BfB5123898BA36b01dE7ab93d98"),
            address!("0x9008D19f58AAbD9eD0D60971565AA8510560ab41"),
            address!("0x5FF137D4b0FDCD49DcA30c7CF57E578a026d2789"),
        ];
        let max_ca_addrs = (CA_IN_BLOCK * self.batch_blocks) as usize;
        assert!(
            addrs.len() >= max_ca_addrs,
            "not enough contract addresses for simulating blocks"
        );
        for addr in addrs[..max_ca_addrs].iter() {
            let mut keys = HashSet::new();
            for key in 0..max_reads_in_tx {
                // MAX_TX_GAS / 2000 gas for a sload  = 8000
                let key: u64 = key;
                let key = keccak256(key.to_be_bytes());
                let key: StorageKey = key.into();
                keys.insert(key);
            }

            bal_read.insert(*addr, keys);
        }

        bal_read
    }
}

#[inline]
fn max_reads_in_one_tx() -> u64 {
    let gas_read_per_key = 2000 + 70; // sload + keccak cost
    MAX_TX_GAS / gas_read_per_key
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
