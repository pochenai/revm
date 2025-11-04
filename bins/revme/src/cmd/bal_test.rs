use std::{
    collections::BTreeMap,
    iter::zip,
    path::{self, PathBuf},
    sync::{mpsc::channel, Arc},
    time::Duration,
};

use alloy_primitives::{bytes, Bytes};
use k256::elliptic_curve::consts::{False, True};
use revm::{
    bytecode::bitvec::index,
    context::{
        self,
        block_states::{
            envelope_to_txenv, import_struct, prestates_to_cachedbs, write_data, PreblockState,
            RethBlock,
        },
        cfg::CfgEnv,
        transaction::AccessList,
        BlockEnv, ContextTr, TxEnv,
    },
    context_interface::block::BlobExcessGasAndPrice,
    database::{
        bal::{self, BalDatabase},
        states::{cache, changes},
        Cache, CacheState, State,
    },
    primitives::{
        address, alloy_primitives, hardfork::SpecId, hex::FromHex, Address, HashMap, B256,
        KECCAK_EMPTY, U256,
    },
    state::{
        bal::{Bal, BalWrites},
        Account, AccountInfo, Bytecode,
    },
    Context, Database, DatabaseCommit, ExecuteCommitEvm, ExecuteEvm, MainBuilder, MainContext,
    SystemCallEvm,
};

use alloy_consensus::{EthereumTxEnvelope, Transaction, TxEip4844};

use rayon::prelude::*;
use rayon::ThreadPoolBuilder;

use clap::Parser;
use std::time::Instant;

pub const SYSTEM_ADDRESS: Address = address!("0xfffffffffffffffffffffffffffffffffffffffe");

/// The address for the EIP-4788 beacon roots contract.
pub const BEACON_ROOTS_ADDRESS: Address = address!("0x000F3df6D732807Ef1319fB7B8bB8522d0Beac02");

/// The code for the EIP-4788 beacon roots contract.
pub static BEACON_ROOTS_CODE: Bytes = bytes!("3373fffffffffffffffffffffffffffffffffffffffe14604d57602036146024575f5ffd5b5f35801560495762001fff810690815414603c575f5ffd5b62001fff01545f5260205ff35b5f5ffd5b62001fff42064281555f359062001fff015500");

/// The address for the EIP-2935 history storage contract.
pub const HISTORY_STORAGE_ADDRESS: Address = address!("0x0000F90827F1C53a10cb7A02335B175320002935");

/// The code for the EIP-2935 history storage contract.
pub static HISTORY_STORAGE_CODE: Bytes = bytes!("3373fffffffffffffffffffffffffffffffffffffffe14604657602036036042575f35600143038111604257611fff81430311604257611fff9006545f5260205ff35b5f5ffd5b5f35611fff60014303065500");

pub const WITHDRAW_QUEUE_ADDRESS: Address = address!("0x00000961Ef480Eb55e80D19ad83579A64c007002");
pub const CONSPLICATION_QUEUE_ADDRESS: Address =
    address!("0x0000BBdDc7CE488642fb579F8B00f3a590007251");

pub static SYSTEM_CA_ADDRESSES: [Address; 5] = [
    SYSTEM_ADDRESS,
    BEACON_ROOTS_ADDRESS,
    HISTORY_STORAGE_ADDRESS,
    WITHDRAW_QUEUE_ADDRESS,
    CONSPLICATION_QUEUE_ADDRESS,
];

/// EIP-2935: Serve historical block hashes from state
///
/// Number of block hashes the EVM can access in the past (Prague).
///
/// # Note
///
/// Updated from 8192 to 8191 in <https://github.com/ethereum/EIPs/pull/9144>
pub const HISTORY_SERVE_WINDOW: usize = 8191;

/// `baltest` subcommand
#[derive(Parser, Debug)]
pub struct Cmd {
    /// Run tests in multiple thread.
    #[arg(short = 't', default_value_t = 1)]
    threads: usize,
    /// Enable parallel execution by default (exe sequentially is the same as setting -t 1).
    #[arg(short = 'p', default_value_t = true)]
    par: bool,
    /// Process txs prioritized by gas limit.
    #[arg(short = 'o', default_value_t = false)]
    priority_by_gaslimit: bool,
    /// Show debug info.
    #[arg(short = 'd', default_value_t = false)]
    debug: bool,
}

macro_rules! measure {
    ($debug:expr, $name:expr, $block:expr) => {{
        let start = Instant::now();
        let result = $block;
        let elapsed = start.elapsed();
        if ($debug) {
            println!("{} total execution time: {:?}", $name, elapsed);
        }
        (elapsed, result)
    }};
}

impl Cmd {
    /// Runs `baltest` command.
    pub fn run(&self) -> Result<(), super::Error> {
        // Push the file in revme/data directory
        let blocks = import_struct("./data/blocks.json");
        let bals: Vec<Bal> = import_struct("./data/bals.json");
        let prestates = import_struct("./data/prestates.json");
        let block_hashes = import_struct("./data/blockHashes.json");

        let caches = prestates_to_cachedbs(prestates);

        let task_name = format!("threads: {}, blocks: {},", self.threads, bals.len(),);
        measure!(
            true,
            task_name,
            if self.par {
                execute_blocks_par(
                    blocks,
                    bals,
                    caches,
                    block_hashes,
                    self.threads,
                    self.priority_by_gaslimit,
                    self.debug,
                );
            } else {
                execute_blocks(blocks, bals, caches, block_hashes, self.debug);
            }
        );

        Ok(())
    }
}

#[test]
fn test_bal() {
    let mut state = BalDatabase::new(State::builder().build()).with_bal_builder();
    state.bal_index = 0;
    let acct1 = AccountInfo {
        balance: U256::MAX,
        // Account nonce.
        nonce: 0,
        // Hash of the raw bytes in `code`, or [`KECCAK_EMPTY`].
        code_hash: KECCAK_EMPTY,
        // Storage id.
        storage_id: None,
        code: Some(Bytecode::default()),
    };
    let addr1 = Address::from_hex("0x4838B106FCe9647Bdf1E7877BF73cE8B0BAD5f97").unwrap();

    let acct2 = AccountInfo {
        balance: U256::ZERO,
        // Account nonce.
        nonce: 1,
        // Hash of the raw bytes in `code`, or [`KECCAK_EMPTY`].
        code_hash: KECCAK_EMPTY,
        // Storage id.
        storage_id: None,
        code: Some(Bytecode::default()),
    };
    let addr2 = Address::from_hex("0xC6093Fd9cc143F9f058938868b2df2daF9A91d28").unwrap();

    let mut genesis_state = BTreeMap::<Address, AccountInfo>::new();
    genesis_state.insert(addr1, acct1);
    genesis_state.insert(addr2, acct2);

    for (address, account) in genesis_state {
        state.insert_account_with_storage(address, account, HashMap::new());
    }

    let block_env = BlockEnv::default();
    // Create EVM context for each transaction to ensure fresh state access
    let evm_context = Context::mainnet()
        .with_block(&block_env)
        .with_db(&mut state);

    let mut evm = evm_context.build_mainnet();
    evm.db_mut().bal_index += 1;

    let tx1 = TxEnv::builder_for_bench()
        .caller(addr1)
        .to(address!("0xc000000000000000000000000000000000000000"))
        .value(U256::ONE)
        .build_fill();
    let exe_result = evm.transact(tx1).ok().unwrap();

    evm.commit(exe_result.state);

    evm.db_mut().bal_index += 1;
    let mut acl = AccessList::default();
    acl.add_address(address!("0x00000000000000000000000000000000000000ff"));
    let tx2 = TxEnv::builder_for_bench()
        .caller(address!("0x00000000000000000000000000000000000000ff"))
        .access_list(acl)
        .to(address!("0xc000000000000000000000000000000000000000"))
        .build_fill();
    let exe_result = evm.transact(tx2).ok().unwrap();

    evm.commit(exe_result.state);

    if let Some(bal) = state.bal_builder.take() {
        println!("{}", serde_json::to_string_pretty(&bal).unwrap());
        // println!("{:?}", bal);
    }
}

/// execute blocks sequentially
fn execute_blocks(
    blocks: Vec<RethBlock>,
    bals: Vec<Bal>,
    caches: Vec<CacheState>,
    block_hashes: BTreeMap<u64, B256>,
    debug: bool,
) {
    for (index, (block, (mut bal, cache))) in zip(blocks, zip(bals, caches)).into_iter().enumerate()
    {
        let block_env = BlockEnv {
            number: U256::from(block.number),
            beneficiary: block.beneficiary,
            timestamp: U256::from(block.timestamp),
            gas_limit: block.gas_limit,
            basefee: block.base_fee_per_gas.unwrap(),
            difficulty: block.difficulty,
            prevrandao: Some(block.mix_hash),
            blob_excess_gas_and_price: Some(BlobExcessGasAndPrice::new_with_spec(
                block.excess_blob_gas.unwrap(),
                SpecId::PRAGUE,
            )),
        };

        let bal_arc = Arc::new(bal.clone());

        let parent_hash = block.parent_hash;
        let parent_beacon_root = block.parent_beacon_block_root.unwrap();
        let body = block.into_body();

        // // TODO: pre-tx bals
        // let block_env_clone = block_env.clone();
        // let cached_state = State::builder()
        //     .with_block_hashes(block_hashes.clone())
        //     .with_cached_prestate(cache.clone())
        //     .build();
        // let mut state = BalDatabase::new(cached_state)
        //     .with_bal_builder()
        //     .with_bal_option(Some(bal_arc.clone()));
        // state.bal_index = 0;
        // let evm_context = Context::mainnet()
        //     .with_block(block_env_clone)
        //     .with_db(&mut state);

        // let mut evm = evm_context.build_mainnet();
        // // pre-tx: apply_blockhashes_contract_call
        // let exe_result = evm.system_call_one_with_caller(
        //     SYSTEM_ADDRESS,
        //     HISTORY_STORAGE_ADDRESS,
        //     parent_hash.into(),
        // );
        // if exe_result.is_err() {
        //     eprintln!("{:?}", exe_result.err());
        //     panic!(
        //         "hash execution error for block: {} tx: {}",
        //         block_env.number, 0
        //     )
        // }
        // // pre-tx: apply_beacon_root_contract_call
        // let exe_result = evm.system_call_one_with_caller(
        //     SYSTEM_ADDRESS,
        //     BEACON_ROOTS_ADDRESS,
        //     parent_beacon_root.into(),
        // );
        // if exe_result.is_err() {
        //     eprintln!("{:?}", exe_result.err());
        //     panic!(
        //         "root execution error for block: {} tx: {}",
        //         block_env.number, 0
        //     )
        // }
        // let changes = state.changes;
        // output_bals.merge_bal(changes, state.bal_index);

        // txs
        let mut results = Vec::with_capacity(body.transactions.len());
        for (tx_index, tx) in body.transactions.iter().enumerate() {
            let changes = handle_tx(
                block_env.clone(),
                block_hashes.clone(),
                bal_arc.clone(),
                cache.clone(),
                tx_index as u64,
                tx,
                debug,
            );
            results.push((tx_index as u64 + 1, changes));
        }

        // TODO: add post-tx bals

        if debug {
            let mut output_bals = Bal::default();
            for (bal_index, bal) in results {
                if let Some(bal) = bal {
                    output_bals.merge_bal(bal, bal_index);
                }
            }
            output_bals.accounts.sort_keys();
            // remove pre-tx and post-tx bals
            bal.remove_first_last();
            bal.remove_at_address(&SYSTEM_CA_ADDRESSES);
            bal.accounts.sort_keys();
            assert_eq!(
                output_bals, bal,
                "bals for tx {} in block {} is not equal",
                index, block_env.number
            )
        }
    }
}

fn handle_tx(
    block_env: BlockEnv,
    block_hashes: BTreeMap<u64, B256>,
    bal_arc: Arc<Bal>,
    cache: CacheState,
    tx_index: u64, // tx index start from 0, while the first tx's bal index is 1
    tx: &EthereumTxEnvelope<TxEip4844>,
    debug: bool,
) -> Option<Bal> {
    if debug {
        println!(
            "txindex:{:>3}, gaslimit:{:>8} start",
            tx_index,
            tx.gas_limit()
        );
    }
    let cached_state = State::builder()
        .with_block_hashes(block_hashes)
        .with_cached_prestate(cache)
        .build();
    let mut state = BalDatabase::new(cached_state)
        .with_bal_builder()
        .with_bal_option(Some(bal_arc));
    state.bal_index = tx_index + 1;

    let blocknumber = block_env.number;
    // Create EVM context for each transaction to ensure fresh state access
    let evm_context = Context::mainnet().with_block(block_env).with_db(&mut state);

    let mut evm = evm_context.build_mainnet();
    let txenv = envelope_to_txenv(tx);
    // println!(
    //     "txid {} sender: {:?}, kind:{:?}",
    //     index, txenv.caller, txenv.tx_type
    // );
    let exe_result = evm.transact(txenv);
    if exe_result.is_err() {
        eprintln!("{:?}", exe_result.err());
        panic!(
            "execution error for block: {} tx: {}",
            blocknumber, tx_index
        )
    } else {
        // println!(
        //     "execute success for block: {} tx: {}",
        //     blocknumber, tx_index
        // )
    }

    // must commit state changes, or bal builder will have nothing
    let result_state = exe_result.unwrap().state;
    evm.commit(result_state);
    state.bal_builder
    // print!("exe_result:{:?}", exe_result)
}

/// execute blocks sequentially
fn execute_blocks_par(
    blocks: Vec<RethBlock>,
    bals: Vec<Bal>,
    caches: Vec<CacheState>,
    block_hashes: BTreeMap<u64, B256>,
    num_threads: usize,
    priority_by_gaslimit: bool,
    debug: bool,
) {
    for (index, (block, (mut bal, cache))) in zip(blocks, zip(bals, caches)).into_iter().enumerate()
    {
        let block_env = BlockEnv {
            number: U256::from(block.number),
            beneficiary: block.beneficiary,
            timestamp: U256::from(block.timestamp),
            gas_limit: block.gas_limit,
            basefee: block.base_fee_per_gas.unwrap(),
            difficulty: block.difficulty,
            prevrandao: Some(block.mix_hash),
            blob_excess_gas_and_price: Some(BlobExcessGasAndPrice::new_with_spec(
                block.excess_blob_gas.unwrap(),
                SpecId::PRAGUE,
            )),
        };

        let bal_arc = Arc::new(bal.clone());

        let parent_hash = block.parent_hash;
        let parent_beacon_root = block.parent_beacon_block_root.unwrap();
        let body = block.into_body();

        let mut indexed_txs: Vec<_> = body.transactions.into_iter().enumerate().collect();
        if priority_by_gaslimit {
            println!("priority_by_gaslimit");
            measure!(
                debug,
                "sort_tx",
                indexed_txs.sort_by_key(|(_, tx)| std::cmp::Reverse(tx.gas_limit()))
            );
        }

        // parallel execute txs
        let pool = ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .expect("Failed to build Rayon pool");

        let (tx_sender, rx_receiver) = channel::<(u64, Option<Bal>, Duration)>();

        pool.install(|| {
            for chunk in indexed_txs.chunks(num_threads) {
                let txs_sender = tx_sender.clone();

                chunk.par_iter().for_each(|(index, tx)| {
                    let (elapsed, bal) = measure!(
                        false,
                        format!("tx {index}"),
                        handle_tx(
                            block_env.clone(),
                            block_hashes.clone(),
                            bal_arc.clone(),
                            cache.clone(),
                            *index as u64,
                            tx,
                            debug,
                        )
                    );

                    txs_sender
                        .send((*index as u64 + 1, bal, elapsed))
                        .expect("Failed to send result");
                });
            }
        });

        // Drop the original sender so the iterator ends
        drop(tx_sender);

        // Collect all results from the channel
        let mut results: Vec<_> = rx_receiver.into_iter().collect();

        if debug {
            let mut output_bals = Bal::default();
            let mut max_elapsed = Duration::ZERO;
            let mut max_elapsed_idx = 0;
            results.sort_by_key(|(bal_index, _, _)| *bal_index);
            for (bal_index, bal, elapsed) in results {
                if let Some(bal) = bal {
                    output_bals.merge_bal(bal, bal_index);
                }
                if elapsed > max_elapsed {
                    max_elapsed = elapsed;
                    max_elapsed_idx = bal_index - 1;
                }
            }
            println!(
                "Block {} → tx #{} (0-based index) took the longest: {:?}",
                block_env.number, max_elapsed_idx, max_elapsed
            );

            // remove pre-tx and post-tx bals
            bal.remove_first_last();
            bal.remove_at_address(&SYSTEM_CA_ADDRESSES);
            bal.accounts.sort_keys();

            output_bals.accounts.sort_keys();
            bal.accounts.sort_keys();
            assert_eq!(
                output_bals, bal,
                "bals for block {} is not equal",
                block_env.number
            )
        }
    }
}

#[test]
fn test_exe_blocks() {
    let blocks = import_struct("./data/blocks.json");
    let bals: Vec<Bal> = import_struct("./data/bals.json");
    let prestates = import_struct("./data/prestates.json");
    let block_hashes = import_struct("./data/blockHashes.json");

    let caches = prestates_to_cachedbs(prestates);

    execute_blocks(blocks, bals, caches, block_hashes, true);
}

#[test]
fn test_par_exe_blocks() {
    let cwd = std::env::current_dir().unwrap();
    let blocks = import_struct(cwd.join("./data/blocks.json"));
    let bals: Vec<Bal> = import_struct(cwd.join("./data/bals.json"));
    let prestates = import_struct(cwd.join("./data/prestates.json"));
    let block_hashes = import_struct(cwd.join("./data/blockHashes.json"));

    let caches = prestates_to_cachedbs(prestates);
    execute_blocks_par(blocks, bals, caches, block_hashes, 5, true, true);
}
