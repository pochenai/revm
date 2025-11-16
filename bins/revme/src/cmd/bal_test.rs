use std::{
    collections::{BTreeMap, HashMap},
    iter::zip,
    sync::Arc,
    time::Duration,
};

use crossbeam::channel::unbounded;

use alloy_primitives::{bytes, Bytes};
use revm::{
    context::{
        block_states::{
            envelope_to_txenv, import_struct, prestates_to_cachedbs, write_data, RecoveredBlockVec,
            RethBlock,
        },
        BlockEnv,
    },
    context_interface::block::{self, BlobExcessGasAndPrice},
    database::{bal::BalDatabase, states::CacheAccount, CacheState, PlainAccount, State},
    precompile::kzg_point_evaluation::init_load_kzg_trusted_setup,
    primitives::{address, alloy_primitives, hardfork::SpecId, Address, B256, KECCAK_EMPTY, U256},
    state::bal::Bal,
    Context, ExecuteCommitEvm, ExecuteEvm, MainBuilder, MainContext,
};

use alloy_consensus::{transaction::Recovered, Block, EthereumTxEnvelope, Transaction, TxEip4844};

use rayon::ThreadPoolBuilder;
use rayon::{iter::Either, prelude::*};

use clap::{Parser, ValueEnum};
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
#[derive(Parser, Debug, Default)]
pub struct Cmd {
    /// Run tests in multiple thread.
    #[arg(short = 't', default_value_t = 1)]
    threads: usize,
    /// Enable parallel execution by default (exe sequentially is the same as setting -t 1).
    #[arg(short = 'p', default_value_t = false)]
    par: bool,
    /// Number of blocks (n) for file blocks_n.json, bals_n.json, blockHashes_n.json, prestates_n.json.
    #[arg(short = 'n', default_value_t = 1)]
    nblocks: u64,
    /// Process txs prioritized by gas used or limit order, "gasUsedDo":gas used descending order, "gasLimitDo": gas limit descending order, "ao": gas limit ascending order, "none": random shedule.
    #[arg(short = 's', value_enum, default_value = "none")]
    schedule_by_gaslimit: PriorityOrder,
    /// Enable showing debug info.
    #[arg(short = 'd', default_value_t = false)]
    debug: bool,
    /// Enable checking the re-execute generated bals is the same with input bals.
    #[arg(short = 'c', default_value_t = false)]
    check_bal: bool,
    /// Disable parallel sender recovery for 7702 tx.
    #[arg(short = 'a', default_value_t = false)]
    par_7702: bool,
    /// Batch size to process multiple blocks.
    #[arg(short = 'b', default_value_t = 1)]
    batch_blocks: usize,
    /// Enable pre-tx state. Default: false (pre-block state + bal), --pre-tx-state: true (pre-tx-state without bal).
    #[arg(long, default_value_t = false)]
    pre_tx_state: bool,
}

#[derive(Copy, Clone, Debug, ValueEnum, Default)]
pub enum PriorityOrder {
    /// Sort transactions by descending gas limit.
    #[default]
    #[clap(alias = "gasUsedDo")]
    GasUsedDescending,

    #[clap(alias = "gasLimitDo")]
    GasLimitDescending,
    /// Sort transactions by ascending gas limit.
    #[clap(alias = "ao")]
    GasLimitAscending,

    /// Do not sort by gas limit.
    #[clap(alias = "none")]
    None,
}

enum Scheduler {
    RoundRobin,
    ConsumerProducer,
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
        let nblocks = self.nblocks;
        let recovered_blocks = import_struct(format!("./data/blocks_{nblocks}.json"));
        let blocks: Vec<alloy_consensus::Block<Recovered<EthereumTxEnvelope<TxEip4844>>>> =
            RecoveredBlockVec(recovered_blocks).into();

        let bals: Vec<Bal> = import_struct(format!("./data/bals_{nblocks}.json"));
        let prestates = import_struct(format!("./data/prestates_{nblocks}.json"));
        let block_hashes = import_struct(format!("./data/blockHashes_{nblocks}.json"));

        let caches = prestates_to_cachedbs(prestates);

        let prestates = caches_to_prestates(caches, &bals, &blocks, self.pre_tx_state);

        // preload kzg trusted setup
        init_load_kzg_trusted_setup();

        println!("start executing......");
        let task_name = format!("threads: {}, blocks: {},", self.threads, bals.len(),);
        let (elapsed, gas_used) = measure!(
            true,
            task_name,
            if self.par {
                let gasused = import_struct(format!("./data/gasused_{nblocks}.json"));
                match self.schedule_by_gaslimit {
                    PriorityOrder::None => {
                        execute_blocks_par(blocks, bals, prestates, block_hashes, gasused, self)
                    }
                    _ => execute_blocks_par_scheduler(
                        blocks,
                        bals,
                        prestates,
                        block_hashes,
                        gasused,
                        self,
                    ),
                }
            } else {
                execute_blocks(blocks, bals, prestates, block_hashes, self.debug)
            }
        );

        println!(
            "total gas used:{}M, gas per second:{:?} MGas/s",
            gas_used / 1_000_000,
            gas_used / (elapsed.as_millis() as u64) / 1000
        );

        Ok(())
    }
}

fn derive_pre_tx_states(pre_block_state: CacheState, bal: &Bal, len_txs: u64) -> Vec<CacheState> {
    let mut res = vec![];
    let mut pre_tx_state = pre_block_state;
    for tx_index in 0..len_txs {
        let bal_index = tx_index + 1;
        let mut created_accounts: HashMap<Address, (CacheAccount, bool)> = HashMap::new();

        for (addr, cached_acct) in &mut pre_tx_state.accounts {
            // for some contracts that will be created later, we must set it as none before it's created or the gas cost will be wrong due to the account exists check.
            let is_none = cached_acct.account.is_none();
            // contract created, https://etherscan.io/tx/0x11dd9d8d64bd0cfe39c1644b8a68fce33f9eb101aa4aa4af8794644764f2b4fb
            let acct = if is_none {
                let addr = *addr;
                created_accounts.insert(addr, (CacheAccount::default(), false));
                let (new_cached_acct, _) = created_accounts.get_mut(&addr).unwrap();
                if new_cached_acct.account.is_none() {
                    new_cached_acct.account = Some(PlainAccount::default());
                }
                new_cached_acct.account.as_mut().unwrap()
            } else {
                cached_acct.account.as_mut().unwrap()
            };

            let info = &mut acct.info;
            let changed = bal.populate_account_info(*addr, bal_index, info).unwrap();

            let storage = &mut acct.storage;
            for (key, value) in storage {
                bal.populate_storage_slot(*addr, bal_index, *key, value)
                    .unwrap();
            }

            if is_none && changed {
                let new_account = created_accounts.get_mut(addr).unwrap();
                new_account.1 = changed;
            }
        }

        // insert newly created accounts and contracts
        // e.g: create contract then use it in the same block.
        // https://etherscan.io/tx/0xbe0bec6662d53c30c49e82bcf867ca099dccdb85053211931a3a4dc53a2b4046
        // https://etherscan.io/tx/0x0240b04e544ed2808fd8a47d05f58e33a38be1f7312d1ba3f3ab8fb2f1be9847
        for (addr, (mut cached_acct, created)) in created_accounts {
            if created {
                cached_acct.status = revm::database::AccountStatus::Loaded;
                let acct = cached_acct.account.as_mut().unwrap();
                let info = &mut acct.info;
                if info.code_hash != KECCAK_EMPTY {
                    pre_tx_state
                        .contracts
                        .insert(info.code_hash, info.code.clone().unwrap());
                }

                pre_tx_state.accounts.insert(addr, cached_acct);
            }
        }
        res.push(pre_tx_state.clone());
    }

    res
}

fn full_pre_tx_states(
    caches: Vec<CacheState>,
    bals: &[Bal],
    full_len_txs: Vec<u64>,
) -> Vec<Vec<CacheState>> {
    let mut res = vec![];
    for (block_index, pre_block_state) in caches.into_iter().enumerate() {
        let bal = &bals[block_index];
        let len_txs = full_len_txs[block_index];
        let pre_tx_states = derive_pre_tx_states(pre_block_state, bal, len_txs);

        res.push(pre_tx_states);
    }
    res
}

fn caches_to_prestates<Tx>(
    caches: Vec<CacheState>,
    bals: &[Bal],
    blocks: &Vec<Block<Tx>>,
    pre_tx_state: bool,
) -> Vec<Either<CacheState, Vec<CacheState>>> {
    if pre_tx_state {
        let mut full_len_txs = vec![];
        for b in blocks.iter() {
            full_len_txs.push(b.body.transactions.len() as u64);
        }
        let pre_tx_states = full_pre_tx_states(caches, &bals, full_len_txs);
        pre_tx_states
            .into_iter()
            .map(|s| Either::Right(s))
            .collect::<Vec<_>>()
    } else {
        caches
            .into_iter()
            .map(|s| Either::Left(s))
            .collect::<Vec<_>>()
    }
}

/// execute blocks sequentially
fn execute_blocks(
    blocks: Vec<RethBlock>,
    bals: Vec<Bal>,
    caches: Vec<Either<CacheState, Vec<CacheState>>>,
    block_hashes: BTreeMap<u64, B256>,
    debug: bool,
) -> u64 {
    let mut blocks_gas_used = vec![];
    let block_hashes = Arc::new(block_hashes);
    let num_blocks = blocks.len();

    let mut total_clone_time = Duration::ZERO;

    let mut total_gas_used = 0;
    for (index, (block, (mut bal, cache))) in zip(blocks, zip(bals, caches)).into_iter().enumerate()
    {
        total_gas_used += block.gas_used;
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

        let (elasped, bal_clone) = measure!(false, "bal_clone", bal.clone());
        let bal_ref = &bal;
        total_clone_time += elasped;
        let bn = block.number;

        let body = block.into_body();

        // // TODO: pre-tx bals
        // let parent_hash = block.parent_hash;
        // let parent_beacon_root = block.parent_beacon_block_root.unwrap();
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
            let (elasped, (block_hashes_clone, bal_ref)) =
                measure!(false, "clone", { (Arc::clone(&block_hashes), bal_ref) });
            let (prestate, bal) = match &cache {
                Either::Left(cache) => (cache, Some(bal_ref)),
                Either::Right(cache) => (&cache[tx_index], None),
            };

            let changes = handle_tx(
                &block_env,
                block_hashes_clone,
                bal,
                prestate,
                tx_index as u64,
                tx,
                debug,
                false,
            );
            results.push((tx_index as u64 + 1, changes));
            total_clone_time += elasped;
        }

        // TODO: add post-tx bals

        if debug {
            let mut block_gas_used = Vec::with_capacity(results.len());
            let mut output_bals = Bal::default();
            for (bal_index, (bal, gas_used)) in results {
                if let Some(bal) = bal {
                    output_bals.merge_bal(bal, bal_index);
                }
                block_gas_used.push(gas_used);
            }

            // remove pre-tx and post-tx bals
            output_bals.remove_at_address(&SYSTEM_CA_ADDRESSES);
            output_bals.accounts.sort_keys();

            bal.remove_first_last();
            bal.remove_at_address(&SYSTEM_CA_ADDRESSES);
            bal.accounts.sort_keys();

            if output_bals != bal {
                write_data("bals-in.json", &bal);
                write_data("bals-out.json", &output_bals);
                panic!("bals for block {} is not equal", block_env.number)
            }

            // write gas used
            blocks_gas_used.push(block_gas_used);
        }
        println!("block execution:{} done", bn);
    }
    println!("total clone time:{:?}", total_clone_time);
    println!("write block gas used!");
    write_data(
        format!("gasused_{}.json", num_blocks).as_str(),
        &blocks_gas_used,
    );
    total_gas_used
}

fn handle_tx(
    block_env: &BlockEnv,
    block_hashes: Arc<BTreeMap<u64, B256>>,
    bal_ref: Option<&Bal>,
    cache: &CacheState,
    tx_index: u64, // tx index start from 0, while the first tx's bal index is 1
    tx: &Recovered<EthereumTxEnvelope<TxEip4844>>,
    debug: bool,
    par_7702: bool,
) -> (Option<Bal>, u64) {
    // if debug {
    //     println!(
    //         "txindex:{:>3}, gaslimit:{:>8} start",
    //         tx_index,
    //         tx.gas_limit()
    //     );
    // }
    let cached_state = State::builder()
        .with_block_hashes(block_hashes)
        .with_database_ref(cache)
        .build();
    let mut state = BalDatabase::new(cached_state)
        .with_bal_builder()
        .with_bal_option(bal_ref);
    state.bal_index = tx_index + 1;

    let blocknumber = block_env.number;
    // Create EVM context for each transaction to ensure fresh state access
    let evm_context = Context::mainnet_par7702(par_7702)
        .with_block(block_env)
        .with_db(&mut state);

    let mut evm = evm_context.build_mainnet();
    let txenv = envelope_to_txenv(tx);
    // println!(
    //     "txid {} sender: {:?}, kind:{:?}",
    //     index, txenv.caller, txenv.tx_type
    // );
    let exe_result = evm.transact(txenv);
    if exe_result.is_err() {
        eprintln!("{:?}", exe_result);
        panic!(
            "execution error for block: {} tx: {}, hash:{:?}",
            blocknumber,
            tx_index,
            tx.hash()
        )
    }
    // must commit state changes, or bal builder will have nothing
    let exe_result = exe_result.unwrap();
    let gas_used = exe_result.result.gas_used();
    let result_state = exe_result.state;
    evm.commit(result_state);
    (state.bal_builder, gas_used)
    // print!("exe_result:{:?}", exe_result)
}

static mut SCHEDULER_OVERHEAD: Duration = Duration::ZERO;
static mut SCHEDULER_SENDER_OVERHEAD: Duration = Duration::ZERO;
static mut EXECUTION: Duration = Duration::ZERO;

/// execute blocks parallel with scheduler
fn execute_blocks_par_scheduler(
    blocks: Vec<RethBlock>,
    bals: Vec<Bal>,
    prestates: Vec<Either<CacheState, Vec<CacheState>>>,
    block_hashes: BTreeMap<u64, B256>,
    txs_gas_used: Vec<Vec<u64>>,
    cmd_env: &Cmd,
) -> u64 {
    let mut sum_longest_tx_time = Duration::ZERO;
    let debug = cmd_env.debug;
    let scheduler = Scheduler::ConsumerProducer;
    let mut total_gas_used = 0;
    let batch = cmd_env.batch_blocks;

    let block_hashes = Arc::new(block_hashes);
    for (_, (blocks, (bals, (caches, txs_gas_used)))) in zip(
        blocks.chunks(batch),
        zip(
            bals.chunks(batch),
            zip(prestates.chunks(batch), txs_gas_used.chunks(batch)),
        ),
    )
    .into_iter()
    .enumerate()
    {
        let start = Instant::now();
        let mut indexed_txs = vec![];

        let mut block_envs = Vec::with_capacity(blocks.len());
        let mut bal_refs = Vec::with_capacity(blocks.len());

        for (block_index, (block, block_txs_gas_used)) in
            zip(blocks, txs_gas_used).into_iter().enumerate()
        {
            total_gas_used += block.gas_used;

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

            block_envs.push(block_env);
            bal_refs.push(&bals[block_index]);

            let body = block.clone().into_body();

            for (tx_index, tx) in body.transactions.into_iter().enumerate() {
                indexed_txs.push((tx_index, tx, block_index, block_txs_gas_used[tx_index]));
            }
        }

        match cmd_env.schedule_by_gaslimit {
            PriorityOrder::GasLimitAscending => {
                measure!(
                    false,
                    "sort_tx_ascending",
                    indexed_txs.sort_by_key(|(_, tx, _, _)| tx.gas_limit())
                );
            }
            PriorityOrder::GasLimitDescending => {
                measure!(
                    false,
                    "sort_tx_descending",
                    indexed_txs.sort_by_key(|(_, tx, _, _)| std::cmp::Reverse(tx.gas_limit()))
                );
            }
            PriorityOrder::GasUsedDescending => {
                measure!(
                    false,
                    "sort_tx_descending",
                    indexed_txs.sort_by_key(|(_, _, _, gas_used)| std::cmp::Reverse(*gas_used))
                );
            }
            PriorityOrder::None => { /* no sort */ }
        }

        unsafe {
            SCHEDULER_OVERHEAD += start.elapsed();
        }

        let exe_start = Instant::now();
        let results = match scheduler {
            Scheduler::RoundRobin => round_robin_schedule(
                cmd_env,
                &indexed_txs,
                block_envs,
                block_hashes.clone(),
                bal_refs,
                caches,
            ),
            Scheduler::ConsumerProducer => channel_schedule(
                cmd_env,
                &indexed_txs,
                block_envs,
                block_hashes.clone(),
                bal_refs,
                caches,
            ),
        };

        if debug {
            let mut max_elapsed = Duration::ZERO;
            let mut max_elapsed_idx = 0;
            let mut max_elapsed_tx = &indexed_txs[0].1;
            let mut max_block_index: usize = 0;
            for (bal_index, _, elapsed, tx, block_index, _gas_used) in &results {
                if elapsed > &max_elapsed {
                    max_elapsed = *elapsed;
                    max_elapsed_idx = bal_index - 1;
                    max_elapsed_tx = tx;
                    max_block_index = **block_index;
                }
            }

            if max_elapsed > Duration::from_millis(10) {
                println!(
                    "Block {} → tx #{} (0-based index), type:{},hash:{}, took the longest: {:?}",
                    max_block_index,
                    max_elapsed_idx,
                    max_elapsed_tx.tx_type(),
                    max_elapsed_tx.hash(),
                    max_elapsed
                );
            }

            sum_longest_tx_time += max_elapsed;

            if cmd_env.check_bal {
                // let mut output_bals = Bal::default();
                // results.sort_by_key(|(bal_index, _, _, _, _)| *bal_index);
                // for (bal_index, bal, elapsed, _, _) in results {
                //     if let Some(bal) = bal {
                //         output_bals.merge_bal(bal, bal_index);
                //     }
                // }
                // // remove pre-tx and post-tx bals
                // output_bals.remove_at_address(&SYSTEM_CA_ADDRESSES);
                // output_bals.accounts.sort_keys();

                // bal.remove_first_last();
                // bal.remove_at_address(&SYSTEM_CA_ADDRESSES);
                // bal.accounts.sort_keys();

                // if output_bals != bal {
                //     write_data("bals-in.json", &bal);
                //     write_data("bals-out.json", &output_bals);
                //     panic!("bals for block {} is not equal", block_env.number)
                // }
            }
        }

        unsafe {
            EXECUTION += exe_start.elapsed();
        }
    }

    if debug {
        println!(
            "Sum of most time-consuming tx durations per block: {:?}",
            sum_longest_tx_time
        );
    }

    unsafe {
        println!(
            "pre-process:{:?}, task-sender:{:?}, execution:{:?}",
            SCHEDULER_OVERHEAD, SCHEDULER_SENDER_OVERHEAD, EXECUTION
        );
    }

    total_gas_used
}

fn execute_blocks_par(
    blocks: Vec<RethBlock>,
    bals: Vec<Bal>,
    prestates: Vec<Either<CacheState, Vec<CacheState>>>,
    block_hashes: BTreeMap<u64, B256>,
    txs_gas_used: Vec<Vec<u64>>,
    cmd_env: &Cmd,
) -> u64 {
    let debug = cmd_env.debug;
    let mut total_gas_used = 0;
    let batch = cmd_env.batch_blocks;

    let block_hashes = Arc::new(block_hashes);
    for (_, (blocks, (bals, (caches, txs_gas_used)))) in zip(
        blocks.chunks(batch),
        zip(
            bals.chunks(batch),
            zip(prestates.chunks(batch), txs_gas_used.chunks(batch)),
        ),
    )
    .into_iter()
    .enumerate()
    {
        let chunk_results = blocks
            .par_iter()
            .enumerate()
            .flat_map(|(i, block)| {
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

                let bal_ref = &bals[i];
                let cache = &caches[i];

                let results = block
                    .body
                    .transactions
                    .par_iter()
                    .enumerate()
                    .map(|(tx_index, tx)| {
                        let (prestate, bal) = match cache {
                            Either::Left(cache) => (cache, Some(bal_ref)),
                            Either::Right(cache) => (&cache[tx_index], None),
                        };

                        let (elapsed, (bal, gas_used)) = measure!(
                            false,
                            format!("tx {}", tx_index),
                            handle_tx(
                                &block_env,
                                block_hashes.clone(),
                                bal,
                                prestate,
                                tx_index as u64,
                                tx,
                                debug,
                                cmd_env.par_7702,
                            )
                        );
                        // collect bal cause huge memeory allocation thus decrease performance about 8%.
                        (tx_index as u64 + 1, 0, elapsed, tx, i, gas_used)
                    })
                    .collect::<Vec<_>>();
                results
            })
            .collect::<Vec<_>>();

        if debug {
            let mut max_elapsed = Duration::ZERO;
            let mut max_elapsed_idx = 0;
            let mut max_elapsed_tx = &chunk_results[0].3;
            let mut max_block_index: usize = 0;
            for (bal_index, _, elapsed, tx, block_index, _gas_used) in &chunk_results {
                if elapsed > &max_elapsed {
                    max_elapsed = *elapsed;
                    max_elapsed_idx = bal_index - 1;
                    max_elapsed_tx = tx;
                    max_block_index = *block_index;
                }
            }

            if max_elapsed > Duration::from_millis(10) {
                println!(
                    "Block {} → tx #{} (0-based index), type:{},hash:{}, took the longest: {:?}",
                    max_block_index,
                    max_elapsed_idx,
                    max_elapsed_tx.tx_type(),
                    max_elapsed_tx.hash(),
                    max_elapsed
                );
            }
        }
    }

    for block in blocks.iter() {
        total_gas_used += block.gas_used;
    }
    total_gas_used
}

fn round_robin_schedule<'a>(
    cmd_env: &Cmd,
    indexed_txs: &'a Vec<(usize, Recovered<EthereumTxEnvelope<TxEip4844>>, usize, u64)>,
    block_envs: Vec<BlockEnv>,
    block_hashes: Arc<BTreeMap<u64, B256>>,
    bal_arcs: Vec<&Bal>,
    caches: &[Either<CacheState, Vec<CacheState>>],
) -> Vec<(
    u64,
    Option<Bal>,
    Duration,
    &'a Recovered<EthereumTxEnvelope<TxEip4844>>,
    &'a usize,
    u64,
)> {
    let threads = cmd_env.threads;
    let debug = cmd_env.debug;

    // Build thread pool
    let pool = ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .expect("Failed to build Rayon pool");

    let results: Vec<_> = pool.install(|| {
        let results: Vec<_> = (0..threads)
            .into_par_iter()
            .flat_map(|tid| {
                let mut result = Vec::with_capacity(indexed_txs.len() / threads + 1);
                // each thread {tid, tid+threads, tid+2*threads, ...}
                let mut i = tid;
                while i < indexed_txs.len() {
                    let (tx_index, tx, block_index, _) = &indexed_txs[i];
                    let (prestate, bal) = match &caches[*block_index] {
                        Either::Left(cache) => (cache, Some(bal_arcs[*block_index])),
                        Either::Right(cache) => (&cache[*tx_index], None),
                    };

                    let (elapsed, (bal, gas_used)) = measure!(
                        false,
                        format!("tx {}", tx_index),
                        handle_tx(
                            &block_envs[*block_index],
                            block_hashes.clone(),
                            bal,
                            prestate,
                            *tx_index as u64,
                            tx,
                            debug,
                            cmd_env.par_7702,
                        )
                    );
                    result.push((
                        *tx_index as u64 + 1,
                        bal,
                        elapsed,
                        tx,
                        block_index,
                        gas_used,
                    ));

                    i += threads;
                }

                result
            })
            .collect();

        results
    });
    results
}

// only a bit faster than manual round robin schedule.
fn channel_schedule<'a>(
    cmd_env: &Cmd,
    indexed_txs: &'a Vec<(usize, Recovered<EthereumTxEnvelope<TxEip4844>>, usize, u64)>,
    block_envs: Vec<BlockEnv>,
    block_hashes: Arc<BTreeMap<u64, B256>>,
    bal_arcs: Vec<&Bal>,
    caches: &[Either<CacheState, Vec<CacheState>>],
) -> Vec<(
    u64,
    Option<Bal>,
    Duration,
    &'a Recovered<EthereumTxEnvelope<TxEip4844>>,
    &'a usize,
    u64,
)> {
    let start = Instant::now();
    let threads = cmd_env.threads;
    let debug = cmd_env.debug;
    // Build thread pool
    let pool = ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .expect("Failed to build Rayon pool");

    // Work channel
    let (task_sender, task_receiver) = unbounded();
    for task in indexed_txs.iter() {
        task_sender.send(task).unwrap();
    }
    drop(task_sender); // close sender so workers know when finished

    unsafe {
        SCHEDULER_SENDER_OVERHEAD += start.elapsed();
    }

    // Result channel
    let (res_sender, res_receiver) = unbounded();

    pool.install(|| {
        (0..threads).into_par_iter().for_each(|_| {
            // let task_receiver = task_receiver.iter().cloned();
            while let Ok((tx_index, tx, block_index, _)) = task_receiver.recv() {
                let (prestate, bal) = match &caches[*block_index] {
                    Either::Left(cache) => (cache, Some(bal_arcs[*block_index])),
                    Either::Right(cache) => (&cache[*tx_index], None),
                };
                let (elapsed, (bal, gas_used)) = measure!(
                    false,
                    format!("tx {}", tx_index),
                    handle_tx(
                        &block_envs[*block_index],
                        block_hashes.clone(),
                        bal,
                        prestate,
                        *tx_index as u64,
                        tx,
                        debug,
                        cmd_env.par_7702,
                    )
                );

                res_sender
                    .send((
                        *tx_index as u64 + 1,
                        bal,
                        elapsed,
                        tx,
                        block_index,
                        gas_used,
                    ))
                    .expect("Failed to send result");
            }
        });
    });

    // Drop the last res_sender to close the channel
    drop(res_sender);
    // Collect all results from the channel
    res_receiver.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use revm::{
        context::{
            block_states::{import_struct, prestates_to_cachedbs},
            transaction::AccessList,
            BlockEnv, ContextTr, TxEnv,
        },
        database::{bal::BalDatabase, State},
        primitives::{address, hex::FromHex, Address, HashMap, KECCAK_EMPTY, U256},
        state::{bal::Bal, AccountInfo, Bytecode},
        Context, ExecuteCommitEvm, ExecuteEvm, MainBuilder, MainContext,
    };
    use std::collections::BTreeMap;

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

    fn test_exe_blocks_with_state(pre_tx_state: bool) {
        let bn = 1;
        let recovered_blocks = import_struct(format!("./data/blocks_{bn}.json"));
        let blocks = RecoveredBlockVec(recovered_blocks).into();
        let bals: Vec<Bal> = import_struct(format!("./data/bals_{bn}.json"));
        let prestates = import_struct(format!("./data/prestates_{bn}.json"));
        let block_hashes = import_struct(format!("./data/blockHashes_{bn}.json"));

        let caches = prestates_to_cachedbs(prestates);

        let prestates = caches_to_prestates(caches, &bals, &blocks, pre_tx_state);

        execute_blocks(blocks, bals, prestates, block_hashes, true);
    }

    #[test]
    fn test_exe_blocks() {
        test_exe_blocks_with_state(false);
        test_exe_blocks_with_state(true);
    }

    fn test_par_exe_blocks_state(pre_tx_state: bool) {
        let cwd = std::env::current_dir().unwrap();
        let bn = 1;
        let recovered_blocks = import_struct(format!("./data/blocks_{bn}.json"));
        let blocks = RecoveredBlockVec(recovered_blocks).into();
        let bals: Vec<Bal> = import_struct(cwd.join(format!("./data/bals_{bn}.json")));
        let prestates = import_struct(cwd.join(format!("./data/prestates_{bn}.json")));
        let block_hashes = import_struct(cwd.join(format!("./data/blockHashes_{bn}.json")));
        let gas_used = import_struct(cwd.join(format!("./data/gasused_{bn}.json")));

        let caches = prestates_to_cachedbs(prestates);
        let mut cmd_env = Cmd::default();
        cmd_env.threads = 5;
        cmd_env.check_bal = true;
        cmd_env.debug = true;
        cmd_env.batch_blocks = 1;

        let prestates = caches_to_prestates(caches, &bals, &blocks, pre_tx_state);

        let task_name = format!("threads: {}, blocks: {},", cmd_env.threads, bals.len(),);
        measure!(
            cmd_env.debug,
            task_name,
            execute_blocks_par_scheduler(blocks, bals, prestates, block_hashes, gas_used, &cmd_env,)
        );
    }

    #[test]
    fn test_par_exe_blocks() {
        test_par_exe_blocks_state(false);
        test_par_exe_blocks_state(true);
    }
}
