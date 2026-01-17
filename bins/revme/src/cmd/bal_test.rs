use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    iter::zip,
    ops::Deref,
    sync::Arc,
    time::Duration,
};

use crossbeam::channel::unbounded;

use alloy_primitives::{bytes, Bytes};
use flatdb::{
    MainnetProviderRW, MockProviderRW, ProviderRW, mtcache::{MTCache, SharedCache}, preblock_db_provider::{PreBlockStateCache}
};
use librocksdb::store::Store;
use revm::{
    Context, DatabaseRef, ExecuteCommitEvm, ExecuteEvm, MainBuilder, MainContext, context::{
        BlockEnv, block_states::{
            PreBlockState, RecoveredBlockVec, RethBlock, envelope_to_txenv, import_struct, prestates_to_cachedbs, write_data
        }
    }, context_interface::block::{self, BlobExcessGasAndPrice}, database::{
        self, CacheState, PlainAccount, State, bal::{BAL_READS, BalDatabase, BalReadsTy, DEBUG, DUMP_BAL_READ}, states::{CacheAccount, cache::MyError}
    }, precompile::{bn254::pair, kzg_point_evaluation::init_load_kzg_trusted_setup}, primitives::{Address, B256, KECCAK_EMPTY, U256, address, alloy_primitives, hardfork::SpecId}, state::bal::Bal
};

use alloy_consensus::{transaction::Recovered, Block, EthereumTxEnvelope, Transaction, TxEip4844};

use rayon::ThreadPoolBuilder;
use rayon::{iter::Either, prelude::*};

use clap::{Parser, ValueEnum};
use std::thread;
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
    /// Skip 7702 txs. Default: false.
    #[arg(long, default_value_t = false)]
    skip_7702: bool,
    /// Pre recover sender. Default: false.
    #[arg(long, default_value_t = false)]
    pre_recover_sender: bool,
    #[arg(long)]
    datadir: Option<String>,
    #[arg(long, value_enum, default_value = "par")]
    io: IOPattern,
    #[arg(long)]
    db: Option<DBTy>,
    #[arg(long, default_value_t = false)]
    recover_db: bool,
    #[arg(long, default_value_t = false)]
    mock_db: bool,
    #[arg(long, default_value_t = 16)]
    io_threads: usize,
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

    /// Do not sort by gas limit.
    #[clap(alias = "bnone")]
    BigBlocksNone,
}

#[derive(Clone, Debug, ValueEnum, Default)]
pub enum IOPattern {
    /// Parallel I/O with tx-parallelism.
    #[default]
    #[clap(alias = "par")]
    Parallel,
    #[clap(alias = "batched")]
    /// Batched prefetching I/O given bal read locations.
    Batched,
}

#[derive(Clone, Debug, ValueEnum, Default)]
enum DBTy {
    #[default]
    #[clap(alias = "mdbx")]
    Mdbx,
    #[clap(alias = "rocksdb")]
    RocksDB,
}

#[derive(Clone)]
enum DBProvider {
    MdbxMockDB(MockProviderRW),
    MdbxMainnetDB(MainnetProviderRW),
    RocksDB(Store)
}

impl Deref for DBProvider {
    type Target = dyn flatdb::ProviderRW;

    // same as as_rw, just for convenent usage.
    fn deref(&self) -> &Self::Target {
        match self {
            DBProvider::MdbxMockDB(db) => db as _,
            DBProvider::MdbxMainnetDB(db) => db as _,
            DBProvider::RocksDB(db) => db as _,
        }
    }
}

impl DBProvider {
    pub fn as_rw(&self) -> &dyn flatdb::ProviderRW {
        match self {
            DBProvider::MdbxMockDB(db) => db,
            DBProvider::MdbxMainnetDB(db) => db,
            DBProvider::RocksDB(db) => db as _,
        }
    }
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
        println!("loading data......");
        // let recovered_blocks = import_struct(format!("./data/blocks_{nblocks}.json"));
        // let bals: Vec<Bal> = import_struct(format!("./data/bals_{nblocks}.json"));
        // let prestates = import_struct(format!("./data/prestates_{nblocks}.json"));
        // let block_hashes = import_struct(format!("./data/blockHashes_{nblocks}.json"));

        let blocks_f =
            thread::spawn(move || import_struct(format!("./data/blocks_{nblocks}.json")));

        let bals_f = thread::spawn(move || import_struct(format!("./data/bals_{nblocks}.json")));

        let prestates_f =
            thread::spawn(move || import_struct(format!("./data/prestates_{nblocks}.json")));

        let block_hashes_f =
            thread::spawn(move || import_struct(format!("./data/blockHashes_{nblocks}.json")));

        let recovered_blocks = blocks_f.join().unwrap();
        let bals: Vec<Bal> = bals_f.join().unwrap();
        let prestates: Vec<PreBlockState> = prestates_f.join().unwrap();
        let block_hashes = block_hashes_f.join().unwrap();

        println!("prestates to cache......");
        let blocks: Vec<alloy_consensus::Block<Recovered<EthereumTxEnvelope<TxEip4844>>>> =
            RecoveredBlockVec(recovered_blocks).into();
        let last_finalized_block = blocks[0].number - 1;

        let database_provider = self.datadir.as_ref().map(|datadir| {
            let db = self.db.as_ref().expect("Please provide db type!");
            match db {
                DBTy::Mdbx => {
                    if self.mock_db {
                        let datadir = "./tmp_mdbx";
                        // println!("Mdbx mocked tmp path:{:?}", datadir);
                        let provider_rw = MockProviderRW::new(datadir.into());
                        let all_pre_state = derive_pre_all_execution_state(&prestates);
                        provider_rw.set_preblock_state(&all_pre_state);
                        DBProvider::MdbxMockDB(provider_rw) 
                    } else {
                        // Don't prestate preblock state here to avoid prefetching the state.
                        let provider_rw = flatdb::MainnetProviderRW::new(datadir.into());
                        let db_finalized_bn = provider_rw.last_finalized_block_number().unwrap();
                        if db_finalized_bn != last_finalized_block {
                            panic!("Database finalized block number {} does not match the first block's parent number {}. Please ensure the database is synced to the correct state.", db_finalized_bn, last_finalized_block);
                        }
                        println!("Mdbx {} loaded at finalized block number {}", datadir,db_finalized_bn);
                        DBProvider::MdbxMainnetDB(provider_rw) 
                    }
                },
                DBTy::RocksDB => {
                    if self.mock_db {
                        // let tempdir = tempfile::Builder::new()
                        //     .prefix("_path_for_rocksdb_storage")
                        //     .tempdir()
                        //     .expect("Failed to create temporary path for the _path_for_rocksdb_storage");
                        // let datadir = tempdir.path();
                        let datadir = "./tmp_rocksdb";
                        println!("Rocksdb mocked tmp path:{:?}", datadir);
                        let provider_rw = Store::new_rocksdb_backend(datadir);
                        let all_pre_state = derive_pre_all_execution_state(&prestates);
                        provider_rw.set_preblock_state(&all_pre_state);
                        DBProvider::RocksDB(provider_rw) 
                    } else {
                        // Don't prestate preblock state here to avoid prefetching the state.
                        let provider_rw = Store::new_rocksdb_backend(datadir);
                        println!("RocksDB {} loaded at finalized block number {}", datadir, last_finalized_block);
                        DBProvider::RocksDB(provider_rw) 
                    }
                }
            }

        });

        // recover db separately, then drop caches and re-run to avoid prefetching effect.
        if self.recover_db {
            if let Some(provider_rw) = database_provider {
                let all_pre_state = derive_pre_all_execution_state(&prestates);
                provider_rw.as_rw().set_preblock_state(&all_pre_state);
                println!("state rewinds to block at {}", blocks[0].number-1);
            }
            return Ok(());
        }

        let caches = prestates_to_cachedbs(prestates.clone());

        let (gasused, batch_merged_bals, batch_merged_bal_reads) = if self.par {
            let gasused = import_struct(format!("./data/gasused_{nblocks}.json"));
            let batch_merged_bals = batch_merged_bal(&bals, &gasused, self);
            let bal_reads = import_struct(format!("./data/balread_{nblocks}.json"));
            let batch_merged_bal_reads = batch_merged_bal_read(&bal_reads, self);
            (gasused, batch_merged_bals, batch_merged_bal_reads)
        } else {
            (vec![], vec![], vec![])
        };

        let adpated_prestates = caches_to_prestates(caches, &bals, &blocks, self.pre_tx_state);

        println!("preloading kzg......");
        // preload kzg trusted setup
        init_load_kzg_trusted_setup(false);

        // set global threads number
        rayon::ThreadPoolBuilder::new()
            .num_threads(self.threads)
            .thread_name(|i| format!("rayon-{}", i))
            .build_global()
            .unwrap();

        println!("start executing......");
        let task_name = format!("threads: {}, blocks: {},", self.threads, bals.len(),);
        let (elapsed, (gas_used, commit_time)) = measure!(
            true,
            task_name,
            if self.par {
                assert_eq!(gasused.len(), blocks.len());
                match self.schedule_by_gaslimit {
                    PriorityOrder::None | PriorityOrder::BigBlocksNone => execute_blocks_par(
                        blocks,
                        batch_merged_bals,
                        adpated_prestates,
                        database_provider.clone(),
                        block_hashes,
                        gasused,
                        self,
                        batch_merged_bal_reads
                    ),
                    _ => (
                        execute_blocks_par_scheduler(
                            blocks,
                            bals,
                            adpated_prestates,
                            block_hashes,
                            gasused,
                            self,
                        ),
                        Duration::ZERO,
                    ),
                }
            } else {
                (
                    execute_blocks(blocks, bals, adpated_prestates, block_hashes, self),
                    Duration::ZERO,
                )
            }
        );

        println!(
            "total gas used:{}M, gas per second:{:?} MGas/s, execution time without commit:{:?}",
            gas_used / 1_000_000,
            gas_used / ((elapsed - commit_time).as_millis() as u64) / 1000,
            elapsed - commit_time,
        );

        // recover db after proccesing blocks separately, then drop caches and re-run to avoid prefetching effect.
        if let Some(provider_rw) = database_provider {
            let all_pre_state = derive_pre_all_execution_state(&prestates);
            provider_rw.as_rw().set_preblock_state(&all_pre_state);
            println!("state rewinds to block at {}", last_finalized_block);
        }

        Ok(())
    }
}

fn batch_merged_bal(bals: &Vec<Bal>, txs_gas_used: &Vec<Vec<u64>>, cmd_env: &Cmd) -> Vec<Bal> {
    let chunk_size = cmd_env.batch_blocks;
    let mut res = vec![];
    zip(bals.chunks(chunk_size), txs_gas_used.chunks(chunk_size)).for_each(
        |(chunked_bals, chunked_txs)| {
            let mut offset = 0;
            let mut bal = Bal::default();
            for (other, txs) in zip(chunked_bals, chunked_txs) {
                bal.merge_bal_with_offset(other.clone(), offset);
                offset += txs.len() as u64 + 2;
            }

            res.push(bal);
        },
    );

    res
}

fn batch_merged_bal_read(preblock_cache: &Vec<BalReadsTy>, cmd_env: &Cmd) -> Vec<BalReadsTy> {
    let chunk_size = cmd_env.batch_blocks;
    let mut res = vec![];
    preblock_cache.chunks(chunk_size).for_each(
        |chunked_preblock_cache| {
            let mut bal_read = BalReadsTy::default();
            for cache in chunked_preblock_cache {
                for (addr, keys) in cache {
                    let entry = bal_read.entry(*addr).or_default();
                    for key in keys {
                        entry.insert(*key);
                    }
                }
            }

            res.push(bal_read);
        },
    );

    res
}

/* for mock db, derive accessed state at the start block number.
 need to filter created account, so when it's first access if it's none don't update; if it doesn't exist just insert none.
 For account, if key doesn't exist (don't care where value non or not), insert;
 For storage, is account exist & code_hash !=None
    if storage key doesm't exist insert
For code, if codeHash doesn't exist insert.
 */
fn derive_pre_all_execution_state(caches: &[PreBlockState]) -> PreBlockState {
    let mut pre_all_state = PreBlockState::default();
    for cache in caches {
        // insert account and storage on if the addr or storage key is not exist (not value).
        for (addr, acct) in &cache.accounts {
            pre_all_state.insert_account(*addr, acct);
        }
    }

    pre_all_state
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
    cmd_env: &Cmd,
) -> u64 {
    let mut blocks_gas_used = vec![];
    let mut bal_reads = vec![];
    unsafe { DUMP_BAL_READ = true };
    let block_hashes = Arc::new(block_hashes);

    let mut total_clone_time = Duration::ZERO;

    let debug = cmd_env.debug;
    let par_7702 = cmd_env.par_7702;
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

        // // TODO: pre-block bals
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
            // println!("executing block:{} tx:{}, txhash:{}", bn, tx_index, tx.hash());
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
                0,
                tx,
                debug,
                par_7702,
                cmd_env.pre_recover_sender,
            );
            results.push((tx_index as u64 + 1, changes));
            total_clone_time += elasped;
        }

        // TODO: add post-block bals

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
        unsafe {
            bal_reads.push(BAL_READS.clone());
            BAL_READS = std::sync::LazyLock::new(|| BalReadsTy::default());
        }
        println!("block execution:{} done", bn);
    }
    println!("total clone time:{:?}", total_clone_time);
    println!("write block gas used!");
    write_data(
        format!("gasused_{}.json", cmd_env.nblocks).as_str(),
        &blocks_gas_used,
    );
    println!("write block bal reads!");
    write_data(
        format!("balread_{}.json", cmd_env.nblocks).as_str(),
        &bal_reads,
    );
    total_gas_used
}

fn handle_tx(
    block_env: &BlockEnv,
    block_hashes: Arc<BTreeMap<u64, B256>>,
    bal_ref: Option<&Bal>,
    cache: impl DatabaseRef,
    tx_index: u64, // tx index start from 0, while the first tx's bal index is 1
    offset: u64,
    tx: &Recovered<EthereumTxEnvelope<TxEip4844>>,
    debug: bool,
    par_7702: bool,
    pre_recover_sender: bool,
) -> (Option<Bal>, u64) {
    // println!(
    //     "=====block:{}, txidx:{}, txhash:{}=====",
    //     block_env.number,
    //     tx_index,
    //     tx.hash()
    // );

    let cached_state = State::builder()
        .with_block_hashes(block_hashes)
        .with_database_ref(cache)
        .build();
    let mut state = BalDatabase::new(cached_state)
        .with_bal_builder()
        .with_bal_option(bal_ref);
    state.bal_index = tx_index + offset + 1;

    let blocknumber = block_env.number;
    // Create EVM context for each transaction to ensure fresh state access
    let evm_context = Context::mainnet_par7702(par_7702)
        .with_block(block_env)
        .with_db(&mut state);

    let mut evm = evm_context.build_mainnet();
    let txenv = envelope_to_txenv(tx, pre_recover_sender);
    // println!(
    //     "txid {} sender: {:?}, kind:{:?}",
    //     index, txenv.caller, txenv.tx_type
    // );
    let exe_result = evm.transact(txenv);
    if exe_result.is_err() {
        eprintln!("{:?}", exe_result);
        panic!(
            "execution error for block: {} tx: {}, merged bal_index:{}, hash:{:?}",
            blocknumber,
            tx_index,
            state.bal_index,
            tx.hash()
        )
    }
    // must commit state changes, or bal builder will have nothing
    let exe_result = exe_result.unwrap();
    let gas_used = exe_result.result.gas_used();
    let result_state = exe_result.state;
    evm.commit(result_state);
    // println!(
    //     "bn:{}, txindex:{}, bal_index:{}, gasused:{}",
    //     blocknumber, tx_index, state.bal_index, gas_used
    // );
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
                total_gas_used += if cmd_env.skip_7702 && tx.is_eip7702() {
                    0
                } else {
                    block_txs_gas_used[tx_index]
                };
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
            PriorityOrder::None | PriorityOrder::BigBlocksNone => { /* no sort */ }
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

            // if max_elapsed > Duration::from_millis(10) {
            //     println!(
            //         "Block {} → tx #{} (0-based index), type:{},hash:{}, took the longest: {:?}",
            //         max_block_index,
            //         max_elapsed_idx,
            //         max_elapsed_tx.tx_type(),
            //         max_elapsed_tx.hash(),
            //         max_elapsed
            //     );
            // }

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
    batch_merged_bals: Vec<Bal>,
    prestates: Vec<Either<CacheState, Vec<CacheState>>>,
    db_rw: Option<DBProvider>,
    block_hashes: BTreeMap<u64, B256>,
    txs_gas_used: Vec<Vec<u64>>,
    cmd_env: &Cmd,
    batch_merged_bal_reads: Vec<BalReadsTy>,
) -> (u64, Duration) {
    let debug = cmd_env.debug;
    let start_block = blocks[0].number;
    let mut total_gas_used = 0;
    let batch = cmd_env.batch_blocks;
    let mut current_bn = blocks[0].number;
    let mut commit_time = Duration::ZERO;
    let mut prefetch_time = Duration::ZERO;
    let mut acct_prefetch_time = Duration::ZERO;
    let mut storage_prefetch_time = Duration::ZERO;

    let mut in_mem = VecDeque::with_capacity(2);

    let mut sum_longest_tx_time = Duration::ZERO;
    let block_hashes = Arc::new(block_hashes);
    for (chunk_idx, (chunk_blocks, batch_merged_bal)) in
        zip(blocks.chunks(batch), &batch_merged_bals)
            .into_iter()
            .enumerate()
    {
        let mut txs_bal_offsets = Vec::with_capacity(chunk_blocks.len());
        let mut tx_offset: u64 = 0;
        chunk_blocks.iter().for_each(|b| {
            txs_bal_offsets.push(tx_offset);
            tx_offset += b.body.transactions.len() as u64 + 2;
        });

        let shared_cache = Arc::new(SharedCache::new());
        let preblock_fetcher  = match db_rw.as_ref() {
            Some(db) => {
                // batched prefetching pre-block state
                let provider = match cmd_env.io {
                    IOPattern::Batched => {
                        let start = Instant::now();

                        let mut p = PreBlockStateCache::new(db.as_rw());
                        let bal_read = &batch_merged_bal_reads[chunk_idx];
                        let (acct_time, storage_time) = p.batch_preblock_state(bal_read, cmd_env.io_threads);
                        println!("prefetched all state for blocks:[{}-{}]", start_block*(chunk_idx as u64), start_block*((chunk_idx as u64)+1));

                        prefetch_time += start.elapsed();
                        acct_prefetch_time += acct_time;
                        storage_prefetch_time += storage_time;
                        Some(p)
                    },
                    IOPattern::Parallel => None,
                };
                provider
            },
            None => None,
        };

        let chunk_results = chunk_blocks
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

                let bal_ref = batch_merged_bal;
                let bal_offset = txs_bal_offsets[i];

                let results = block
                    .body
                    .transactions
                    .par_iter()
                    .enumerate()
                    .map(|(tx_index, tx)| {
                        if cmd_env.skip_7702 && tx.is_eip7702() {
                            return (tx_index as u64, 0, Duration::ZERO, tx, i, 0);
                        }
                        let (prestate, bal) = match db_rw.as_ref() {
                            Some(db) => {
                                let prestate = match &preblock_fetcher {
                                    Some(p) => Either::Left(Either::Left(p)),
                                    None => {
                                        // create a provider for each thread because read is a tx is rethdb.
                                        let provider = db.as_rw().lastest_provider_ro();
                                        let mt_cache = MTCache::new(provider, Arc::clone(&shared_cache), Some(&in_mem));
                                        Either::Left(Either::Right(mt_cache))
                                    }
                                };
                                (prestate, Some(bal_ref))
                            }
        
                            None => {
                                let block_idx = i + chunk_idx * batch;
                                let cache = &prestates[block_idx];
                                match cache {
                                    Either::Left(cache) => (Either::Right(cache), Some(bal_ref)),
                                    Either::Right(tx_caches) => {
                                        (Either::Right(&tx_caches[tx_index]), None)
                                    }
                                }
                            }
                        };

                        let (elapsed, (bal, gas_used)) = measure!(
                            false,
                            format!("tx {}", tx_index),
                            handle_tx(
                                &block_env,
                                block_hashes.clone(),
                                bal,
                                prestate,
                                tx_index as _,
                                bal_offset,
                                tx,
                                debug,
                                cmd_env.par_7702,
                                cmd_env.pre_recover_sender
                            )
                        );
                        // collect bal cause huge memeory allocation thus decrease performance about 8%.
                        (tx_index as u64 + 1, 0, elapsed, tx, i, gas_used)
                    })
                    .collect::<Vec<_>>();
                results
            })
            .collect::<Vec<_>>();

        println!(
            "commit block=====:{}-{}",
            start_block + (chunk_idx * batch) as u64,
            start_block + ((chunk_idx + 1) * batch) as u64
        );
        let commit_start = Instant::now();
        current_bn += chunk_blocks.len() as u64;
        if let Some(db) = db_rw.as_ref() {
            let latest_state = db.as_rw().commit_bal_changes(batch_merged_bal, current_bn);
            // cache last 2 block's state changes
            if in_mem.len() >= 2 {
                in_mem.pop_back();
            }
            in_mem.push_front(latest_state);
        }

        // if chunk_idx==1 { // only for debug purpose
        //     unsafe {DEBUG = false};
        //     panic!("exit batch 1");
        // }

        commit_time += commit_start.elapsed();

        if debug {
            let mut max_elapsed = Duration::ZERO;
            let mut max_elapsed_idx = 0;
            // should check chunk_results len first, because there might be empty blocks.
            // let mut max_elapsed_tx = &chunk_results[0].3;
            let mut max_block_index: usize = 0;
            for (bal_index_unmerged, _, elapsed, tx, block_index, _gas_used) in &chunk_results {
                if elapsed > &max_elapsed {
                    max_elapsed = *elapsed;
                    max_elapsed_idx = bal_index_unmerged - 1;
                    // max_elapsed_tx = tx;
                    max_block_index = *block_index;
                }
            }

            sum_longest_tx_time += max_elapsed;
            // if max_elapsed > Duration::from_millis(10) {
            //     println!(
            //         "Block {} → tx #{} (0-based index), type:{},hash:{}, took the longest: {:?}",
            //         max_block_index,
            //         max_elapsed_idx,
            //         max_elapsed_tx.tx_type(),
            //         max_elapsed_tx.hash(),
            //         max_elapsed
            //     );
            // }
        }
    }

    if debug {
        println!(
            "Sum of most time-consuming tx durations per block: {:?}",
            sum_longest_tx_time
        );
    }

    for (block, block_txs_gas_used) in zip(blocks, txs_gas_used) {
        for (tx_index, tx) in block.body.transactions.iter().enumerate() {
            total_gas_used += if cmd_env.skip_7702 && tx.is_eip7702() {
                0
            } else {
                block_txs_gas_used[tx_index]
            };
        }
    }


    // IO metrics
    let mut acct_reads = 0;
    let mut storage_reads = 0;
    for bal in &batch_merged_bal_reads{
        acct_reads += bal.len();
        for (_addr, keys) in bal {
            storage_reads += keys.len();
        }
    }

    println!("total commit time:{:?}", commit_time);
    println!("total prefetch time:{:?}, acct:{:?}(0 should be ignored), storage:{:?}(0 should be ignored)", prefetch_time, acct_prefetch_time, storage_prefetch_time);
    println!("total I/O:{}, account reads:{}, storage reads:{}, avg per I/O cost:{:.2} µs (0 should be ignored), avg acct cost:{:.2} µs, avg storage cost:{:.2} µs", acct_reads + storage_reads, acct_reads, storage_reads, prefetch_time.as_micros() as f64 / ((acct_reads + storage_reads)/cmd_env.io_threads) as f64, acct_prefetch_time.as_micros() as f64 / (acct_reads / cmd_env.io_threads) as f64, storage_prefetch_time.as_micros() as f64 / (storage_reads / cmd_env.io_threads) as f64);
    println!("execute_blocks_par complete!");
    (total_gas_used, commit_time)
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
                    if cmd_env.skip_7702 && tx.is_eip7702() {
                        result.push((
                            *tx_index as u64 + 1,
                            None,
                            Duration::ZERO,
                            tx,
                            block_index,
                            0,
                        ));

                        i += threads;
                        continue;
                    }
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
                            0,
                            tx,
                            debug,
                            cmd_env.par_7702,
                            cmd_env.pre_recover_sender
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
                if cmd_env.skip_7702 && tx.is_eip7702() {
                    res_sender
                        .send((
                            *tx_index as u64 + 1,
                            None,
                            Duration::ZERO,
                            tx,
                            block_index,
                            0,
                        ))
                        .expect("Failed to send result");
                    continue;
                }
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
                        0,
                        tx,
                        debug,
                        cmd_env.par_7702,
                        cmd_env.pre_recover_sender
                    )
                );

                res_sender
                    .send((
                        *tx_index as u64 + 1,
                        None,
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
    use alloy_primitives::{hex, keccak256};
    use revm::{
        context::{
            block_states::{import_struct, prestates_to_cachedbs, MyPlainAccount},
            transaction::AccessList,
            BlockEnv, ContextTr, TxEnv,
        },
        database::{bal::BalDatabase, State},
        primitives::{address, hex::FromHex, Address, HashMap, StorageKey, KECCAK_EMPTY, U256},
        state::{bal::Bal, AccountInfo, Bytecode},
        Context, ExecuteCommitEvm, ExecuteEvm, MainBuilder, MainContext,
    };
    use std::collections::BTreeMap;

    #[test]
    fn test_derive_pre_all_execution_state_works() {
        // case 1: cache1 a1 is none, cache2 a1 is non-none => cache should be none (cache 1).
        let mut caches = vec![];
        let mut c1 = PreBlockState::default();
        let a1 = address!("0x0000000000000000000000000000000000000001");
        c1.insert_account(a1, &MyPlainAccount::default());
        caches.push(c1.clone());

        let mut c1_new = PreBlockState::default();
        let mut cache_acct_1 = MyPlainAccount::default();
        let mut acct_1 = AccountInfo::default();
        acct_1.nonce = 1;
        cache_acct_1.info = Some(acct_1);
        c1_new.accounts.insert(a1, cache_acct_1);
        caches.push(c1_new);

        let res = derive_pre_all_execution_state(&caches);
        assert_eq!(res, c1);

        // case 2: cache1 a1 is non-none with nonce 1, cache2 a1 is non-none with nonce 2 => cache should be cache1.
        let mut caches = vec![];
        let mut c1 = PreBlockState::default();
        let a1 = address!("0x0000000000000000000000000000000000000001");
        let mut cache_acct_1 = MyPlainAccount::default();
        let mut acct_1 = AccountInfo::default();
        acct_1.nonce = 1;
        cache_acct_1.info = Some(acct_1);
        c1.accounts.insert(a1, cache_acct_1);
        caches.push(c1.clone());

        let mut c1_new = PreBlockState::default();
        let mut cache_acct_1 = MyPlainAccount::default();
        let mut acct_1 = AccountInfo::default();
        acct_1.nonce = 2;
        cache_acct_1.info = Some(acct_1);
        c1_new.accounts.insert(a1, cache_acct_1);
        caches.push(c1_new);

        let res = derive_pre_all_execution_state(&caches);
        assert_eq!(res, c1);

        // case 3: cache1 a1 is non-none with nonce 1, cache2 a2 is non-none with nonce 2 => cache should be cache1 ∪ cache 2.
        let mut caches = vec![];
        let mut c1 = PreBlockState::default();
        let a1 = address!("0x0000000000000000000000000000000000000001");
        let mut cache_acct_1 = MyPlainAccount::default();
        let mut acct_1 = AccountInfo::default();
        acct_1.nonce = 1;
        cache_acct_1.info = Some(acct_1);
        c1.accounts.insert(a1, cache_acct_1.clone());
        caches.push(c1.clone());

        let mut c2 = PreBlockState::default();
        let a2 = address!("0x0000000000000000000000000000000000000002");
        let mut cache_acct_2 = MyPlainAccount::default();
        let mut acct_2 = AccountInfo::default();
        acct_2.nonce = 2;
        cache_acct_2.info = Some(acct_2);
        c2.accounts.insert(a2, cache_acct_2.clone());
        caches.push(c2);

        let res = derive_pre_all_execution_state(&caches);
        assert_eq!(res.accounts.get(&a1), Some(&cache_acct_1));
        assert_eq!(res.accounts.get(&a2), Some(&cache_acct_2));

        // case 4: cache1 a1 is non-none with nonce 1, slot:{1:0}, cache2 a1 is non-none with nonce 2, slot{2:1} => cache should be nonce 1, slots: {1:0, 2:1}.
        let mut caches = vec![];
        let mut c1 = PreBlockState::default();
        let a1 = address!("0x0000000000000000000000000000000000000001");
        let mut cache_acct_1 = MyPlainAccount::default();
        let mut acct_1 = AccountInfo::default();
        acct_1.nonce = 1;
        cache_acct_1
            .storage
            .insert(StorageKey::from(1), StorageKey::from(1));
        cache_acct_1.info = Some(acct_1);
        c1.accounts.insert(a1, cache_acct_1.clone());
        caches.push(c1.clone());

        let mut c1_new = PreBlockState::default();
        let mut cache_acct_1 = MyPlainAccount::default();
        let mut acct_1 = AccountInfo::default();
        acct_1.nonce = 2;
        cache_acct_1
            .storage
            .insert(StorageKey::from(2), StorageKey::from(1));
        cache_acct_1.info = Some(acct_1);
        c1_new.accounts.insert(a1, cache_acct_1.clone());
        caches.push(c1_new);

        let res = derive_pre_all_execution_state(&caches);
        let mut expected_cache_acct = MyPlainAccount::default();
        let mut expected_acct = AccountInfo::default();
        expected_acct.nonce = 1;
        expected_cache_acct
            .storage
            .insert(StorageKey::from(1), StorageKey::from(1));
        expected_cache_acct
            .storage
            .insert(StorageKey::from(2), StorageKey::from(1));
        expected_cache_acct.info = Some(expected_acct);
        assert_eq!(res.accounts.get(&a1), Some(&expected_cache_acct));

        // case 5: cache1 a1 is non-none with code1, cache2 a1 with code2, a2 is code3 => cache should be {a1: code1, c2:code3}.
        let mut caches = vec![];
        let mut c1 = PreBlockState::default();
        let mut cache_acct_1 = MyPlainAccount::default();
        let code1 = hex!("01");
        let code_hash1 = keccak256(code1);
        let code1 = Bytecode::new_raw(code1.to_vec().into());
        let mut info = AccountInfo::default();
        info.code = Some(code1.clone());
        info.code_hash = code_hash1;
        cache_acct_1.info = Some(info);
        c1.accounts.insert(a1, cache_acct_1.clone());
        caches.push(c1.clone());

        let mut c1 = PreBlockState::default();
        let mut cache_acct_2 = MyPlainAccount::default();
        let code2 = hex!("02");
        let code_hash2 = keccak256(code2);
        let code2 = Bytecode::new_raw(code2.to_vec().into());
        let mut info = AccountInfo::default();
        info.code = Some(code2.clone());
        info.code_hash = code_hash2;
        cache_acct_2.info = Some(info);
        c1.accounts.insert(a1, cache_acct_2.clone());
        caches.push(c1.clone());

        let mut c1 = PreBlockState::default();
        let mut cache_acct_3 = MyPlainAccount::default();
        let code3 = hex!("03");
        let code_hash3 = keccak256(code3);
        let code3 = Bytecode::new_raw(code3.to_vec().into());
        let mut info = AccountInfo::default();
        info.code = Some(code3.clone());
        info.code_hash = code_hash3;
        cache_acct_3.info = Some(info);
        c1.accounts.insert(a2, cache_acct_3.clone());
        caches.push(c1.clone());

        let res = derive_pre_all_execution_state(&caches);
        assert_eq!(res.accounts.get(&a1), Some(&cache_acct_1));
        assert_eq!(res.accounts.get(&a2), Some(&cache_acct_3));
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
            state.insert_account_with_storage(address, account, HashMap::default());
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

        let mut cmd_env = Cmd::default();
        cmd_env.threads = 5;
        cmd_env.debug = true;
        execute_blocks(blocks, bals, prestates, block_hashes, &cmd_env);
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
