use crossbeam::channel;
use rayon::ThreadPoolBuilder;
use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use alloy_primitives::{Address, B256};
use rayon::prelude::*;
use revm::{
    database::{bal::BalReadsTy, states::cache::MyError},
    primitives::{StorageKey, StorageValue},
    state::{bal::Bal, AccountInfo, Bytecode},
    DatabaseRef,
};
use std::sync::Arc;

use crate::ProviderRW;

///
pub struct PreBlockStateCache<'a> {
    db: &'a dyn ProviderRW,
    accounts: HashMap<Address, PlainAccount>,
    // storage: HashMap<Address, HashMap<StorageKey, StorageValue>>,
    // Created contracts
    // contracts: HashMap<B256, Bytecode>,
}

#[derive(Debug)]
struct PlainAccount {
    info: Option<AccountInfo>,
    storage: HashMap<StorageKey, StorageValue>,
}

impl<'a> PreBlockStateCache<'a> {
    ///
    pub fn new(db: &'a dyn ProviderRW) -> Self {
        Self {
            db: db,
            accounts: HashMap::default(),
            // storage: HashMap::default(),
            // contracts: HashMap::default(),
        }
    }

    /// schedule with crossbeam channels + rayon: nothing improved
    pub fn batch_preblock_state_channel(
        &mut self,
        bal_read: &BalReadsTy,
        threads: usize,
    ) -> (Duration, Duration) {
        // =========================
        // 1. account initialization
        // =========================
        let start = Instant::now();

        let db = &self.db;
        let mut accounts: HashMap<_, _> = bal_read
            .par_iter()
            .map_init(
                || db.lastest_provider_ro(),
                |provider_ro, (address, _)| {
                    let info = provider_ro.basic_ref(*address).unwrap();
                    (
                        *address,
                        PlainAccount {
                            info,
                            storage: HashMap::default(),
                        },
                    )
                },
            )
            .collect();

        let acct_duration = start.elapsed();

        // =========================
        // 2. task / result channel
        // =========================
        type Task = (Address, StorageKey);
        type ResultItem = (Address, StorageKey, StorageValue);

        let (task_tx, task_rx) = channel::unbounded::<Task>();
        let (res_tx, res_rx) = channel::unbounded::<ResultItem>();

        // =========================
        // 3. producer (address, slot)
        // =========================
        let start = Instant::now();
        for (address, slots) in bal_read {
            for key in slots {
                let k: StorageKey = (*key).into();
                task_tx.send((*address, k)).unwrap();
            }
        }
        drop(task_tx); // close task channel

        // =========================
        // 4. workers：parallel fetching storage_ref
        // =========================

        let collector = std::thread::spawn(move || {
            while let Ok((addr, key, value)) = res_rx.recv() {
                if let Some(acct) = accounts.get_mut(&addr) {
                    acct.storage.insert(key, value);
                }
            }
            accounts
        });

        rayon::scope(|s| {
            let n = rayon::current_num_threads();
            println!("PreBlockStateCache: spawn {} workers", n);

            for _ in 0..n {
                let task_rx = task_rx.clone();
                let res_tx = res_tx.clone();

                s.spawn(move |_| {
                    let provider = db.lastest_provider_ro();
                    while let Ok((addr, key)) = task_rx.recv() {
                        let value = provider.storage_ref(addr, key).unwrap();
                        res_tx.send((addr, key, value)).unwrap();
                    }
                });
            }
        });
        drop(res_tx); // close result channel after all workers finished

        // =========================
        // 5. collector：merge storage in a single thread
        // =========================
        self.accounts = collector.join().unwrap();
        let storage_duration = start.elapsed();
        (acct_duration, storage_duration)
    }

    /// schedule with rayon
    /// TODO: reset rayon threads number
    pub fn batch_preblock_state(
        &mut self,
        bal_read: &BalReadsTy,
        threads: usize,
    ) -> (Duration, Duration) {
        let start = Instant::now();
        let mut accounts = bal_read
            .par_iter()
            .map_init(
                || self.db.lastest_provider_ro(), // create a provider for each thread
                |provider_ro, (address, _)| {
                    let info = provider_ro.basic_ref(*address).unwrap();
                    (
                        *address,
                        PlainAccount {
                            info,
                            storage: HashMap::default(),
                        },
                    )
                },
            )
            .collect::<HashMap<_, _>>();
        let acct_duration = start.elapsed();

        // storage
        let start = Instant::now();
        let mut storage: HashMap<_, _> = bal_read
            .par_iter()
            .map(|(address, slots)| {
                let storage = slots
                    .par_iter()
                    .map_init(
                        || self.db.lastest_provider_ro(),
                        |provider_ro, key| {
                            let k: StorageKey = (*key).into();
                            let v = provider_ro.storage_ref(*address, k).unwrap();
                            (k, v)
                        },
                    )
                    .collect::<HashMap<_, _>>();

                (*address, storage)
            })
            .collect();

        for (addr, plain_acct) in &mut accounts {
            let s = storage.remove(addr).unwrap();
            plain_acct.storage = s;
        }
        self.accounts = accounts;
        let storage_duration = start.elapsed();

        (acct_duration, storage_duration)
    }

    /// nested parallel prefetching scheduler
    pub fn batch_preblock_state_nested(
        &mut self,
        bal_read: &BalReadsTy,
        threads: usize,
    ) -> (Duration, Duration) {
        // nested thread pool: no performance degradation found compared with parallel account then parallel storage
        let pool = ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap();

        pool.install(|| {
            // lastest_provider_ro is a wrapper for LatestStateProvider
            let accounts = bal_read
                .par_iter()
                .map_init(
                    || self.db.lastest_provider_ro(), // create a provider for each thread
                    |provider_ro, (address, slots)| {
                        let info = provider_ro.basic_ref(*address).unwrap();
                        let storage = slots
                            .par_iter()
                            .map_init(
                                || self.db.lastest_provider_ro(),
                                |provider_ro, key| {
                                    let k: StorageKey = (*key).into();
                                    let v = provider_ro.storage_ref(*address, k.into()).unwrap();
                                    (k, v)
                                },
                            )
                            .collect::<HashMap<_, _>>();

                        (*address, PlainAccount { info, storage })
                    },
                )
                .collect::<HashMap<_, _>>();
            self.accounts = accounts;
        });
        (Duration::ZERO, Duration::ZERO)
    }
}

impl<'a> DatabaseRef for PreBlockStateCache<'a> {
    #[doc = " The database error type."]
    type Error = MyError;

    #[doc = " Gets basic account information."]
    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        if let Some(acct) = self.accounts.get(&address) {
            Ok(acct.info.clone())
        } else {
            Ok(None)
        }
    }

    #[doc = " Gets account code by its hash."]
    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        todo!("basic_ref already fetchs code")
    }

    #[doc = " Gets storage value of address at index."]
    fn storage_ref(
        &self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        if let Some(s) = self.accounts.get(&address) {
            if let Some(val) = s.storage.get(&index) {
                return Ok(*val);
            }
        }
        Ok(StorageValue::ZERO)
    }

    #[doc = " Gets block hash by block number."]
    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        todo!()
    }
}
