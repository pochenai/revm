use rayon::ThreadPoolBuilder;
use std::collections::{HashMap, HashSet};

use alloy_primitives::{Address, B256};
use rayon::prelude::*;
use revm::{
    database::states::cache::MyError,
    primitives::{StorageKey, StorageValue},
    state::{bal::Bal, AccountInfo, Bytecode},
    DatabaseRef,
};

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

///
#[derive(Debug, Default)]
pub struct BALRead {
    ///
    pub reads: HashMap<Address, HashSet<StorageKey>>,
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

    /// TODO: reset rayon threads number
    pub fn batch_preblock_state(&mut self, bal_read: &BALRead, threads: usize) {
        let pool = ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap();

        pool.install(|| {
            // lastest_provider_ro is a wrapper for LatestStateProvider
            let accounts = bal_read
                .reads
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

            // storage
            // let storage: HashMap<_, _> = addrs
            //     .par_iter()
            //     .map_init(
            //         || self.db.lastest_provider_ro(), // worker-local
            //         |provider_ro_up, address| {
            //             let acct_bal = bal.accounts.get(address).unwrap();

            //             let storage = acct_bal
            //                 .storage
            //                 .storage
            //                 .par_iter()
            //                 .map(|(key, _)| {
            //                     let k: StorageKey = (*key).into();
            //                     let v = provider_ro_up.storage_ref(*address, k.into()).unwrap();
            //                     (k, v)
            //                 })
            //                 .collect::<HashMap<_, _>>();

            //             (*address, storage)
            //         },
            //     )
            //     .collect();

            self.accounts = accounts;
        });

        // not need, because basic_ref will fetch acct code too
        // let code_hashes = accounts
        //     .iter()
        //     .filter_map(|(address, acct)| {
        //         if let Some(info) = &acct.info {
        //             Some(info.code_hash)
        //         } else {
        //             None
        //         }
        //     })
        //     .collect::<Vec<_>>();

        // let contracts = code_hashes
        //     .par_iter()
        //     .map(|code_hash| {
        //         let code = provider_ro.code_by_hash_ref(*code_hash).unwrap();
        //         (*code_hash, code.into())
        //     })
        //     .collect::<HashMap<_, _>>();
        // self.contracts = contracts;
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
