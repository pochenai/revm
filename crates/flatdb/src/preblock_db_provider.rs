use std::collections::HashMap;

use alloy_primitives::{Address, B256};
use rayon::prelude::*;
use revm::{
    primitives::{StorageKey, StorageValue},
    state::{bal::Bal, Bytecode},
};

use crate::MainnetProviderRW;
use reth_provider::{ProviderFactory, *};
struct PreBlockStateCache {
    db: MainnetProviderRW,
    pub accounts: HashMap<Address, Option<PlainAccount>>,
    /// Created contracts
    pub contracts: HashMap<B256, Bytecode>,
}

struct PlainAccount {
    info: Option<reth_primitives_traits::Account>,
    storage: HashMap<StorageKey, StorageValue>,
}

impl PreBlockStateCache {
    pub fn new(provider: MainnetProviderRW) -> Self {
        Self {
            db: provider,
            accounts: HashMap::new(),
            contracts: HashMap::new(),
        }
    }
    

    // TODO: reset rayon threads number
    pub fn batch_preblock_state(&mut self, bal: &Bal) {
        let addrs = bal
            .accounts
            .iter()
            .map(|(k, _)| *k)
            .collect::<Vec<Address>>();
        let provider_ro = self.db.latest().unwrap();
        let accounts = addrs
            .par_iter()
            .map(|address| {
                let acct = provider_ro.basic_account(address).unwrap();
                match acct {
                    Some(acct) => {
                        let acct_bal = bal.accounts.get(address).unwrap();
                        let storage = acct_bal
                            .storage
                            .storage
                            .par_iter()
                            .map(|(key, _)| {
                                (
                                    *key as StorageKey,
                                    provider_ro
                                        .storage(*address, (*key as StorageKey).into())
                                        .unwrap()
                                        .unwrap(),
                                )
                            })
                            .collect::<HashMap<_, _>>();
                        (
                            *address,
                            Some(PlainAccount {
                                info: Some(acct),
                                storage,
                            }),
                        )
                    }
                    None => (*address, None),
                }
            })
            .collect::<HashMap<_, _>>();

        let code_hashes = accounts
            .iter()
            .filter_map(|(address, acct)| {
                if let Some(acct) = acct {
                    if let Some(info) = acct.info {
                        info.bytecode_hash
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let contracts = code_hashes
            .par_iter()
            .map(|code_hash| {
                let code = provider_ro.bytecode_by_hash(code_hash).unwrap().unwrap();
                (*code_hash, code.into())
            })
            .collect::<HashMap<_, _>>();

        self.accounts = accounts;
        self.contracts = contracts;
    }
}
