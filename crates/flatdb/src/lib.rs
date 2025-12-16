//! flatdb
pub mod node;

use std::{path::PathBuf, sync::Arc};

use alloy_primitives::{Address, BlockNumber, B256};
use reth_chainspec::{ChainSpec, MAINNET};
use reth_db::{init_db, mdbx::DatabaseArguments, tables, test_utils::TempDatabase, DatabaseEnv};
use reth_db_api::transaction::DbTxMut;
use reth_primitives_traits::StorageEntry;
use reth_provider::{
    providers::{ProviderNodeTypes, StaticFileProvider},
    test_utils::MockNodeTypesWithDB,
    ProviderFactory, *,
};
use revm::{
    database::{states::cache::MyError, CacheState},
    primitives::{HashMap, StorageKey, StorageValue},
    state::{bal::Bal, AccountInfo},
    DatabaseRef,
};

use revm::bytecode::Bytecode;

use crate::node::EthereumNode;

///
pub trait ProviderRW: DatabaseRef {
    ///
    fn set_preblock_state(&self, prestate: CacheState);
    ///
    fn set_accounts(&self, accts: HashMap<Address, AccountInfo>);
    ///
    fn set_codes(&self, codes: HashMap<B256, Bytecode>);
    ///
    fn set_storage(&self, addr: Address, storage: HashMap<StorageKey, StorageValue>);
    ///
    fn commit_bal_changes(&self, bal: &Bal, finalized_bn: BlockNumber);
    ///
    fn last_finalized_block_number(&self) -> Option<BlockNumber>;
}

///
pub type MainnetProviderRW = ProviderFactoryWrapper<EthereumNode>;

///
#[derive(Debug)]
pub struct ProviderFactoryWrapper<N: ProviderNodeTypes> {
    inner: ProviderFactory<N>,
}

impl ProviderFactoryWrapper<MockNodeTypesWithDB> {
    /// initialize mock database
    pub fn new(rootdir: PathBuf) -> Self {
        let db_path = rootdir.join("db");
        let sf_path = rootdir.join("static_files");

        let db = init_db(db_path.clone(), DatabaseArguments::default()).unwrap();
        let db = Arc::new(TempDatabase::new(db, db_path));

        let factory: ProviderFactory<MockNodeTypesWithDB> = ProviderFactory::new(
            db,
            MAINNET.clone(),
            StaticFileProvider::read_write(sf_path).expect("static file provider"),
        )
        .unwrap();

        ProviderFactoryWrapper { inner: factory }
    }
}

impl ProviderFactoryWrapper<EthereumNode> {
    /// initialize database
    pub fn new(rootdir: PathBuf) -> Self {
        let db_path = rootdir.join("db");
        let sf_path = rootdir.join("static_files");

        let db = init_db(db_path.clone(), DatabaseArguments::default()).unwrap();
        let db: Arc<DatabaseEnv> = Arc::new(db);

        let factory = ProviderFactory::new(
            db,
            MAINNET.clone(),
            StaticFileProvider::read_write(sf_path).expect("static file provider"),
        )
        .unwrap();

        ProviderFactoryWrapper { inner: factory }
    }
}

// impl<N: ProviderNodeTypes<DB = Arc<DatabaseEnv>, ChainSpec = ChainSpec>> ProviderFactoryWrapper<N> {
//     /// initialize database
//     pub fn new(rootdir: PathBuf) -> Self {
//         let db_path = rootdir.join("db");
//         let sf_path = rootdir.join("static_files");

//         let db = init_db(db_path.clone(), DatabaseArguments::default()).unwrap();
//         let db = Arc::new(db);

//         let factory = ProviderFactory::new(
//             db,
//             MAINNET.clone(),
//             StaticFileProvider::read_write(sf_path).expect("static file provider"),
//         )
//         .unwrap();

//         ProviderFactoryWrapper { inner: factory }
//     }
// }

impl<N: ProviderNodeTypes> ProviderRW for ProviderFactoryWrapper<N> {
    /// set preblock state in database before processing each block
    /// It's only used for testing with mock provider.
    fn set_preblock_state(&self, prestate: CacheState) {
        let provider = self.inner.provider_rw().unwrap();
        let db_tx = provider.tx_ref();

        for (addr, info) in prestate.accounts {
            if let Some(acct) = info.account {
                db_tx
                    .put::<tables::PlainAccountState>(addr, acct.info.into())
                    .unwrap();
                for (key, value) in acct.storage {
                    db_tx
                        .put::<tables::PlainStorageState>(
                            addr,
                            StorageEntry {
                                key: key.into(),
                                value,
                            },
                        )
                        .unwrap();
                }
            }
        }

        for (hash, code) in prestate.contracts {
            db_tx
                .put::<tables::Bytecodes>(
                    hash,
                    reth_primitives_traits::Bytecode::new_raw(code.bytes()),
                )
                .unwrap();
        }
        provider.commit().unwrap();
    }

    /// set account info
    fn set_accounts(&self, accts: HashMap<Address, AccountInfo>) {
        let provider = self.inner.provider_rw().unwrap();
        let db_tx = provider.tx_ref();
        for (addr, info) in accts {
            db_tx
                .put::<tables::PlainAccountState>(addr, info.into())
                .unwrap();
        }

        provider.commit().unwrap();
    }

    /// set code
    fn set_codes(&self, codes: HashMap<B256, Bytecode>) {
        let provider = self.inner.provider_rw().unwrap();
        let db_tx = provider.tx_ref();
        for (code_hash, code) in codes {
            db_tx
                .put::<tables::Bytecodes>(code_hash, code.into())
                .unwrap();
        }

        provider.commit().unwrap();
    }

    /// set storage
    fn set_storage(&self, addr: Address, storage: HashMap<StorageKey, StorageValue>) {
        let provider = self.inner.provider_rw().unwrap();
        let db_tx = provider.tx_ref();
        for (key, value) in storage {
            db_tx
                .put::<tables::PlainStorageState>(
                    addr,
                    StorageEntry {
                        key: key.into(),
                        value,
                    },
                )
                .unwrap();
        }

        provider.commit().unwrap();
    }

    fn commit_bal_changes(&self, bal: &Bal, finalized_bn: BlockNumber) {
        let provider_rw = self.inner.provider_rw().unwrap();
        let db_tx = provider_rw.tx_ref();

        for (addr, acct_bal) in bal.accounts.iter() {
            let info_bal = &acct_bal.account_info;
            let storage_bals = &acct_bal.storage;

            // fetch changed account info first
            let prev_info = provider_rw.basic_account(addr).unwrap().unwrap_or_default();
            let mut info: AccountInfo = prev_info.into();
            let max_bal_index = info_bal
                .balance
                .writes
                .len()
                .max(info_bal.nonce.writes.len())
                .max(info_bal.code.writes.len())
                + 1;

            // update changed codes in database
            if info_bal.code.writes.len() > 0 {
                let (code_hash, code) = info_bal.code.writes.last().unwrap().1.clone();
                // let code = Bytecode::new_raw(code_bytes.to_vec().into());
                db_tx
                    .put::<tables::Bytecodes>(code_hash, code.into())
                    .unwrap();
            }
            // update changed account info in database
            bal.populate_account_info(*addr, max_bal_index as _, &mut info)
                .unwrap();

            db_tx
                .put::<tables::PlainAccountState>(*addr, info.into())
                .unwrap();

            // update changed storages in database
            for (key, bal_writes) in storage_bals.storage.iter() {
                if bal_writes.writes.len() > 0 {
                    db_tx
                        .put::<tables::PlainStorageState>(
                            *addr,
                            StorageEntry {
                                key: (*key).into(),
                                value: bal_writes.writes.last().unwrap().1,
                            },
                        )
                        .unwrap();
                }
            }
        }

        provider_rw
            .save_finalized_block_number(finalized_bn)
            .unwrap();

        provider_rw.commit().unwrap();
    }

    fn last_finalized_block_number(&self) -> Option<BlockNumber> {
        let provider = self.inner.provider().unwrap();
        provider.last_finalized_block_number().ok().flatten()
    }
}

impl<N: ProviderNodeTypes> DatabaseRef for ProviderFactoryWrapper<N> {
    #[doc = " The database error type."]
    type Error = MyError;

    #[doc = " Gets basic account information."]
    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let provider = self.inner.provider().unwrap();
        let acct = provider.basic_account(&address);
        match acct {
            Ok(Some(acct)) => Ok(Some(acct.into())),
            Ok(None) => Ok(None),
            Err(_) => panic!("provider basic_ref error,addr:{:?}", address),
        }
    }

    #[doc = " Gets account code by its hash."]
    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        let provider = self.inner.latest().unwrap();
        let code = provider.bytecode_by_hash(&code_hash);
        match code {
            Ok(Some(code)) => Ok(code.into()),
            Ok(None) => Err(MyError {
                message: format!("code for codehash:{code_hash} not found"),
            }),
            Err(_) => panic!("provider code_by_hash_ref error,code_hash:{:?}", code_hash),
        }
    }

    #[doc = " Gets storage value of address at index."]
    fn storage_ref(
        &self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        let provider = self.inner.latest().unwrap();
        let val = provider.storage(address, index.into());
        match val {
            Ok(Some(val)) => Ok(val.into()),
            Ok(None) => Err(MyError {
                message: format!("storage for addr:{address}, key:{index} not found"),
            }),
            Err(_) => panic!("provider storage_ref error, addr:{address}, key:{index}"),
        }
    }

    #[doc = " Gets block hash by block number."]
    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{address, hex, keccak256, U256};
    use revm::{primitives::KECCAK_EMPTY, state::bal::AccountBal};

    use super::*;
    fn provider_with_db_type(mock: bool) {
        let provider: Box<dyn ProviderRW<Error = MyError>> = if mock {
            let p = ProviderFactoryWrapper::<EthereumNode>::new("./temp".into());
            Box::new(p)
        } else {
            let p = ProviderFactoryWrapper::<MockNodeTypesWithDB>::new("./temp".into());
            Box::new(p)
        };

        // set some accounts
        let mut prestate = CacheState::default();
        let addr = address!("0x1000000000000000000000000000000000000000");
        let mut acct = AccountInfo::default();
        acct.nonce = 1;
        acct.balance = U256::from(0x3635c9adc5dea00000u128);
        prestate.insert_account(addr, acct.clone());
        // preset some codes
        let code = hex!("5a465a905090036002900360015500");
        let code_hash = keccak256(code);
        let code = Bytecode::new_raw(code.to_vec().into());
        let mut codes = HashMap::default();
        codes.insert(code_hash, code.clone());
        // preset some storages
        let key = StorageKey::ONE;
        let value = StorageValue::ONE * StorageValue::from(4);
        let mut storage = HashMap::default();
        storage.insert(key, value);

        // set initial state in db
        provider.set_preblock_state(prestate);
        provider.set_codes(codes);
        provider.set_storage(addr, storage);

        // fetch account
        assert!(matches!(provider.basic_ref(addr), Ok(Some(acct_info)) if acct_info == acct));
        // fetch storage
        assert!(matches!(provider.storage_ref(addr, key), Ok(val_1) if val_1 == value));
        // fetch code
        assert!(matches!(provider.code_by_hash_ref(code_hash), Ok(code_1) if code_1 == code));
    }

    #[test]
    fn test_provider_should_work() {
        provider_with_db_type(true);
        provider_with_db_type(false);
    }

    #[test]
    fn test_provider_commit_bal_should_match() {
        let p = ProviderFactoryWrapper::<MockNodeTypesWithDB>::new("./temp".into());

        let addr1 = address!("0x0000000000000000000000000000000000000001");
        let addr2 = address!("0x0000000000000000000000000000000000000002");

        let mut bal = Bal::default();
        let mut acct_bal = AccountBal::default();
        acct_bal.account_info.nonce.writes.push((0, 1));
        acct_bal.account_info.nonce.writes.push((1, 2));
        acct_bal
            .account_info
            .balance
            .writes
            .push((0, U256::from(1000u64)));
        acct_bal
            .account_info
            .balance
            .writes
            .push((1, U256::from(2000u64)));

        bal.accounts.insert(addr1, acct_bal.clone());

        let code = hex!("5a465a905090036002900360015500");
        let code_hash = keccak256(code);
        let code = Bytecode::new_raw(code.to_vec().into());
        acct_bal
            .account_info
            .code
            .writes
            .push((0, (code_hash, code.clone())));

        bal.accounts.insert(addr2, acct_bal);

        // commit bal changes to db
        p.commit_bal_changes(&bal, 1);

        // verify changes
        assert!(
            matches!(p.basic_ref(addr1), Ok(Some(acct_info)) if acct_info.nonce == 2 && acct_info.balance == U256::from(2000u64) && acct_info.code_hash == KECCAK_EMPTY)
        );
        assert!(
            matches!(p.basic_ref(addr2), Ok(Some(acct_info)) if acct_info.code_hash == code_hash)
        );
        assert!(
            matches!(p.code_by_hash_ref(code_hash), Ok(code_1) if code_1 == code && keccak256(code_1.original_bytes()) == code_hash)
        );
    }

    #[test]
    fn test_mainnet_data() {
        let provider = ProviderFactoryWrapper::<EthereumNode>::new(
            "/root/test_nodes/ethereum/execution/reth_full_bak23600000/reth_full".into(),
        );

        let bn = provider.inner.best_block_number().unwrap();
        println!("best block number:{}", bn);
        let bn = provider
            .inner
            .provider()
            .unwrap()
            .last_finalized_block_number()
            .unwrap();
        println!("last block number:{:?}", bn);
        let provider_rw = provider.inner.provider_rw().unwrap();

        // let prev_bn = 23600000;
        provider_rw.save_finalized_block_number(23600000).unwrap();
        provider_rw.commit().unwrap();
        // set some accounts
        let addr = address!("0xdAC17F958D2ee523a2206206994597C13D831ec7");

        // fetch account
        let acct = provider.basic_ref(addr).unwrap();
        println!("acct:{:?}", acct);

        if let Some(acct) = acct {
            // fetch storage
            let key = StorageKey::ZERO;
            let storage = provider.storage_ref(addr, key);
            println!("storage:{:?}", storage);
            // fetch code
            let code = provider.code_by_hash_ref(acct.code_hash).unwrap();
            assert_eq!(keccak256(code.original_bytes()), acct.code_hash);
            println!("code len:{}", code.len());
        }
    }
}
