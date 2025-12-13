//! flatdb
pub mod node;

use std::{path::PathBuf, sync::Arc};

use alloy_primitives::{Address, B256};
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
    state::AccountInfo,
    DatabaseRef,
};

use revm::bytecode::Bytecode;

use crate::node::EthereumNode;

///
pub trait ProviderOps: DatabaseRef {
    ///
    fn initialize_state(&self, prestate: CacheState);
    ///
    fn set_accounts(&self, accts: HashMap<Address, AccountInfo>);
    ///
    fn set_codes(&self, codes: HashMap<B256, Bytecode>);
    ///
    fn set_storage(&self, addr: Address, storage: HashMap<StorageKey, StorageValue>);
}

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

impl<N: ProviderNodeTypes> ProviderOps for ProviderFactoryWrapper<N> {
    /// initialize database
    fn initialize_state(&self, prestate: CacheState) {
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

    use super::*;
    fn provider_with_db_type(mock: bool) {
        let provider: Box<dyn ProviderOps<Error = MyError>> = if mock {
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
        provider.initialize_state(prestate);
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
}
