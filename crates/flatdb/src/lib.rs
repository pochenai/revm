//! flatdb
///
pub mod mtcache;
///
pub mod node;
///
pub mod preblock_db_provider;

use std::{ops::Deref, path::PathBuf, sync::Arc};

use alloy_primitives::{address, b256, Address, BlockNumber, B256, KECCAK256_EMPTY, U256};
use reth_chainspec::{ChainSpec, MAINNET};
use reth_db::{
    init_db, mdbx::DatabaseArguments, tables, test_utils::TempDatabase, transaction::DbTx,
    DatabaseEnv,
};
use reth_db_api::cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW};
use reth_db_api::transaction::DbTxMut;
use reth_primitives_traits::StorageEntry;
// re-export
pub use reth_provider::{
    providers::{ProviderNodeTypes, StaticFileProvider},
    test_utils::MockNodeTypesWithDB,
    ProviderFactory, *,
};
use revm::context::block_states::MyPlainAccount;
use revm::database::states::plain_account::PlainStorage;
use revm::{
    context::block_states::PreBlockState,
    database::{states::cache::MyError, CacheState},
    primitives::{HashMap, StorageKey, StorageValue},
    state::{bal::Bal, AccountInfo},
    DatabaseRef,
};

use revm::bytecode::Bytecode;

use crate::node::EthereumNode;

///
pub trait ProviderRW: Sync {
    ///
    fn set_preblock_state(&self, prestate: &PreBlockState);
    ///
    fn set_storage(&self, addr: Address, storage: HashMap<StorageKey, StorageValue>);
    ///
    fn commit_bal_changes(&self, bal: &Bal, finalized_bn: BlockNumber) -> PreBlockState;
    ///
    fn last_finalized_block_number(&self) -> Option<BlockNumber>;
    /// Create a shared provider for one tx to avoid redudant heap allocation,
    ///  it's almost 50% faster than create a lastest provider for each read.
    fn lastest_provider_ro<'a>(&'a self) -> Box<dyn DatabaseRef<Error = MyError> + 'a>;
}

///
pub type MainnetProviderRW = ProviderFactoryWrapper<EthereumNode>;
///
pub type MockProviderRW = ProviderFactoryWrapper<MockNodeTypesWithDB>;
///
#[derive(Debug, Clone)]
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

    /// inner provider
    pub fn provider_ro(
        &self,
    ) -> DatabaseProvider<reth_db::mdbx::tx::Tx<reth_db::mdbx::RO>, EthereumNode> {
        self.inner.provider().unwrap()
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
    fn set_preblock_state(&self, prestate: &PreBlockState) {
        let provider = self.inner.provider_rw().unwrap();
        let db_tx = provider.into_tx();

        for (addr, all) in &prestate.accounts {
            // if acct doesn't exist, we must set it to None! Or the mainnet db state will be incorrect!
            let acct = match &all.info {
                Some(v) => v,
                None => &AccountInfo::default(),
            };
            db_tx
                .put::<tables::PlainAccountState>(*addr, acct.into())
                .unwrap();
            if let Some(code) = &acct.code {
                db_tx
                    .put::<tables::Bytecodes>(acct.code_hash, code.clone().into())
                    .unwrap();
            }

            for (key, value) in &all.storage {
                let mut cursor = db_tx
                    .cursor_dup_write::<tables::PlainStorageState>()
                    .unwrap();
                let entry = StorageEntry::new((*key).into(), value.clone().into());
                if let Some(mut db_entry) = cursor.seek_by_key_subkey(*addr, entry.key).unwrap() {
                    loop {
                        // Break if the subkey changes, to prevent iterating into a different dup range.
                        if db_entry.key != entry.key {
                            break;
                        }

                        cursor.delete_current().unwrap();

                        match cursor.next_dup().unwrap() {
                            Some((addr, next)) => {
                                db_entry = next;
                            }
                            None => break,
                        }
                    }
                }
                db_tx
                    .put::<tables::PlainStorageState>(*addr, entry)
                    .unwrap();
            }
        }

        db_tx.commit().unwrap();
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

    fn commit_bal_changes(&self, bal: &Bal, finalized_bn: BlockNumber) -> PreBlockState {
        let mut latest_state = PreBlockState::default();
        let factory = self.inner.provider_rw().unwrap();
        let db_tx = factory.into_tx();

        for (addr, acct_bal) in bal.accounts.iter() {
            let info_bal = &acct_bal.account_info;
            let storage_bals = &acct_bal.storage;

            // fetch changed account info first. Must use basic_ref instead of provider_ro.basic_account to also fetch code.
            let prev_info = self.basic_ref(*addr).unwrap().unwrap_or_default();
            let mut info: AccountInfo = prev_info.clone();
            let max_bal_index = info_bal
                .balance
                .writes
                .last()
                .map_or(0, |(v, _)| *v)
                .max(info_bal.nonce.writes.last().map_or(0, |(v, _)| *v))
                .max(info_bal.code.writes.last().map_or(0, |(v, _)| *v))
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
            if max_bal_index > 1 {
                acct_bal.populate_account_info(max_bal_index as _, &mut info);
            }

            let mut latest_info = None;

            if info.is_empty() {
                db_tx
                    .delete::<tables::PlainAccountState>(*addr, Some(prev_info.into()))
                    .unwrap();
            } else {
                latest_info = Some(info.clone());
                db_tx
                    .put::<tables::PlainAccountState>(*addr, info.into())
                    .unwrap();
            }

            let mut latest_storage = PlainStorage::default();
            // update changed storages in database
            for (key, bal_writes) in storage_bals.storage.iter() {
                if bal_writes.writes.len() > 0 {
                    let mut cursor = db_tx
                        .cursor_dup_write::<tables::PlainStorageState>()
                        .unwrap();
                    let entry = StorageEntry::new(
                        (*key).into(),
                        bal_writes.writes.last().unwrap().1.into(),
                    );

                    latest_storage.insert(*key, bal_writes.writes.last().unwrap().1);

                    // must delete existing plainStorage first, becaust put in plainStorage is actually append then sort. If use get, you'll alway get the smallest slot value instead of the recent value.
                    // ref: https://github.com/paradigmxyz/reth/blob/a672700b4fae17fc3622a93e62e7fefe64ccc78d/crates/storage/provider/src/providers/database/provider.rs#L1786
                    if let Some(mut db_entry) = cursor.seek_by_key_subkey(*addr, entry.key).unwrap()
                    {
                        loop {
                            // Break if the subkey changes, to prevent iterating into a different dup range.
                            if db_entry.key != entry.key {
                                break;
                            }

                            cursor.delete_current().unwrap();

                            match cursor.next_dup().unwrap() {
                                Some((addr, next)) => {
                                    db_entry = next;
                                }
                                None => break,
                            }
                        }
                    }

                    cursor.upsert(*addr, &entry).expect("upsert error");
                }
            }

            let latest_acct = MyPlainAccount {
                info: latest_info,
                storage: latest_storage,
            };

            latest_state.accounts.insert(*addr, latest_acct);
        }

        db_tx.commit().unwrap();
        // self.inner
        //     .provider_rw()
        //     .unwrap()
        //     .save_finalized_block_number(finalized_bn)
        //     .unwrap();

        latest_state
    }

    fn last_finalized_block_number(&self) -> Option<BlockNumber> {
        let provider = self.inner.provider().unwrap();
        provider.last_finalized_block_number().ok().flatten()
    }

    /// create a lastest provider for a batched blocks.
    fn lastest_provider_ro(&self) -> Box<dyn DatabaseRef<Error = MyError>> {
        // here the Boxed provider is returned. I've tried with non-boxed provider, but the perf diff is minimal.
        Box::new(LatestProvider(self.inner.latest().unwrap()))
    }
}

/// Wrapper for latest provider to minize Box allocation for each underlying latest provider.
/// If without this, the performance will downgrade ~50%!.
pub struct LatestProvider(Box<dyn StateProvider>);

impl DatabaseRef for LatestProvider {
    #[doc = " The database error type."]
    type Error = MyError;

    #[doc = " Gets basic account information."]
    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let provider = &self.0;
        let acct = provider.basic_account(&address);
        match acct {
            Ok(Some(acct)) => {
                // must get code along with basic account.
                let code_hash = acct.get_bytecode_hash();
                let code = if code_hash != KECCAK256_EMPTY {
                    Some(
                        provider
                            .bytecode_by_hash(&code_hash)
                            .unwrap()
                            .unwrap()
                            .into(),
                    )
                } else {
                    None
                };
                let mut acct_info: AccountInfo = acct.into();
                acct_info.code = code;
                Ok(Some(acct_info))
            }
            Ok(None) => Ok(None),
            Err(_) => panic!("provider basic_ref error,addr:{:?}", address),
        }
    }

    #[doc = " Gets account code by its hash."]
    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        let provider = &self.0;
        let code = provider.bytecode_by_hash(&code_hash);
        match code {
            Ok(Some(code)) => Ok(code.into()),
            Ok(None) => Err(MyError {
                message: format!("code for codehash:{code_hash} not found"),
            }),
            // Ok(None) => Ok(Bytecode::new()),
            Err(_) => panic!("provider code_by_hash_ref error,code_hash:{:?}", code_hash),
        }
    }

    #[doc = " Gets storage value of address at index."]
    fn storage_ref(
        &self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        let provider = &self.0;
        let val = provider.storage(address, index.into());
        match val {
            Ok(Some(val)) => Ok(val.into()),
            Ok(None) => Ok(StorageValue::default()),
            Err(_) => panic!("provider storage_ref error, addr:{address}, key:{index}"),
        }
    }

    #[doc = " Gets block hash by block number."]
    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        todo!()
    }
}

// Don't use this provider due to it'll alloc a lastet provider for each read!
// It only exists to experiment the performance diff.
impl<N: ProviderNodeTypes> DatabaseRef for ProviderFactoryWrapper<N> {
    #[doc = " The database error type."]
    type Error = MyError;

    #[doc = " Gets basic account information."]
    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let provider: Box<dyn StateProvider> = self.inner.latest().unwrap();
        let acct = provider.basic_account(&address);
        match acct {
            Ok(Some(acct)) => {
                // must get code along with basic account.
                let code_hash = acct.get_bytecode_hash();
                let code = if code_hash != KECCAK256_EMPTY {
                    Some(
                        provider
                            .bytecode_by_hash(&code_hash)
                            .unwrap()
                            .unwrap()
                            .into(),
                    )
                } else {
                    None
                };
                let mut acct_info: AccountInfo = acct.into();
                acct_info.code = code;
                Ok(Some(acct_info))
            }
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

// impl<N: ProviderNodeTypes> Deref for ProviderFactoryWrapper<N> {
//     type Target = ProviderFactory<N>;

//     fn deref(&self) -> &Self::Target {
//         &self.inner
//     }
// }

#[cfg(test)]
mod tests {
    use alloy_primitives::{address, hex, keccak256, U256};
    use reth_primitives_traits::Account;
    use revm::{
        context::block_states::MyPlainAccount,
        primitives::KECCAK_EMPTY,
        state::bal::{AccountBal, BalWrites},
    };

    use super::*;
    fn provider_with_db_type(mock: bool) {
        let factory: Box<dyn ProviderRW> = if mock {
            let p = ProviderFactoryWrapper::<EthereumNode>::new("./temp".into());
            Box::new(p)
        } else {
            let p = ProviderFactoryWrapper::<MockNodeTypesWithDB>::new("./temp".into());
            Box::new(p)
        };

        // set some accounts
        let mut prestate: PreBlockState = PreBlockState::default();
        let addr = address!("0x1000000000000000000000000000000000000000");
        let mut acct = AccountInfo::default();
        acct.nonce = 1;
        acct.balance = U256::from(0x3635c9adc5dea00000u128);
        // preset some codes
        let code = hex!("5a465a905090036002900360015500");
        let code_hash = keccak256(code);
        let code = Bytecode::new_raw(code.to_vec().into());
        acct.code_hash = code_hash;
        acct.code = Some(code.clone());
        prestate.update_accountinfo(addr, acct.clone());

        // preset some storages
        let key = StorageKey::ONE;
        let value = StorageValue::ONE * StorageValue::from(4);
        let mut storage = HashMap::default();
        storage.insert(key, value);

        // set initial state in db
        factory.set_preblock_state(&prestate);
        factory.set_storage(addr, storage);

        let provider = factory.lastest_provider_ro();
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
        let addr1 = address!("0x0000000000000000000000000000000000000001");
        let addr2 = address!("0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2");

        let p = ProviderFactoryWrapper::<MockNodeTypesWithDB>::new("./temp".into());

        let slot_key = StorageKey::from_str_radix(
            "f81d8d79f42adb4c73cc3aa0c78e25d3343882d0313c0b80ece3d3a103ef1ec2",
            16,
        )
        .unwrap();
        let slot_val =
            StorageKey::from_str_radix("6912101300000000000000005d48e1115dd60da0", 16).unwrap();
        let mut c1 = PreBlockState::default();
        let mut cache_acct_1 = MyPlainAccount::default();
        cache_acct_1.storage.insert(slot_key, slot_val);
        c1.accounts.insert(addr2, cache_acct_1.clone());
        p.set_preblock_state(&c1);

        assert!(
            matches!(p.lastest_provider_ro().storage_ref(addr2, slot_key), Ok(val) if val == slot_val)
        );

        let factory = Some(p);

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
        let slot_val_new =
            StorageValue::from_str_radix("6912103700000000000000005d48e1115dd60da0", 16).unwrap();
        acct_bal
            .storage
            .storage
            .insert(slot_key, BalWrites::new(vec![(1, slot_val_new)]));

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
        if let Some(p) = factory.as_ref() {
            // let p = provider.as_ref().unwrap();
            p.commit_bal_changes(&bal, 0);

            let provider = p.lastest_provider_ro();
            // verify changes
            assert!(
                matches!(provider.basic_ref(addr1), Ok(Some(acct_info)) if acct_info.nonce == 2 && acct_info.balance == U256::from(2000u64) && acct_info.code_hash == KECCAK_EMPTY)
            );
            assert!(
                matches!(provider.basic_ref(addr2), Ok(Some(acct_info)) if acct_info.code_hash == code_hash)
            );
            assert!(
                matches!(provider.code_by_hash_ref(code_hash), Ok(code_1) if code_1 == code && keccak256(code_1.original_bytes()) == code_hash)
            );
            assert!(
                matches!(provider.storage_ref(addr2, slot_key), Ok(val) if val == slot_val_new)
            );
        }
    }

    #[test]
    fn test_mainnet_data() {
        let factory = ProviderFactoryWrapper::<EthereumNode>::new(
            "/root/test_nodes/ethereum/execution/reth_full_bak".into(),
        );

        let bn = factory.inner.best_block_number().unwrap();
        println!("best block number:{}", bn);
        let bn = factory
            .inner
            .provider()
            .unwrap()
            .last_finalized_block_number()
            .unwrap()
            .unwrap();
        println!("last block number:{:?}", bn);
        let provider_rw = factory.inner.provider_rw().unwrap();

        // let bn = 23769999;
        provider_rw.save_finalized_block_number(bn).unwrap();
        provider_rw.commit().unwrap();
        // get some accounts
        let addr = address!("0xdAC17F958D2ee523a2206206994597C13D831ec7");
        let addr = address!("0x70bC1e16513aD49Bd28c20b7b50679381a71ADF5");

        let provider = factory.lastest_provider_ro();
        // fetch account
        let acct = provider.basic_ref(addr).unwrap();
        println!("acct:{:?}", Account::from(acct.clone().unwrap()));

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
