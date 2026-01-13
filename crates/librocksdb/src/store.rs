use alloy_primitives::{Address, StorageKey, StorageValue, B256};
pub use ethrex_storage::api::tables::{ACCOUNT_CODES, ACCOUNT_FLATKEYVALUE, STORAGE_FLATKEYVALUE};
use ethrex_storage::api::{PrefixResult, StorageReadView};
use ethrex_storage::backend::rocksdb::RocksDBBackend;
use ethrex_storage::{api::StorageBackend, error::StoreError};
use flatdb::ProviderRW;
use reth_db::models::CompactU256;
use reth_db_api::table::{Compress, Decode, Decompress, Encode};
use reth_primitives_traits::{Account, Bytecode};
use revm::database::states::cache::MyError;
use revm::DatabaseRef;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::compress_to_buf_or_ref;

const ADDR_SLOT_SPERATOR: u8 = 17;

/// Database error type.
#[derive(Debug, thiserror::Error)]
pub enum RocksDBError {
    /// Failed to open the database.
    #[error("[rocksdb] failed to read the database: {_0}")]
    ReadError(#[from] StoreError),
    #[error("[rocksdb] failed to decode value:{_0}")]
    DecodeError(#[from] reth_db::DatabaseError),
    #[error("[rocksdb] {_0}")]
    Other(String),
}

pub struct Store {
    backend: Arc<dyn StorageBackend>,
}

// no need to impl this, as ProviderRW require sync.
// unsafe impl Sync for Store {}

impl Store {
    pub fn new(backend: impl StorageBackend + 'static) -> Self {
        Store {
            backend: Arc::new(backend),
        }
    }

    pub fn new_rocksdb_backend(path: impl AsRef<Path>) -> Self {
        let backend = RocksDBBackend::open(path).unwrap();
        Self::new(backend)
    }

    pub fn print_all<K: Decompress, V: Decompress>(
        &self,
        table: &'static str,
        prefix: &[u8],
    ) -> Result<(), RocksDBError> {
        let tx_read = self.backend.begin_read().unwrap();
        let res = tx_read.prefix_iterator(table, prefix)?;

        for item in res {
            if let Ok((k, v)) = item {
                let decoded_key: K = Decompress::decompress(&k)?;
                let decoded_val: V = Decompress::decompress(&v)?;
                println!("k:{:?}, v:{:?}", decoded_key, decoded_val);
            }
        }

        Ok(())
    }

    pub fn basic_account(&self, address: Address) -> Result<Option<Account>, RocksDBError> {
        let tx_read = self.backend.begin_read().unwrap();
        let encoded_key = address.encode();
        let v = tx_read.get(ACCOUNT_FLATKEYVALUE, &encoded_key)?;
        match v {
            Some(v) => {
                let decoded = Decompress::decompress(&v)?;
                Ok(Some(decoded))
            }
            None => Ok(None),
        }
    }

    pub fn bytecode_by_hash(&self, code_hash: B256) -> Result<Option<Bytecode>, RocksDBError> {
        let tx_read = self.backend.begin_read().unwrap();
        let encoded_key = code_hash.encode();
        let v = tx_read.get(ACCOUNT_CODES, &encoded_key)?;
        match v {
            Some(v) => {
                let decoded = Decompress::decompress(&v)?;
                Ok(Some(decoded))
            }
            None => Ok(None),
        }
    }

    pub fn storage(
        &self,
        address: Address,
        key: StorageKey,
    ) -> Result<Option<StorageValue>, RocksDBError> {
        let tx_read = self.backend.begin_read().unwrap();
        let encoded_addr = address.encode();
        let encoded_slot = key.encode();
        // Apply a prefix with an invalid nibble (17) as a separator
        let encoded_key = addr_slot_key(&encoded_addr[..], &encoded_slot[..]);
        let v = tx_read.get(STORAGE_FLATKEYVALUE, &encoded_key)?;
        match v {
            Some(v) => {
                let decoded: CompactU256 = Decompress::decompress(&v)?;
                let decoded = decoded.into();
                Ok(Some(decoded))
            }
            None => Ok(None),
        }
    }

    fn set_kvs<K: Encode, V: Compress>(
        &self,
        table: &'static str,
        items: HashMap<K, V>,
    ) -> Result<(), RocksDBError> {
        let batched_items = items
            .into_iter()
            .map(|(addr, acct)| {
                let key = addr.encode().into();
                let mut value = vec![];
                compress_to_buf_or_ref!(value, acct);
                (key, value)
            })
            .collect::<Vec<_>>();

        let mut txn = self.backend.begin_write()?;
        txn.put_batch(table, batched_items)?;
        Ok(txn.commit()?)
    }

    pub fn set_accounts<K: Encode, V: Compress>(
        &self,
        accounts: HashMap<K, V>,
    ) -> Result<(), RocksDBError> {
        self.set_kvs(ACCOUNT_FLATKEYVALUE, accounts)
    }

    pub fn set_codes<K: Encode, V: Compress>(
        &self,
        codes: HashMap<K, V>,
    ) -> Result<(), RocksDBError> {
        self.set_kvs(ACCOUNT_CODES, codes)
    }

    fn set_dup_kvs<K: Encode, SK: Encode, V: Compress>(
        &self,
        table: &'static str,
        items: Vec<(K, SK, V)>,
    ) -> Result<(), RocksDBError> {
        let batched_items = items
            .into_iter()
            .map(|(key, slot, val)| {
                let encoded_addr = key.encode();
                let encoded_slot = slot.encode();
                let key = addr_slot_key(encoded_addr.as_ref(), encoded_slot.as_ref());
                let mut value = Vec::with_capacity(32);
                compress_to_buf_or_ref!(value, val);
                (key, value)
            })
            .collect::<Vec<_>>();

        let mut txn = self.backend.begin_write()?;
        txn.put_batch(table, batched_items)?;
        Ok(txn.commit()?)
    }

    pub fn set_storages<K: Encode, SK: Encode, V: Compress>(
        &self,
        storages: Vec<(K, SK, V)>,
    ) -> Result<(), RocksDBError> {
        self.set_dup_kvs(STORAGE_FLATKEYVALUE, storages)
    }
}

#[inline]
pub(crate) fn addr_slot_key(encoded_addr: &[u8], encoded_slot: &[u8]) -> Vec<u8> {
    [encoded_addr, &[ADDR_SLOT_SPERATOR], encoded_slot].concat()
}

impl From<RocksDBError> for MyError {
    fn from(value: RocksDBError) -> Self {
        MyError {
            message: value.to_string(),
        }
    }
}

pub struct RocksDbProvider<'a>(Box<dyn StorageReadView + 'a>);

impl<'a> DatabaseRef for RocksDbProvider<'a> {
    type Error = MyError;
    fn basic_ref(&self, address: Address) -> Result<Option<revm::state::AccountInfo>, Self::Error> {
        println!("basic_ref...");
        let tx_read = &self.0;
        let encoded_key = address.encode();
        let v = tx_read
            .get(ACCOUNT_FLATKEYVALUE, &encoded_key)
            .map_err(|e| MyError::from(RocksDBError::from(e)))?;
        match v {
            Some(v) => {
                let decoded: Account =
                    Decompress::decompress(&v).map_err(|e| MyError::from(RocksDBError::from(e)))?;
                Ok(Some(decoded.into()))
            }
            None => Ok(None),
        }
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<revm::state::Bytecode, Self::Error> {
        println!("code_by_hash_ref...");
        let tx_read = &self.0;
        let encoded_key = code_hash.encode();
        let v = tx_read
            .get(ACCOUNT_CODES, &encoded_key)
            .map_err(|e| MyError::from(RocksDBError::from(e)))?;
        match v {
            Some(v) => {
                let decoded: Bytecode =
                    Decompress::decompress(&v).map_err(|e| MyError::from(RocksDBError::from(e)))?;
                Ok(decoded.into())
            }
            None => Err(MyError {
                message: format!("[rocksdb] code for codehash:{code_hash} not found"),
            }),
        }
    }

    fn storage_ref(
        &self,
        address: Address,
        index: revm::primitives::StorageKey,
    ) -> Result<revm::primitives::StorageValue, Self::Error> {
        println!("storage ref...");
        let tx_read = &self.0;
        let encoded_addr = address.encode();
        let slot: StorageKey = index.into();
        let encoded_slot = slot.encode();
        // Apply a prefix with an invalid nibble (17) as a separator
        let encoded_key = addr_slot_key(&encoded_addr[..], &encoded_slot[..]);
        let v = tx_read
            .get(STORAGE_FLATKEYVALUE, &encoded_key)
            .map_err(|e| MyError::from(RocksDBError::from(e)))?;
        match v {
            Some(v) => {
                let decoded: CompactU256 =
                    Decompress::decompress(&v).map_err(|e| MyError::from(RocksDBError::from(e)))?;
                let decoded = decoded.into();
                Ok(decoded)
            }
            None => Ok(revm::primitives::StorageValue::default()),
        }
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        todo!()
    }
}

impl ProviderRW for Store {
    fn set_preblock_state(&self, prestate: &revm::context::block_states::PreBlockState) {
        todo!()
    }

    fn set_storage(
        &self,
        addr: Address,
        storage: revm::primitives::HashMap<
            revm::primitives::StorageKey,
            revm::primitives::StorageValue,
        >,
    ) {
        todo!()
    }

    fn commit_bal_changes(
        &self,
        bal: &revm::state::bal::Bal,
        finalized_bn: alloy_primitives::BlockNumber,
    ) -> revm::context::block_states::PreBlockState {
        todo!()
    }

    fn last_finalized_block_number(&self) -> Option<alloy_primitives::BlockNumber> {
        todo!()
    }

    fn lastest_provider_ro<'a>(&'a self) -> Box<dyn DatabaseRef<Error = MyError> + 'a> {
        let tx_read = self.backend.begin_read().unwrap();
        Box::new(RocksDbProvider(tx_read))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, hex, keccak256, U256};
    use ethrex_storage::api::tables::ACCOUNT_FLATKEYVALUE;
    use ethrex_storage::api::{StorageBackend, StorageReadView, StorageWriteBatch};
    use ethrex_storage::backend::in_memory::InMemoryBackend;
    use ethrex_storage::backend::rocksdb::RocksDBBackend;
    use revm::primitives::StorageValue;

    use std::sync::Arc;
    use std::thread;

    fn store_basic_account(backend: impl StorageBackend + 'static) {
        let store = Store::new(backend);
        let address = Address::default();
        let acct = store.basic_account(address);
        assert!(matches!(acct, Ok(None)));

        // insert some accts
        let mut accts = HashMap::new();
        let addr1 = address!("0x1000000000000000000000000000000000000000");
        let mut acct1 = Account::default();
        acct1.nonce = 1;
        accts.insert(addr1, acct1);

        let addr2 = address!("0x2000000000000000000000000000000000000000");
        let mut acct2 = Account::default();
        acct2.nonce = 2;
        accts.insert(addr2, acct2);

        store.set_accounts(accts).unwrap();

        // get updated acct
        assert!(matches!(store.basic_account(addr1), Ok(Some(acct)) if acct == acct1));
        assert!(matches!(store.basic_account(addr2), Ok(Some(acct)) if acct == acct2));
    }

    fn store_code_by_hash(backend: impl StorageBackend + 'static) {
        let store = Store::new(backend);
        let hash = B256::default();
        let acct = store.bytecode_by_hash(hash);
        assert!(matches!(acct, Ok(None)));

        // insert some code
        let mut codes = HashMap::new();
        let code = hex!("01");
        let code_hash1 = keccak256(code);
        let code1 = Bytecode::new_raw(code.to_vec().into());
        codes.insert(code_hash1, code1.clone());

        let code = hex!("02");
        let code_hash2 = keccak256(code);
        let code2 = Bytecode::new_raw(code.to_vec().into());
        codes.insert(code_hash2, code2.clone());

        store.set_codes(codes).unwrap();

        // get updated acct
        assert!(matches!(store.bytecode_by_hash(code_hash1), Ok(Some(code)) if code == code1));
        assert!(matches!(store.bytecode_by_hash(code_hash2), Ok(Some(code)) if code == code2));
    }

    fn store_storage(backend: impl StorageBackend + 'static) {
        let store = Store::new(backend);
        let address = Address::default();
        let slot = StorageKey::default();
        let acct = store.storage(address, slot);
        assert!(matches!(acct, Ok(None)));

        // insert some storages
        let mut storages = vec![];
        let addr1 = address!("0x1000000000000000000000000000000000000000");
        let slot1: StorageKey = U256::from(1).into();
        let val1 = StorageValue::from(1);
        let val1: CompactU256 = val1.into();
        storages.push((addr1, slot1, val1.clone()));

        let slot2 = U256::from(2).into();
        let val2 = StorageValue::from(2);
        let val2: CompactU256 = val2.into();
        storages.push((addr1, slot2, val2.clone()));

        let addr2 = address!("0x1000000000000000000000000000000000000001");
        let slot3 = U256::from(3).into();
        let val3 = StorageValue::from(3);
        let val3: CompactU256 = val3.into();
        storages.push((addr2, slot3, val3.clone()));

        store.set_storages(storages).unwrap();

        // get updated storage
        assert!(matches!(store.storage(addr1, slot1), Ok(Some(val)) if val1 == val.into()));
        assert!(matches!(store.storage(addr1, slot2), Ok(Some(val)) if val2 == val.into()));
        assert!(matches!(store.storage(addr2, slot1), Ok(None)));
        assert!(matches!(store.storage(addr2, slot2), Ok(None)));
        assert!(matches!(store.storage(addr2, slot3), Ok(Some(val)) if val3 == val.into()));
    }

    fn setup_backend() -> RocksDBBackend {
        let tempdir = tempfile::Builder::new()
            .prefix("_path_for_rocksdb_storage")
            .tempdir()
            .expect("Failed to create temporary path for the _path_for_rocksdb_storage");
        let path = tempdir.path();
        println!("tmp path:{:?}", path);

        RocksDBBackend::open(path).unwrap()
    }

    #[test]
    fn test_store_basic_account_works() {
        let backend = setup_backend();
        store_basic_account(backend);
    }

    #[test]
    fn test_store_code_by_hash_works() {
        let backend = setup_backend();
        store_code_by_hash(backend);
    }

    #[test]
    fn test_storage_works() {
        let backend = setup_backend();
        store_storage(backend);
    }

    #[test]
    fn test_db_basic_usage() {
        let tempdir = tempfile::Builder::new()
            .prefix("_path_for_rocksdb_storage")
            .tempdir()
            .expect("Failed to create temporary path for the _path_for_rocksdb_storage");
        let path = tempdir.path();
        println!("tmp path:{:?}", path);

        let backend = Arc::new(RocksDBBackend::open(path).unwrap());
        // let backend = Arc::new(InMemoryBackend::open().unwrap());
        let table = ACCOUNT_FLATKEYVALUE;

        let backend_clone = backend.clone();
        let handle = thread::spawn(move || {
            let tx_read = backend_clone.begin_read().unwrap();
            let result = tx_read.get(table, b"1111");
            println!("result before:{:?}", result);
        });
        handle.join().unwrap();

        let mut tx_write = backend.begin_write().unwrap();
        tx_write.put(table, b"1111", b"v111").unwrap();
        tx_write.commit().unwrap();

        let backend_clone = backend.clone();
        let handle = thread::spawn(move || {
            let tx_read = backend_clone.begin_read().unwrap();
            let result = tx_read.get(table, b"1111");
            println!("result after:{:?}", result);
        });

        handle.join().unwrap();
    }

    #[test]
    fn test_mainnet_rocksdb_provider() {
        let path = "/root/test_nodes/ethereum/execution/reth_rocksdb";
        let backend = RocksDBBackend::open(path).unwrap();
        let store = Store::new(backend);
        println!("getting latest provider");
        let provider = store.lastest_provider_ro();
        println!("got latest provider");

        let addr = address!("0x70bC1e16513aD49Bd28c20b7b50679381a71ADF5");
        // fetch account
        let acct = provider.basic_ref(addr).unwrap();
        println!("acct:{:?}", Account::from(acct.clone().unwrap()));

        if let Some(acct) = acct {
            // fetch storage
            let key = StorageKey::ZERO;
            let storage = provider.storage_ref(addr, key.into());
            println!("storage:{:?}", storage);
            // fetch code
            let code = provider.code_by_hash_ref(acct.code_hash).unwrap();
            assert_eq!(keccak256(code.original_bytes()), acct.code_hash);
            println!("code len:{}", code.len());
        }
    }
}
