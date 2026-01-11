use alloy_primitives::{Address, StorageKey, StorageValue, B256};
use ethrex_storage::api::tables::{ACCOUNT_CODES, ACCOUNT_FLATKEYVALUE, STORAGE_FLATKEYVALUE};
use ethrex_storage::{api::StorageBackend, error::StoreError};
use reth_db::models::CompactU256;
use reth_db_api::table::{Compress, Decode, Decompress, Encode};
use reth_primitives_traits::{Account, Bytecode};
use std::collections::HashMap;
use std::sync::Arc;

use crate::compress_to_buf_or_ref;

const ADDR_SLOT_SPERATOR: u8 = 17;

/// Database error type.
#[derive(Debug, thiserror::Error)]
pub enum DatabaseError {
    /// Failed to open the database.
    #[error("failed to read the database: {_0}")]
    ReadError(#[from] StoreError),
    #[error("failed to decode value:{_0}")]
    DecodeError(#[from] reth_db::DatabaseError),
    #[error("{_0}")]
    Other(String),
}

pub struct Store {
    backend: Arc<dyn StorageBackend>,
}

impl Store {
    pub fn new(backend: impl StorageBackend + 'static) -> Self {
        Store {
            backend: Arc::new(backend),
        }
    }

    pub fn basic_account(&self, address: Address) -> Result<Option<Account>, DatabaseError> {
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

    pub fn bytecode_by_hash(&self, code_hash: B256) -> Result<Option<Bytecode>, DatabaseError> {
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
    ) -> Result<Option<StorageValue>, DatabaseError> {
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

    pub fn set_accounts(&self, accounts: HashMap<Address, Account>) -> Result<(), DatabaseError> {
        let batched_items = accounts
            .into_iter()
            .map(|(addr, acct)| {
                let key = addr.encode().into();
                let mut value = Vec::with_capacity(72);
                compress_to_buf_or_ref!(value, acct);
                (key, value)
            })
            .collect::<Vec<_>>();

        let mut txn = self.backend.begin_write()?;
        txn.put_batch(ACCOUNT_FLATKEYVALUE, batched_items)?;
        Ok(txn.commit()?)
    }

    pub fn set_codes(&self, codes: HashMap<B256, Bytecode>) -> Result<(), DatabaseError> {
        let batched_items = codes
            .into_iter()
            .map(|(hash, code)| {
                let key = hash.encode().into();
                let mut value = vec![];
                compress_to_buf_or_ref!(value, code);
                (key, value)
            })
            .collect::<Vec<_>>();

        let mut txn = self.backend.begin_write()?;
        txn.put_batch(ACCOUNT_CODES, batched_items)?;
        Ok(txn.commit()?)
    }

    pub fn set_storages(
        &self,
        storages: HashMap<Address, HashMap<B256, StorageValue>>,
    ) -> Result<(), DatabaseError> {
        let batched_items = storages
            .into_iter()
            .flat_map(|(addr, storage)| {
                let encoded_addr = addr.encode();

                let mut kvs = Vec::with_capacity(storage.len());
                for (slot, val) in storage {
                    let encoded_slot = slot.encode();
                    let key = addr_slot_key(&encoded_addr[..], &encoded_slot[..]);
                    let mut value = Vec::with_capacity(32);
                    let val: CompactU256 = val.into();
                    compress_to_buf_or_ref!(value, val);
                    kvs.push((key, value));
                }
                kvs
            })
            .collect::<Vec<_>>();

        let mut txn = self.backend.begin_write()?;
        txn.put_batch(STORAGE_FLATKEYVALUE, batched_items)?;
        Ok(txn.commit()?)
    }
}

#[inline]
fn addr_slot_key(encoded_addr: &[u8], encoded_slot: &[u8]) -> Vec<u8> {
    [encoded_addr, &[ADDR_SLOT_SPERATOR], encoded_slot].concat()
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
        let mut storages = HashMap::new();
        let addr1 = address!("0x1000000000000000000000000000000000000000");
        let mut storage = HashMap::new();
        let slot1: StorageKey = U256::from(1).into();
        let val1 = StorageValue::from(1);
        storage.insert(slot1, val1);
        let slot2 = U256::from(2).into();
        let val2 = StorageValue::from(2);
        storage.insert(slot2, val2);

        storages.insert(addr1, storage);

        let addr2 = address!("0x1000000000000000000000000000000000000001");
        let mut storage = HashMap::new();
        let slot3 = U256::from(3).into();
        let val3 = StorageValue::from(3);
        storage.insert(slot3, val3);
        storages.insert(addr2, storage);

        store.set_storages(storages).unwrap();

        // get updated storage
        assert!(matches!(store.storage(addr1, slot1), Ok(Some(val)) if val == val1));
        assert!(matches!(store.storage(addr1, slot2), Ok(Some(val)) if val == val2));
        assert!(matches!(store.storage(addr2, slot1), Ok(None)));
        assert!(matches!(store.storage(addr2, slot2), Ok(None)));
        assert!(matches!(store.storage(addr2, slot3), Ok(Some(val)) if val == val3));
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
}
