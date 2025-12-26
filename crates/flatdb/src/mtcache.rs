//! Thread safe cache provider

use dashmap::{DashMap, Entry::Occupied, Entry::Vacant};
use revm::database::states::cache::MyError;
use revm::primitives::StorageKey;
use revm::state::AccountInfo;
use revm::DatabaseRef;
use revm::{bytecode::Bytecode, primitives::StorageValue};

use alloy_primitives::{Address, B256};

/// Thread safe cache provider for each batched block
#[derive(Debug)]
pub struct MTCache<DB> {
    /// Underling database
    db: DB,
    /// Cached accounts
    accounts: DashMap<Address, Option<AccountInfo>>,
    /// Cached storage
    storage: DashMap<Address, DashMap<StorageKey, StorageValue>>,
    /// Cached codes
    contracts: DashMap<B256, Bytecode>,
}

impl<DB> MTCache<DB>
where
    DB: DatabaseRef,
{
    /// Create with a db
    pub fn new(db: DB) -> Self {
        Self {
            db,
            accounts: DashMap::default(),
            storage: DashMap::default(),
            contracts: DashMap::default(),
        }
    }
}

/// Get state information.
///
/// Fast path:
/// - Return cached value if present (lock held briefly).
///
/// Slow path:
/// - Hold any DashMap locks while querying underlying DB to avoid multiple IOs.
/// - Insert result into cache (benign race allowed).
impl<DB> DatabaseRef for MTCache<DB>
where
    DB: DatabaseRef<Error = MyError>,
{
    #[doc = " The database error type."]
    type Error = MyError;

    #[doc = " Gets basic account information."]
    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        // fast path：lock-fress clone
        if let Some(v) = self.accounts.get(&address) {
            return Ok(v.clone());
        }

        // Slow path: query DB while holding locks
        match self.accounts.entry(address) {
            Occupied(e) => Ok(e.get().clone()),
            Vacant(e) => {
                let acct = self.db.basic_ref(address)?;
                e.insert(acct.clone());
                Ok(acct)
            }
        }
    }

    #[doc = " Gets account code by its hash."]
    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        // Fast path
        if let Some(code) = self.contracts.get(&code_hash) {
            return Ok(code.clone());
        }

        // Slow path: query DB while holding locks
        match self.contracts.entry(code_hash) {
            Occupied(e) => Ok(e.get().clone()),
            Vacant(e) => {
                let code = self.db.code_by_hash_ref(code_hash)?;
                e.insert(code.clone());
                Ok(code)
            }
        }
    }

    #[doc = " Gets storage value of address at index."]
    fn storage_ref(
        &self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        // Fast path: address + slot both cached
        if let Some(inner) = self.storage.get(&address) {
            if let Some(val) = inner.get(&index) {
                return Ok(*val);
            }
        }

        // Slow path: query DB while holding locks
        // Ensure inner map exists, then insert value
        let inner = self.storage.entry(address).or_insert_with(DashMap::default);
        let val = self.db.storage_ref(address, index)?;
        inner.insert(index, val);

        Ok(val)
    }

    #[doc = " Gets block hash by block number."]
    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayon::prelude::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    #[derive(Clone)]
    struct MockDB {
        calls: Arc<AtomicUsize>,
        result: Option<AccountInfo>,
    }

    impl DatabaseRef for MockDB {
        type Error = MyError;

        fn basic_ref(&self, _address: Address) -> Result<Option<AccountInfo>, Self::Error> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.result.clone())
        }

        fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
            unimplemented!()
        }

        fn storage_ref(
            &self,
            _address: Address,
            _index: StorageKey,
        ) -> Result<StorageValue, Self::Error> {
            unimplemented!()
        }

        fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
            unimplemented!()
        }
    }

    fn test_basic_ref_cache_hit(res: Option<AccountInfo>, prewarm: bool) {
        let calls = Arc::new(AtomicUsize::new(0));

        let db = MockDB {
            calls: calls.clone(),
            result: res.clone(),
        };

        let cache = MTCache::new(db);

        let addr = Address::random();

        if prewarm {
            // First call: cache miss → DB should be hit
            let v1 = cache.basic_ref(addr).unwrap();
            assert_eq!(v1, res);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }

        // Later calls: cache hit → DB should NOT be hit again
        (0..100).into_par_iter().for_each(|_| {
            let v2 = cache.basic_ref(addr).unwrap();
            assert_eq!(v2, res);
        });
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_basic_ref_cache_should_always_incur_one_io() {
        test_basic_ref_cache_hit(None, true);
        test_basic_ref_cache_hit(None, false);
        test_basic_ref_cache_hit(Some(AccountInfo::default()), false);
    }
}
