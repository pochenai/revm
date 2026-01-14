//! Thread safe cache provider

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;

use dashmap::{DashMap, Entry::Occupied, Entry::Vacant};
use revm::context::block_states::PreBlockState;
use revm::database::states::cache::MyError;
use revm::primitives::StorageKey;
use revm::state::AccountInfo;
use revm::DatabaseRef;
use revm::{bytecode::Bytecode, primitives::StorageValue};

use alloy_primitives::{Address, B256};

///
#[derive(Debug)]
pub struct SharedCache {
    /// Cached accounts
    accounts: DashMap<Address, Option<AccountInfo>>,
    /// Cached storage
    storage: DashMap<Address, DashMap<StorageKey, StorageValue>>,
    /// Cached codes
    contracts: DashMap<B256, Bytecode>,
}

impl SharedCache {
    /// creat a shared cache
    pub fn new() -> Self {
        Self {
            accounts: DashMap::with_capacity_and_shard_amount(80000, 32),
            storage: DashMap::with_capacity_and_shard_amount(160000, 32),
            contracts: DashMap::with_capacity_and_shard_amount(40000, 32),
        }
    }
}

///
pub trait Iterable {
    /// Latest state should always be in the front!
    type Iter<'a>: Iterator<Item = &'a PreBlockState>
    where
        Self: 'a;
    /// create a iterator from the underlying type
    fn iter(&self) -> Self::Iter<'_>;
}

impl Iterable for &Vec<PreBlockState> {
    type Iter<'a>
        = std::slice::Iter<'a, PreBlockState>
    where
        Self: 'a;
    fn iter(&self) -> Self::Iter<'_> {
        <Vec<PreBlockState> as std::ops::Deref>::deref(self).iter()
    }
}

impl Iterable for &VecDeque<PreBlockState> {
    type Iter<'a>
        = std::collections::vec_deque::Iter<'a, PreBlockState>
    where
        Self: 'a;
    fn iter(&self) -> Self::Iter<'_> {
        <VecDeque<PreBlockState>>::iter(self)
    }
}

/// Thread safe cache provider for each batched block.
/// Must creat a seperate provider for each evm-tx, because the underlying DB will treat all the read/write as a single db-tx.
/// So if all evm-txs share the same provider, the db-tx is sequential!
#[derive(Debug)]
pub struct MTCache<DB, MEM>
where
    MEM: Iterable,
{
    /// Underling database
    db: DB,
    /// Shared Cache
    shared: Arc<SharedCache>,
    /// The performance diff with or without last 2 block's state changes is minimal.
    in_memory: Option<MEM>,
}

impl<DB, MEM> MTCache<DB, MEM>
where
    DB: DatabaseRef,
    MEM: Iterable,
{
    /// Create with a db
    pub fn new(db: DB, shared: Arc<SharedCache>, in_memory: Option<MEM>) -> Self {
        Self {
            db,
            shared,
            in_memory,
        }
    }
}

/// Get state information.
///
/// Fast path:
/// - Return cached value if present (lock held briefly).
///
/// Slow path:
/// - query underlying DB to avoid holding locks too long (the db has cache too, so it's not a big problem even if we do multiple I/O for the same addr).
/// - short lock shared cache and Insert result into cache (benign race allowed).
impl<DB, MEM> DatabaseRef for MTCache<DB, MEM>
where
    DB: DatabaseRef<Error = MyError>,
    MEM: Iterable + Sync + Send,
{
    #[doc = " The database error type."]
    type Error = MyError;

    #[doc = " Gets basic account information."]
    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        if let Some(mem) = &self.in_memory {
            for state in mem.iter() {
                if let Some(acct) = state.accounts.get(&address) {
                    return Ok(acct.info.clone());
                }
            }
        }
        // fast path：lock-fress clone
        if let Some(v) = self.shared.accounts.get(&address) {
            return Ok(v.clone());
        }

        // slow path: lock-free db access
        let acct = self.db.basic_ref(address)?;

        // short critical section
        match self.shared.accounts.entry(address) {
            Occupied(e) => Ok(e.get().clone()),
            Vacant(e) => {
                e.insert(acct.clone());
                Ok(acct)
            }
        }
    }

    #[doc = " Gets account code by its hash."]
    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        if let Some(mem) = &self.in_memory {
            // not neccesary due to it's already returned by the basic_ref.
        }

        // Fast path
        if let Some(code) = self.shared.contracts.get(&code_hash) {
            return Ok(code.clone());
        }

        // slow path: lock-free db access
        let code = self.db.code_by_hash_ref(code_hash)?;

        // short critical section
        match self.shared.contracts.entry(code_hash) {
            Occupied(e) => Ok(e.get().clone()),
            Vacant(e) => {
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
        if let Some(mem) = &self.in_memory {
            for state in mem.iter() {
                if let Some(acct) = state.accounts.get(&address) {
                    if let Some(val) = acct.storage.get(&index) {
                        return Ok(val.clone());
                    }
                }
            }
        }
        // Fast path: address + slot both cached
        if let Some(inner) = self.shared.storage.get(&address) {
            if let Some(val) = inner.get(&index) {
                return Ok(*val);
            }
        }

        // Slow path: lock-free db access
        let val = self.db.storage_ref(address, index)?;
        // Ensure inner map exists, then insert value
        let inner = self
            .shared
            .storage
            .entry(address)
            .or_insert_with(DashMap::default);
        let res = match inner.entry(index) {
            Occupied(e) => Ok(e.get().clone()),
            Vacant(e) => {
                e.insert(val);
                Ok(val)
            }
        };
        res
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

        let cache: MTCache<MockDB, &Vec<PreBlockState>> =
            MTCache::new(db, Arc::new(SharedCache::new()), None);

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
        // Without prewarm, there might be multiple db access for the same addr,
        // due to we want the critical section as small as possible.
        // test_basic_ref_cache_hit(None, false);
        // test_basic_ref_cache_hit(Some(AccountInfo::default()), false);
    }
}
