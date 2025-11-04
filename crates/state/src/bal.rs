//! Block Access List (BAL) data structures for efficient state access in blockchain execution.
//!
//! This module provides types for managing Block Access Lists, which optimize state access
//! by pre-computing and organizing data that will be accessed during block execution.
//!
//! ## Key Types
//!
//! - **`BalIndex`**: Block access index (0 for pre-execution, 1..n for transactions, n+1 for post-execution)
//! - **`Bal`**: Main BAL structure containing a map of accounts
//! - **`BalWrites<T>`**: Array of (index, value) pairs representing sequential writes to a state item
//! - **`AccountBal`**: Complete BAL structure for an account (balance, nonce, code, and storage)
//! - **`AccountInfoBal`**: Account info BAL data (nonce, balance, code)
//! - **`StorageBal`**: Storage-level BAL data for an account

pub mod account;
pub mod alloy;
pub mod writes;

use std::{collections::BTreeMap, sync::Arc};

pub use account::{AccountBal, AccountInfoBal, StorageBal};
pub use writes::BalWrites;

use crate::{Account, AccountInfo, EvmState};
use alloy_eip7928::BlockAccessList as AlloyBal;
use primitives::{address, Address, HashSet, IndexMap, StorageKey, StorageValue};

/// Block access index (0 for pre-execution, 1..n for transactions, n+1 for post-execution)
pub type BalIndex = u64;

/// BAL structure.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Bal {
    /// Accounts bal.
    pub accounts: IndexMap<Address, AccountBal>,
}

impl FromIterator<(Address, AccountBal)> for Bal {
    fn from_iter<I: IntoIterator<Item = (Address, AccountBal)>>(iter: I) -> Self {
        Self {
            accounts: iter.into_iter().collect(),
        }
    }
}

impl Bal {
    /// Create a new BAL builder.
    pub fn new() -> Self {
        Self {
            accounts: IndexMap::default(),
        }
    }

    /// Pretty print the entire BAL structure in a human-readable format.
    #[cfg(feature = "std")]
    pub fn pretty_print(&self) {
        println!("=== Block Access List (BAL) ===");
        println!("Total accounts: {}", self.accounts.len());
        println!();

        if self.accounts.is_empty() {
            println!("(empty)");
            return;
        }

        for (idx, (address, account)) in self.accounts.iter().enumerate() {
            println!("Account #{idx} - Address: {address:?}");
            println!("  Account Info:");

            // Print nonce writes
            if account.account_info.nonce.is_empty() {
                println!("    Nonce: (read-only, no writes)");
            } else {
                println!("    Nonce writes:");
                for (bal_index, nonce) in &account.account_info.nonce.writes {
                    println!("      [{bal_index}] -> {nonce}");
                }
            }

            // Print balance writes
            if account.account_info.balance.is_empty() {
                println!("    Balance: (read-only, no writes)");
            } else {
                println!("    Balance writes:");
                for (bal_index, balance) in &account.account_info.balance.writes {
                    println!("      [{bal_index}] -> {balance}");
                }
            }

            // Print code writes
            if account.account_info.code.is_empty() {
                println!("    Code: (read-only, no writes)");
            } else {
                println!("    Code writes:");
                for (bal_index, (code_hash, bytecode)) in &account.account_info.code.writes {
                    println!(
                        "      [{}] -> hash: {:?}, size: {} bytes",
                        bal_index,
                        code_hash,
                        bytecode.len()
                    );
                }
            }

            // Print storage writes
            println!("  Storage:");
            if account.storage.storage.is_empty() {
                println!("    (no storage slots)");
            } else {
                println!("    Total slots: {}", account.storage.storage.len());
                for (storage_key, storage_writes) in &account.storage.storage {
                    println!("    Slot: {storage_key:#x}");
                    if storage_writes.is_empty() {
                        println!("      (read-only, no writes)");
                    } else {
                        println!("      Writes:");
                        for (bal_index, value) in &storage_writes.writes {
                            println!("        [{bal_index}] -> {value:?}");
                        }
                    }
                }
            }

            println!();
        }
        println!("=== End of BAL ===");
    }

    #[inline]
    /// Extend BAL with account.
    pub fn update_account(&mut self, bal_index: BalIndex, address: Address, account: &Account) {
        let bal_account = self.accounts.entry(address).or_default();
        bal_account.update(bal_index, account);
    }

    /// Merge another Bal into self at the given bal_index.
    pub fn merge_bal(&mut self, other: Bal, bal_index: BalIndex) {
        for (addr, other_account) in other.accounts {
            let account = self.accounts.entry(addr).or_insert_with(|| AccountBal {
                account_info: AccountInfoBal {
                    nonce: BalWrites { writes: vec![] },
                    balance: BalWrites { writes: vec![] },
                    code: BalWrites { writes: vec![] },
                },
                storage: StorageBal {
                    storage: BTreeMap::new(),
                },
            });

            account.merge_account_bal(other_account, bal_index);
        }
    }

    /// Extent BAL with each tx's changes
    pub fn merge_changes(&mut self, changes: EvmState, bal_index: BalIndex) {
        for (address, account) in changes.iter() {
            self.update_account(bal_index, *address, account);
        }
    }

    /// Remove bals whose index is in idxs
    pub fn remove_at_ids(&mut self, idxs: Vec<BalIndex>) {
        if idxs.is_empty() {
            return;
        }

        let id_set: HashSet<BalIndex> = idxs.into_iter().collect();

        for (_address, bal_account) in self.accounts.iter_mut() {
            // remove account info writes for matching indices
            bal_account
                .account_info
                .nonce
                .writes
                .retain(|(i, _)| !id_set.contains(i));
            bal_account
                .account_info
                .balance
                .writes
                .retain(|(i, _)| !id_set.contains(i));
            bal_account
                .account_info
                .code
                .writes
                .retain(|(i, _)| !id_set.contains(i));

            // remove storage writes for matching indices
            for (_key, slot) in bal_account.storage.storage.iter_mut() {
                slot.writes.retain(|(i, _)| !id_set.contains(i));
            }
        }
    }

    /// remove pre and post tx bals
    pub fn remove_first_last(&mut self) {
        let idxs = vec![0, self.accounts.len() as u64 + 1];
        self.remove_at_ids(idxs);
    }

    /// remove system contracts bals
    pub fn remove_at_address(&mut self, addrs: &[Address]) {
        for addr in addrs {
            self.accounts.shift_remove(addr);
        }
    }

    /// Populate account from BAL. Return true if account info got changed
    pub fn populate_account_info(
        &self,
        address: Address,
        bal_index: BalIndex,
        account: &mut AccountInfo,
    ) -> Result<bool, BalError> {
        let Some((index, _, bal_account)) = self.accounts.get_full(&address) else {
            return Err(BalError::AccountNotFound);
        };
        account.storage_id = Some(index);

        Ok(bal_account.populate_account_info(bal_index, account))
    }

    /// Populate storage slot from BAL.
    ///
    /// If slot is not found in BAL, it will return an error.
    #[inline]
    pub fn populate_storage_slot_by_account_id(
        &self,
        account_index: usize,
        bal_index: BalIndex,
        key: StorageKey,
        value: &mut StorageValue,
    ) -> Result<(), BalError> {
        let Some((_, bal_account)) = self.accounts.get_index(account_index) else {
            return Err(BalError::AccountNotFound);
        };

        if let Some(bal_value) = bal_account.storage.get(key, bal_index)? {
            *value = bal_value;
        };

        Ok(())
    }

    /// Populate storage slot from BAL by account address.
    #[inline]
    pub fn populate_storage_slot(
        &self,
        account_address: Address,
        bal_index: BalIndex,
        key: StorageKey,
        value: &mut StorageValue,
    ) -> Result<(), BalError> {
        let Some(bal_account) = self.accounts.get(&account_address) else {
            return Err(BalError::AccountNotFound);
        };

        if let Some(bal_value) = bal_account.storage.get(key, bal_index)? {
            *value = bal_value;
        };
        Ok(())
    }

    /// Get storage from BAL.
    pub fn account_storage(
        &self,
        account_index: usize,
        key: StorageKey,
        bal_index: BalIndex,
    ) -> Result<StorageValue, BalError> {
        let Some((_, bal_account)) = self.accounts.get_index(account_index) else {
            return Err(BalError::AccountNotFound);
        };

        let Some(storage_value) = bal_account.storage.get(key, bal_index)? else {
            return Err(BalError::SlotNotFound);
        };

        Ok(storage_value)
    }

    /// Consume Bal and create [`AlloyBal`]
    pub fn into_alloy_bal(self) -> AlloyBal {
        AlloyBal::from_iter(
            self.accounts
                .into_iter()
                .map(|(address, account)| account.into_alloy_account(address)),
        )
    }
}

/// Arc BAL structure with bal index.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BalWithIndex {
    /// Bal index.
    bal_index: BalIndex,
    /// BAL structure.
    bal: Arc<Bal>,
}

impl BalWithIndex {
    /// Create a new BAL with index.
    pub fn new(bal_index: BalIndex, bal: Arc<Bal>) -> Self {
        Self { bal_index, bal }
    }

    /// Return bal index.
    pub fn bal_index(&self) -> BalIndex {
        self.bal_index
    }

    /// Return BAL.
    pub fn bal(&self) -> Arc<Bal> {
        self.bal.clone()
    }

    /// Set bal index.
    pub fn set_bal_index(&mut self, bal_index: BalIndex) {
        self.bal_index = bal_index;
    }

    /// Populate account from BAL. Return true if account info got changed
    pub fn populate_account(
        &self,
        address: Address,
        account: &mut Account,
    ) -> Result<bool, BalError> {
        self.bal
            .populate_account_info(address, self.bal_index, &mut account.info)
    }

    /// Populate account from BAL. Return true if account info got changed
    pub fn populate_account_info(
        &self,
        address: Address,
        account: &mut AccountInfo,
    ) -> Result<bool, BalError> {
        self.bal
            .populate_account_info(address, self.bal_index, account)
    }

    /// Populate storage slot from BAL.
    pub fn populate_storage_slot_by_account_id(
        &self,
        account_index: usize,
        key: StorageKey,
        value: &mut StorageValue,
    ) -> Result<(), BalError> {
        self.bal
            .populate_storage_slot_by_account_id(account_index, self.bal_index, key, value)
    }

    /// Populate storage slot from BAL by account index.
    pub fn populate_storage_slot(
        &self,
        account_address: Address,
        key: StorageKey,
        value: &mut StorageValue,
    ) -> Result<(), BalError> {
        self.bal
            .populate_storage_slot(account_address, self.bal_index, key, value)
    }
}

/// BAL error.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum BalError {
    /// Account not found in BAL.
    AccountNotFound,
    /// Slot not found in BAL.
    SlotNotFound,
}

impl core::fmt::Display for BalError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::AccountNotFound => write!(f, "Account not found in BAL"),
            Self::SlotNotFound => write!(f, "Slot not found in BAL"),
        }
    }
}

#[cfg(test)]
mod tests {
    use primitives::U256;

    use super::*;

    #[test]
    fn test_merge_bal() {
        let mut bal1 = Bal {
            accounts: IndexMap::new(),
        };

        // Bal #1 with one account
        let addr1 = address!("0x0000000000000000000000000000000000000001");
        bal1.accounts.insert(
            addr1,
            AccountBal {
                account_info: AccountInfoBal {
                    nonce: BalWrites {
                        writes: vec![(1, 10)],
                    },
                    balance: BalWrites {
                        writes: vec![(1, U256::from(100))],
                    },
                    code: BalWrites { writes: vec![] },
                },
                storage: StorageBal {
                    storage: BTreeMap::from([(
                        StorageKey::from(0),
                        BalWrites {
                            writes: vec![(1, StorageValue::from(123))],
                        },
                    )]),
                },
            },
        );

        // Bal #2 with same account, different values
        let mut bal2 = Bal {
            accounts: IndexMap::new(),
        };
        bal2.accounts.insert(
            addr1,
            AccountBal {
                account_info: AccountInfoBal {
                    nonce: BalWrites {
                        writes: vec![(2, 11)],
                    },
                    balance: BalWrites {
                        writes: vec![(2, U256::from(200))],
                    },
                    code: BalWrites { writes: vec![] },
                },
                storage: StorageBal {
                    storage: BTreeMap::from([(
                        StorageKey::from(0),
                        BalWrites {
                            writes: vec![(2, StorageValue::from(456))],
                        },
                    )]),
                },
            },
        );

        // Merge bal2 into bal1 at bal_index = 2
        bal1.merge_bal(bal2, 2);

        let acc = &bal1.accounts[&addr1];
        assert_eq!(
            acc.account_info.nonce.writes.as_slice(),
            vec![(1, 10), (2, 11)].as_slice()
        );
        assert_eq!(
            acc.account_info.balance.writes.as_slice(),
            vec![(1, U256::from(100)), (2, U256::from(200))].as_slice()
        );
        assert_eq!(
            acc.storage.storage[&StorageKey::from(0)].writes.as_slice(),
            vec![(1, StorageValue::from(123)), (2, StorageValue::from(456))].as_slice()
        );
    }
}
