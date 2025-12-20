//! This module contains [`block_state`] and how to import json data to rust structures for blocks, bals, block-hashes, pre-block states.
use std::io::Read;
use std::path::{Path, PathBuf};
use std::{any::Any, fs::File};
use std::{fs, os};

use crate::TxEnv;
use alloy_consensus::transaction::Recovered;
use alloy_consensus::{BlockBody, EthereumTxEnvelope, Header, Transaction, TxEip4844};
use bitvec::vec;
use context_interface::block::{BlobExcessGasAndPrice, Block};
use context_interface::either::Either;
use context_interface::transaction::{AccessList, RecoveredAuthorization, SignedAuthorization};
use database::states::plain_account::PlainStorage;
use database::states::CacheAccount;
use database::{AccountState, AccountStatus, Cache, CacheState, DbAccount, PlainAccount};
use primitives::HashMap;
use primitives::{eip4844::BLOB_BASE_FEE_UPDATE_FRACTION_PRAGUE, Address, B256, U256};
use serde::{Deserialize, Serialize};
use state::AccountInfo;

///
pub type RethBlock = alloy_consensus::Block<Recovered<EthereumTxEnvelope<TxEip4844>>>;

///
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct RecoveredBlock<T = EthereumTxEnvelope<TxEip4844>> {
    /// Block
    block: SealedBlock<T>,
    /// List of senders that match the transactions in the block
    senders: Vec<Address>,
}

///
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SealedBlock<T, H = Header> {
    /// Sealed Header.
    header: SealedHeader<H>,
    /// the block's body.
    body: BlockBody<T, H>,
}

///
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SealedHeader<H = Header> {
    header: H,
}

impl From<RecoveredBlock> for RethBlock {
    fn from(b: RecoveredBlock) -> Self {
        let recoverd_txs: Vec<_> = b
            .block
            .body
            .transactions
            .into_iter()
            .enumerate()
            .map(|(i, tx)| Recovered::new_unchecked(tx, b.senders[i]))
            .collect();

        RethBlock {
            header: b.block.header.header,
            body: BlockBody {
                transactions: recoverd_txs,
                ommers: b.block.body.ommers,
                withdrawals: b.block.body.withdrawals,
            },
        }
    }
}

/// Create a wrapper struct for Vec<RecoveredBlock>
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct RecoveredBlockVec(pub Vec<RecoveredBlock>);

// Implement From for the wrapper struct
impl From<RecoveredBlockVec> for Vec<RethBlock> {
    fn from(recovered_block_vec: RecoveredBlockVec) -> Self {
        recovered_block_vec
            .0
            .into_iter()
            .map(|b| b.into())
            .collect()
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
///
pub struct MyPlainAccount {
    /// account state and code
    pub info: Option<AccountInfo>,
    /// account storage
    pub storage: PlainStorage,
}

#[derive(Debug, Default, Serialize, Deserialize)]
///
pub struct PreblockState {
    /// code is in acct info
    pub accounts: HashMap<Address, MyPlainAccount>,
}

/// import blocks from json file
pub fn import_struct<T: for<'a> Deserialize<'a>, P: AsRef<Path>>(filename: P) -> T {
    let mut file =
        File::open(&filename).expect(&format!("file:{:?} not found", filename.as_ref().to_str()));
    let mut contents = String::new();
    file.read_to_string(&mut contents).ok().unwrap();

    serde_json::from_str(&contents).unwrap()
}

/// convert prestates to hashdb
pub fn prestates_to_cachedbs(p: Vec<PreblockState>) -> Vec<CacheState> {
    let mut caches = Vec::with_capacity(p.len());
    for prestate in p {
        let cache = prestate_to_cachedb(prestate);
        caches.push(cache);
    }
    caches
}

fn prestate_to_cachedb(prestate: PreblockState) -> CacheState {
    let mut cache = CacheState::default();
    for (addr, acct) in prestate.accounts {
        let mut code = None;
        let plain_account = if let Some(acct_info) = acct.info {
            if acct_info.code.is_some() {
                code = acct_info.code.clone();
            }
            Some(PlainAccount {
                info: acct_info,
                storage: acct.storage,
            })
        } else {
            // some newly created account't doesn't have account info, but will read some other slots.
            Some(PlainAccount {
                info: AccountInfo::default(),
                storage: acct.storage,
            })
        };

        // insert code
        if let Some(code) = code {
            cache
                .contracts
                .insert(plain_account.as_ref().unwrap().info.code_hash.clone(), code);
        }

        // insert plain account
        let cached_acct = CacheAccount {
            account: plain_account,
            status: AccountStatus::default(),
        };
        // insert account
        cache.accounts.insert(addr, cached_acct);
    }
    cache
}

/// Convert json tx data to revm::TxEnv
pub fn envelope_to_txenv(
    envelope: &Recovered<EthereumTxEnvelope<TxEip4844>>,
    pre_recover_sender: bool,
) -> TxEnv {
    // Extract inner transaction
    let blob_hashes = if let Some(h) = envelope.blob_versioned_hashes() {
        h.into()
    } else {
        vec![]
    };
    let acl = if let Some(a) = envelope.access_list() {
        a.clone()
    } else {
        AccessList::default()
    };

    TxEnv {
        tx_type: envelope.tx_type() as u8,
        caller: if pre_recover_sender {
            envelope.signer()
        } else {
            envelope
                .signature()
                .recover_address_from_prehash(&envelope.signature_hash())
                .ok()
                .unwrap()
        },
        gas_limit: envelope.gas_limit(),
        gas_price: envelope.max_fee_per_gas(), // you can use effective gas price here if needed
        kind: envelope.kind(),
        value: envelope.value(),
        data: envelope.input().clone(),
        nonce: envelope.nonce(),
        chain_id: envelope.chain_id(),
        access_list: acl,
        gas_priority_fee: envelope.max_priority_fee_per_gas(),
        blob_hashes: blob_hashes,
        max_fee_per_blob_gas: envelope.max_fee_per_blob_gas().unwrap_or_default(),
        authorization_list: signauth_to_eitherauth(envelope.authorization_list()),
    }
}

fn signauth_to_eitherauth(
    auth_list: Option<&[SignedAuthorization]>,
) -> Vec<Either<SignedAuthorization, RecoveredAuthorization>> {
    let auth_list: Vec<SignedAuthorization> = if let Some(list) = auth_list {
        list.into()
    } else {
        vec![]
    };
    let mut res = Vec::with_capacity(auth_list.len());
    for auth in auth_list {
        res.push(Either::Left(auth));
    }
    res
}

/// serialize data to json and write to filename
pub fn write_data<T: Serialize>(filename: &str, data: &T) {
    let fs = File::create(filename).unwrap();
    serde_json::to_writer(fs, data).unwrap();
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use primitives::address;
    use state::bal::Bal;

    use super::*;

    #[test]
    fn test_import_block() {
        // relative directory is the current crate root
        let filename = "../../bins/revme/data/blocks_1.json";
        let blocks: Vec<RecoveredBlock> = import_struct(filename);
        let blocks = RecoveredBlockVec(blocks);
        // println!("{:?}", blocks);
        let recovered_block: Vec<RethBlock> = blocks.into();
        println!("{:?}", recovered_block[0].body.transactions[0]);
    }

    #[test]
    fn test_import_prestates() {
        let filename = "../../bins/revme/data/prestates_1.json";
        let prestates: Vec<PreblockState> = import_struct(filename);
        println!("{:?}", prestates)
    }

    #[test]
    fn test_import_bals() {
        let filename = "../../bins/revme/data/bals_1.json";
        let mut bals: Vec<Bal> = import_struct(filename);
        // println!("{:?}", bals);

        let mut bal = bals.pop().unwrap();
        let a1 = bal
            .accounts
            .get_key_value(&address!("0xd9105edf00a15807accf1158b0c04a5e118e4b80"))
            .unwrap();
        println!("{:?}", a1);
        let a2 = bal
            .accounts
            .get_key_value(&address!("0xe3aF8532F6D4335dE2c6A0a3aD1cD290E87EE6AE"))
            .unwrap();
        println!("{:?}", a2);
    }

    #[test]
    fn test_import_blockhashes() {
        let filename = "../../bins/revme/data/blockHashes_1.json";
        let blockhashes: BTreeMap<u64, B256> = import_struct(filename);
        println!("{:?}", blockhashes)
    }
}
