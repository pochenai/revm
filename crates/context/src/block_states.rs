//! This module contains [`block_state`] and how to import json data to rust structures for blocks, bals, block-hashes, pre-block states.
use std::io::Read;
use std::{any::Any, fs::File};
use std::{fs, os};

use crate::TxEnv;
use alloy_consensus::{EthereumTxEnvelope, Header, Transaction, TxEip4844};
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
pub type RethBlock = alloy_consensus::Block<EthereumTxEnvelope<TxEip4844>>;

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
pub fn import_struct<T: for<'a> Deserialize<'a>>(filename: &str) -> T {
    let mut file = File::open(filename).unwrap();
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
            None
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
pub fn envelope_to_txenv(envelope: &EthereumTxEnvelope<TxEip4844>) -> TxEnv {
    // Extract inner transaction
    let sig_hash = envelope.signature_hash();
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
        caller: envelope
            .signature()
            .recover_address_from_prehash(&sig_hash)
            .ok()
            .unwrap(), // depends on your envelope source
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

    use state::bal::Bal;

    use super::*;

    #[test]
    fn test_import_block() {
        // relative directory is the current crate root
        let filename = "../../bins/revme/data/blocks.json";
        let blocks = import_struct::<Vec<RethBlock>>(filename);
        println!("{:?}", blocks)
    }

    #[test]
    fn test_import_prestates() {
        let filename = "../../bins/revme/data/prestates.json";
        let prestates = import_struct::<Vec<PreblockState>>(filename);
        println!("{:?}", prestates)
    }

    #[test]
    fn test_import_bals() {
        let filename = "../../bins/revme/data/bals.json";
        let bals = import_struct::<Vec<Bal>>(filename);
        println!("{:?}", bals)
    }

    #[test]
    fn test_import_blockhashes() {
        let filename = "../../bins/revme/data/blockHashes.json";
        let blockhashes = import_struct::<BTreeMap<u64, B256>>(filename);
        println!("{:?}", blockhashes)
    }
}
