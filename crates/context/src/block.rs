//! This module contains [`BlockEnv`] and it implements [`Block`] trait.
use std::io::Read;
use std::{any::Any, fs::File};

use alloy_consensus::{EthereumTxEnvelope, Header, Transaction, TxEip4844};
use bitvec::vec;
use context_interface::block::{BlobExcessGasAndPrice, Block};
use context_interface::either::Either;
use context_interface::transaction::{AccessList, RecoveredAuthorization, SignedAuthorization};
use primitives::{eip4844::BLOB_BASE_FEE_UPDATE_FRACTION_PRAGUE, Address, B256, U256};

use crate::TxEnv;

/// The block environment
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BlockEnv {
    /// The number of ancestor blocks of this block (block height).
    pub number: U256,
    /// Beneficiary (Coinbase or miner) is a address that have signed the block.
    ///
    /// This is the receiver address of all the gas spent in the block.
    pub beneficiary: Address,

    /// The timestamp of the block in seconds since the UNIX epoch
    pub timestamp: U256,
    /// The gas limit of the block
    pub gas_limit: u64,
    /// The base fee per gas, added in the London upgrade with [EIP-1559]
    ///
    /// [EIP-1559]: https://eips.ethereum.org/EIPS/eip-1559
    pub basefee: u64,
    /// The difficulty of the block
    ///
    /// Unused after the Paris (AKA the merge) upgrade, and replaced by `prevrandao`.
    pub difficulty: U256,
    /// The output of the randomness beacon provided by the beacon chain
    ///
    /// Replaces `difficulty` after the Paris (AKA the merge) upgrade with [EIP-4399].
    ///
    /// Note: `prevrandao` can be found in a block in place of `mix_hash`.
    ///
    /// [EIP-4399]: https://eips.ethereum.org/EIPS/eip-4399
    pub prevrandao: Option<B256>,
    /// Excess blob gas and blob gasprice
    ///
    ///
    /// Incorporated as part of the Cancun upgrade via [EIP-4844].
    ///
    /// [EIP-4844]: https://eips.ethereum.org/EIPS/eip-4844
    pub blob_excess_gas_and_price: Option<BlobExcessGasAndPrice>,
}

impl BlockEnv {
    /// Takes `blob_excess_gas` saves it inside env
    /// and calculates `blob_fee` with [`BlobExcessGasAndPrice`].
    pub fn set_blob_excess_gas_and_price(
        &mut self,
        excess_blob_gas: u64,
        base_fee_update_fraction: u64,
    ) {
        self.blob_excess_gas_and_price = Some(BlobExcessGasAndPrice::new(
            excess_blob_gas,
            base_fee_update_fraction,
        ));
    }
}

impl Block for BlockEnv {
    #[inline]
    fn number(&self) -> U256 {
        self.number
    }

    #[inline]
    fn beneficiary(&self) -> Address {
        self.beneficiary
    }

    #[inline]
    fn timestamp(&self) -> U256 {
        self.timestamp
    }

    #[inline]
    fn gas_limit(&self) -> u64 {
        self.gas_limit
    }

    #[inline]
    fn basefee(&self) -> u64 {
        self.basefee
    }

    #[inline]
    fn difficulty(&self) -> U256 {
        self.difficulty
    }

    #[inline]
    fn prevrandao(&self) -> Option<B256> {
        self.prevrandao
    }

    #[inline]
    fn blob_excess_gas_and_price(&self) -> Option<BlobExcessGasAndPrice> {
        self.blob_excess_gas_and_price
    }
}

impl Default for BlockEnv {
    fn default() -> Self {
        Self {
            number: U256::ZERO,
            beneficiary: Address::ZERO,
            timestamp: U256::ONE,
            gas_limit: u64::MAX,
            basefee: 0,
            difficulty: U256::ZERO,
            prevrandao: Some(B256::ZERO),
            blob_excess_gas_and_price: Some(BlobExcessGasAndPrice::new(
                0,
                BLOB_BASE_FEE_UPDATE_FRACTION_PRAGUE,
            )),
        }
    }
}

///
pub type RethBlock = alloy_consensus::Block<EthereumTxEnvelope<TxEip4844>>;

/// import blocks from json file
pub fn import_blocks() -> Vec<RethBlock> {
    let mut file = File::open("/home/po/now/reth/crates/cli/commands/src/blocks.json")
        .ok()
        .unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).ok().unwrap();

    serde_json::from_str(&contents).unwrap()
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_import_block() {
        let blocks = import_blocks();
        println!("{:?}", blocks)
    }
}
