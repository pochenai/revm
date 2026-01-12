use alloy_primitives::{Address, StorageKey};
use reth_db::table::{Compress, Decode, Decompress, Encode};

use crate::store::addr_slot_key;

#[derive(Debug)]
pub struct StorageTableKey {
    pub addr: Address,
    pub key: StorageKey,
}

impl Compress for StorageTableKey {
    type Compressed = Vec<u8>;

    fn compress_to_buf<B: alloy_primitives::bytes::BufMut + AsMut<[u8]>>(&self, buf: &mut B) {
        let encoded_addr = self.addr.encode();
        let encoded_slot = self.key.encode();
        let full_key = addr_slot_key(encoded_addr.as_ref(), encoded_slot.as_ref());
        buf.put(&full_key[..]);
    }
}

impl Decompress for StorageTableKey {
    fn decompress(value: &[u8]) -> Result<Self, reth_db::DatabaseError> {
        let addr = &value[..20];
        let addr = Address::decode(addr)?;

        let key = &value[21..];
        let key = StorageKey::decode(key)?;

        Ok(StorageTableKey { addr, key })
    }
}
