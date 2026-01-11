use std::{borrow::Cow, fmt::Debug};

use alloy_primitives::{Address, Bytes, StorageKey, B256, U256};
use reth_primitives_traits::Bytecode;

use reth_db::mdbx::{cursor, Error};
use reth_db::{Bytecodes, PlainAccountState, PlainStorageState};
use reth_db_api::table::{Compress, Decode, Encode};
use reth_primitives_traits::{Account, StorageEntry};

#[macro_export]
macro_rules! compress_to_buf_or_ref {
    ($buf:expr, $value:expr) => {
        if let Some(value) = $value.uncompressable_ref() {
            Some(value)
        } else {
            $value.compress_to_buf(&mut $buf);
            None
        }
    };
}

#[test]
fn encode() {
    // test account encode/decode
    let mut a = Account::default();
    a.nonce = 1;
    let key = Address::default();
    let key = key.encode();
    let key_ref = key.as_ref();
    let mut buf = Vec::with_capacity(72);

    compress_to_buf_or_ref!(buf, a);

    let res: Result<Option<(Cow<'_, [u8]>, Cow<'_, [u8]>)>, Error> =
        Ok(Some((Cow::Borrowed(key_ref), Cow::Borrowed(&buf))));
    let decoded_a = cursor::decode::<PlainAccountState>(res);
    println!("decode account:{:?}", decoded_a);

    // test storage encode/decode
    let mut e = StorageEntry::default();
    let key = Address::default();
    let key = key.encode();
    let key_ref = key.as_ref();
    let mut buf = Vec::with_capacity(72);

    compress_to_buf_or_ref!(buf, e);

    let res: Result<Option<(Cow<'_, [u8]>, Cow<'_, [u8]>)>, Error> =
        Ok(Some((Cow::Borrowed(key_ref), Cow::Borrowed(&buf))));
    let decoded_a = cursor::decode::<PlainStorageState>(res);
    println!("decode storage:{:?}", decoded_a);

    // test code encode/decode
    let key = B256::default();
    let key = key.encode();
    let key_ref = key.as_ref();
    let mut buf = Vec::with_capacity(72);

    let code = Bytecode::new_raw(Bytes::from_static(b"0001"));

    compress_to_buf_or_ref!(buf, code);

    let res: Result<Option<(Cow<'_, [u8]>, Cow<'_, [u8]>)>, Error> =
        Ok(Some((Cow::Borrowed(key_ref), Cow::Borrowed(&buf))));
    let decoded_a = cursor::decode::<Bytecodes>(res);
    println!("decode code:{:?}", decoded_a);
}
