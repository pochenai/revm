//!
use crate::api::{
    table::{Compress, Decode, Decompress, Encode},
    DatabaseError,
};
use alloy_primitives::{Address, Bytes, Log, B256, U256};
use reth_codecs::{add_arbitrary_tests, Compact};

impl Encode for Vec<u8> {
    type Encoded = Self;

    fn encode(self) -> Self::Encoded {
        self
    }
}

impl Decode for Vec<u8> {
    fn decode(value: &[u8]) -> Result<Self, DatabaseError> {
        Ok(value.to_vec())
    }

    fn decode_owned(value: Vec<u8>) -> Result<Self, DatabaseError> {
        Ok(value)
    }
}

impl Encode for Address {
    type Encoded = [u8; 20];

    fn encode(self) -> Self::Encoded {
        self.0 .0
    }
}

impl Decode for Address {
    fn decode(value: &[u8]) -> Result<Self, DatabaseError> {
        Ok(Self::from_slice(value))
    }
}

impl Encode for B256 {
    type Encoded = [u8; 32];

    fn encode(self) -> Self::Encoded {
        self.0
    }
}

impl Decode for B256 {
    fn decode(value: &[u8]) -> Result<Self, DatabaseError> {
        Ok(Self::new(
            value.try_into().map_err(|_| DatabaseError::Decode)?,
        ))
    }
}

macro_rules! impl_compression_fixed_compact {
    ($($name:tt),+) => {
        $(
            impl Compress for $name {
                type Compressed = Vec<u8>;

                fn uncompressable_ref(&self) -> Option<&[u8]> {
                    Some(self.as_ref())
                }

                fn compress_to_buf<B: bytes::BufMut + AsMut<[u8]>>(&self, buf: &mut B) {
                    let _ = Compact::to_compact(self, buf);
                }
            }

            impl Decompress for $name {
                fn decompress(value: &[u8]) -> Result<$name, $crate::api::DatabaseError> {
                    let (obj, _) = Compact::from_compact(&value, value.len());
                    Ok(obj)
                }
            }

        )+
    };
}

impl_compression_fixed_compact!(B256, Address);
