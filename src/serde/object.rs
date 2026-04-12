use crate::serde::{self, Deserializer, Serializer};
use std::result;

impl Serializer for String {
    fn serialize(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

impl Deserializer for String {
    fn deserialize(buffer: &[u8]) -> result::Result<Self, serde::Error> {
        String::from_utf8(buffer.to_vec()).map_err(|_| serde::Error::InvalidUtf8)
    }
}

impl Serializer for Vec<u8> {
    fn serialize(&self) -> Vec<u8> {
        self.clone()
    }
}

impl Deserializer for Vec<u8> {
    fn deserialize(buffer: &[u8]) -> result::Result<Self, serde::Error> {
        Ok(buffer.to_vec())
    }
}

impl Serializer for bool {
    fn serialize(&self) -> Vec<u8> {
        vec![*self as u8]
    }
}

impl Deserializer for bool {
    fn deserialize(buffer: &[u8]) -> result::Result<Self, serde::Error> {
        if buffer.len() != 1 {
            return Err(serde::Error::InvalidBufferSize);
        }
        match buffer[0] {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(serde::Error::Unknown),
        }
    }
}

macro_rules! impl_fixed_serde {
    ($t:ty) => {
        impl Serializer for $t {
            fn serialize(&self) -> Vec<u8> {
                self.to_le_bytes().to_vec()
            }
        }

        impl Deserializer for $t {
            fn deserialize(buffer: &[u8]) -> result::Result<Self, serde::Error> {
                const N: usize = std::mem::size_of::<$t>();
                if buffer.len() != N {
                    return Err(serde::Error::InvalidBufferSize);
                }
                Ok(<$t>::from_le_bytes(buffer.try_into().unwrap()))
            }
        }
    };
}

impl_fixed_serde!(u8);
impl_fixed_serde!(u16);
impl_fixed_serde!(u32);
impl_fixed_serde!(u64);
impl_fixed_serde!(u128);
impl_fixed_serde!(i8);
impl_fixed_serde!(i16);
impl_fixed_serde!(i32);
impl_fixed_serde!(i64);
impl_fixed_serde!(i128);
impl_fixed_serde!(f32);
impl_fixed_serde!(f64);
