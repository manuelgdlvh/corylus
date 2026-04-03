use std::{hash::Hash, io, marker::PhantomData};

use crate::{
    instance::{
        Instance,
        operation::{DeserializeError, Deserializer, Serializer},
    },
    object::map::{Get, Put},
};

pub mod map;

pub struct DistributedMap<K, V>
where
    K: Serializer + Deserializer + Hash + Eq + Clone,
    V: Serializer + Deserializer + Clone,
{
    instance: Instance,
    id: String,
    _state: PhantomData<(K, V)>,
}

impl<K, V> DistributedMap<K, V>
where
    K: Serializer + Deserializer + Hash + Eq + Clone + 'static,
    V: Serializer + Deserializer + Clone + 'static,
{
    pub fn new(id: String, instance: Instance) -> Self {
        Self {
            id,
            instance,
            _state: Default::default(),
        }
    }

    pub fn put(&self, k: K, v: V) -> io::Result<()> {
        let op = Put::<K, V> { key: k, value: v };
        self.instance.write(self.id.as_str(), op)
    }

    pub fn get(&self, k: K) -> io::Result<Option<V>> {
        let op = Get::<K, V> {
            key: k,
            _value: Default::default(),
        };

        let result = self.instance.read(self.id.as_str(), op)?;
        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(
                V::deserialize(result.as_slice())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
            ))
        }
    }
}

// --- Serializers / Deserializers ---

impl Serializer for String {
    fn serialize(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

impl Deserializer for String {
    type Error = DeserializeError;

    fn deserialize(buffer: &[u8]) -> Result<Self, Self::Error> {
        String::from_utf8(buffer.to_vec()).map_err(DeserializeError::InvalidUtf8)
    }
}

impl Serializer for Vec<u8> {
    fn serialize(&self) -> Vec<u8> {
        self.clone()
    }
}

impl Deserializer for Vec<u8> {
    type Error = DeserializeError;

    fn deserialize(buffer: &[u8]) -> Result<Self, Self::Error> {
        Ok(buffer.to_vec())
    }
}

impl Serializer for bool {
    fn serialize(&self) -> Vec<u8> {
        vec![*self as u8]
    }
}

impl Deserializer for bool {
    type Error = DeserializeError;

    fn deserialize(buffer: &[u8]) -> Result<Self, Self::Error> {
        if buffer.len() != 1 {
            return Err(DeserializeError::InvalidBufferSize {
                expected: 1,
                got: buffer.len(),
            });
        }
        match buffer[0] {
            0 => Ok(false),
            1 => Ok(true),
            v => Err(DeserializeError::Unknown(format!(
                "Invalid bool byte: {v}"
            ))),
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
            type Error = DeserializeError;

            fn deserialize(buffer: &[u8]) -> Result<Self, Self::Error> {
                const N: usize = std::mem::size_of::<$t>();
                if buffer.len() != N {
                    return Err(DeserializeError::InvalidBufferSize {
                        expected: N,
                        got: buffer.len(),
                    });
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
