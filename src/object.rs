use std::{hash::Hash, io, marker::PhantomData};

use crate::{
    instance::{
        Instance,
        operation::{Deserializer, Serializer},
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
            Ok(Some(V::deserialize(result.as_slice())))
        }
    }
}

// --- DeSerializers ---

impl Serializer for String {
    fn serialize(&self) -> Vec<u8> {
        let mut buff = Vec::with_capacity(self.len());
        buff.extend_from_slice(self.as_bytes());
        buff
    }
}

impl Deserializer for String {
    fn deserialize(buffer: &[u8]) -> Self {
        let mut buff = Vec::with_capacity(buffer.len());
        buff.extend_from_slice(buffer);
        unsafe { String::from_utf8_unchecked(buff) }
    }
}

impl Serializer for u64 {
    fn serialize(&self) -> Vec<u8> {
        let mut buff = Vec::with_capacity(8);
        buff.extend_from_slice(self.to_le_bytes().as_slice());
        buff
    }
}

impl Deserializer for u64 {
    fn deserialize(buffer: &[u8]) -> Self {
        u64::from_le_bytes(buffer.try_into().expect("buffer must be exactly 8 bytes"))
    }
}
