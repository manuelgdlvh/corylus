use std::{any::Any, collections::HashMap, hash::Hash, io, marker::PhantomData};

use crate::{
    object,
    serde::{Deserializer, Serializer},
};

use crate::object::operation::{self, GenericRead, GenericWrite, Read, Write};
use std::convert::TryInto;

pub trait Key: Serializer + Deserializer + Hash + Eq + Clone + Send + Sync + 'static {}
pub trait Value: Serializer + Deserializer + Clone + Send + Sync + 'static {}
impl<T: Serializer + Deserializer + Hash + Eq + Clone + Send + Sync + 'static> Key for T {}
impl<T: Serializer + Deserializer + Clone + Send + Sync + 'static> Value for T {}

impl<K, V> object::Raw for HashMap<K, V>
where
    K: Key,
    V: Value,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn as_raw(&self) -> Vec<u8> {
        if self.is_empty() {
            return vec![];
        }
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&(self.len() as u32).to_le_bytes());
        for (k, v) in self {
            let raw_key = k.serialize();
            buffer.extend_from_slice(&(raw_key.len() as u32).to_le_bytes());
            buffer.extend_from_slice(raw_key.as_slice());

            let raw_value = v.serialize();
            buffer.extend_from_slice(&(raw_value.len() as u32).to_le_bytes());
            buffer.extend_from_slice(raw_value.as_slice());
        }

        buffer
    }

    fn rebuild(&mut self, raw: Vec<u8>) {
        let mut result;
        let mut offset = 0;
        if raw.is_empty() {
            result = HashMap::new();
        } else {
            // TODO: Adapt all others using like this
            let size = u32::from_le_bytes(raw[offset..size_of::<u32>()].try_into().unwrap());
            offset += size_of::<u32>();

            result = HashMap::with_capacity(size as usize);
            for _ in 0..size {
                let key_len =
                    u32::from_le_bytes(raw[offset..offset + size_of::<u32>()].try_into().unwrap());
                offset += size_of::<u32>();
                let key = K::deserialize(&raw[offset..offset + key_len as usize]).unwrap();
                offset += key_len as usize;

                let value_len =
                    u32::from_le_bytes(raw[offset..offset + size_of::<u32>()].try_into().unwrap());
                offset += size_of::<u32>();
                let value = V::deserialize(&raw[offset..offset + value_len as usize]).unwrap();
                offset += value_len as usize;

                result.insert(key, value);
            }
        }

        *self = result;
    }
}

// --- Put ---

pub struct Put<K, V>
where
    K: Key,
    V: Value,
{
    pub(crate) key: K,
    pub(crate) value: V,
}

impl<K, V> Serializer for Put<K, V>
where
    K: Key,
    V: Value,
{
    fn serialize(&self) -> Vec<u8> {
        let key = self.key.serialize();
        let val = self.value.serialize();

        let mut buf = Vec::new();
        buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
        buf.extend_from_slice(&key);
        buf.extend_from_slice(&(val.len() as u32).to_be_bytes());
        buf.extend_from_slice(&val);

        buf
    }
}

impl<K, V> operation::Base for Put<K, V>
where
    K: Key,
    V: Value,
{
    fn static_id() -> &'static str {
        "map:put"
    }

    fn id(&self) -> &str {
        Self::static_id()
    }

    fn partition_key(&self) -> Vec<u8> {
        self.key.serialize()
    }
}

impl<K, V> Write for Put<K, V>
where
    K: Key,
    V: Value,
{
    fn execute(&mut self, segment: &mut dyn object::Raw) {
        let map = segment
            .as_mut_any()
            .downcast_mut::<HashMap<K, V>>()
            .expect("segment is not HashMap<K,V>");

        map.insert(self.key.clone(), self.value.clone());
    }

    fn deserialize(val: &[u8]) -> Result<GenericWrite, io::Error>
    where
        Self: Sized,
    {
        let mut pos = 0;

        if val.len() < pos + 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "buffer too short for key length",
            ));
        }
        let key_len = u32::from_be_bytes(val[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        if val.len() < pos + key_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "buffer too short for key data",
            ));
        }
        let key = K::deserialize(&val[pos..pos + key_len])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        pos += key_len;

        if val.len() < pos + 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "buffer too short for value length",
            ));
        }
        let val_len = u32::from_be_bytes(val[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        if val.len() < pos + val_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "buffer too short for value data",
            ));
        }
        let value = V::deserialize(&val[pos..pos + val_len])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(GenericWrite {
            inner: Box::new(Put { key, value }),
        })
    }
}

// --- Get ---

pub struct Get<K, V>
where
    K: Key,
    V: Value,
{
    pub(crate) key: K,
    pub(crate) _value: PhantomData<V>,
}

impl<K, V> Serializer for Get<K, V>
where
    K: Key,
    V: Value,
{
    fn serialize(&self) -> Vec<u8> {
        let key = self.key.serialize();

        let mut buf = Vec::new();
        buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
        buf.extend_from_slice(&key);

        buf
    }
}

impl<K, V> operation::Base for Get<K, V>
where
    K: Key,
    V: Value,
{
    fn static_id() -> &'static str {
        "map:get"
    }

    fn id(&self) -> &str {
        Self::static_id()
    }

    fn partition_key(&self) -> Vec<u8> {
        self.key.serialize()
    }
}

impl<K, V> Read for Get<K, V>
where
    K: Key,
    V: Value,
{
    fn execute(&self, segment: &dyn object::Raw) -> Vec<u8> {
        let map = segment
            .as_any()
            .downcast_ref::<HashMap<K, V>>()
            .expect("segment is not HashMap<K,V>");

        match map.get(&self.key) {
            Some(val) => val.serialize(),
            None => {
                vec![]
            }
        }
    }

    fn deserialize(val: &[u8]) -> Result<GenericRead, io::Error>
    where
        Self: Sized,
    {
        let mut pos = 0;

        if val.len() < pos + 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "buffer too short for key length",
            ));
        }
        let key_len = u32::from_be_bytes(val[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        if val.len() < pos + key_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "buffer too short for key data",
            ));
        }
        let key = K::deserialize(&val[pos..pos + key_len])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(GenericRead {
            inner: Box::new(Get {
                key,
                _value: PhantomData::<V>,
            }),
        })
    }
}
