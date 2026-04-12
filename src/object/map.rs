use std::{any::Any, collections::HashMap, hash::Hash, io, marker::PhantomData};

use crate::{
    instance::operation::{self, GenericRead, GenericWrite, Read, Write},
    partition::{self, RawSegment},
    serde::{self, Deserializer, Serializer},
};

use std::convert::TryInto;

impl<K, V> RawSegment for HashMap<K, V>
where
    K: Serializer + Deserializer + Send + Sync + Hash + Eq + 'static,
    V: Serializer + Deserializer + Send + Sync + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }
}

// --- Put ---

pub struct Put<K, V>
where
    K: Serializer + Deserializer + Hash + Eq,
    V: Serializer + Deserializer,
{
    pub(crate) key: K,
    pub(crate) value: V,
}

impl<K, V> serde::Serializer for Put<K, V>
where
    K: Serializer + Deserializer + Hash + Eq,
    V: Serializer + Deserializer,
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
    K: Serializer + Deserializer + Hash + Eq,
    V: Serializer + Deserializer,
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
    K: Serializer + Deserializer + Hash + Eq + Clone + 'static,
    V: Serializer + Deserializer + Clone + 'static,
{
    fn execute(&mut self, segment: &mut dyn crate::partition::RawSegment) {
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
    K: Serializer + Deserializer + Hash + Eq,
    V: Serializer + Deserializer,
{
    pub(crate) key: K,
    pub(crate) _value: PhantomData<V>,
}

impl<K, V> Serializer for Get<K, V>
where
    K: Serializer + Deserializer + Hash + Eq,
    V: Serializer + Deserializer,
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
    K: Serializer + Deserializer + Hash + Eq,
    V: Serializer + Deserializer,
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
    K: Serializer + Deserializer + Hash + Eq + 'static,
    V: Serializer + Deserializer + 'static,
{
    fn execute(&self, segment: &dyn partition::RawSegment) -> Vec<u8> {
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
