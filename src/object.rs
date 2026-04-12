use std::{hash::Hash, marker::PhantomData};

use crate::{
    CorylusResult,
    instance::{self},
    object::map::{Get, Put},
    serde::{Deserializer, Serializer},
};

pub mod map;

pub struct DistributedMap<K, V>
where
    K: Serializer + Deserializer + Hash + Eq + Clone,
    V: Serializer + Deserializer + Clone,
{
    instance: instance::Weak,
    id: String,
    _state: PhantomData<(K, V)>,
}

impl<K, V> DistributedMap<K, V>
where
    K: Serializer + Deserializer + Hash + Eq + Clone + 'static,
    V: Serializer + Deserializer + Clone + 'static,
{
    pub fn new(id: String, instance: instance::Weak) -> Self {
        Self {
            id,
            instance,
            _state: Default::default(),
        }
    }

    pub fn put(&self, k: K, v: V) -> CorylusResult<()> {
        let op = Put::<K, V> { key: k, value: v };

        if let Some(ref_) = self.instance.as_ref().upgrade() {
            ref_.write(self.id.as_str(), op)
        } else {
            panic!("Instance was destroyed")
        }
    }

    pub fn get(&self, k: K) -> CorylusResult<Option<V>> {
        let op = Get::<K, V> {
            key: k,
            _value: Default::default(),
        };

        if let Some(ref_) = self.instance.as_ref().upgrade() {
            let result = ref_.read(self.id.as_str(), op)?;
            if result.is_empty() {
                Ok(None)
            } else {
                let value = V::deserialize(result.as_slice())?;
                Ok(Some(value))
            }
        } else {
            panic!("Instance was destroyed")
        }
    }
}
