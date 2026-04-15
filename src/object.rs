use crate::object::operation::{ReadBuilder, WriteBuilder};
use crate::{
    CorylusResult,
    instance::{self},
    object::map::{Get, Put},
};
use std::any::Any;
use std::marker::PhantomData;

pub mod map;
pub mod operation;

pub type Id = String;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Operation not found")]
    ObjectNotFound,
    #[error("Operation not found")]
    OperationNotFound,
}

pub trait Raw: Send + Sync + Any {
    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;
    fn as_raw(&self) -> Vec<u8>;
    fn rebuild(&mut self, raw: Vec<u8>);
}

#[derive(Copy, Clone)]
pub enum Replication {
    Sync,
    Async,
    None,
}

pub struct Metadata {
    ops: operation::Registry,
    repl_config: ReplicationConfig,
}

#[derive(Copy, Clone)]
pub struct ReplicationConfig {
    repl_factor: usize,
    repl: Replication,
    repl_read: bool,
}

impl ReplicationConfig {
    pub fn none() -> Self {
        Self::default()
    }

    pub fn synchronous(factor: usize, repl_read: bool) -> Self {
        assert!(factor > 0);
        Self {
            repl_factor: factor,
            repl: Replication::Sync,
            repl_read,
        }
    }

    pub fn asynchronous(factor: usize, repl_read: bool) -> Self {
        assert!(factor > 0);
        Self {
            repl_factor: factor,
            repl: Replication::Async,
            repl_read,
        }
    }
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            repl_factor: 0,
            repl: Replication::None,
            repl_read: false,
        }
    }
}

impl Metadata {
    pub fn new(ops: operation::Registry, repl_config: ReplicationConfig) -> Self {
        Self { ops, repl_config }
    }

    pub fn read_fn(&self, opart_id: &str) -> Result<ReadBuilder, Error> {
        self.ops.read_fn(opart_id)
    }

    pub fn write_fn(&self, opart_id: &str) -> Result<WriteBuilder, Error> {
        self.ops.write_fn(opart_id)
    }

    pub fn repl_factor(&self) -> usize {
        self.repl_config.repl_factor
    }

    pub fn repl(&self) -> Replication {
        self.repl_config.repl
    }

    pub fn repl_read(&self) -> bool {
        self.repl_config.repl_read
    }
}

pub struct DistributedMap<K, V>
where
    K: map::Key,
    V: map::Value,
{
    instance: instance::Weak,
    id: String,
    _state: PhantomData<(K, V)>,
}

impl<K, V> DistributedMap<K, V>
where
    K: map::Key,
    V: map::Value,
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

#[cfg(test)]
mod tests {
    use crate::object::ReplicationConfig;

    #[test]
    #[should_panic]
    pub fn should_fail_when_sync_repl_and_zero_factor() {
        ReplicationConfig::synchronous(0, false);
    }

    #[test]
    #[should_panic]
    pub fn should_fail_when_async_repl_and_zero_factor() {
        ReplicationConfig::asynchronous(0, false);
    }
}
