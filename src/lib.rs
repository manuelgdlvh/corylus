use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    io, result,
    sync::{Arc, Mutex},
};

use uuid::Uuid;

use crate::{
    instance::{
        Shutdown,
        operation::{self, Deserializer, Serializer},
    },
    object::DistributedMap,
};

pub mod instance;
pub mod network;
pub mod object;
pub mod partition;

pub type CorylusResult<T> = result::Result<T, CorylusError>;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum CorylusError {
    #[error("IO error")]
    Io(#[from] io::Error),
    #[error("Partition error")]
    Partition(#[from] partition::Error),
    #[error("Operation error")]
    Operation(#[from] operation::Error),
}

#[derive(Clone)]
pub struct Instance {
    inner: Arc<instance::Inner>,
}

impl Instance {
    fn new(
        id: Uuid,
        net: network::Sender,
        part_group: partition::Group,
        shutdown: Shutdown,
    ) -> Self {
        let mut members = HashSet::new();
        members.insert(id);

        let inner = Arc::new(instance::Inner {
            id,
            net,
            part_group,
            members: Mutex::new(members),
            shutdown,
        });

        Self { inner }
    }

    fn downgrade(&self) -> instance::Weak {
        instance::Weak::new(Arc::downgrade(&self.inner))
    }

    pub fn members(&self) -> Vec<Uuid> {
        self.inner.members()
    }

    pub fn part_group_version(&self) -> u128 {
        self.inner.part_group.version()
    }

    pub fn get_map<K, V>(&self, id: &str) -> Option<DistributedMap<K, V>>
    where
        K: Serializer + Deserializer + Hash + Eq + Clone + 'static,
        V: Serializer + Deserializer + Clone + 'static,
    {
        let id = format!("map:{}", id);
        let result = self.inner.part_group.with_segment_read(0, &id, |segment| {
            segment
                .data
                .as_any()
                .downcast_ref::<HashMap<K, V>>()
                .is_some()
        });

        if let Ok(val) = result
            && val
        {
            let instance = self.downgrade();
            Some(DistributedMap::new(id, instance))
        } else {
            None
        }
    }
}
