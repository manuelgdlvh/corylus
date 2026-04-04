use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::{Arc, Mutex},
};

use uuid::Uuid;

use crate::{
    instance::operation::{Deserializer, Serializer},
    object::DistributedMap,
};

pub mod instance;
pub mod network;
pub mod object;
pub mod partition;

#[derive(Clone)]
pub struct Instance {
    inner: Arc<instance::Inner>,
}

impl Instance {
    fn new(id: Uuid, net: network::Sender, partition: partition::Group) -> Self {
        let mut members = HashSet::new();
        members.insert(id);
        let inner = Arc::new(instance::Inner {
            id,
            net,
            partition,
            members: Mutex::new(members),
        });

        Self { inner }
    }

    fn downgrade(&self) -> instance::Weak {
        instance::Weak::new(Arc::downgrade(&self.inner))
    }

    pub fn members(&self) -> Vec<Uuid> {
        self.inner.members()
    }

    pub fn get_map<K, V>(&self, id: &str) -> Option<DistributedMap<K, V>>
    where
        K: Serializer + Deserializer + Hash + Eq + Clone + 'static,
        V: Serializer + Deserializer + Clone + 'static,
    {
        let id = format!("map:{}", id);
        let result = self.inner.partition.with_segment_read(0, &id, |segment| {
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
