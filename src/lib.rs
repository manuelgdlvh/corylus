use std::{collections::HashMap, io, sync::Arc};

use uuid::Uuid;

use crate::{
    instance::{Membership, Shutdown},
    object::DistributedMap,
};

pub mod instance;
pub mod network;
pub mod object;
pub mod partition;
pub mod serde;
mod sync;

pub type CorylusResult<T> = Result<T, CorylusError>;

use crate::object::map;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CorylusError {
    #[error("IO error")]
    Io(#[from] io::Error),
    #[error("Partition error")]
    Partition(#[from] partition::Error),
    #[error("Operation error")]
    Object(#[from] object::Error),
    #[error("Serde error")]
    Serde(#[from] serde::Error),
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
        objects: HashMap<object::Id, object::Metadata>,
        config: instance::Config,
        shutdown: Shutdown,
    ) -> Self {
        let membership = Membership::new();
        membership.add(id);

        let inner = Arc::new(instance::Inner {
            id,
            net,
            config,
            part_group,
            membership,
            objects,
            shutdown,
        });

        Self { inner }
    }

    fn downgrade(&self) -> instance::Weak {
        instance::Weak::new(Arc::downgrade(&self.inner))
    }

    pub fn members(&self) -> Vec<Uuid> {
        self.inner.membership.all()
    }

    pub fn part_group_version(&self) -> u128 {
        self.inner.part_group.version()
    }

    pub fn id(&self) -> Uuid {
        self.inner.id
    }

    pub fn get_map<K, V>(&self, id: &str) -> Option<DistributedMap<K, V>>
    where
        K: map::Key,
        V: map::Value,
    {
        let id = format!("map:{}", id);
        if !self.inner.objects.contains_key(&id) {
            return None;
        }

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
