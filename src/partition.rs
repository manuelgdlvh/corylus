use std::{
    any::Any,
    array,
    collections::{BinaryHeap, HashMap, HashSet},
    sync::{Mutex, RwLock},
};

use thiserror::Error;
use twox_hash::{XxHash3_64, XxHash3_128};
use uuid::Uuid;

use crate::{instance::operation, partition};

const RING_CAPACITY: usize = 1024;
const PARTITION_HASH_SEED: u64 = 1234;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Partition not found ")]
    PartitionNotFound,
    #[error("Segment not found ")]
    SegmentNotFound,
    #[error("Rebalance in progress")]
    Rebalance,
}

pub trait RawSegment: Send + Sync + Any {
    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;
}

pub struct Segment {
    id: String,
    pub(crate) data: Box<dyn RawSegment>,
    pub(crate) op_reg: operation::Registry,
    // Useful for entropy process of partitions after membership changes or corruption detection
    seq: u64,
    checksum: u64,
}

impl Segment {
    pub fn new<S: RawSegment + 'static>(id: String, data: S, op_reg: operation::Registry) -> Self {
        Self {
            id,
            data: Box::new(data),
            op_reg,
            seq: 0,
            checksum: 0,
        }
    }
}

pub struct Group {
    partitions: [Partition; RING_CAPACITY],
    version: Mutex<u128>,
}

impl Group {
    pub fn new<F: Fn() -> Segment>(member_id: Uuid, segment_fns: &[F]) -> Self {
        let partitions: [Partition; RING_CAPACITY] =
            array::from_fn(|id| Partition::new(id, member_id, segment_fns));

        Self {
            partitions,
            version: Mutex::new(Self::compute_version(&[member_id])),
        }
    }

    pub fn partition_of(&self, key: &[u8]) -> u64 {
        let hash = XxHash3_64::oneshot_with_seed(PARTITION_HASH_SEED, key);
        hash & (RING_CAPACITY as u64 - 1)
    }

    pub fn owner_of(&self, partition_id: u64) -> Uuid {
        if partition_id >= RING_CAPACITY as u64 {
            panic!("Partition id out of range");
        }
        let partition = &self.partitions[partition_id as usize];
        let metadata = partition
            .metadata
            .read()
            .expect("Critical section cannot be poisoned");

        metadata.master_id
    }

    pub fn is_replica(&self, member_id: Uuid, partition_id: u64) -> bool {
        if partition_id >= RING_CAPACITY as u64 {
            panic!("Partition id out of range");
        }
        let partition = &self.partitions[partition_id as usize];
        let metadata = partition
            .metadata
            .read()
            .expect("Critical section cannot be poisoned");

        metadata.replica_ids.contains(&member_id)
    }

    // The algorithm should be after updated, i get as read lock all the states that i should migrate. Then i send to assigned peer the snapshot. When OK i clear and unlock.
    pub fn update(&self, member_ids: &[Uuid]) -> HashMap<usize, Vec<MembershipChange>> {
        let mut result = HashMap::with_capacity(RING_CAPACITY);
        for p_id in 0..RING_CAPACITY {
            let partition = &self.partitions[p_id];
            let changes = partition.update(member_ids);
            if !changes.is_empty() {
                result.insert(p_id, changes);
            }
        }

        let mut version = self.version.lock().expect("Cannot be poisoned");
        *version = Self::compute_version(member_ids);
        result
    }

    pub fn compute_version(member_ids: &[Uuid]) -> u128 {
        const COUNT_BITS: u32 = 16;
        const COUNT_MASK: u128 = (1u128 << COUNT_BITS) - 1;
        const HASH_MASK: u128 = !COUNT_MASK;
        let mut hasher = XxHash3_128::with_seed(PARTITION_HASH_SEED);
        for member_id in member_ids {
            hasher.write(member_id.as_bytes());
        }

        (hasher.finish_128() & HASH_MASK) | (member_ids.len() as u128 & COUNT_MASK)
    }

    pub fn with_segment_read<F, O>(
        &self,
        p_id: usize,
        s_id: &str,
        f: F,
    ) -> Result<O, partition::Error>
    where
        F: FnOnce(&Segment) -> O,
    {
        if p_id >= RING_CAPACITY {
            return Err(Error::PartitionNotFound);
        }

        let partition = &self.partitions[p_id];
        if let Some(segment) = partition.segments.get(s_id) {
            let guard = segment.read().expect("Cannot be poisoned");
            Ok(f(&guard))
        } else {
            Err(Error::SegmentNotFound)
        }
    }

    pub fn with_segment_write<F>(
        &self,
        p_id: usize,
        s_id: &str,
        f: F,
    ) -> Result<(), partition::Error>
    where
        F: FnOnce(&mut Segment),
    {
        if p_id >= RING_CAPACITY {
            return Err(Error::PartitionNotFound);
        }

        let partition = &self.partitions[p_id];
        if let Some(segment) = partition.segments.get(s_id) {
            let mut guard = segment.write().expect("Cannot be poisoned");
            f(&mut guard);
            Ok(())
        } else {
            Err(Error::SegmentNotFound)
        }
    }

    pub fn version(&self) -> u128 {
        *self.version.lock().expect("Cannot be poisoned")
    }
}

pub struct Partition {
    metadata: RwLock<Metadata>,
    segments: HashMap<String, RwLock<Segment>>,
}

impl Partition {
    pub fn new<F: Fn() -> Segment>(id: usize, member_id: Uuid, segment_fns: &[F]) -> Self {
        let (master_id, replica_ids) = Partition::compute_membership(id, &[member_id]);

        let mut segments = HashMap::with_capacity(segment_fns.len());
        for f in segment_fns {
            let segment = f();
            segments.insert(segment.id.to_string(), RwLock::new(segment));
        }

        Self {
            metadata: RwLock::new(Metadata::new(id, master_id, replica_ids)),
            segments,
        }
    }

    pub fn with_segment(mut self, segment: Segment) -> Self {
        self.segments
            .insert(segment.id.to_string(), RwLock::new(segment));
        self
    }

    fn compute_membership(partiton_id: usize, member_ids: &[Uuid]) -> (Uuid, Vec<Uuid>) {
        if 0.eq(&member_ids.len()) {
            panic!("Members cannot be empty");
        }

        let mut top_k = BinaryHeap::with_capacity(member_ids.len());
        for member_id in member_ids {
            let mut hasher = XxHash3_128::with_seed(PARTITION_HASH_SEED);
            hasher.write(&partiton_id.to_le_bytes());
            hasher.write(member_id.as_bytes());
            let rank = hasher.finish_128();
            let result: (u128, Uuid) = (rank, *member_id);
            top_k.push(result);
        }

        let master_id = top_k
            .pop()
            .expect("No zero member list previously checked")
            .1;

        let replicas_len = top_k.len();
        let mut replica_ids = Vec::with_capacity(replicas_len);
        for _ in 0..replicas_len {
            let replica_id = top_k
                .pop()
                .expect("Replica availability previously checked")
                .1;

            replica_ids.push(replica_id);
        }

        (master_id, replica_ids)
    }

    fn update(&self, member_ids: &[Uuid]) -> Vec<MembershipChange> {
        let mut metadata = self
            .metadata
            .write()
            .expect("Critical section cannot be poisoned");
        let (master_id, replica_ids) = Partition::compute_membership(metadata.id, member_ids);
        metadata.update(master_id, replica_ids)
    }
}

pub struct Metadata {
    id: usize,
    master_id: Uuid,
    replica_ids: Vec<Uuid>,
}

impl Metadata {
    pub fn new(id: usize, master_id: Uuid, replica_ids: Vec<Uuid>) -> Self {
        Self {
            id,
            master_id,
            replica_ids,
        }
    }

    pub fn update(&mut self, master_id: Uuid, replica_ids: Vec<Uuid>) -> Vec<MembershipChange> {
        let mut changes = Vec::with_capacity(METADATA_CHANGE_VARIANT_LEN);

        if !self.master_id.eq(&master_id) {
            let old_master_id = self.master_id;
            self.master_id = master_id;
            changes.push(MembershipChange::MasterChanged {
                old: old_master_id,
                new: self.master_id,
            });
        }

        let old_replicas: HashSet<&Uuid> = self.replica_ids.iter().collect();
        let new_replicas: HashSet<&Uuid> = replica_ids.iter().collect();
        let added: Vec<Uuid> = new_replicas
            .difference(&old_replicas)
            .map(|s| **s)
            .collect();
        let removed: Vec<Uuid> = old_replicas
            .difference(&new_replicas)
            .map(|s| **s)
            .collect();

        if !added.is_empty() || !removed.is_empty() {
            self.replica_ids = replica_ids;
            changes.push(MembershipChange::ReplicasChanged { added, removed });
        }

        changes
    }
}

const METADATA_CHANGE_VARIANT_LEN: usize = 2;
pub enum MembershipChange {
    MasterChanged {
        old: Uuid,
        new: Uuid,
    },
    ReplicasChanged {
        added: Vec<Uuid>,
        removed: Vec<Uuid>,
    },
}
