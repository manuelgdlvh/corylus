use std::{
    array,
    collections::{BinaryHeap, HashMap, HashSet},
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use twox_hash::{XxHash3_64, XxHash3_128};
use uuid::Uuid;

const RING_CAPACITY: usize = 1024;
const PARTITION_HASH_SEED: u64 = 1234;

pub trait RawSegment {
    fn rebuild(&mut self, raw: Vec<u8>);
    fn as_raw(&self) -> &[u8];
}

pub struct Segment {
    id: &'static str,
    data: Box<dyn RawSegment>,
    seq: u64,
    checksum: u64,
}

impl Segment {
    pub fn new<S: RawSegment + Default + 'static>(id: &'static str) -> Self {
        Self {
            id,
            data: Box::new(S::default()),
            seq: 0,
            checksum: 0,
        }
    }
}

pub struct Group {
    replica_factor: usize,
    partitions: [Partition; RING_CAPACITY],
}

impl Group {
    pub fn new(member_ids: &[Uuid], replica_factor: usize) -> Self {
        let partitions: [Partition; RING_CAPACITY] =
            array::from_fn(|id| Partition::new(id, replica_factor, member_ids));

        Self {
            partitions,
            replica_factor,
        }
    }

    pub fn with_segment<F>(mut self, f: F) -> Self
    where
        F: Fn() -> Segment,
    {
        for idx in 0..RING_CAPACITY {
            // SAFELY move out the partition without needing Default
            let partition = unsafe {
                // get a raw pointer to the element
                let ptr = self.partitions.as_mut_ptr().add(idx);
                std::ptr::read(ptr) // moves the value out
            };

            let segment = f();
            unsafe {
                // put the modified partition back
                let ptr = self.partitions.as_mut_ptr().add(idx);
                std::ptr::write(ptr, partition.with_segment(segment));
            }
        }

        self
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

    pub fn segment_read(
        &self,
        segment_id: &'static str,
        partition_id: u64,
    ) -> Option<SegmentReadGuard<'_>> {
        if partition_id >= RING_CAPACITY as u64 {
            panic!("Partition id out of range");
        }
        let partition = &self.partitions[partition_id as usize];
        partition
            .segments
            .get(segment_id)
            .map(|s| s.read().expect("No poisoned"))
            .map(|g| SegmentReadGuard { g })
    }

    pub fn segment_write(
        &self,
        segment_id: &'static str,
        partition_id: u64,
    ) -> Option<SegmentWriteGuard<'_>> {
        if partition_id >= RING_CAPACITY as u64 {
            panic!("Partition id out of range");
        }
        let partition = &self.partitions[partition_id as usize];
        partition
            .segments
            .get(segment_id)
            .map(|s| s.write().expect("No poisoned"))
            .map(|g| SegmentWriteGuard { g })
    }

    // The algorithm should be after updated, i get as read lock all the states that i should migrate. Then i send to assigned peer the snapshot. When OK i clear and unlock.
    pub fn update(&self, member_ids: &[Uuid]) -> HashMap<usize, Vec<MembershipChange>> {
        let mut result = HashMap::with_capacity(RING_CAPACITY);
        for p_id in 0..RING_CAPACITY {
            let partition = &self.partitions[p_id];
            let changes = partition.update(self.replica_factor, member_ids);
            if !changes.is_empty() {
                result.insert(p_id, changes);
            }
        }

        result
    }
}

pub struct Partition {
    metadata: RwLock<Metadata>,
    // TODO: Think about it. Checksum and seq to track inconsistencies
    segments: HashMap<&'static str, RwLock<Segment>>,
}

impl Partition {
    pub fn new(id: usize, replica_factor: usize, member_ids: &[Uuid]) -> Self {
        let (master_id, replica_ids) =
            Partition::compute_membership(id, replica_factor, member_ids);

        Self {
            metadata: RwLock::new(Metadata::new(id, master_id, replica_ids)),
            segments: Default::default(),
        }
    }

    pub fn with_segment(mut self, segment: Segment) -> Self {
        self.segments.insert(segment.id, RwLock::new(segment));
        self
    }

    fn compute_membership(
        partiton_id: usize,
        replica_factor: usize,
        member_ids: &[Uuid],
    ) -> (Uuid, Vec<Uuid>) {
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

        let replicas_len = top_k.len().min(replica_factor);
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

    fn update(&self, replica_factor: usize, member_ids: &[Uuid]) -> Vec<MembershipChange> {
        let mut metadata = self
            .metadata
            .write()
            .expect("Critical section cannot be poisoned");
        let (master_id, replica_ids) =
            Partition::compute_membership(metadata.id, replica_factor, member_ids);
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

pub struct SegmentReadGuard<'a> {
    g: RwLockReadGuard<'a, Segment>,
}

pub struct SegmentWriteGuard<'a> {
    g: RwLockWriteGuard<'a, Segment>,
}
