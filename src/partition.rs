use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Instant;
use std::{
    array,
    collections::{BinaryHeap, HashMap, HashSet},
    sync::{
        Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};
use thiserror::Error;
use twox_hash::{XxHash3_64, XxHash3_128};
use uuid::Uuid;

use crate::network::packet::Reply;
use crate::network::{Response, packet};
use crate::sync::AsyncState;
use crate::{instance, object, partition};

const RING_CAPACITY: usize = 1024;
const PARTITION_HASH_SEED: u64 = 1234;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Partition not found")]
    PartitionNotFound,
    #[error("Partition not ready")]
    PartitionNotReady,
    #[error("Rebalance in progress")]
    Rebalance,
    #[error("Not enough members")]
    NotEnoughMembers,
    #[error("Not enough replicas")]
    NotEnoughReplicas,
}

pub struct Segment {
    id: String,
    pub(crate) data: Box<dyn object::Raw>,
    // Useful for entropy process of partitions after membership changes or corruption detection
    seq: u64,
    checksum: u64,
}

impl Segment {
    pub fn new<S: object::Raw + 'static>(id: String, data: S) -> Self {
        Self {
            id,
            data: Box::new(data),
            seq: 0,
            checksum: 0,
        }
    }
}

pub struct Group {
    partitions: [Partition; RING_CAPACITY],
    version: Mutex<u128>,
    initialized: AtomicBool,
}

impl Group {
    pub fn new<F: Fn() -> Segment>(member_id: Uuid, segment_fns: &[F]) -> Self {
        let partitions: [Partition; RING_CAPACITY] =
            array::from_fn(|id| Partition::new(id, member_id, segment_fns));

        Self {
            partitions,
            version: Mutex::new(version(&[member_id])),
            initialized: AtomicBool::new(false),
        }
    }

    pub async fn await_ready(
        &self,
        part_id: usize,
        timeout: Option<Duration>,
    ) -> Result<(), Error> {
        if part_id >= RING_CAPACITY {
            return Err(Error::PartitionNotFound);
        }

        let partition = &self.partitions[part_id];
        if !partition.await_ready(timeout).await {
            return Err(Error::PartitionNotReady);
        }

        Ok(())
    }

    pub fn partition_of(&self, key: &[u8]) -> u64 {
        let hash = XxHash3_64::oneshot_with_seed(PARTITION_HASH_SEED, key);
        hash & (RING_CAPACITY as u64 - 1)
    }

    pub fn owner_of(&self, part_id: u64) -> Uuid {
        if part_id >= RING_CAPACITY as u64 {
            panic!("Partition id out of range");
        }
        let partition = &self.partitions[part_id as usize];
        let metadata = partition
            .metadata
            .read()
            .expect("partition metadata RwLock poisoned");

        metadata.master_id
    }

    pub fn replicas_of(&self, part_id: u64, size: usize) -> Vec<Uuid> {
        if part_id >= RING_CAPACITY as u64 {
            panic!("Partition id out of range");
        }

        let partition = &self.partitions[part_id as usize];
        let metadata = partition
            .metadata
            .read()
            .expect("partition metadata RwLock poisoned");

        metadata.replica_ids[0..size].to_vec()
    }

    pub fn is_replica(&self, member_id: Uuid, part_id: u64, repl_factor: usize) -> bool {
        if part_id >= RING_CAPACITY as u64 {
            panic!("Partition id out of range");
        }

        if repl_factor == 0 {
            false
        } else {
            let partition = &self.partitions[part_id as usize];
            let p_metadata = partition
                .metadata
                .read()
                .expect("partition metadata RwLock poisoned");

            p_metadata.replica_ids[0..repl_factor].contains(&member_id)
        }
    }

    pub fn with_partition_read<F, O>(&self, part_id: usize, f: F) -> Result<O, Error>
    where
        F: FnOnce(&Partition) -> O,
    {
        if part_id >= RING_CAPACITY {
            return Err(Error::PartitionNotFound);
        }

        let partition = &self.partitions[part_id];
        Ok(f(partition))
    }

    pub fn with_partitions_read<F>(&self, f: F)
    where
        F: Fn(&Partition),
    {
        for part in self.partitions.iter() {
            f(part);
        }
    }

    pub fn update_partitions(&self, member_ids: &[Uuid]) -> HashMap<usize, Vec<MembershipChange>> {
        let mut result = HashMap::with_capacity(RING_CAPACITY);
        for part_id in 0..RING_CAPACITY {
            let partition = &self.partitions[part_id];
            let changes = partition.update(member_ids);
            if !changes.is_empty() {
                result.insert(part_id, changes);
            }
        }

        result
    }

    pub fn update_version(&self, member_ids: &[Uuid]) {
        let mut version = self
            .version
            .lock()
            .expect("partition group version mutex poisoned");
        *version = partition::version(member_ids);
    }

    pub fn with_segment_read<F, O>(
        &self,
        part_id: usize,
        obj_id: &str,
        f: F,
    ) -> Result<O, partition::Error>
    where
        F: FnOnce(&Segment) -> O,
    {
        if part_id >= RING_CAPACITY {
            return Err(Error::PartitionNotFound);
        }

        let partition = &self.partitions[part_id];
        let segment = partition
            .segments
            .get(obj_id)
            .expect("segment must exist for object id (caller precondition)")
            .read()
            .expect("segment RwLock poisoned");
        Ok(f(&segment))
    }

    pub fn with_segment_write<F>(
        &self,
        part_id: usize,
        obj_id: &str,
        f: F,
    ) -> Result<(), partition::Error>
    where
        F: FnOnce(&mut Segment),
    {
        if part_id >= RING_CAPACITY {
            return Err(Error::PartitionNotFound);
        }

        let partition = &self.partitions[part_id];
        let mut segment = partition
            .segments
            .get(obj_id)
            .expect("segment must exist for object id (caller precondition)")
            .write()
            .expect("segment RwLock poisoned");
        f(&mut segment);
        Ok(())
    }

    pub fn version(&self) -> u128 {
        *self
            .version
            .lock()
            .expect("partition group version mutex poisoned")
    }

    pub fn is_initialized(&self) -> bool {
        self.initialized.load(Ordering::Acquire)
    }

    pub fn initialize(&self) {
        self.initialized.store(true, Ordering::Release);
    }
}

pub struct Partition {
    metadata: RwLock<Metadata>,
    segments: HashMap<object::Id, RwLock<Segment>>,
    state: AsyncState<Lifecycle>,
}

impl Partition {
    pub fn new<F: Fn() -> Segment>(id: usize, member_id: Uuid, segment_fns: &[F]) -> Self {
        let (master_id, replica_ids) = members_rank(id, &[member_id]);

        let mut segments = HashMap::with_capacity(segment_fns.len());
        for f in segment_fns {
            let segment = f();
            segments.insert(segment.id.to_string(), RwLock::new(segment));
        }

        Self {
            metadata: RwLock::new(Metadata::new(id, master_id, replica_ids)),
            segments,
            state: AsyncState::new(),
        }
    }

    pub fn with_segment(mut self, segment: Segment) -> Self {
        self.segments
            .insert(segment.id.to_string(), RwLock::new(segment));
        self
    }

    pub fn update(&self, member_ids: &[Uuid]) -> Vec<MembershipChange> {
        let mut metadata = self
            .metadata
            .write()
            .expect("partition metadata RwLock poisoned");
        let (master_id, replica_ids) = members_rank(metadata.id, member_ids);
        metadata.update(master_id, replica_ids)
    }

    pub fn set_state(&self, lifecycle: Lifecycle) {
        self.state.update(lifecycle);
    }

    pub fn await_ready(&self, timeout: Option<Duration>) -> impl Future<Output = bool> {
        self.state.await_until(Lifecycle::Ready, timeout)
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

#[derive(Copy, Clone, Default, Eq, PartialEq)]
pub enum Lifecycle {
    Ready,
    #[default]
    Migration,
}

pub fn version(member_ids: &[Uuid]) -> u128 {
    const COUNT_BITS: u32 = 16;
    const COUNT_MASK: u128 = (1u128 << COUNT_BITS) - 1;
    const HASH_MASK: u128 = !COUNT_MASK;
    let mut hasher = XxHash3_128::with_seed(PARTITION_HASH_SEED);
    for member_id in member_ids {
        hasher.write(member_id.as_bytes());
    }

    (hasher.finish_128() & HASH_MASK) | (member_ids.len() as u128 & COUNT_MASK)
}

fn members_rank(part_id: usize, member_ids: &[Uuid]) -> (Uuid, Vec<Uuid>) {
    if member_ids.is_empty() {
        panic!("Members cannot be empty");
    }

    let mut top_k = BinaryHeap::with_capacity(member_ids.len());
    for member_id in member_ids {
        let mut hasher = XxHash3_128::with_seed(PARTITION_HASH_SEED);
        hasher.write(&part_id.to_le_bytes());
        hasher.write(member_id.as_bytes());
        let rank = hasher.finish_128();
        let result: (u128, Uuid) = (rank, *member_id);
        top_k.push(result);
    }

    let master_id = top_k
        .pop()
        .expect("BinaryHeap must yield master: member_ids was non-empty")
        .1;

    let replicas_len = top_k.len();
    let mut replica_ids = Vec::with_capacity(replicas_len);
    for _ in 0..replicas_len {
        let replica_id = top_k
            .pop()
            .expect("BinaryHeap pop count must match replica slot count")
            .1;

        replica_ids.push(replica_id);
    }

    (master_id, replica_ids)
}

pub struct RebalanceScheduler {
    tx: mpsc::Sender<()>,
}

impl RebalanceScheduler {
    pub fn schedule(&self) {
        self.tx.send(()).expect("Failed to invoke rebalance");
    }
}

pub(crate) fn rebalance_sched(instance: instance::Weak) -> (RebalanceScheduler, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel();
    let id = instance.as_ref().upgrade().unwrap().id;
    let h = thread::Builder::new()
        .name("partition-rebalance".to_string())
        .spawn(move || {
            log::info!("Partition rebalance scheduler initialized. Id: {}.", id);

            while rx.recv().is_ok() {
                if let Some(ref_) = instance.as_ref().upgrade() {
                    let mut members;

                    'rebalance: loop {
                        members = ref_.membership.all();
                        let target_version = version(members.as_slice());
                        if target_version.eq(&ref_.part_group.version()) {
                            break 'rebalance;
                        }

                        struct FetchEntry<'a> {
                            part_id: usize,
                            member_id: Uuid,
                            segments: HashSet<&'a String>,
                        }

                        log::info!("rebalance has started. Id: {}.", ref_.id);

                        ref_.part_group.with_partitions_read(|part| {
                            part.set_state(Lifecycle::Ready);
                        });

                        let mut fetch: HashMap<usize, FetchEntry> = HashMap::new();
                        for (part_id, changes) in
                            ref_.part_group.update_partitions(members.as_slice())
                        {
                            let mut ready = true;
                            for change in changes {
                                match change {
                                    MembershipChange::MasterChanged { old, new } => {
                                        if new.eq(&ref_.id) && members.contains(&old) {
                                            ready = false;
                                            fetch.insert(
                                                part_id,
                                                FetchEntry {
                                                    part_id,
                                                    member_id: old,
                                                    segments: ref_
                                                        .objects
                                                        .keys()
                                                        .collect::<HashSet<_>>(),
                                                },
                                            );
                                        }

                                        if old.eq(&ref_.id) {
                                            ready = false;
                                        }
                                    }

                                    MembershipChange::ReplicasChanged {
                                        added: _,
                                        removed: _,
                                    } => {}
                                }
                            }

                            if ready {
                                if ref_.part_group.is_initialized() {
                                    ref_.part_group
                                        .with_partition_read(part_id, |part| {
                                            part.set_state(Lifecycle::Ready)
                                        })
                                        .expect("partition must exist");
                                } else if ref_.part_group.owner_of(part_id as u64).eq(&ref_.id) {
                                    let member_id = *ref_
                                        .part_group
                                        .replicas_of(part_id as u64, 1)
                                        .first()
                                        .expect(
                                            "replicas_of(..., 1) must return one replica for fetch",
                                        );
                                    fetch.insert(
                                        part_id,
                                        FetchEntry {
                                            part_id,
                                            member_id,
                                            segments: ref_.objects.keys().collect::<HashSet<_>>(),
                                        },
                                    );
                                }
                            }
                        }

                        enum FetchResponse<'a> {
                            FetchObject {
                                obj_id: &'a String,
                                resp: Response<'a>,
                            },
                            Completion(Response<'a>),
                        }

                        let v = ref_.part_group.version();
                        let mut pending_partitions = fetch.keys().copied().collect::<HashSet<_>>();
                        loop {
                            if pending_partitions.is_empty() {
                                break 'rebalance;
                            }

                            // Some peers changed in the meantime, restarting process
                            if !members.eq(&ref_.membership.all()) {
                                continue 'rebalance;
                            }

                            let mut timeout = ref_.config.partition.rebalance_timeout;
                            let start = Instant::now();
                            let mut response_map = HashMap::new();
                            for (part_id, req) in fetch.iter() {
                                if !pending_partitions.contains(part_id) {
                                    continue;
                                }

                                response_map.insert(
                                    req.part_id,
                                    Vec::with_capacity(req.segments.len().max(1)),
                                );
                                if req.segments.is_empty() {
                                    match ref_.net.request_sync(
                                        req.member_id,
                                        packet::Request::PartitionFetchCompletion {
                                            v,
                                            corr_id: Uuid::new_v4(),
                                            part_id: *part_id as u16,
                                        },
                                    ) {
                                        Ok(response) => {
                                            if let Some(entry) = response_map.get_mut(&req.part_id)
                                            {
                                                entry.push(FetchResponse::Completion(response));
                                            }
                                        }
                                        Err(err) => {
                                            log::error!(
                                                "PartitionFetchCompletion request failed. Err: {}.",
                                                err
                                            );
                                        }
                                    }

                                    continue;
                                }

                                for &obj_id in req.segments.iter() {
                                    match ref_.net.request_sync(
                                        req.member_id,
                                        packet::Request::FetchObject {
                                            v,
                                            corr_id: Uuid::new_v4(),
                                            part_id: req.part_id as u16,
                                            obj_id,
                                        },
                                    ) {
                                        Ok(response) => {
                                            if let Some(entry) = response_map.get_mut(&req.part_id)
                                            {
                                                entry.push(FetchResponse::FetchObject {
                                                    obj_id,
                                                    resp: response,
                                                });
                                            }
                                        }
                                        Err(err) => {
                                            log::error!(
                                                "FetchObject request failed. Err: {}.",
                                                err
                                            );
                                        }
                                    }
                                }
                            }

                            for (part_id, responses) in response_map {
                                for entry in responses {
                                    match entry {
                                        FetchResponse::FetchObject { obj_id, resp } => {
                                            match resp.blocking_recv_timeout(timeout) {
                                                Ok(raw) => match Reply::try_from(&raw) {
                                                    Ok(Reply::FetchObject { result, .. }) => {
                                                        if let Err(err) = ref_.rebuild(
                                                            obj_id,
                                                            part_id as u16,
                                                            result,
                                                        ) {
                                                            log::error!(
                                                                "rebuild failed. Err: {}.",
                                                                err
                                                            );
                                                        } else if let Some(entry) =
                                                            fetch.get_mut(&part_id)
                                                        {
                                                            entry.segments.remove(obj_id);
                                                        }
                                                    }
                                                    Ok(_) => {
                                                        log::error!(
                                                            "unexpected reply variant for FetchObject."
                                                        );
                                                    }
                                                    Err(err) => {
                                                        log::error!(
                                                            "FetchObject reply decode failed. Err: {}.",
                                                            err
                                                        );
                                                    }
                                                },
                                                Err(err) => {
                                                    log::error!(
                                                        "Fetch object error. Err: {}.",
                                                        err
                                                    );
                                                }
                                            }
                                        }
                                        FetchResponse::Completion(resp) => {
                                            match resp.blocking_recv_timeout(timeout) {
                                                Ok(_) => {
                                                    pending_partitions.remove(&part_id);
                                                    ref_.part_group
                                                        .with_partition_read(part_id, |part| {
                                                            part.set_state(
                                                                partition::Lifecycle::Ready,
                                                            );
                                                        })
                                                        .expect("partition must exist");
                                                }
                                                Err(err) => {
                                                    log::error!(
                                                        "Partition Completion error. Err: {}.",
                                                        err
                                                    );
                                                }
                                            }
                                        }
                                    }

                                    timeout = timeout.saturating_sub(start.elapsed());
                                }
                            }
                        }
                    }

                    ref_.part_group.update_version(&members);
                    ref_.part_group.initialize();
                    ref_.part_group
                        .with_partitions_read(|part| part.set_state(Lifecycle::Ready));

                    log::info!("Rebalance has finished. Id: {}.", ref_.id);
                }
            }

            log::info!("Partition rebalance scheduler destroyed. Id: {}.", id);
        })
        .expect("partition rebalance thread: OS refused or thread count invalid");

    (RebalanceScheduler { tx }, h)
}
