use std::{
    collections::{HashMap, HashSet},
    io,
    marker::PhantomData,
    sync::{self, Arc, Mutex},
    thread::JoinHandle,
    time::Duration,
};

use uuid::Uuid;

use crate::network::packet::Reply;
use crate::object::operation;
use crate::{
    CorylusError, CorylusResult, Instance,
    network::{
        self,
        packet::{self},
    },
    object::{
        self,
        map::{Get, Put},
    },
    partition::{self, Segment},
    runtime::Logger,
};

pub mod task;

// --- State markers ---

pub struct NeedsId;
pub struct NeedsConfig;
pub struct NeedsDiscovery;
pub struct Ready;

// --- Builder ---

pub struct Builder<S> {
    id: Option<Uuid>,
    c: Option<Config>,
    d: Option<network::Discovery>,
    objects: HashMap<object::Id, object::Metadata>,
    segment_fns: Vec<Box<dyn Fn() -> Segment>>,
    _state: PhantomData<S>,
}

impl Default for Builder<NeedsId> {
    fn default() -> Self {
        Self::new()
    }
}

impl Builder<NeedsId> {
    pub fn new() -> Self {
        Self {
            id: None,
            c: None,
            d: None,
            objects: HashMap::new(),
            segment_fns: Vec::new(),
            _state: PhantomData,
        }
    }
    pub fn with_id(self, id: Uuid) -> Builder<NeedsConfig> {
        Builder {
            id: Some(id),
            c: self.c,
            d: self.d,
            objects: self.objects,
            segment_fns: self.segment_fns,
            _state: PhantomData,
        }
    }
}

impl Builder<NeedsConfig> {
    pub fn with_config(self, c: Config) -> Builder<NeedsDiscovery> {
        Builder {
            id: self.id,
            c: Some(c),
            d: self.d,
            objects: self.objects,
            segment_fns: self.segment_fns,
            _state: PhantomData,
        }
    }
}

impl Builder<NeedsDiscovery> {
    pub fn with_discovery(self, d: network::Discovery) -> Builder<Ready> {
        Builder {
            id: self.id,
            c: self.c,
            d: Some(d),

            objects: self.objects,
            segment_fns: self.segment_fns,
            _state: PhantomData,
        }
    }
}

impl Builder<Ready> {
    pub fn with_map<K, V>(mut self, id: &str, repl_config: object::ReplicationConfig) -> Self
    where
        K: object::map::Key,
        V: object::map::Value,
    {
        let id = format!("map:{}", id);

        let op_reg = operation::Registry::new()
            .with_read_op::<Get<K, V>>()
            .with_write_op::<Put<K, V>>();
        self.objects
            .insert(id.to_string(), object::Metadata::new(op_reg, repl_config));
        self.segment_fns.push(Box::new(move || {
            let data: HashMap<K, V> = HashMap::new();
            Segment::new(id.to_string(), data)
        }));

        self
    }

    pub fn build<L: Logger>(self, logger: L) -> io::Result<Instance<L>> {
        let id = self
            .id
            .expect("Builder<Ready>: with_id must have been called");
        let d = self
            .d
            .expect("Builder<Ready>: with_discovery must have been called");
        let c = self
            .c
            .expect("Builder<Ready>: with_config must have been called");

        let shutdown = Shutdown::new();

        let (net_sender, net_receiver) =
            network::handle(id, d, shutdown.clone(), c.network, logger.clone())?;
        let partition = partition::Group::new(id, self.segment_fns.as_slice());
        let instance = Instance::new(
            id,
            logger,
            net_sender,
            partition,
            self.objects,
            c,
            shutdown.clone(),
        );

        shutdown.register(net_receiver.start(instance.downgrade())?);

        Ok(instance)
    }
}

// --- Shutdown ---
mod shutdown {
    use crate::sync::State;
    use std::{sync::Mutex, thread::JoinHandle, time::Duration};

    pub struct Inner {
        state: State<bool>,
        handles: Mutex<Vec<JoinHandle<()>>>,
    }

    impl Default for Inner {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Inner {
        pub fn new() -> Self {
            Self {
                state: State::new(),
                handles: Mutex::new(Vec::new()),
            }
        }

        pub fn update(&self, val: bool) {
            self.state.update(val);
        }

        pub fn with_handles_write<F, O>(&self, f: F) -> O
        where
            F: FnOnce(&mut Vec<JoinHandle<()>>) -> O,
        {
            let mut handles = self
                .handles
                .lock()
                .expect("shutdown join-handles mutex poisoned");
            f(&mut handles)
        }

        pub fn checkpoint(&self, timeout: Option<Duration>) -> bool {
            !self.state.await_until(true, timeout)
        }
    }
}

#[derive(Clone)]
pub struct Shutdown {
    inner: Arc<shutdown::Inner>,
}

impl AsRef<shutdown::Inner> for Shutdown {
    fn as_ref(&self) -> &shutdown::Inner {
        &self.inner
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

impl Shutdown {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(shutdown::Inner::new()),
        }
    }

    pub fn register(&self, h: JoinHandle<()>) {
        self.as_ref().with_handles_write(|handles| {
            handles.push(h);
        })
    }

    pub fn checkpoint(&self, timeout: Option<Duration>) -> bool {
        self.as_ref().checkpoint(timeout)
    }

    pub fn destroy(&self) {
        self.inner.update(true);
        let handles = self.as_ref().with_handles_write(std::mem::take);
        for h in handles {
            let _ = h.join();
        }
    }
}

/// Thread pool sizing for inbound packet tasks. Rebalance uses a fixed single-thread pool.
#[derive(Clone, Copy)]
pub struct TaskConfig {
    pub read_threads: usize,
    pub write_threads: usize,
}

impl Default for TaskConfig {
    fn default() -> Self {
        Self {
            read_threads: 16,
            write_threads: 4,
        }
    }
}

#[derive(Clone, Copy)]
pub struct PartitionConfig {
    /// Initial budget for partition fetch / completion waits during rebalance (shared across responses in one iteration).
    pub rebalance_timeout: Duration,
    /// Max time to wait for partition readiness before read/write paths fail.
    pub ready_timeout: Duration,
}

impl Default for PartitionConfig {
    fn default() -> Self {
        Self {
            rebalance_timeout: Duration::from_secs(2),
            ready_timeout: Duration::from_secs(3),
        }
    }
}

#[derive(Clone, Copy)]
pub struct ClusterConfig {
    pub min_size: usize,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self { min_size: 1 }
    }
}

#[derive(Clone, Copy, Default)]
pub struct Config {
    pub network: network::Config,
    pub partition: PartitionConfig,
    pub task: TaskConfig,
    pub cluster: ClusterConfig,
}

#[derive(Clone)]
pub struct Weak<L: Logger> {
    inner: sync::Weak<Inner<L>>,
}

impl<L: Logger> Weak<L> {
    pub fn new(inner: sync::Weak<Inner<L>>) -> Self {
        Self { inner }
    }
}

impl<L: Logger> AsRef<sync::Weak<Inner<L>>> for Weak<L> {
    fn as_ref(&self) -> &sync::Weak<Inner<L>> {
        &self.inner
    }
}

pub struct Membership(Mutex<HashSet<Uuid>>);

impl Default for Membership {
    fn default() -> Self {
        Self::new()
    }
}

impl Membership {
    pub fn new() -> Self {
        Self(Mutex::new(HashSet::new()))
    }

    pub(crate) fn remove(&self, id: Uuid) {
        let mut members = self.0.lock().expect("cluster membership mutex poisoned");
        members.remove(&id);
    }

    pub(crate) fn add(&self, id: Uuid) {
        let mut members = self.0.lock().expect("cluster membership mutex poisoned");
        members.insert(id);
    }

    pub(crate) fn all(&self) -> Vec<Uuid> {
        let mut member_ids = self
            .0
            .lock()
            .expect("cluster membership mutex poisoned")
            .iter()
            .copied()
            .collect::<Vec<_>>();
        member_ids.sort_unstable();
        member_ids
    }

    pub(crate) fn size(&self) -> usize {
        self.0
            .lock()
            .expect("cluster membership mutex poisoned")
            .len()
    }
}

pub struct Inner<L: Logger> {
    pub(crate) id: Uuid,
    pub(crate) logger: L,
    pub(crate) net: network::Sender<L>,
    pub(crate) config: Config,
    pub(crate) part_group: partition::Group,
    pub(crate) objects: HashMap<object::Id, object::Metadata>,
    pub(crate) membership: Membership,
    pub(crate) shutdown: Shutdown,
}

impl<L: Logger> Drop for Inner<L> {
    fn drop(&mut self) {
        self.shutdown.destroy();
    }
}

impl<L: Logger> Inner<L> {
    pub(crate) fn rebuild(&self, obj_id: &str, part_id: u16, raw: &[u8]) -> CorylusResult<()> {
        if !self.objects.contains_key(obj_id) {
            return Err(CorylusError::Object(object::Error::ObjectNotFound));
        }
        self.part_group
            .with_segment_write(part_id as usize, obj_id, |segment| {
                (*segment.data).rebuild(raw)
            })
            .map_err(|err| err.into())
    }

    pub(crate) fn fetch(&self, obj_id: &str, part_id: u16) -> CorylusResult<Vec<u8>> {
        if !self.objects.contains_key(obj_id) {
            return Err(CorylusError::Object(object::Error::ObjectNotFound));
        }
        self.part_group
            .with_segment_read(part_id as usize, obj_id, |segment| (*segment.data).as_raw())
            .map_err(|err| err.into())
    }

    pub(crate) fn remote_write<O: operation::Write>(
        &self,
        obj_id: &str,
        part_id: u16,
        v: u128,
        mut op: O,
    ) -> CorylusResult<()> {
        if !self.objects.contains_key(obj_id) {
            return Err(CorylusError::Object(object::Error::ObjectNotFound));
        }
        let owner = self.part_group.owner_of(part_id as u64);

        if self.id.eq(&owner) {
            self.check_repl_requirements(obj_id)?;
        }

        self.part_group
            .await_ready(part_id as usize, Some(self.config.partition.ready_timeout))?;
        if !self.part_group.version().eq(&v) {
            return Err(partition::Error::Rebalance.into());
        }

        // Implement double check of part group version inside lock.
        // Implement await of status change if not ready and tryLock.
        self.part_group
            .with_segment_write(part_id as usize, obj_id, |segment| {
                op.execute(&mut *segment.data);
            })?;

        if self.id.eq(&owner) {
            self.replicate(obj_id, part_id as u64, op)
        } else {
            Ok(())
        }
    }

    pub(crate) fn remote_read<O: operation::Read>(
        &self,
        obj_id: &str,
        part_id: u16,
        v: u128,
        op: O,
    ) -> CorylusResult<Vec<u8>> {
        if !self.objects.contains_key(obj_id) {
            return Err(CorylusError::Object(object::Error::ObjectNotFound));
        }
        // If partition version is correct is ensured that this node is master or replica.

        self.part_group
            .await_ready(part_id as usize, Some(self.config.partition.ready_timeout))?;
        if !self.part_group.version().eq(&v) {
            return Err(partition::Error::Rebalance.into());
        }

        // Implement double check of part group version inside lock.
        // Implement await of status change if not ready and tryLock.
        self.part_group
            .with_segment_read(part_id as usize, obj_id, |segment| {
                op.execute(&*segment.data)
            })
            .map_err(|err| err.into())
    }

    pub(crate) fn write<O: operation::Write>(&self, obj_id: &str, mut op: O) -> CorylusResult<()> {
        if !self.objects.contains_key(obj_id) {
            return Err(CorylusError::Object(object::Error::ObjectNotFound));
        }

        if self.config.cluster.min_size > self.membership.size() {
            return Err(CorylusError::Partition(partition::Error::NotEnoughMembers));
        }

        let key = op.partition_key();
        let part_id = self.part_group.partition_of(&key);

        self.part_group
            .await_ready(part_id as usize, Some(self.config.partition.ready_timeout))?;
        let owner = self.part_group.owner_of(part_id);

        if self.id.eq(&owner) {
            self.check_repl_requirements(obj_id)?;
            self.part_group
                .with_segment_write(part_id as usize, obj_id, |segment| {
                    op.execute(&mut *segment.data);
                })?;
            self.replicate(obj_id, part_id, op)
        } else {
            let raw_op = op.serialize();
            let response = self.net.request_sync(
                owner,
                packet::Request::WriteOp {
                    v: self.part_group.version(),
                    corr_id: Uuid::new_v4(),
                    part_id: part_id as u16,
                    obj_id,
                    op_id: op.id(),
                    raw_op: &raw_op,
                },
            )?;

            let raw: packet::Raw = response.get()?;
            let reply = Reply::try_from(&raw).map_err(CorylusError::from)?;
            match reply {
                Reply::WriteOp { status, .. } => match status {
                    packet::Status::Success => Ok(()),
                    s => Err(CorylusError::try_from(s).unwrap_or_else(|_| {
                        CorylusError::Io(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid WriteOp status",
                        ))
                    })),
                },
                _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid data").into()),
            }
        }
    }

    fn check_repl_requirements(&self, obj_id: &str) -> CorylusResult<()> {
        let metadata = self
            .objects
            .get(obj_id)
            .ok_or(object::Error::ObjectNotFound)?;
        let repl_factor = metadata.repl_factor();
        if repl_factor != 0 && repl_factor > (self.membership.size() - 1) {
            return Err(CorylusError::from(partition::Error::NotEnoughReplicas));
        }

        Ok(())
    }

    fn replicate<O: operation::Write>(
        &self,
        obj_id: &str,
        part_id: u64,
        op: O,
    ) -> CorylusResult<()> {
        let metadata = self
            .objects
            .get(obj_id)
            .ok_or(object::Error::ObjectNotFound)?;
        let repl_factor = metadata.repl_factor();
        if repl_factor != 0 {
            let raw_op = op.serialize();
            let replica_ids = self.part_group.replicas_of(part_id, repl_factor);

            match metadata.repl() {
                object::Replication::Sync => {
                    let mut responses = Vec::with_capacity(replica_ids.len());
                    for replica_id in replica_ids {
                        let response = self.net.request_sync(
                            replica_id,
                            packet::Request::WriteOp {
                                v: self.part_group.version(),
                                corr_id: Uuid::new_v4(),
                                part_id: part_id as u16,
                                obj_id,
                                op_id: op.id(),
                                raw_op: &raw_op,
                            },
                        )?;
                        responses.push(response);
                    }

                    for response in responses {
                        let raw = response.get()?;
                        let reply = Reply::try_from(&raw).map_err(CorylusError::from)?;
                        match reply {
                            Reply::WriteOp { status, .. } => match status {
                                packet::Status::Success => Ok(()),
                                s => Err(CorylusError::try_from(s).unwrap_or_else(|_| {
                                    CorylusError::Io(io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        "invalid WriteOp status",
                                    ))
                                })),
                            },
                            _ => {
                                Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid data")
                                    .into())
                            }
                        }?;
                    }
                }
                object::Replication::Async => {
                    for replica_id in replica_ids {
                        self.net.request(
                            replica_id,
                            packet::Request::WriteOp {
                                v: self.part_group.version(),
                                corr_id: Uuid::new_v4(),
                                part_id: part_id as u16,
                                obj_id,
                                op_id: op.id(),
                                raw_op: &raw_op,
                            },
                        )?;
                    }
                }
                object::Replication::None => {}
            }
        }

        Ok(())
    }

    pub(crate) fn read<O: operation::Read>(&self, obj_id: &str, op: O) -> CorylusResult<Vec<u8>> {
        if !self.objects.contains_key(obj_id) {
            return Err(CorylusError::Object(object::Error::ObjectNotFound));
        }
        let key = op.partition_key();
        let part_id = self.part_group.partition_of(&key);

        self.part_group
            .await_ready(part_id as usize, Some(self.config.partition.ready_timeout))?;
        let owner = self.part_group.owner_of(part_id);

        let metadata = self
            .objects
            .get(obj_id)
            .ok_or(object::Error::ObjectNotFound)?;

        let local_exec = self.id.eq(&owner)
            || (metadata.repl_read()
                && self
                    .part_group
                    .is_replica(self.id, part_id, metadata.repl_factor()));
        if local_exec {
            self.part_group
                .with_segment_read(part_id as usize, obj_id, |segment| {
                    op.execute(&*segment.data)
                })
                .map_err(|err| err.into())
        } else {
            let raw_op = op.serialize();
            let response = self.net.request_sync(
                owner,
                packet::Request::GetOp {
                    v: self.part_group.version(),
                    corr_id: Uuid::new_v4(),
                    part_id: part_id as u16,
                    obj_id,
                    op_id: op.id(),
                    raw_op: &raw_op,
                },
            )?;

            let raw = response.get()?;
            let reply = Reply::try_from(&raw).map_err(CorylusError::from)?;
            match reply {
                packet::Reply::GetOp { status, result, .. } => match status {
                    packet::Status::Success => Ok(result.to_vec()),
                    s => Err(CorylusError::try_from(s).unwrap_or_else(|_| {
                        CorylusError::Io(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid GetOp status",
                        ))
                    })),
                },
                _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid data").into()),
            }
        }
    }
}
