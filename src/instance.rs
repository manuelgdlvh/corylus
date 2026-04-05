use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    io,
    marker::PhantomData,
    sync::{self, Arc, Mutex},
    thread::JoinHandle,
    time::Duration,
};

use uuid::Uuid;

use crate::{
    Instance,
    instance::operation::{Deserializer, Serializer},
    network::{self, packet::Packet},
    object::map::{Get, Put},
    partition::{self, Segment},
};

pub mod operation;
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
    segments: Vec<Box<dyn Fn() -> Segment>>,
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
            segments: Vec::new(),
            _state: PhantomData,
        }
    }
    pub fn with_id(self, id: Uuid) -> Builder<NeedsConfig> {
        Builder {
            id: Some(id),
            c: self.c,
            d: self.d,
            segments: self.segments,
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
            segments: self.segments,
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
            segments: self.segments,
            _state: PhantomData,
        }
    }
}

impl Builder<Ready> {
    pub fn with_map<K, V>(mut self, id: &str) -> Self
    where
        K: Serializer + Deserializer + Send + Sync + Hash + Eq + Clone + 'static,
        V: Serializer + Deserializer + Send + Sync + Clone + 'static,
    {
        let id = format!("map:{}", id);

        let op_reg = operation::Registry::new()
            .with_read_op::<Get<K, V>>()
            .with_write_op::<Put<K, V>>();

        self.segments.push(Box::new(move || {
            let data: HashMap<K, V> = HashMap::new();
            Segment::new(id.to_string(), data, op_reg.clone())
        }));

        self
    }

    pub fn build(self) -> io::Result<Instance> {
        let id = self.id.expect("Forced to always be filled");
        let d = self.d.expect("Forced to always be filled");
        let c = self.c.expect("Forced to always be filled");

        let shutdown = Shutdown::new();

        let (net_sender, net_receiver) = network::handle(id, d, shutdown.clone(), c.network)?;
        let partition = partition::Group::new(id, self.segments.as_slice());
        let instance = Instance::new(id, net_sender, partition, shutdown.clone());

        shutdown.register(net_receiver.start(instance.downgrade())?);

        Ok(instance)
    }
}

// --- Shutdown ---
mod shutdown {
    use std::{
        sync::{Condvar, Mutex},
        thread::JoinHandle,
        time::Duration,
    };

    pub struct Inner {
        flag: Mutex<bool>,
        notify: Condvar,
        handles: Mutex<Vec<JoinHandle<()>>>,
    }

    impl Inner {
        pub fn new() -> Self {
            Self {
                flag: Mutex::new(false),
                notify: Condvar::new(),
                handles: Mutex::new(Vec::new()),
            }
        }

        pub fn set_flag(&self, val: bool) {
            let mut flag = self.flag.lock().expect("Cannot be poisoned");
            *flag = val;
        }

        pub fn flag(&self) -> bool {
            *self.flag.lock().expect("Cannot be poisoned")
        }

        pub fn with_handles_write<F, O>(&self, f: F) -> O
        where
            F: FnOnce(&mut Vec<JoinHandle<()>>) -> O,
        {
            let mut handles = self.handles.lock().expect("Cannot be poisoned");
            f(&mut *handles)
        }

        pub fn checkpoint(&self, timeout: Option<Duration>) -> bool {
            let flag = self.flag.lock().expect("Cannot be poisoned");
            if *flag {
                return false;
            }

            match timeout {
                Some(val) => match self.notify.wait_timeout(flag, val) {
                    Ok((_, res)) => res.timed_out(),
                    Err(_) => false,
                },
                None => true,
            }
        }

        pub fn notify_all(&self) {
            self.notify.notify_all();
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
        self.as_ref().set_flag(true);
        self.as_ref().notify_all();

        let handles = self
            .as_ref()
            .with_handles_write(|handles| handles.drain(..).collect::<Vec<_>>());
        for h in handles {
            let _ = h.join();
        }
    }
}

pub struct Config {
    pub network: network::Config,
}

#[derive(Clone)]
pub struct Weak {
    inner: sync::Weak<Inner>,
}

impl Weak {
    pub fn new(inner: sync::Weak<Inner>) -> Self {
        Self { inner }
    }
}

impl AsRef<sync::Weak<Inner>> for Weak {
    fn as_ref(&self) -> &sync::Weak<Inner> {
        &self.inner
    }
}

pub struct Inner {
    pub(crate) id: Uuid,
    pub(crate) net: network::Sender,
    pub(crate) part_group: partition::Group,
    pub(crate) members: Mutex<HashSet<Uuid>>,
    pub(crate) shutdown: Shutdown,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.shutdown.destroy();
    }
}

impl Inner {
    pub(crate) fn remove_member(&self, id: Uuid) {
        let mut members = self.members.lock().expect("Cannot be poisoned");
        members.remove(&id);
    }

    pub(crate) fn add_member(&self, id: Uuid) {
        let mut members = self.members.lock().expect("Cannot be poisoned");
        members.insert(id);
    }

    pub(crate) fn members(&self) -> Vec<Uuid> {
        self.members
            .lock()
            .expect("Cannot be poisoned")
            .iter()
            .copied()
            .collect()
    }

    pub(crate) fn write<O: operation::Write>(&self, s_id: &str, mut op: O) -> io::Result<()> {
        let key = op.partition_key();
        let p_id = self.part_group.partition_of(&key);
        let owner = self.part_group.owner_of(p_id);

        if self.id.eq(&owner) {
            self.part_group
                .with_segment_write(p_id as usize, s_id, |segment| {
                    op.execute(&mut *segment.data);
                })
                .expect("As internal operation, partition and segment must always exist");
            Ok(())
        } else {
            let raw_op = op.serialize();
            let response = self.net.sync_send(
                owner,
                network::packet::Packet::WriteOp {
                    corr_id: Uuid::new_v4(),
                    partition_id: p_id as u16,
                    segment_id: s_id.to_string(),
                    op_id: op.id().to_string(),
                    raw_op,
                },
                None,
            )?;

            if let Ok(packet) = response.get(Duration::from_secs(1)) {
                match packet {
                    Packet::WriteOpReply { ok, .. } => {
                        if ok {
                            Ok(())
                        } else {
                            Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid input"))
                        }
                    }
                    _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid data")),
                }
            } else {
                Err(io::Error::new(io::ErrorKind::TimedOut, "Timeout"))
            }
        }
    }

    pub(crate) fn read<O: operation::Read>(&self, s_id: &str, op: O) -> io::Result<Vec<u8>> {
        let key = op.partition_key();
        let p_id = self.part_group.partition_of(&key);
        let owner = self.part_group.owner_of(p_id);
        if self.id.eq(&owner) {
            let result = self
                .part_group
                .with_segment_read(p_id as usize, s_id, |segment| op.execute(&*segment.data))
                .expect("As internal operation, partition and segment must always exist");
            Ok(result)
        } else {
            let raw_op = op.serialize();
            let response = self.net.sync_send(
                owner,
                network::packet::Packet::GetOp {
                    corr_id: Uuid::new_v4(),
                    partition_id: p_id as u16,
                    segment_id: s_id.to_string(),
                    op_id: op.id().to_string(),
                    raw_op,
                },
                None,
            )?;

            if let Ok(packet) = response.get(Duration::from_secs(1)) {
                match packet {
                    Packet::GetOpReply { ok, result, .. } => {
                        if ok {
                            Ok(result)
                        } else {
                            Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid input"))
                        }
                    }
                    _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid data")),
                }
            } else {
                Err(io::Error::new(io::ErrorKind::TimedOut, "Timeout"))
            }
        }
    }
}
