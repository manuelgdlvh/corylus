use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    io,
    marker::PhantomData,
    sync::{self, Arc, Condvar, Mutex},
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

        let (net_sender, net_receiver) = network::handle(id, d, Arc::clone(&shutdown), c.network)?;
        let partition = partition::Group::new(id, self.segments.as_slice());
        let instance = Instance::new(id, net_sender, partition, Arc::clone(&shutdown));

        shutdown.register(net_receiver.start(instance.downgrade())?);

        Ok(instance)
    }
}

// --- Shutdown ---

pub struct Shutdown {
    flag: Mutex<bool>,
    notify: Condvar,
    handles: Mutex<Vec<JoinHandle<()>>>,
}

impl Shutdown {
    pub fn new() -> Arc<Self> {
        let self_ = Self {
            flag: Mutex::new(false),
            notify: Condvar::new(),
            handles: Mutex::new(Vec::new()),
        };

        Arc::new(self_)
    }

    pub fn register(&self, h: JoinHandle<()>) {
        self.handles.lock().expect("Cannot be poisoned").push(h);
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

    pub fn destroy(&self) {
        {
            let mut flag = self.flag.lock().expect("Cannot be poisoned");
            *flag = true;
        }

        self.notify.notify_all();

        let handles = self
            .handles
            .lock()
            .expect("Cannot be poisoned")
            .drain(..)
            .collect::<Vec<_>>();
        for h in handles {
            let _ = h.join();
        }
    }
}

pub struct Config {
    network: network::Config,
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
    pub(crate) partition: partition::Group,
    pub(crate) members: Mutex<HashSet<Uuid>>,
    pub(crate) shutdown: Arc<Shutdown>,
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
        let p_id = self.partition.partition_of(&key);
        let owner = self.partition.owner_of(p_id);

        if self.id.eq(&owner) {
            self.partition
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
        let p_id = self.partition.partition_of(&key);
        let owner = self.partition.owner_of(p_id);
        if self.id.eq(&owner) {
            let result = self
                .partition
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

#[cfg(test)]
mod tests {
    use std::{
        io,
        net::{Ipv4Addr, SocketAddr},
        thread::sleep,
        time::{Duration, Instant},
    };

    use tracing_subscriber::FmtSubscriber;
    use uuid::Uuid;

    use crate::{
        instance::{self, Config, Instance},
        network::{self, Discovery},
    };

    #[test]
    pub fn should_register_map_successfully() -> io::Result<()> {
        let (instance_1, instance_2) = setup()?;

        assert!(instance_1.get_map::<String, String>("str-str").is_some());
        assert!(instance_2.get_map::<String, String>("str-str").is_some());

        assert!(instance_1.get_map::<String, String>("wrong").is_none());
        assert!(instance_2.get_map::<String, String>("wrong").is_none());

        assert!(instance_1.get_map::<u64, String>("str-str").is_none());
        assert!(instance_2.get_map::<String, u64>("str-str").is_none());
        Ok(())
    }

    #[test]
    pub fn should_put_and_get_map_successfully() -> io::Result<()> {
        let (instance_1, instance_2) = setup()?;

        let map_1 = instance_1.get_map::<String, String>("str-str").unwrap();
        let map_2 = instance_2.get_map::<String, String>("str-str").unwrap();

        // Local write
        map_1.put("key-1".to_string(), "value-1".to_string())?;
        // Local read
        assert_eq!(Some("value-1".to_string()), map_1.get("key-1".to_string())?);
        // Remote read
        assert_eq!(Some("value-1".to_string()), map_2.get("key-1".to_string())?);

        // Remote Write
        map_1.put("key-2".to_string(), "value-2".to_string())?;
        // Remote read
        assert_eq!(Some("value-2".to_string()), map_1.get("key-2".to_string())?);
        // Local read
        assert_eq!(Some("value-2".to_string()), map_2.get("key-2".to_string())?);

        Ok(())
    }

    fn setup() -> io::Result<(Instance, Instance)> {
        let subscriber = FmtSubscriber::new();
        let _ = tracing::subscriber::set_global_default(subscriber);

        let id_1 = Uuid::from_u128(1);
        let id_2 = Uuid::from_u128(2);

        let instance_1 = instance::Builder::new()
            .with_id(Uuid::from_u128(1))
            .with_config(Config {
                network: network::Config {
                    addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 8090)),
                    hb: network::HeartbeatConfig {
                        poll_interval: Duration::from_secs(1),
                        tolerance: Duration::from_secs(3),
                    },
                    ..Default::default()
                },
            })
            .with_discovery(Discovery::List {
                addresses: vec![
                    SocketAddr::from((Ipv4Addr::LOCALHOST, 8090)),
                    SocketAddr::from((Ipv4Addr::LOCALHOST, 8091)),
                ],
            })
            .with_map::<String, String>("str-str")
            .build()?;

        let instance_2 = instance::Builder::new()
            .with_id(Uuid::from_u128(2))
            .with_config(Config {
                network: network::Config {
                    addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 8091)),
                    hb: network::HeartbeatConfig {
                        poll_interval: Duration::from_secs(1),
                        tolerance: Duration::from_secs(3),
                    },
                    ..Default::default()
                },
            })
            .with_discovery(Discovery::List {
                addresses: vec![
                    SocketAddr::from((Ipv4Addr::LOCALHOST, 8090)),
                    SocketAddr::from((Ipv4Addr::LOCALHOST, 8091)),
                ],
            })
            .with_map::<String, String>("str-str")
            .build()?;

        wait_until(
            || instance_1.members().iter().any(|id| id.eq(&id_2)),
            Duration::from_millis(100),
            Duration::from_secs(5),
        );

        wait_until(
            || instance_2.members().iter().any(|id| id.eq(&id_1)),
            Duration::from_millis(100),
            Duration::from_secs(5),
        );

        Ok((instance_1, instance_2))
    }

    pub fn wait_until<F>(f: F, poll_interval: Duration, timeout: Duration)
    where
        F: Fn() -> bool,
    {
        let start = Instant::now();

        loop {
            if f() {
                return;
            }

            if start.elapsed() >= timeout {
                assert!(false, "Max wait time to be ready elapsed")
            }

            sleep(poll_interval);
        }
    }
}
