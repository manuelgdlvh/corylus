use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    io,
    marker::PhantomData,
    sync::{Arc, Mutex},
    time::Duration,
};

use uuid::Uuid;

use crate::{
    instance::operation::{Deserializer, Serializer},
    network::{self, packet::Packet},
    object::{
        DistributedMap,
        map::{Get, Put},
    },
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

        let (net_sender, net_receiver) = network::handle(id, d, c.network)?;
        let partition = partition::Group::new(id, self.segments.as_slice());
        let instance = Instance::new(id, net_sender, partition);

        net_receiver.start(instance.clone())?;

        Ok(instance)
    }
}
pub struct Config {
    network: network::Config,
}

#[derive(Clone)]
pub struct Instance {
    inner: Arc<Inner>,
}

impl Instance {
    fn new(id: Uuid, net: network::Sender, partition: partition::Group) -> Self {
        let mut members = HashSet::new();
        members.insert(id);
        let inner = Arc::new(Inner {
            id,
            net,
            partition,
            members: Mutex::new(members),
        });

        Self { inner }
    }
    pub(crate) fn remove_member(&self, id: Uuid) {
        let mut members = self.as_ref().members.lock().expect("Cannot be poisoned");
        members.remove(&id);
    }

    pub(crate) fn add_member(&self, id: Uuid) {
        let mut members = self.as_ref().members.lock().expect("Cannot be poisoned");
        members.insert(id);
    }

    pub(crate) fn members(&self) -> Vec<Uuid> {
        self.inner
            .members
            .lock()
            .expect("Cannot be poisoned")
            .iter()
            .copied()
            .collect()
    }

    pub(crate) fn write<O: operation::Write>(&self, s_id: &str, mut op: O) -> io::Result<()> {
        let key = op.partition_key();
        let p_id = self.as_ref().partition.partition_of(&key);
        let owner = self.as_ref().partition.owner_of(p_id);

        if self.as_ref().id.eq(&owner) {
            let _ = self
                .as_ref()
                .partition
                .with_segment_write(p_id as usize, s_id, |segment| {
                    op.execute(&mut *segment.data);
                })
                .expect("As internal operation, partition and segment must always exist");
            Ok(())
        } else {
            let raw_op = op.serialize();
            let response = self.as_ref().net.sync_send(
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
        let p_id = self.as_ref().partition.partition_of(&key);
        let owner = self.as_ref().partition.owner_of(p_id);
        if self.as_ref().id.eq(&owner) {
            let result = self
                .as_ref()
                .partition
                .with_segment_read(p_id as usize, s_id, |segment| op.execute(&*segment.data))
                .expect("As internal operation, partition and segment must always exist");
            Ok(result)
        } else {
            let raw_op = op.serialize();
            let response = self.as_ref().net.sync_send(
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

    pub fn get_map<K, V>(&self, id: &str) -> Option<DistributedMap<K, V>>
    where
        K: Serializer + Deserializer + Hash + Eq + Clone + 'static,
        V: Serializer + Deserializer + Clone + 'static,
    {
        let id = format!("map:{}", id);
        let result = self
            .as_ref()
            .partition
            .with_segment_read(0, &id, |segment| {
                segment
                    .data
                    .as_any()
                    .downcast_ref::<HashMap<K, V>>()
                    .is_some()
            });

        if let Ok(val) = result
            && val
        {
            let instance = self.clone();
            Some(DistributedMap::new(id, instance))
        } else {
            None
        }
    }
}

pub(crate) struct Inner {
    pub(crate) id: Uuid,
    pub(crate) net: network::Sender,
    partition: partition::Group,
    members: Mutex<HashSet<Uuid>>,
}

impl AsRef<Inner> for Instance {
    fn as_ref(&self) -> &Inner {
        &self.inner
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

    // #[test]
    // pub fn should_register_map_successfully() -> io::Result<()> {
    //     let (instance_1, instance_2) = setup()?;

    //     assert!(instance_1.get_map::<String, String>("str-str").is_some());
    //     assert!(instance_2.get_map::<String, String>("str-str").is_some());

    //     assert!(instance_1.get_map::<String, String>("wrong").is_none());
    //     assert!(instance_2.get_map::<String, String>("wrong").is_none());

    //     assert!(instance_1.get_map::<u64, String>("str-str").is_none());
    //     assert!(instance_2.get_map::<String, u64>("str-str").is_none());
    //     Ok(())
    // }

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
