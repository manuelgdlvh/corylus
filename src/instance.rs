use std::{
    collections::HashSet,
    io,
    sync::{Arc, Mutex},
};

use uuid::Uuid;

use crate::{network, partition};

pub mod task;

pub struct Config {
    network: network::Config,
}

pub fn new_instance(id: Uuid, d: network::Discovery, c: Config) -> io::Result<Instance> {
    let (net_sender, net_receiver) = network::handle(id, d, c.network)?;
    let partition = partition::Group::new(id);
    let instance = Instance::new(id, net_sender, partition);

    net_receiver.start(instance.clone())?;

    Ok(instance)
}

#[derive(Clone)]
pub struct Instance {
    inner: Arc<Inner>,
}

impl Instance {
    pub fn new(id: Uuid, net: network::Sender, partition: partition::Group) -> Self {
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
    pub fn remove_member(&self, id: Uuid) {
        let mut members = self.as_ref().members.lock().expect("Cannot be poisoned");
        members.remove(&id);
    }

    pub fn add_member(&self, id: Uuid) {
        let mut members = self.as_ref().members.lock().expect("Cannot be poisoned");
        members.insert(id);
    }

    pub fn members(&self) -> Vec<Uuid> {
        self.inner
            .members
            .lock()
            .expect("Cannot be poisoned")
            .iter()
            .copied()
            .collect()
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
        instance::{Config, Instance, new_instance},
        network::{self, Discovery, packet::Packet},
    };

    #[test]
    pub fn should_recv_response_when_sync_send() -> io::Result<()> {
        let (instance_1, instance_2) = setup()?;

        let response = instance_1.as_ref().net.sync_send(
            Uuid::from_u128(2),
            Packet::NoOp { corr_id: 1 },
            None,
        )?;

        assert!(matches!(
            response.get(Duration::from_secs(1)),
            Ok(Packet::NoOpReply { corr_id: 1 })
        ));
        Ok(())
    }

    fn setup() -> io::Result<(Instance, Instance)> {
        let subscriber = FmtSubscriber::new();
        let _ = tracing::subscriber::set_global_default(subscriber);

        let id_1 = Uuid::from_u128(1);
        let id_2 = Uuid::from_u128(2);

        let config_1 = Config {
            network: network::Config {
                addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 8090)),
                hb: network::HeartbeatConfig {
                    poll_interval: Duration::from_secs(1),
                    tolerance: Duration::from_secs(3),
                },
                ..Default::default()
            },
        };

        let config_2 = Config {
            network: network::Config {
                addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 8091)),
                hb: network::HeartbeatConfig {
                    poll_interval: Duration::from_secs(1),
                    tolerance: Duration::from_secs(3),
                },
                ..Default::default()
            },
        };

        let instance_1 = new_instance(
            id_1,
            Discovery::List {
                addresses: vec![
                    SocketAddr::from((Ipv4Addr::LOCALHOST, 8090)),
                    SocketAddr::from((Ipv4Addr::LOCALHOST, 8091)),
                ],
            },
            config_1,
        )?;

        let instance_2 = new_instance(
            id_2,
            Discovery::List {
                addresses: vec![
                    SocketAddr::from((Ipv4Addr::LOCALHOST, 8090)),
                    SocketAddr::from((Ipv4Addr::LOCALHOST, 8091)),
                ],
            },
            config_2,
        )?;

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
