use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    sync::Mutex,
    thread::sleep,
    time::{Duration, Instant},
};

use corylus::{
    Instance, instance,
    network::{self, Discovery},
};
use tracing_subscriber::FmtSubscriber;
use uuid::Uuid;

mod map;

pub static WITH_INSTANCES_LOCK: Mutex<()> = Mutex::new(());

fn with_instances<F: FnOnce(Instance, Instance) -> io::Result<()>>(f: F) -> io::Result<()> {
    let _guard = WITH_INSTANCES_LOCK.lock();

    let subscriber = FmtSubscriber::new();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let id_1 = Uuid::from_u128(1);
    let id_2 = Uuid::from_u128(2);

    let instance_1 = instance::Builder::new()
        .with_id(Uuid::from_u128(1))
        .with_config(instance::Config {
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
        .with_config(instance::Config {
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
        || {
            instance_1.members().iter().any(|id| id.eq(&id_2))
                && instance_1.part_group_version() == 1
        },
        Duration::from_millis(100),
        Duration::from_secs(5),
    );

    wait_until(
        || {
            instance_2.members().iter().any(|id| id.eq(&id_1))
                && instance_2.part_group_version() == 1
        },
        Duration::from_millis(100),
        Duration::from_secs(5),
    );

    f(instance_1, instance_2)
}

fn wait_until<F>(f: F, poll_interval: Duration, timeout: Duration)
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

mod tests {
    mod map {
        use std::io;

        use crate::{map, with_instances};
        #[test]
        pub fn should_register_map_successfully() -> io::Result<()> {
            with_instances(map::should_register_map_successfully)
        }

        #[test]
        pub fn should_put_and_get_map_successfully() -> io::Result<()> {
            with_instances(map::should_put_and_get_map_successfully)
        }
    }
}
