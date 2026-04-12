use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Mutex,
    thread::sleep,
    time::{Duration, Instant},
};

use corylus::{
    CorylusResult, Instance, instance,
    network::{self, Discovery},
    object,
    partition::{self},
};
use tracing_subscriber::FmtSubscriber;
use uuid::Uuid;

mod map;
mod repl;

pub static WITH_INSTANCES_LOCK: Mutex<()> = Mutex::new(());

fn with_instances_no_repl<F: FnOnce(Instance, Instance) -> CorylusResult<()>>(
    f: F,
) -> CorylusResult<()> {
    with_instances(f, object::ReplicationConfig::default())
}

fn with_instances<F: FnOnce(Instance, Instance) -> CorylusResult<()>>(
    f: F,
    repl_config: object::ReplicationConfig,
) -> CorylusResult<()> {
    let _guard = WITH_INSTANCES_LOCK.lock();

    let subscriber = FmtSubscriber::new();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let id_1 = Uuid::from_u128(1);
    let id_2 = Uuid::from_u128(2);
    let version = partition::Group::compute_version(&[id_1, id_2]);

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
        .with_map::<String, String>("str-str", repl_config)
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
        .with_map::<String, String>("str-str", repl_config)
        .build()?;

    wait_until(
        || {
            instance_1.members().iter().any(|id| id.eq(&id_2))
                && instance_1.part_group_version() == version
        },
        Duration::from_millis(100),
        Duration::from_secs(5),
    );

    wait_until(
        || {
            instance_2.members().iter().any(|id| id.eq(&id_1))
                && instance_2.part_group_version() == version
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

// Tests under same parent module to avoid binary split
mod tests {
    mod map {

        use corylus::CorylusResult;

        use crate::{map, with_instances_no_repl};
        #[test]
        pub fn should_register_map_successfully() -> CorylusResult<()> {
            with_instances_no_repl(map::should_register_map_successfully)
        }

        #[test]
        pub fn should_put_and_get_map_successfully() -> CorylusResult<()> {
            with_instances_no_repl(map::should_put_and_get_map_successfully)
        }
    }

    mod repl {
        use corylus::{CorylusResult, object::ReplicationConfig};

        use crate::{repl, with_instances};

        #[test]
        pub fn should_read_success_when_sync_repl_and_allow_replica_read() -> CorylusResult<()> {
            with_instances(
                repl::should_read_success_when_sync_repl_and_allow_replica_read,
                ReplicationConfig::synchronous(1, true),
            )
        }

        #[test]
        pub fn should_read_fail_when_sync_repl_and_no_allow_replica_read() -> CorylusResult<()> {
            with_instances(
                repl::should_read_fail_when_sync_repl_and_no_allow_replica_read,
                ReplicationConfig::synchronous(1, false),
            )
        }
    }
}
