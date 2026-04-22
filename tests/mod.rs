use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    sync::Mutex,
    thread::sleep,
    time::{Duration, Instant},
};

use corylus::{
    CorylusResult, instance,
    network::{self, Discovery},
    object,
    partition::{self},
    runtime,
};
use tracing_subscriber::FmtSubscriber;
use uuid::Uuid;

#[path = "cases/map.rs"]
mod map;
#[path = "cases/rebalance.rs"]
mod rebalance;
#[path = "cases/repl.rs"]
mod repl;

#[derive(Copy, Clone, Default)]
pub struct TracingLogger {}

impl runtime::Logger for TracingLogger {
    fn info(&self, message: std::fmt::Arguments<'_>) {
        tracing::event!(tracing::Level::INFO, "{}", message);
    }

    fn debug(&self, message: std::fmt::Arguments<'_>) {
        tracing::event!(tracing::Level::DEBUG, "{}", message);
    }

    fn trace(&self, message: std::fmt::Arguments<'_>) {
        tracing::event!(tracing::Level::TRACE, "{}", message);
    }

    fn warn(&self, message: std::fmt::Arguments<'_>) {
        tracing::event!(tracing::Level::WARN, "{}", message);
    }

    fn error(&self, message: std::fmt::Arguments<'_>) {
        tracing::event!(tracing::Level::ERROR, "{}", message);
    }
}

pub type Instance = corylus::Instance<TracingLogger>;

pub static WITH_INSTANCES_LOCK: Mutex<()> = Mutex::new(());

pub(crate) fn new_instance(
    id: Uuid,
    port: u16,
    repl_config: object::ReplicationConfig,
) -> io::Result<Instance> {
    instance::Builder::new()
        .with_id(id)
        .with_config(instance::Config {
            network: network::Config {
                addr: SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
                hb: network::HeartbeatConfig {
                    poll_interval: Duration::from_secs(1),
                    tolerance: Duration::from_secs(3),
                },
                ..Default::default()
            },
            cluster: Default::default(),
            partition: Default::default(),
            task: Default::default(),
        })
        .with_discovery(Discovery::List {
            addresses: vec![
                SocketAddr::from((Ipv4Addr::LOCALHOST, 8090)),
                SocketAddr::from((Ipv4Addr::LOCALHOST, 8091)),
                SocketAddr::from((Ipv4Addr::LOCALHOST, 8092)),
                SocketAddr::from((Ipv4Addr::LOCALHOST, 8093)),
                SocketAddr::from((Ipv4Addr::LOCALHOST, 8094)),
            ],
        })
        .with_map::<String, String>("str-str", repl_config)
        .build(TracingLogger::default())
}

fn with_instances<F: FnOnce(Instance, Instance) -> CorylusResult<()>>(
    f: F,
    repl_config: object::ReplicationConfig,
) -> CorylusResult<()> {
    let _guard = WITH_INSTANCES_LOCK.lock();

    let subscriber = FmtSubscriber::new();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let instance_1 = new_instance(Uuid::from_u128(1), 8090, repl_config)?;
    let instance_2 = new_instance(Uuid::from_u128(2), 8091, repl_config)?;

    wait_until_ready(&[&instance_1, &instance_2]);

    f(instance_1, instance_2)
}

pub(crate) fn wait_until_ready(instances: &[&Instance]) {
    let mut member_ids = instances.iter().map(|inst| inst.id()).collect::<Vec<_>>();
    member_ids.sort_unstable();

    let version = partition::version(member_ids.as_slice());
    for inst in instances {
        wait_until(
            || inst.part_group_version() == version,
            Duration::from_millis(100),
            Duration::from_secs(10),
        );
    }
}

pub(crate) fn wait_until<F>(f: F, poll_interval: Duration, timeout: Duration)
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

        use crate::{map, with_instances};
        #[test]
        pub fn should_register_map_successfully() -> CorylusResult<()> {
            with_instances(map::should_register_map_successfully, Default::default())
        }

        #[test]
        pub fn should_put_and_get_map_successfully() -> CorylusResult<()> {
            with_instances(map::should_put_and_get_map_successfully, Default::default())
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

    mod rebalance {
        use corylus::CorylusResult;

        use crate::{rebalance, with_instances};

        #[test]
        pub fn should_transfer_partition_ownership_after_rebalance() -> CorylusResult<()> {
            with_instances(
                rebalance::should_transfer_partition_ownership_after_rebalance,
                Default::default(),
            )
        }
    }
}
