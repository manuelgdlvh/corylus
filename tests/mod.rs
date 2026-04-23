use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    sync::Mutex,
    thread::sleep,
    time::{Duration, Instant},
};

use corylus::runtime::{Options, Runtime};
use corylus::{
    CorylusResult, instance,
    network::{self, Discovery},
    object,
    partition::{self},
    runtime,
};
use log::LevelFilter;
use uuid::Uuid;

#[path = "cases/map.rs"]
mod map;
#[path = "cases/rebalance.rs"]
mod rebalance;
#[path = "cases/repl.rs"]
mod repl;

pub type Instance = corylus::Instance;

pub struct TokioRuntime {
    runtime: tokio::runtime::Runtime,
}

impl Runtime for TokioRuntime {
    fn spawn<F>(&self, f: F)
    where
        F: Future<Output = ()> + 'static + Send,
    {
        self.runtime.spawn(f);
    }
}

#[derive(Clone, Default)]
pub struct TokioBuilder {}

impl runtime::Builder for TokioBuilder {
    type Runtime = TokioRuntime;

    fn build(&self, opts: Options) -> io::Result<Self::Runtime> {
        let mut builder;
        if opts.threads > 1 {
            builder = tokio::runtime::Builder::new_multi_thread();
        } else {
            builder = tokio::runtime::Builder::new_current_thread();
        }
        let runtime = builder
            .thread_name(opts.name)
            .enable_all()
            .worker_threads(opts.threads)
            .build()?;

        Ok(TokioRuntime { runtime })
    }
}

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
        .build(TokioBuilder::default())
}

async fn with_instances<F, Fut>(f: F, repl_config: object::ReplicationConfig) -> CorylusResult<()>
where
    F: FnOnce(Instance, Instance) -> Fut,
    Fut: Future<Output = CorylusResult<()>>,
{
    let _guard = WITH_INSTANCES_LOCK.lock();

    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .is_test(true)
        .filter_level(LevelFilter::Info)
        .try_init();

    let instance_1 = new_instance(Uuid::from_u128(1), 8090, repl_config)?;
    let instance_2 = new_instance(Uuid::from_u128(2), 8091, repl_config)?;

    wait_until_ready(&[&instance_1, &instance_2]);

    f(instance_1, instance_2).await
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
        #[tokio::test(flavor = "multi_thread")]
        pub async fn should_register_map_successfully() -> CorylusResult<()> {
            with_instances(
                |a, b| async move { map::should_register_map_successfully(a, b) },
                Default::default(),
            )
            .await
        }

        #[tokio::test(flavor = "multi_thread")]
        pub async fn should_put_and_get_map_successfully() -> CorylusResult<()> {
            with_instances(map::should_put_and_get_map_successfully, Default::default()).await
        }
    }

    mod repl {
        use corylus::{CorylusResult, object::ReplicationConfig};

        use crate::{repl, with_instances};

        #[tokio::test(flavor = "multi_thread")]
        pub async fn should_read_success_when_sync_repl_and_allow_replica_read() -> CorylusResult<()>
        {
            with_instances(
                repl::should_read_success_when_sync_repl_and_allow_replica_read,
                ReplicationConfig::synchronous(1, true),
            )
            .await
        }

        #[tokio::test(flavor = "multi_thread")]
        pub async fn should_read_fail_when_sync_repl_and_no_allow_replica_read() -> CorylusResult<()>
        {
            with_instances(
                repl::should_read_fail_when_sync_repl_and_no_allow_replica_read,
                ReplicationConfig::synchronous(1, false),
            )
            .await
        }
    }

    mod rebalance {
        use corylus::CorylusResult;

        use crate::{rebalance, with_instances};

        #[tokio::test(flavor = "multi_thread")]
        pub async fn should_transfer_partition_ownership_after_rebalance() -> CorylusResult<()> {
            with_instances(
                rebalance::should_transfer_partition_ownership_after_rebalance,
                Default::default(),
            )
            .await
        }
    }
}
