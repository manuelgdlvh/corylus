use rayon::ThreadPoolBuilder;

use crate::{
    instance::Instance,
    network::packet::{InboundPacket, Packet},
};

pub enum Task {
    PartitionRebalance,
    Request { packet: InboundPacket },
}

impl Task {
    pub fn execute(self, instance: Instance) {
        match self {
            Self::PartitionRebalance => {
                let members = instance.members();
                let changes = instance.as_ref().partition.update(members.as_slice());
            }
            Self::Request { packet } => {
                let from = packet.from;

                match packet.p {
                    Packet::WhoIs { .. }
                    | Packet::WhoIsReply { .. }
                    | Packet::HeartBeat
                    | Packet::NoOpReply { .. } => {}
                    Packet::NoOp { corr_id } => {
                        let _ =
                            instance
                                .as_ref()
                                .net
                                .send(from, Packet::NoOpReply { corr_id }, None);
                    }
                }
            }
        }
    }
}

pub struct Executor {
    t_01: rayon::ThreadPool,
    t_02: rayon::ThreadPool,
}

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}

impl Executor {
    pub fn new() -> Self {
        let t_01 = ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .expect("Failed to build thread pool");
        let t_02 = ThreadPoolBuilder::new()
            .num_threads(16)
            .build()
            .expect("Failed to build thread pool");

        Self { t_01, t_02 }
    }
    pub fn spawn(&self, instance: Instance, task: Task) {
        let pool = match task {
            Task::PartitionRebalance => &self.t_01,
            Task::Request { .. } => &self.t_02,
        };

        pool.spawn(move || {
            task.execute(instance);
        });
    }
}
