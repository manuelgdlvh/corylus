use rayon::ThreadPoolBuilder;
use uuid::Uuid;

use crate::{
    CorylusError, instance,
    network::packet::{self, Packet},
};

pub enum Task {
    PartitionRebalance,
    Read { from: Uuid, packet: packet::Read },
    Write { from: Uuid, packet: packet::Write },
}

impl Task {
    pub fn execute(self, instance: instance::Weak) {
        match self {
            Self::PartitionRebalance => {
                if let Some(ref_) = instance.as_ref().upgrade() {
                    let members = ref_.membership.all();
                    let _ = ref_.part_group.update(members.as_slice());
                }
            }
            Self::Read { from, packet } => match packet {
                packet::Read::WhoIs { .. } => {}
                packet::Read::GetOp {
                    v,
                    corr_id,
                    partition_id,
                    segment_id,
                    op_id,
                    raw_op,
                } => {
                    if let Some(ref_) = instance.as_ref().upgrade() {
                        let (status, result) = ref_
                            .part_group
                            .metadata(&segment_id)
                            .map_err(CorylusError::from)
                            .and_then(|m| m.read_fn(&op_id).map_err(CorylusError::from))
                            .and_then(|f| f(raw_op.as_slice()).map_err(CorylusError::from))
                            .and_then(|op| ref_.remote_read(&segment_id, partition_id, v, op))
                            .map(|val| (packet::Status::Success, val))
                            .unwrap_or_else(|err| (packet::Status::from(err), vec![]));

                        let _ = ref_.net.send(
                            from,
                            Packet::Reply(packet::Reply::GetOp {
                                corr_id,
                                status,
                                result,
                            }),
                            None,
                        );
                    }
                }
            },
            Self::Write { from, packet } => match packet {
                packet::Write::HeartBeat => {}
                packet::Write::WriteOp {
                    v,
                    corr_id,
                    partition_id,
                    segment_id,
                    op_id,
                    raw_op,
                } => {
                    if let Some(ref_) = instance.as_ref().upgrade() {
                        let status = ref_
                            .part_group
                            .metadata(&segment_id)
                            .map_err(CorylusError::from)
                            .and_then(|m| m.write_fn(&op_id).map_err(CorylusError::from))
                            .and_then(|f| f(raw_op.as_slice()).map_err(CorylusError::from))
                            .and_then(|op| ref_.remote_write(&segment_id, partition_id, v, op))
                            .map(|_| packet::Status::Success)
                            .unwrap_or_else(packet::Status::from);

                        let _ = ref_.net.send(
                            from,
                            Packet::Reply(packet::Reply::WriteOp { corr_id, status }),
                            None,
                        );
                    }
                }
            },
        }
    }
}

pub struct Executor {
    vacuum: rayon::ThreadPool,
    read: rayon::ThreadPool,
    write: rayon::ThreadPool,
}

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}

impl Executor {
    pub fn new() -> Self {
        let vacuum = ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .expect("Failed to build thread pool");
        let read = ThreadPoolBuilder::new()
            .num_threads(16)
            .build()
            .expect("Failed to build thread pool");

        let write = ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .expect("Failed to build thread pool");

        Self {
            vacuum,
            read,
            write,
        }
    }
    pub fn spawn(&self, instance: instance::Weak, task: Task) {
        let pool = match task {
            Task::PartitionRebalance => &self.vacuum,
            Task::Read { .. } => &self.read,
            Task::Write { .. } => &self.write,
        };

        pool.spawn(move || {
            task.execute(instance);
        });
    }
}
