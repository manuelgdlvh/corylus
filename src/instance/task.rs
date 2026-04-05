use rayon::ThreadPoolBuilder;
use uuid::Uuid;

use crate::{
    instance,
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
                    let members = ref_.members();
                    let _ = ref_.part_group.update(members.as_slice());
                }
            }
            Self::Read { from, packet } => match packet {
                packet::Read::WhoIs { .. } => {}
                packet::Read::GetOp {
                    corr_id,
                    partition_id,
                    segment_id,
                    op_id,
                    raw_op,
                } => {
                    if let Some(ref_) = instance.as_ref().upgrade() {
                        let ok;
                        let result;
                        if let Ok(Some(f)) = ref_.part_group.with_segment_read(
                            partition_id as usize,
                            &segment_id,
                            |segment| segment.op_reg.read_fn(&op_id),
                        ) {
                            match f(raw_op.as_slice()) {
                                Ok(read_op) => match ref_.read(&segment_id, read_op) {
                                    Ok(val) => {
                                        result = val;
                                        ok = true;
                                    }
                                    Err(_) => {
                                        result = vec![];
                                        ok = false;
                                    }
                                },
                                Err(_) => {
                                    result = vec![];
                                    ok = false;
                                }
                            }
                        } else {
                            ok = false;
                            result = vec![];
                        }

                        let _ = ref_.net.send(
                            from,
                            Packet::Reply(packet::Reply::GetOp {
                                corr_id,
                                ok,
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
                    corr_id,
                    partition_id,
                    segment_id,
                    op_id,
                    raw_op,
                } => {
                    if let Some(ref_) = instance.as_ref().upgrade() {
                        let ok;
                        if let Ok(Some(f)) = ref_.part_group.with_segment_read(
                            partition_id as usize,
                            &segment_id,
                            |segment| segment.op_reg.write_fn(&op_id),
                        ) {
                            match f(raw_op.as_slice()) {
                                Ok(write_op) => match ref_.write(&segment_id, write_op) {
                                    Ok(_) => {
                                        ok = true;
                                    }
                                    Err(_) => {
                                        ok = false;
                                    }
                                },
                                Err(_) => {
                                    ok = false;
                                }
                            }
                        } else {
                            ok = false;
                        }

                        let _ = ref_.net.send(
                            from,
                            Packet::Reply(packet::Reply::WriteOp { corr_id, ok }),
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
