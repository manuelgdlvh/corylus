use rayon::ThreadPoolBuilder;

use crate::{
    instance::Instance,
    network::packet::{InboundPacket, Packet},
};

pub enum Task {
    PartitionRebalance,
    Read { packet: InboundPacket },
    Write { packet: InboundPacket },
}

impl Task {
    pub fn execute(self, instance: Instance) {
        match self {
            Self::PartitionRebalance => {
                let members = instance.members();
                let _ = instance.as_ref().partition.update(members.as_slice());
            }
            Self::Read { packet } => {
                let from = packet.from;

                match packet.p {
                    Packet::WhoIs { .. }
                    | Packet::WhoIsReply { .. }
                    | Packet::HeartBeat
                    | Packet::WriteOpReply { .. }
                    | Packet::GetOpReply { .. }
                    | Packet::WriteOp { .. } => {}
                    Packet::GetOp {
                        corr_id,
                        partition_id,
                        segment_id,
                        op_id,
                        raw_op,
                    } => {
                        let ok;
                        let result;
                        if let Ok(Some(f)) = instance.as_ref().partition.with_segment_read(
                            partition_id as usize,
                            &segment_id,
                            |segment| segment.op_reg.read_fn(&op_id),
                        ) {
                            let read_op = f(raw_op.as_slice());
                            match instance.read(&segment_id, read_op) {
                                Ok(val) => {
                                    result = val;
                                    ok = true;
                                }
                                Err(_) => {
                                    result = vec![];
                                    ok = false;
                                }
                            }
                        } else {
                            ok = false;
                            result = vec![];
                        }

                        let _ = instance.as_ref().net.send(
                            from,
                            Packet::GetOpReply {
                                corr_id,
                                ok,
                                result,
                            },
                            None,
                        );
                    }
                }
            }

            Self::Write { packet } => {
                let from = packet.from;

                match packet.p {
                    Packet::WhoIs { .. }
                    | Packet::WhoIsReply { .. }
                    | Packet::HeartBeat
                    | Packet::WriteOpReply { .. }
                    | Packet::GetOpReply { .. }
                    | Packet::GetOp { .. } => {}

                    Packet::WriteOp {
                        corr_id,
                        partition_id,
                        segment_id,
                        op_id,
                        raw_op,
                    } => {
                        let ok;
                        if let Ok(Some(f)) = instance.as_ref().partition.with_segment_read(
                            partition_id as usize,
                            &segment_id,
                            |segment| segment.op_reg.write_fn(&op_id),
                        ) {
                            let write_op = f(raw_op.as_slice());
                            match instance.write(&segment_id, write_op) {
                                Ok(_) => {
                                    ok = true;
                                }
                                Err(_) => {
                                    ok = false;
                                }
                            }
                        } else {
                            ok = false;
                        }

                        let _ = instance.as_ref().net.send(
                            from,
                            Packet::WriteOpReply { corr_id, ok },
                            None,
                        );
                    }
                }
            }
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
    pub fn spawn(&self, instance: Instance, task: Task) {
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
