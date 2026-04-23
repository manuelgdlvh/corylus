use crate::network::packet::{Inbound, Reply, Request, Status};
use crate::runtime::{Options, Runtime};
use crate::{
    CorylusError,
    instance::{self},
    object, runtime,
};

pub enum Task {
    Read(Inbound),
    Write(Inbound),
}

impl Task {
    pub async fn execute(self, instance: instance::Weak) {
        match self {
            Self::Read(inbound) => {
                let from = inbound.from;
                if let Ok(packet) = Request::try_from(&inbound.p) {
                    match packet {
                        Request::GetOp {
                            v,
                            corr_id,
                            part_id,
                            obj_id,
                            op_id,
                            raw_op,
                        } => {
                            if let Some(ref_) = instance.as_ref().upgrade() {
                                let (status, result) = match ref_
                                    .objects
                                    .get(obj_id)
                                    .ok_or(object::Error::ObjectNotFound.into())
                                    .and_then(|m| m.read_fn(op_id).map_err(CorylusError::from))
                                    .and_then(|f| f(raw_op).map_err(CorylusError::from))
                                {
                                    Ok(op) => ref_
                                        .forwarded_read(obj_id, part_id, v, op)
                                        .await
                                        .map(|val| (Status::Success, val))
                                        .unwrap_or_else(|err| (Status::from(err), vec![])),
                                    Err(err) => (Status::from(err), vec![]),
                                };

                                let _ = ref_.net.reply(
                                    from,
                                    Reply::GetOp {
                                        corr_id,
                                        status,
                                        result: &result,
                                    },
                                );
                            }
                        }

                        Request::FetchObject {
                            v: _,
                            corr_id,
                            part_id,
                            obj_id,
                        } => {
                            if let Some(ref_) = instance.as_ref().upgrade() {
                                let (status, result) = match ref_.fetch(obj_id, part_id) {
                                    Ok(res) => (Status::Success, res),
                                    Err(err) => (Status::from(err), vec![]),
                                };

                                let _ = ref_.net.reply(
                                    from,
                                    Reply::FetchObject {
                                        corr_id,
                                        status,
                                        result: &result,
                                    },
                                );
                            }
                        }
                        Request::HeartBeat => {}
                        Request::WriteOp { .. } => {}
                        Request::PartitionFetchCompletion { .. } => {}
                        Request::WhoIs { .. } => {}
                    }
                }
            }

            Self::Write(inbound) => {
                let from = inbound.from;
                if let Ok(packet) = Request::try_from(&inbound.p) {
                    match packet {
                        Request::WriteOp {
                            v,
                            corr_id,
                            part_id,
                            obj_id,
                            op_id,
                            raw_op,
                        } => {
                            if let Some(ref_) = instance.as_ref().upgrade() {
                                let status = match ref_
                                    .objects
                                    .get(obj_id)
                                    .ok_or(object::Error::ObjectNotFound.into())
                                    .and_then(|m| m.write_fn(op_id).map_err(CorylusError::from))
                                    .and_then(|f| f(raw_op).map_err(CorylusError::from))
                                {
                                    Ok(op) => ref_
                                        .forwarded_write(obj_id, part_id, v, op)
                                        .await
                                        .map(|_| Status::Success)
                                        .unwrap_or_else(Status::from),
                                    Err(err) => Status::from(err),
                                };

                                let _ = ref_.net.reply(from, Reply::WriteOp { corr_id, status });
                            }
                        }
                        Request::PartitionFetchCompletion {
                            v: _,
                            corr_id,
                            part_id: _,
                        } => {
                            if let Some(ref_) = instance.as_ref().upgrade() {
                                // Delete obsolete data.
                                let _ = ref_.net.reply(
                                    from,
                                    Reply::PartitionFetchCompletion {
                                        corr_id,
                                        status: Status::Success,
                                    },
                                );
                            }
                        }
                        Request::HeartBeat
                        | Request::WhoIs { .. }
                        | Request::GetOp { .. }
                        | Request::FetchObject { .. } => {}
                    }
                }
            }
        }
    }
}

pub struct Executor<T: Runtime> {
    read: T,
    write: T,
}

impl<T: Runtime> Executor<T> {
    pub fn new<B>(task: instance::TaskConfig, builder: B) -> Self
    where
        B: runtime::Builder<Runtime = T>,
    {
        let read = builder
            .build(Options {
                name: "read-task-runtime".to_string(),
                threads: task.read_threads,
            })
            .expect("read-ops thread pool: OS refused or thread count invalid");

        let write = builder
            .build(Options {
                name: "write-task-runtime".to_string(),
                threads: task.write_threads,
            })
            .expect("write-ops thread pool: OS refused or thread count invalid");

        Self { read, write }
    }

    pub fn spawn(&self, instance: instance::Weak, task: Task) {
        let runtime = match task {
            Task::Read { .. } => &self.read,
            Task::Write { .. } => &self.write,
        };

        runtime.spawn(task.execute(instance));
    }
}
