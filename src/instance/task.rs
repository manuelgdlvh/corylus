use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use rayon::ThreadPoolBuilder;
use tracing::info;
use uuid::Uuid;

use crate::{
    CorylusError,
    instance::{self},
    network::{
        packet::{self, Packet},
        registry::Response,
    },
    partition,
};

pub enum Task {
    PartitionRebalance,
    Read { from: Uuid, packet: packet::Read },
    Write { from: Uuid, packet: packet::Write },
}

impl Task {
    pub fn execute(self, instance: instance::Weak) {
        match self {
            // TODO: Maybe is good idea to make this as persistent thread that receive changes to react fast to the changes (new node added in the middle of the proccess) and not being processed  1:1 sequential tasks
            Self::PartitionRebalance => {
                if let Some(ref_) = instance.as_ref().upgrade() {
                    let members = ref_.membership.all();

                    struct FetchEntry<'a> {
                        p_id: usize,
                        member_id: Uuid,
                        segments: HashSet<&'a String>,
                    }

                    info!(id = %ref_.id, "STARTED REBALANCE");

                    ref_.part_group
                        .set_all_lifecycle(partition::Lifecycle::Migration);

                    let mut fetch: HashMap<usize, FetchEntry> = HashMap::new();
                    let metadata = ref_.part_group.objs_metadata();
                    for (p_id, changes) in ref_.part_group.update(members.as_slice()) {
                        let mut ready = true;
                        for change in changes {
                            match change {
                                partition::MembershipChange::MasterChanged { old, new } => {
                                    if new.eq(&ref_.id) {
                                        let old_alive = members.contains(&old);
                                        // TODO: Check also replicators
                                        if old_alive {
                                            ready = false;
                                            fetch.insert(
                                                p_id,
                                                FetchEntry {
                                                    p_id,
                                                    member_id: old,
                                                    segments: metadata
                                                        .keys()
                                                        .collect::<HashSet<_>>(),
                                                },
                                            );
                                        }
                                    }

                                    if old.eq(&ref_.id) {
                                        ready = false;
                                    }
                                }

                                partition::MembershipChange::ReplicasChanged { added, removed } => {
                                }
                            }
                        }

                        if ready {
                            if ref_.part_group.is_initialized() {
                                ref_.part_group
                                    .set_lifecycle(p_id, partition::Lifecycle::Ready);
                            } else if ref_.part_group.owner_of(p_id as u64).eq(&ref_.id) {
                                let member_id = *ref_
                                    .part_group
                                    .replicas_of(p_id as u64, 1)
                                    .first()
                                    .expect("CHECK");
                                fetch.insert(
                                    p_id,
                                    FetchEntry {
                                        p_id,
                                        member_id,
                                        segments: metadata.keys().collect::<HashSet<_>>(),
                                    },
                                );
                            }
                        }
                    }

                    enum FetchResponse<'a> {
                        FetchObject { s_id: String, resp: Response<'a> },
                        Completion(Response<'a>),
                    }

                    let v = ref_.part_group.version();
                    let mut pending_partitions = fetch.keys().copied().collect::<HashSet<_>>();
                    loop {
                        if pending_partitions.is_empty() {
                            break;
                        }

                        let mut timeout = Duration::from_secs(2);
                        let start = Instant::now();

                        let mut response_map = HashMap::new();
                        for (p_id, req) in fetch.iter() {
                            if pending_partitions.get(p_id).is_none() {
                                continue;
                            }

                            response_map
                                .insert(req.p_id, Vec::with_capacity(req.segments.len().max(1)));
                            if req.segments.is_empty() {
                                let response = ref_
                                    .net
                                    .sync_send(
                                        req.member_id,
                                        Packet::Request(packet::Request::Write(
                                            packet::Write::PartitionFetchCompletion {
                                                v,
                                                corr_id: Uuid::new_v4(),
                                                partition_id: *p_id as u16,
                                            },
                                        )),
                                        None,
                                    )
                                    .expect("TODO: Handle this error");

                                response_map
                                    .get_mut(&req.p_id)
                                    .expect("TODO")
                                    .push(FetchResponse::Completion(response));

                                continue;
                            }

                            for s_id in req.segments.iter() {
                                let s_id = s_id.to_string();
                                let response = ref_
                                    .net
                                    .sync_send(
                                        req.member_id,
                                        Packet::Request(packet::Request::Read(
                                            packet::Read::FetchObject {
                                                v,
                                                corr_id: Uuid::new_v4(),
                                                partition_id: req.p_id as u16,
                                                segment_id: s_id.to_string(),
                                            },
                                        )),
                                        None,
                                    )
                                    .expect("TODO: Handle this error");

                                response_map.get_mut(&req.p_id).expect("TODO").push(
                                    FetchResponse::FetchObject {
                                        s_id,
                                        resp: response,
                                    },
                                );
                            }
                        }

                        for (p_id, responses) in response_map {
                            for entry in responses {
                                match entry {
                                    FetchResponse::FetchObject { s_id, resp } => {
                                        if resp.get(timeout).is_ok() {
                                            fetch
                                                .get_mut(&p_id)
                                                .expect("Always available")
                                                .segments
                                                .remove(&s_id);
                                        }
                                    }
                                    FetchResponse::Completion(resp) => {
                                        if resp.get(timeout).is_ok() {
                                            pending_partitions.remove(&p_id);
                                            ref_.part_group
                                                .set_lifecycle(p_id, partition::Lifecycle::Ready);
                                        }
                                    }
                                }

                                timeout = timeout.saturating_sub(start.elapsed());
                            }
                        }
                    }

                    ref_.part_group.update_version(&members);
                    ref_.part_group.initialize();
                    // TODO: This should not be needed and was masking errors
                    ref_.part_group
                        .set_all_lifecycle(partition::Lifecycle::Ready);
                    info!(id = %ref_.id, "REBALANCE FINISHED");
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
                            // TODO: Change segment to object id
                            .obj_metadata(&segment_id)
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

                packet::Read::FetchObject {
                    v,
                    corr_id,
                    partition_id,
                    segment_id,
                } => {
                    if let Some(ref_) = instance.as_ref().upgrade() {
                        let _ = ref_.net.send(
                            from,
                            Packet::Reply(packet::Reply::GetOp {
                                corr_id,
                                status: packet::Status::Success,
                                result: vec![],
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
                            .obj_metadata(&segment_id)
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
                packet::Write::PartitionFetchCompletion {
                    v,
                    corr_id,
                    partition_id,
                } => {
                    if let Some(ref_) = instance.as_ref().upgrade() {
                        ref_.part_group
                            .set_lifecycle(partition_id as usize, partition::Lifecycle::Ready);
                        let _ = ref_.net.send(
                            from,
                            Packet::Reply(packet::Reply::PartitionFetchCompletion {
                                corr_id,
                                status: packet::Status::Success,
                            }),
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
