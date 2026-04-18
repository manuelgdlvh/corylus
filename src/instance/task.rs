use crate::network::Response;
use crate::network::packet::{Inbound, Reply, Request, Status};
use crate::{
    CorylusError,
    instance::{self},
    network::packet::{self},
    object, partition,
};
use rayon::ThreadPoolBuilder;
use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};
use tracing::{error, info};
use uuid::Uuid;

pub enum Task {
    PartitionRebalance,
    Read(Inbound),
    Write(Inbound),
}

impl Task {
    pub fn execute(self, instance: instance::Weak) {
        match self {
            Self::PartitionRebalance => {
                if let Some(ref_) = instance.as_ref().upgrade() {
                    let mut members;

                    'rebalance: loop {
                        members = ref_.membership.all();
                        let target_version = partition::version(members.as_slice());
                        if target_version.eq(&ref_.part_group.version()) {
                            break 'rebalance;
                        }

                        struct FetchEntry<'a> {
                            part_id: usize,
                            member_id: Uuid,
                            segments: HashSet<&'a String>,
                        }

                        info!(id = %ref_.id, "rebalance has started");

                        ref_.part_group
                            .set_all_lifecycle(partition::Lifecycle::Migration);

                        let mut fetch: HashMap<usize, FetchEntry> = HashMap::new();
                        for (part_id, changes) in ref_.part_group.update(members.as_slice()) {
                            let mut ready = true;
                            for change in changes {
                                match change {
                                    partition::MembershipChange::MasterChanged { old, new } => {
                                        if new.eq(&ref_.id) {
                                            let old_alive = members.contains(&old);
                                            if old_alive {
                                                ready = false;
                                                fetch.insert(
                                                    part_id,
                                                    FetchEntry {
                                                        part_id,
                                                        member_id: old,
                                                        segments: ref_
                                                            .objects
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

                                    partition::MembershipChange::ReplicasChanged {
                                        added: _,
                                        removed: _,
                                    } => {}
                                }
                            }

                            if ready {
                                if ref_.part_group.is_initialized() {
                                    ref_.part_group
                                        .set_lifecycle(part_id, partition::Lifecycle::Ready);
                                } else if ref_.part_group.owner_of(part_id as u64).eq(&ref_.id) {
                                    let member_id = *ref_
                                        .part_group
                                        .replicas_of(part_id as u64, 1)
                                        .first()
                                        .expect("CHECK");
                                    fetch.insert(
                                        part_id,
                                        FetchEntry {
                                            part_id,
                                            member_id,
                                            segments: ref_.objects.keys().collect::<HashSet<_>>(),
                                        },
                                    );
                                }
                            }
                        }

                        enum FetchResponse<'a> {
                            FetchObject {
                                obj_id: &'a String,
                                resp: Response<'a>,
                            },
                            Completion(Response<'a>),
                        }

                        let v = ref_.part_group.version();
                        let mut pending_partitions = fetch.keys().copied().collect::<HashSet<_>>();
                        loop {
                            if pending_partitions.is_empty() {
                                break 'rebalance;
                            }

                            // Some peers changed in the meeantime, restarting process
                            if !members.eq(&ref_.membership.all()) {
                                continue 'rebalance;
                            }

                            let mut timeout = Duration::from_secs(2);
                            let start = Instant::now();
                            let mut response_map = HashMap::new();
                            for (part_id, req) in fetch.iter() {
                                if !pending_partitions.contains(part_id) {
                                    continue;
                                }

                                response_map.insert(
                                    req.part_id,
                                    Vec::with_capacity(req.segments.len().max(1)),
                                );
                                if req.segments.is_empty() {
                                    let response = ref_
                                        .net
                                        .request_sync(
                                            req.member_id,
                                            packet::Request::PartitionFetchCompletion {
                                                v,
                                                corr_id: Uuid::new_v4(),
                                                part_id: *part_id as u16,
                                            },
                                            None,
                                        )
                                        .unwrap();

                                    response_map
                                        .get_mut(&req.part_id)
                                        .unwrap()
                                        .push(FetchResponse::Completion(response));

                                    continue;
                                }

                                for &obj_id in req.segments.iter() {
                                    let response = ref_
                                        .net
                                        .request_sync(
                                            req.member_id,
                                            packet::Request::FetchObject {
                                                v,
                                                corr_id: Uuid::new_v4(),
                                                part_id: req.part_id as u16,
                                                obj_id,
                                            },
                                            None,
                                        )
                                        .unwrap();

                                    response_map.get_mut(&req.part_id).unwrap().push(
                                        FetchResponse::FetchObject {
                                            obj_id,
                                            resp: response,
                                        },
                                    );
                                }
                            }

                            for (part_id, responses) in response_map {
                                for entry in responses {
                                    match entry {
                                        FetchResponse::FetchObject { obj_id, resp } => {
                                            match resp.get(timeout) {
                                                Ok(raw) => {
                                                    if let Reply::FetchObject { result, .. } =
                                                        Reply::try_from(&raw).unwrap()
                                                    {
                                                        ref_.rebuild(
                                                            obj_id,
                                                            part_id as u16,
                                                            result,
                                                        )
                                                        .unwrap();
                                                        fetch
                                                            .get_mut(&part_id)
                                                            .expect("Always available")
                                                            .segments
                                                            .remove(obj_id);
                                                    }
                                                }
                                                Err(err) => {
                                                    error!(err = %err, "Fetch object error");
                                                }
                                            }
                                        }
                                        FetchResponse::Completion(resp) => {
                                            match resp.get(timeout) {
                                                Ok(_) => {
                                                    pending_partitions.remove(&part_id);
                                                    ref_.part_group.set_lifecycle(
                                                        part_id,
                                                        partition::Lifecycle::Ready,
                                                    );
                                                }
                                                Err(err) => {
                                                    error!(err = %err, "Partition Completion error");
                                                }
                                            }
                                        }
                                    }

                                    timeout = timeout.saturating_sub(start.elapsed());
                                }
                            }
                        }
                    }

                    ref_.part_group.update_version(&members);
                    ref_.part_group.initialize();
                    ref_.part_group
                        .set_all_lifecycle(partition::Lifecycle::Ready);
                    info!(id = %ref_.id, "Rebalance has finished");
                }
            }

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
                                let (status, result) = ref_
                                    .objects
                                    .get(obj_id)
                                    .ok_or(object::Error::ObjectNotFound.into())
                                    .and_then(|m| m.read_fn(op_id).map_err(CorylusError::from))
                                    .and_then(|f| f(raw_op).map_err(CorylusError::from))
                                    .and_then(|op| ref_.remote_read(obj_id, part_id, v, op))
                                    .map(|val| (Status::Success, val))
                                    .unwrap_or_else(|err| (Status::from(err), vec![]));

                                let _ = ref_.net.reply(
                                    from,
                                    Reply::GetOp {
                                        corr_id,
                                        status,
                                        result: &result,
                                    },
                                    None,
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
                                    None,
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
                                let status = ref_
                                    .objects
                                    .get(obj_id)
                                    .ok_or(object::Error::ObjectNotFound.into())
                                    .and_then(|m| m.write_fn(op_id).map_err(CorylusError::from))
                                    .and_then(|f| f(raw_op).map_err(CorylusError::from))
                                    .and_then(|op| ref_.remote_write(obj_id, part_id, v, op))
                                    .map(|_| Status::Success)
                                    .unwrap_or_else(Status::from);

                                let _ =
                                    ref_.net
                                        .reply(from, Reply::WriteOp { corr_id, status }, None);
                            }
                        }
                        Request::PartitionFetchCompletion {
                            v: _,
                            corr_id,
                            part_id,
                        } => {
                            if let Some(ref_) = instance.as_ref().upgrade() {
                                ref_.part_group
                                    .set_lifecycle(part_id as usize, partition::Lifecycle::Ready);
                                let _ = ref_.net.reply(
                                    from,
                                    Reply::PartitionFetchCompletion {
                                        corr_id,
                                        status: Status::Success,
                                    },
                                    None,
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
