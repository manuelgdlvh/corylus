use raft::{GetEntriesContext, Storage};
use raft::eraftpb::{ConfState, HardState};
use raft::prelude::{Entry, Snapshot};
use raft::storage::MemStorage as MemStorageCore;

use corylus_core::raft_log::RaftLog;

pub struct MemStorage {
    core: MemStorageCore,
    snapshot: Snapshot,
}

impl MemStorage {
    pub fn new() -> Self {
        let core = MemStorageCore::default();
        let snapshot: Snapshot = Default::default();
        Self { core, snapshot }
    }
}
impl RaftLog for MemStorage {
    fn append(&self, entries: &[Entry]) -> anyhow::Result<()> {
        Ok(self.core.wl().append(entries).unwrap())
    }

    fn apply_snapshot(&self, snapshot: Snapshot) -> anyhow::Result<()> {
        Ok(self.core.wl().apply_snapshot(snapshot).unwrap())
    }

    fn set_hard_state(&self, hs: HardState) {
        self.core.wl().set_hardstate(hs);
    }

    fn set_conf_state(&self, cs: ConfState) {
        self.core.wl().set_conf_state(cs);
    }
}

impl Storage for MemStorage {
    fn initial_state(&self) -> raft::Result<raft::prelude::RaftState> {
        self.core.initial_state()
    }

    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>, context: GetEntriesContext) -> raft::Result<Vec<Entry>> {
        self.core.entries(low, high, max_size, context)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        self.core.term(idx)
    }

    fn first_index(&self) -> raft::Result<u64> {
        self.core.first_index()
    }

    fn last_index(&self) -> raft::Result<u64> {
        self.core.last_index()
    }

    fn snapshot(&self, _request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        self.core.snapshot(_request_index, _to)
    }
}
