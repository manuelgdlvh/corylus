use raft::eraftpb::{ConfState, HardState};
use raft::prelude::{Entry, Snapshot};
use raft::storage::MemStorage as MemStorageInner;
use raft::{GetEntriesContext, Storage};

use corylus_core::raft_log::RaftLog;

#[derive(Default)]
pub struct MemStorage {
    inner: MemStorageInner,
}

impl MemStorage {
    pub fn new(inner: MemStorageInner) -> Self {
        Self { inner }
    }
}
impl RaftLog for MemStorage {
    fn append(&self, entries: &[Entry]) -> anyhow::Result<()> {
        self.inner.wl().append(entries).map_err(anyhow::Error::from)
    }

    fn apply_snapshot(&self, snapshot: Snapshot) -> anyhow::Result<()> {
        self.inner
            .wl()
            .apply_snapshot(snapshot)
            .map_err(anyhow::Error::from)
    }

    fn set_hard_state(&self, hs: HardState) {
        self.inner.wl().set_hardstate(hs);
    }

    fn set_conf_state(&self, cs: ConfState) {
        self.inner.wl().set_conf_state(cs);
    }
}

impl Storage for MemStorage {
    fn initial_state(&self) -> raft::Result<raft::prelude::RaftState> {
        self.inner.initial_state()
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        self.inner.entries(low, high, max_size, context)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        self.inner.term(idx)
    }

    fn first_index(&self) -> raft::Result<u64> {
        self.inner.first_index()
    }

    fn last_index(&self) -> raft::Result<u64> {
        self.inner.last_index()
    }

    fn snapshot(&self, _request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        self.inner.snapshot(_request_index, _to)
    }
}
