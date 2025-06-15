use crate::node::GenericError;
use raft::prelude::{ConfState, Entry, HardState, Snapshot};
use raft::storage::MemStorage as MemStorageInner;
use raft::{GetEntriesContext, Storage};

pub trait RaftLog: Storage + Send + 'static {
    fn append(&self, entries: &[Entry]) -> Result<(), GenericError>;
    fn apply_snapshot(&self, snapshot: Snapshot) -> Result<(), GenericError>;
    fn set_hard_state(&self, hs: HardState);
    fn set_conf_state(&self, cs: ConfState);
}

// Implementations

// In Memory
#[derive(Default)]
pub struct InMemoryRaftLog {
    inner: MemStorageInner,
}

impl InMemoryRaftLog {
    pub fn new() -> Self {
        let mut s = Snapshot::default();
        s.mut_metadata().index = 1;
        s.mut_metadata().term = 1;
        s.mut_metadata().mut_conf_state().voters = vec![1];

        let inner = MemStorageInner::new();
        inner.wl().apply_snapshot(s).unwrap();

        Self { inner }
    }
    pub fn new_with_inner(inner: MemStorageInner) -> Self {
        Self { inner }
    }
}
impl RaftLog for InMemoryRaftLog {
    fn append(&self, entries: &[Entry]) -> Result<(), GenericError> {
        self.inner.wl().append(entries).map_err(|err| err.into())
    }

    fn apply_snapshot(&self, snapshot: Snapshot) -> Result<(), GenericError> {
        self.inner
            .wl()
            .apply_snapshot(snapshot)
            .map_err(|err| err.into())
    }

    fn set_hard_state(&self, hs: HardState) {
        self.inner.wl().set_hardstate(hs);
    }

    fn set_conf_state(&self, cs: ConfState) {
        self.inner.wl().set_conf_state(cs);
    }
}

impl Storage for InMemoryRaftLog {
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
