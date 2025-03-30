use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::Storage;

pub trait RaftLog: Storage {
    fn append(&self, entries: &[Entry]) -> anyhow::Result<()>;
    fn apply_snapshot(&self, snapshot: Snapshot) -> anyhow::Result<()>;
    fn set_hard_state(&self, hs: HardState);
    fn set_conf_state(&self, cs: ConfState);
}