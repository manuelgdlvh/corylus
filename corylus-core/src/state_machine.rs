use raft::prelude::{Entry, Snapshot};

pub trait StateMachine {
    fn apply(&self, entry: Entry);
    fn apply_snapshot(&self, snapshot: Snapshot);
}