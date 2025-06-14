use std::net::SocketAddr;
use crate::node::GenericError;
use crate::state_machine::RaftStateMachine;
use raft::prelude::{ConfChangeV2, Message};
use std::sync::mpsc::{Receiver, Sender, SyncSender};

// Read operation is distinct of write, does not must be persisted in log and returns result.
// Read operations must be linearized with writes to avoid locks in write's (Raft single threaded, for more parallelism use MultiRaft).
pub trait ReadOperation<S>: Send + Sync
where
    S: RaftStateMachine,
{
    // No matters what state machine implementation is (in memory, redis, whatever), the output always will be the same
    type Output: Send + 'static;

    // Add tuple in RaftNodeHandle with the notifier
    fn execute(&self, state: &S) -> Option<Self::Output>;
    fn serialize(&self) -> Vec<u8>;
}

pub trait WriteOperation<S>: Send
where
    S: RaftStateMachine,
{
    // Instead block, use notifier primitive to allow block or not.
    fn execute(&self, state: &mut S);

    // Zero copy serialization is allowed here for scoped in which buffer are used
    fn serialize(&self) -> Vec<u8>;
}

// Add serializer, deserializer trait for this to use the caller want.
pub enum RaftCommand {
    ClusterJoin(SocketAddr),
    Raw(Message),
}

pub type NodeId = u64;
pub enum RaftCommandResult {
    ClusterJoin(NodeId),
    None,
}

pub struct AwaitableRaftCommand {
    pub command: RaftCommand,
    pub notifier: SyncSender<Result<RaftCommandResult, GenericError>>,
}

impl AwaitableRaftCommand {
    pub fn new(command: RaftCommand) -> (Self, Receiver<Result<RaftCommandResult, GenericError>>) {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let self_ = Self {
            command,
            notifier: tx,
        };
        (self_, rx)
    }
}

#[cfg(test)]
mod tests {
    use crate::operation::{ReadOperation, RaftStateMachine, WriteOperation};

    #[derive(Default)]
    struct SumOp {
        value: u64,
    }
    impl WriteOperation<InMemoryRaftStateMachine> for SumOp {
        fn execute(&self, state: &mut InMemoryRaftStateMachine) {
            state.value += self.value;
        }

        fn serialize(&self) -> Vec<u8> {
            todo!()
        }
    }

    #[derive(Default)]
    struct ReadSumOp;
    impl ReadOperation<InMemoryRaftStateMachine> for ReadSumOp {
        type Output = u64;

        fn execute(&self, state: &InMemoryRaftStateMachine) -> Option<Self::Output> {
            Some(state.value)
        }

        fn serialize(&self) -> Vec<u8> {
            todo!()
        }
    }
    #[derive(Default)]
    struct InMemoryRaftStateMachine {
        value: u64,
    }
    impl RaftStateMachine for InMemoryRaftStateMachine {}

    #[test]
    fn should_execute_write_operation_successfully() {
        let mut sm = InMemoryRaftStateMachine::default();
        let op = SumOp { value: 1 };
        let op_2 = SumOp { value: 2 };

        op.execute(&mut sm);
        op_2.execute(&mut sm);

        assert_eq!(3, sm.value)
    }

    #[test]
    fn should_execute_read_operation_successfully() {
        let sm = InMemoryRaftStateMachine { value: 7 };
        let result = ReadSumOp::default().execute(&sm);

        assert_eq!(Some(7), result);
    }
}
