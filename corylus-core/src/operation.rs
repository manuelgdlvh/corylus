use crate::state_machine::StateMachine;

// Read operation is distinct of write, does not must be persisted in log and returns result.
// Read operations must be linearized with writes to avoid locks in write's (Raft single threaded, for more parallelism use MultiRaft).
pub trait ReadOperation<S>: Send
where
    S: StateMachine,
{
    // No matters what state machine implementation is (in memory, redis, whatever), the output always will be the same
    type Output: Send + 'static;

    // Add tuple in RaftNodeHandle with the notifier
    fn execute(&self, state: &S) -> Option<Self::Output>;
}

pub trait WriteOperation<S>: Send
where
    S: StateMachine,
{
    // Instead block, use notifier primitive to allow block or not.
    fn execute(&self, state: &mut S);
}

pub trait OpDeSerializer<S>
where
    S: StateMachine,
{
    type Output: WriteOperation<S>;

    // Should be serialized for network travelling
    fn serialize(self) -> Vec<u8>;
    fn deserialize(data: Vec<u8>) -> Self::Output;
}

#[cfg(test)]
mod tests {
    use crate::operation::{ReadOperation, StateMachine, WriteOperation};

    #[derive(Default)]
    struct SumOp {
        value: u64,
    }
    impl WriteOperation<InMemoryStateMachine> for SumOp {
        fn execute(&self, state: &mut InMemoryStateMachine) {
            state.value += self.value;
        }
    }

    #[derive(Default)]
    struct ReadSumOp;
    impl ReadOperation<InMemoryStateMachine> for ReadSumOp {
        type Output = u64;

        fn execute(&self, state: &InMemoryStateMachine) -> Option<Self::Output> {
            Some(state.value)
        }
    }
    #[derive(Default)]
    struct InMemoryStateMachine {
        value: u64,
    }
    impl StateMachine for InMemoryStateMachine {}

    #[test]
    fn should_execute_write_operation_successfully() {
        let mut sm = InMemoryStateMachine::default();
        let op = SumOp { value: 1 };
        let op_2 = SumOp { value: 2 };

        op.execute(&mut sm);
        op_2.execute(&mut sm);

        assert_eq!(3, sm.value)
    }

    #[test]
    fn should_execute_read_operation_successfully() {
        let sm = InMemoryStateMachine { value: 7 };
        let result = ReadSumOp::default().execute(&sm);

        assert_eq!(Some(7), result);
    }
}
