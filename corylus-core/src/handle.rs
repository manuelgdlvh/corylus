use crate::operation::{ReadOperation, WriteOperation};
use crate::state_machine::StateMachine;
use std::marker::PhantomData;
use std::sync::mpsc;

// type ReadOpFn<S> = for<'a> fn(&'a S);
// This won't work because function pointers (fn) cannot capture environment variables.
// Closures that capture variables are compiled into unique, anonymous structs implementing Fn, FnMut, or FnOnce.
// Therefore, they cannot be coerced into plain function pointers.

// `impl FnOnce(&S)` also can't be used directly as a field type in structs or type aliases,
// because each closure that captures variables has a different anonymous type.
// To support capturing closures without boxing, you must use generics to preserve the concrete closure type.

// Use erased types (e.g., trait objects) when results are not needed,
// as they simplify type handling and allow heterogeneous operations.
// If results are needed, using closures (`FnOnce`) with captured response channels
// preserves type safety while still enabling dynamic dispatch.

pub type ReadOpFn<S> = Box<dyn FnOnce(&S) + Send>;

// Instead of erased type with heap allocation, using typed struct with operation erased guarantying fast access to notifier avoiding one more heap indirection.
pub struct AwaitableWriteOp<S>
where
    S: StateMachine,
{
    pub op: Box<dyn WriteOperation<S>>,
    pub notifier: mpsc::SyncSender<()>,
    _phantom_sm: PhantomData<S>,
}

impl<S> AwaitableWriteOp<S>
where
    S: StateMachine,
{
    fn new<Op: WriteOperation<S> + 'static>(op: Op) -> (Self, mpsc::Receiver<()>) {
        let (tx, rx) = mpsc::sync_channel(1);
        let self_ = Self {
            op: Box::new(op),
            notifier: tx,
            _phantom_sm: Default::default(),
        };

        (self_, rx)
    }
}

pub struct RaftNodeHandle<S>
where
    S: StateMachine,
{
    read_channel: mpsc::Sender<ReadOpFn<S>>,
    write_channel: mpsc::Sender<AwaitableWriteOp<S>>,
}

impl<S> RaftNodeHandle<S>
where
    S: StateMachine,
{
    pub fn new(
        read_channel: mpsc::Sender<ReadOpFn<S>>,
        write_channel: mpsc::Sender<AwaitableWriteOp<S>>,
    ) -> Self {
        Self {
            read_channel,
            write_channel,
        }
    }

    // Why 'static in R
    // Differences between sync channel and not sync
    pub fn read<R: ReadOperation<S> + 'static>(&self, op: R) -> mpsc::Receiver<Option<R::Output>> {
        let (tx, rx) = mpsc::sync_channel::<Option<R::Output>>(1);
        let read_op_fn: ReadOpFn<S> = Box::new(move |state: &S| {
            let result = op.execute(state);
            tx.send(result).unwrap();
        });
        self.read_channel.send(read_op_fn).unwrap();
        rx
    }

    pub fn write<W: WriteOperation<S> + 'static>(&self, op: W) -> mpsc::Receiver<()> {
        let (awaitable_op, rx) = AwaitableWriteOp::new(op);
        self.write_channel.send(awaitable_op).unwrap();
        rx
    }
}

#[cfg(test)]
mod tests {
    use crate::handle::RaftNodeHandle;
    use crate::operation::{ReadOperation, WriteOperation};
    use crate::state_machine::StateMachine;
    use std::sync::mpsc;

    #[derive(Default)]
    struct ReadOp;
    impl ReadOperation<InMemoryStateMachine> for ReadOp {
        type Output = u64;
        fn execute(&self, state: &InMemoryStateMachine) -> Option<Self::Output> {
            Some(state.value)
        }
    }

    #[derive(Default)]
    struct IncrementOp;
    impl WriteOperation<InMemoryStateMachine> for IncrementOp {
        fn execute(&self, state: &mut InMemoryStateMachine) {
            state.value += 1;
        }
    }

    #[derive(Default)]
    struct InMemoryStateMachine {
        value: u64,
    }
    impl StateMachine for InMemoryStateMachine {}

    #[test]
    fn should_read_successfully() {
        let (r_tx, r_rx) = mpsc::channel();
        let (w_tx, _) = mpsc::channel();

        let handle = RaftNodeHandle::<InMemoryStateMachine>::new(r_tx, w_tx);

        let result = handle.read(ReadOp::default());

        let sm = InMemoryStateMachine { value: 7 };
        let read_op = r_rx.recv().unwrap();
        read_op(&sm);

        assert_eq!(Some(7), result.recv().unwrap());
    }

    #[test]
    fn should_write_successfully() {
        let (r_tx, _) = mpsc::channel();
        let (w_tx, w_rx) = mpsc::channel();

        let handle = RaftNodeHandle::<InMemoryStateMachine>::new(r_tx, w_tx);

        let _ = handle.write(IncrementOp::default());

        let mut sm = InMemoryStateMachine { value: 0 };
        let write_op = w_rx.recv().unwrap();
        write_op.op.execute(&mut sm);

        assert_eq!(1, sm.value);
    }
}
