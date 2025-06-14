use crate::node::GenericError;
use crate::operation::{
    AwaitableRaftCommand, RaftCommand, RaftCommandResult, ReadOperation, WriteOperation,
};
use crate::peer::MessageId;
use crate::state_machine::RaftStateMachine;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::mpsc::Receiver;
use tokio::sync::mpsc;
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

pub(crate) type ReadOpFn<S> =
    Box<dyn Fn(&S) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

// Instead of erased type with heap allocation, using typed struct with operation erased guarantying fast access to notifier avoiding one more heap indirection.
pub struct AwaitableWriteOp<S>
where
    S: RaftStateMachine,
{
    pub op: Box<dyn WriteOperation<S>>,
    pub notifier: mpsc::Sender<Result<MessageId, GenericError>>,
    _phantom_sm: PhantomData<S>,
}

impl<S> AwaitableWriteOp<S>
where
    S: RaftStateMachine,
{
    fn new<Op: WriteOperation<S> + 'static>(
        op: Op,
    ) -> (Self, mpsc::Receiver<Result<MessageId, GenericError>>) {
        let (tx, rx) = mpsc::channel(1);
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
    S: RaftStateMachine,
{
    read_channel: mpsc::UnboundedSender<ReadOpFn<S>>,
    write_channel: mpsc::UnboundedSender<AwaitableWriteOp<S>>,
    raft_msg_channel: mpsc::UnboundedSender<AwaitableRaftCommand>,
}

impl<S> RaftNodeHandle<S>
where
    S: RaftStateMachine,
{
    pub(crate) fn new(
        read_channel: mpsc::UnboundedSender<ReadOpFn<S>>,
        write_channel: mpsc::UnboundedSender<AwaitableWriteOp<S>>,
        raft_msg_channel: mpsc::UnboundedSender<AwaitableRaftCommand>,
    ) -> Self {
        Self {
            read_channel,
            write_channel,
            raft_msg_channel,
        }
    }

    // Why 'static in R
    // Differences between sync channel and not sync
    pub fn read<R: ReadOperation<S> + 'static>(&self, op: R) -> mpsc::Receiver<Option<R::Output>> {
        let (tx, rx) = mpsc::channel::<Option<R::Output>>(1);

        let read_op_fn: ReadOpFn<S> = Box::new(move |state: &S| {
            let tx = tx.clone();
            let result = op.execute(state);

            Box::pin(async move {
                tx.send(result).await.unwrap();
            })
        });
        self.read_channel.send(read_op_fn).unwrap();
        rx
    }

    pub fn write<W: WriteOperation<S> + 'static>(
        &self,
        op: W,
    ) -> mpsc::Receiver<Result<MessageId, GenericError>> {
        let (awaitable_op, rx) = AwaitableWriteOp::new(op);
        self.write_channel.send(awaitable_op).unwrap();
        rx
    }

    pub fn raft_command(
        &self,
        command: RaftCommand,
    ) -> Receiver<Result<RaftCommandResult, GenericError>> {
        let (awaitable_command, rx) = AwaitableRaftCommand::new(command);
        self.raft_msg_channel.send(awaitable_command).unwrap();
        rx
    }
}

#[cfg(test)]
mod tests {}
