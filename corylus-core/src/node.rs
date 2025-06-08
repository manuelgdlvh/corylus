use crate::callback::CallbackHolder;
use crate::handle::{AwaitableWriteOp, RaftNodeHandle, ReadOpFn};
use crate::leader_proxy::RaftLeaderProxy;
use crate::raft_log::RaftLog;
use crate::state_machine::StateMachine;
use futures::future::join_all;
use protobuf::Message as ProtobufMessage;
use raft::prelude::{ConfChangeV2, Entry, EntryType, Message};
use raft::{Config, RawNode, StateRole, Storage};
use std::error::Error;
use std::thread;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::time;

pub type GenericError = Box<dyn Error + Send + Sync>;

struct OpChannel<S>
where
    S: StateMachine,
{
    r_rx: mpsc::UnboundedReceiver<ReadOpFn<S>>,
    w_rx: mpsc::UnboundedReceiver<AwaitableWriteOp<S>>,
    handle: Option<RaftNodeHandle<S>>,
}

impl<S> OpChannel<S>
where
    S: StateMachine,
{
    fn new() -> Self {
        let (r_tx, r_rx) = mpsc::unbounded_channel();
        let (w_tx, w_rx) = mpsc::unbounded_channel();
        Self {
            r_rx,
            w_rx,
            handle: Some(RaftNodeHandle::new(r_tx, w_tx)),
        }
    }

    fn as_mut(
        &mut self,
    ) -> (
        &mut mpsc::UnboundedReceiver<ReadOpFn<S>>,
        &mut mpsc::UnboundedReceiver<AwaitableWriteOp<S>>,
    ) {
        (&mut self.r_rx, &mut self.w_rx)
    }

    fn try_recv_write_op(&mut self) -> Result<AwaitableWriteOp<S>, TryRecvError> {
        self.w_rx.try_recv()
    }

    fn try_recv_read_op(&mut self) -> Result<ReadOpFn<S>, TryRecvError> {
        self.r_rx.try_recv()
    }

    fn stop(&mut self) {
        self.r_rx.close();
        self.w_rx.close();
    }

    fn is_stopped(&self) -> bool {
        self.r_rx.is_closed() || self.w_rx.is_closed()
    }
}

pub struct RaftNode<S, L, P>
where
    S: StateMachine,
    L: RaftLog,
    P: RaftLeaderProxy<S>,
{
    group: RawNode<L>,
    op_channel: OpChannel<S>,
    state: S,
    leader_proxy: P,
    callbacks: CallbackHolder<S>,
}

impl<S, L, P> Drop for RaftNode<S, L, P>
where
    S: StateMachine,
    L: RaftLog,
    P: RaftLeaderProxy<S>,
{
    fn drop(&mut self) {
        self.op_channel.stop();
    }
}

impl<S, L, P> RaftNode<S, L, P>
where
    S: StateMachine,
    L: RaftLog,
    P: RaftLeaderProxy<S>,
{
    pub fn new(state: S, storage: L, leader_proxy: P) -> Result<Self, GenericError> {
        Self::with_config(state, storage, leader_proxy, Default::default())
    }

    pub fn with_config(
        state: S,
        storage: L,
        leader_proxy: P,
        mut config: Config,
    ) -> Result<Self, GenericError> {
        config.id = 1;
        let group = RawNode::with_default_logger(
            &Config {
                id: 1,
                ..Default::default()
            },
            storage,
        )?;

        let self_ = Self {
            group,
            op_channel: OpChannel::new(),
            state,
            leader_proxy,
            callbacks: CallbackHolder::new(),
        };

        Ok(self_)
    }

    pub fn start(mut self) -> RaftNodeHandle<S> {
        let runtime = Builder::new_current_thread()
            .thread_name("raft-worker")
            .enable_all()
            .build()
            .unwrap();
        let handle = self.op_channel.handle.take().unwrap();

        thread::spawn(move || {
            if let Err(err) = self.group.campaign() {
                panic!("{}", err);
            }
            runtime.block_on(async move {
                let timeout = Duration::from_millis(100);
                let mut w_buffer = Vec::new();

                loop {
                    if self.op_channel.is_stopped() {
                        break;
                    }

                    let mut r_buffer = Vec::new();
                    match self.drain_ops(w_buffer, r_buffer, timeout).await {
                        (new_w_buffer, new_r_buffer) => {
                            w_buffer = new_w_buffer;
                            r_buffer = new_r_buffer;
                        }
                    }

                    self.on_read(r_buffer).await;
                    self.on_write(&mut w_buffer).await;
                    self.tick().await;
                }
            });
            runtime.shutdown_background();
        });

        handle
    }

    // API must be well defined for Remote Server messages
    async fn tick(&mut self) {
        if self.group.has_ready() {
            let mut ready = self.group.ready();
            self.handle_messages(ready.take_messages()).await;
            if !ready.snapshot().is_empty() {
                self.group
                    .store()
                    .apply_snapshot(ready.snapshot().clone())
                    .unwrap();
            }

            // To be persisted in store machine because were commited
            self.handle_entries(ready.take_committed_entries()).await;

            // New entries to be appended to the log
            if !ready.entries().is_empty() {
                self.group.store().append(ready.entries()).unwrap();
            }

            if let Some(hs) = ready.hs() {
                self.group.store().set_hard_state(hs.clone());
            }

            self.handle_messages(ready.take_persisted_messages()).await;

            let mut light_rd = self.group.advance(ready);
            self.handle_messages(light_rd.take_messages()).await;
            self.handle_entries(light_rd.take_committed_entries()).await;
            self.group.advance_apply();
        }

        self.group.tick();
        // Check signals
    }

    async fn handle_entries(&mut self, entries: Vec<Entry>) {
        println!("Entries: {entries:?}");

        let mut _last_apply_index = 0;
        for entry in entries {
            _last_apply_index = entry.index;

            if entry.data.is_empty() {
                continue;
            }

            match entry.get_entry_type() {
                EntryType::EntryNormal => {
                    let buffer = entry.context.as_ref();
                    if buffer.is_empty() {
                        println!("Ignoring entry with empty context buffer");
                        continue;
                    }
                    let buffer: [u8; 16] =
                        buffer.try_into().expect("Slice should be exactly 16 bytes");
                    let message_id = u128::from_be_bytes(buffer);
                    match self.callbacks.remove(message_id) {
                        None => {
                            println!("Ignoring entry with unknown message id: {}", message_id);
                        }
                        Some(w_op) => {
                            w_op.op.execute(&mut self.state);
                            let _ = w_op.notifier.send(Ok(())).await;
                        }
                    }
                }
                EntryType::EntryConfChangeV2 => {
                    let mut cc = ConfChangeV2::default();
                    cc.merge_from_bytes(&entry.data).unwrap();
                    let cs = self.group.apply_conf_change(&cc).unwrap();
                    self.group.store().set_conf_state(cs);
                }
                _ => {}
            };
        }
    }

    async fn handle_messages(&self, messages: Vec<Message>) {
        println!("Messages: {messages:?}");

        // Send to peers
        for msg in messages {}
    }

    // Add if no Leader depends on configuration, read from master or not (Proxied)
    async fn on_read(&self, r_buffer: Vec<ReadOpFn<S>>) {
        if !r_buffer.is_empty() {
            let futures = r_buffer.into_iter().map(|op| op(&self.state));
            join_all(futures).await;
        }
    }

    async fn on_write(&mut self, w_buffer: &mut Vec<AwaitableWriteOp<S>>) {
        for w_op in w_buffer.drain(..) {
            let op_buffer = w_op.op.serialize();
            if self.group.raft.state != StateRole::Leader {
                println!("Node is not leader. Sending over the network");

                // Must return message id.
                // Allow batching.
                // Track Rejections to remove dirty callbacks.
                match self.leader_proxy.write(op_buffer.as_slice()).await {
                    Ok(message_id) => {
                        self.callbacks.add(message_id, w_op);
                    }
                    Err(err) => {
                        let _ = w_op.notifier.send(Err(err.into())).await;
                    }
                };
            } else {
                let message_id = self.callbacks.next();
                if let Err(err) = self
                    .group
                    .propose(message_id.to_be_bytes().to_vec(), op_buffer)
                {
                    // Add log
                    let _ = w_op.notifier.send(Err(err.into())).await;
                } else {
                    self.callbacks.add(message_id, w_op);
                }
            }
        }
    }

    // Limit batch size of op's
    async fn drain_ops<'a>(
        &mut self,
        mut w_buffer: Vec<AwaitableWriteOp<S>>,
        mut r_buffer: Vec<ReadOpFn<S>>,
        timeout: Duration,
    ) -> (Vec<AwaitableWriteOp<S>>, Vec<ReadOpFn<S>>) {
        let (read_mut, write_mut) = self.op_channel.as_mut();
        tokio::select! {
            biased;
            _ = time::sleep(timeout) => {
                return (w_buffer, r_buffer);
            }
            // Read operation received
            Some(r_op) = read_mut.recv() => {
                r_buffer.push(r_op);
            }

            // Write operation received
            Some(w_op) = write_mut.recv() => {
                w_buffer.push(w_op);
            }
            // Both channels closed
            else => {
                return (w_buffer, r_buffer);
            }
        }

        while let Ok(r_op) = self.op_channel.try_recv_read_op() {
            r_buffer.push(r_op);
        }

        while let Ok(w_op) = self.op_channel.try_recv_write_op() {
            w_buffer.push(w_op);
        }

        (w_buffer, r_buffer)
    }
}

#[cfg(test)]
mod test {
    use crate::leader_proxy::NoOpRaftLeaderProxy;
    use crate::node::RaftNode;
    use crate::operation::{ReadOperation, WriteOperation};
    use crate::raft_log::InMemoryRaftLog;
    use crate::state_machine::StateMachine;

    #[derive(Default)]
    struct ReadOp;
    impl ReadOperation<InMemoryStateMachine> for ReadOp {
        type Output = u64;
        fn execute(&self, state: &InMemoryStateMachine) -> Option<Self::Output> {
            Some(state.value)
        }

        fn serialize(&self) -> Vec<u8> {
            todo!()
        }
    }

    #[derive(Default)]
    struct IncrementOp;
    impl WriteOperation<InMemoryStateMachine> for IncrementOp {
        fn execute(&self, state: &mut InMemoryStateMachine) {
            state.value += 1;
        }

        fn serialize(&self) -> Vec<u8> {
            "INCREMENT".bytes().collect()
        }
    }

    #[derive(Default)]
    struct InMemoryStateMachine {
        value: u64,
    }
    impl StateMachine for InMemoryStateMachine {}

    #[tokio::test]
    async fn test() {
        let sm = InMemoryStateMachine::default();
        let rl = InMemoryRaftLog::new();
        let l_proxy = NoOpRaftLeaderProxy::default();

        let node = RaftNode::new(sm, rl, l_proxy).unwrap();
        let handle = node.start();

        (0..99).for_each(|_| {
            handle.write(IncrementOp::default());
        });
        let _ = handle.write(IncrementOp::default()).recv().await;

        assert_eq!(
            Some(100),
            handle.read(ReadOp::default()).recv().await.unwrap()
        );
    }
}
