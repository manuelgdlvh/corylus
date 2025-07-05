use crate::async_task::{AsyncRaftRunnable, AsyncRunnableExecutor, ForwardWrites};
use crate::callback::CallbackHolder;
use crate::handle::{AwaitableWriteOp, RaftNodeHandle, ReadOpFn};
use crate::operation::{AwaitableRaftCommand, RaftCommand, RaftCommandResult, WriteOperation};
use crate::peer::{
    MessageId, OpDeserializer, OperationBucket, RaftPeerClient, RaftPeerServer,
    RaftPeerServerHandle,
};
use crate::raft_log::RaftLog;
use crate::state_machine::RaftStateMachine;
use futures::future::join_all;
use protobuf::{Message as ProtobufMessage, RepeatedField};
use raft::eraftpb::{ConfChangeSingle, ConfChangeType};
use raft::prelude::{ConfChangeV2, Entry, EntryType, Message};
use raft::{Config, RawNode, StateRole};
use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::time;

pub type GenericError = Box<dyn Error + Send + Sync>;
// As trait
struct OperationHandler<SM, D>
where
    SM: RaftStateMachine,
    D: OpDeserializer<SM>,
{
    read_rx: mpsc::UnboundedReceiver<ReadOpFn<SM>>,
    write_rx: mpsc::UnboundedReceiver<AwaitableWriteOp<SM>>,
    raft_msg_rx: mpsc::UnboundedReceiver<AwaitableRaftCommand>,
    deserializer: D,
    handle: Option<RaftNodeHandle<SM>>,
}

impl<SM, D> OperationHandler<SM, D>
where
    SM: RaftStateMachine,
    D: OpDeserializer<SM>,
{
    fn new(deserializer: D) -> Self {
        let (read_tx, read_rx) = mpsc::unbounded_channel();
        let (write_tx, write_rx) = mpsc::unbounded_channel();
        let (raft_msg_tx, raft_msg_rx) = mpsc::unbounded_channel();

        Self {
            read_rx,
            write_rx,
            raft_msg_rx,
            deserializer,
            handle: Some(RaftNodeHandle::new(read_tx, write_tx, raft_msg_tx)),
        }
    }

    fn as_mut(
        &mut self,
    ) -> (
        &mut mpsc::UnboundedReceiver<ReadOpFn<SM>>,
        &mut mpsc::UnboundedReceiver<AwaitableWriteOp<SM>>,
        &mut mpsc::UnboundedReceiver<AwaitableRaftCommand>,
    ) {
        (&mut self.read_rx, &mut self.write_rx, &mut self.raft_msg_rx)
    }

    fn try_recv_write_op(&mut self) -> Result<AwaitableWriteOp<SM>, TryRecvError> {
        self.write_rx.try_recv()
    }

    fn try_recv_read_op(&mut self) -> Result<ReadOpFn<SM>, TryRecvError> {
        self.read_rx.try_recv()
    }

    fn try_recv_raft_msg(&mut self) -> Result<AwaitableRaftCommand, TryRecvError> {
        self.raft_msg_rx.try_recv()
    }

    fn stop(&mut self) {
        self.read_rx.close();
        self.write_rx.close();
    }

    fn deserialize(&self, data: &[u8]) -> Box<dyn WriteOperation<SM>> {
        self.deserializer.deserialize(data)
    }

    fn is_stopped(&self) -> bool {
        self.read_rx.is_closed() || self.write_rx.is_closed()
    }
}

pub struct RaftNode<SM, D, L, C>
where
    SM: RaftStateMachine,
    D: OpDeserializer<SM>,
    L: RaftLog,
    C: RaftPeerClient<SM>,
{
    // Wrap peer id generator / updater to proxy
    next_peer_id: u64,
    group: RawNode<L>,
    op_handler: OperationHandler<SM, D>,
    state: SM,
    client: Arc<C>,
    pub(crate) callbacks: CallbackHolder<SM>,
}

impl<SM, D, L, C> Drop for RaftNode<SM, D, L, C>
where
    SM: RaftStateMachine,
    D: OpDeserializer<SM>,
    L: RaftLog,
    C: RaftPeerClient<SM>,
{
    fn drop(&mut self) {
        self.op_handler.stop();
    }
}

impl<SM, D, L, C> RaftNode<SM, D, L, C>
where
    SM: RaftStateMachine,
    D: OpDeserializer<SM>,
    L: RaftLog,
    C: RaftPeerClient<SM>,
{
    pub fn new(state: SM, deserializer: D, storage: L, client: C) -> Result<Self, GenericError> {
        Self::with_config(state, deserializer, storage, client, Default::default())
    }

    pub fn with_config(
        state: SM,
        deserializer: D,
        storage: L,
        client: C,
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
            next_peer_id: 1,
            group,
            op_handler: OperationHandler::new(deserializer),
            state,
            client: Arc::new(client),
            callbacks: CallbackHolder::new(),
        };

        Ok(self_)
    }

    pub async fn start<S, H>(
        mut self,
        server_proxy: S,
        deserializer: D,
    ) -> Result<Arc<RaftNodeHandle<SM>>, GenericError>
    where
        H: RaftPeerServerHandle,
        S: RaftPeerServer<SM, D, H>,
    {
        let r_handle = Arc::new(self.op_handler.handle.take().expect(""));
        let s_handle = self
            .on_init(r_handle.clone(), deserializer, server_proxy)
            .await?;
        
        let runtime = Builder::new_current_thread()
            .thread_name("raft-worker")
            .enable_all()
            .build()?;
        thread::spawn(move || {
            runtime.block_on(async move {
                let mut async_executor = AsyncRunnableExecutor::<SM, D, L, C>::new();
                let tick_max_time = Duration::from_millis(100);
                loop {
                    async_executor.process(&mut self).await;
                    let (read_buffer, write_buffer, raft_msg_buffer) =
                        self.drain_ops(tick_max_time).await;

                    self.on_raft_command(&s_handle, raft_msg_buffer).await;
                    if self.op_handler.is_stopped() {
                        break;
                    }

                    self.on_read(read_buffer).await;
                    if let Some(fut) = self.on_write(write_buffer).await {
                        async_executor.add(fut);
                    }

                    self.tick().await;
                }
            });
        });

        Ok(r_handle)
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
        if entries.is_empty() {
            return;
        }

        let mut _last_apply_index = 0;
        for entry in entries {
            _last_apply_index = entry.index;

            if entry.data.is_empty() {
                continue;
            }

            match entry.get_entry_type() {
                EntryType::EntryNormal => {
                    let cxt_buffer = entry.context.as_ref();
                    if cxt_buffer.is_empty() {
                        println!("Ignoring entry with empty context buffer");
                        continue;
                    }

                    // Callbacks to notify that execution was end to the caller. If not exists messages must be rebuilt from serialized form.
                    let message_id = MessageId::from_be_bytes(
                        cxt_buffer
                            .try_into()
                            .expect("Slice should be exactly 16 bytes"),
                    );
                    if let Some(callback) = self.callbacks.remove(message_id) {
                        callback.op.execute(&mut self.state);
                        // If write remote we let a callback to avoid deserialization, but not must to be notified because was notified in proposing step
                        if !callback.forwarded {
                            let _ = callback.notifier.send(Ok(message_id)).await;
                        }
                    } else {
                        let operation: Box<dyn WriteOperation<SM>> =
                            self.op_handler.deserialize(entry.data.as_ref());
                        operation.execute(&mut self.state);
                    }
                }
                EntryType::EntryConfChangeV2 => {
                    let mut cc = ConfChangeV2::default();
                    cc.merge_from_bytes(&entry.data).unwrap();
                    let cs = self.group.apply_conf_change(&cc).unwrap();
                    self.group.store().set_conf_state(cs);

                    // At the moment only add command
                    let peer_node_id = cc.changes[0].node_id;
                    self.next_peer_id = peer_node_id;

                    let socket_addr: SocketAddr = String::from_utf8_lossy(cc.context.as_ref())
                        .as_ref()
                        .parse()
                        .expect("Invalid socket address");

                    // On join cluster always is added the leader, must to ensure that when removal or revoke the leader, updated what is the new leader
                    self.client.upsert_peer(peer_node_id, socket_addr, false);

                    // Get SocketAddr from context
                    // Notify event to message proxy to add or remove node.
                    // Update next peer Id to node_id received.
                    // Need to distinct if is add or removal.
                }
                _ => {}
            };
        }
    }

    // Cannot be delayed per iteration to avoid lost messages or network reording.
    // Must be ack'ed ordered.
    async fn handle_messages(&mut self, messages: Vec<Message>) {
        if messages.is_empty() {
            return;
        }

        let mut forward_messages = HashMap::new();
        for msg in messages {
            // My Own node id
            if msg.to == self.group.raft.id {
                if let Err(err) = self.group.step(msg) {
                    println!("Error: {err}");
                }
            } else if msg.from == self.group.raft.id {
                if !forward_messages.contains_key(&msg.to) {
                    forward_messages.insert(msg.to, Vec::new());
                }

                forward_messages.get_mut(&msg.to).unwrap().push(msg);
            }
        }

        if forward_messages.is_empty() {
            return;
        }

        let mut futures = Vec::new();
        for (peer_id, messages) in forward_messages {
            futures.push(self.client.message(peer_id, messages));
        }
        join_all(futures).await;
    }

    async fn on_init<S, H>(
        &mut self,
        handle: Arc<RaftNodeHandle<SM>>,
        deserializer: D,
        server_proxy: S,
    ) -> Result<H, GenericError>
    where
        H: RaftPeerServerHandle,
        S: RaftPeerServer<SM, D, H>,
    {
        let server_handle = server_proxy.listen(handle, deserializer)?;
        let result = self.client.join(server_handle.socket_addr()).await?;

        if let Some(RaftCommandResult::ClusterJoin {
            own_node_id,
            leader_node_id,
            leader_addr,
        }) = result
        {
            self.group.raft.id = own_node_id;
            self.client.upsert_peer(leader_node_id, leader_addr, true);
            println!("Successfully joined as follower");
        } else {
            println!("Successfully joined as master");
        }

        self.group.campaign()?;
        Ok(server_handle)
    }

    // Add if no Leader depends on configuration, read from master or not (Proxied)
    // Could be polled per each iteration without blocking
    async fn on_read(&self, r_buffer: Vec<ReadOpFn<SM>>) {
        if !r_buffer.is_empty() {
            let futures = r_buffer.into_iter().map(|op| op(&self.state));
            join_all(futures).await;
        }
    }

    // Internal Raft Commands, return when enqueued. Must return response
    async fn on_raft_command<H: RaftPeerServerHandle>(
        &mut self,
        server_handle: &H,
        mut r_buffer: Vec<AwaitableRaftCommand>,
    ) {
        for raft_cmd in r_buffer.drain(..) {
            match raft_cmd.command {
                // Received joint consensus over the network. If not leader reject.
                RaftCommand::ClusterJoin(addr) => {
                    if self.group.raft.state != StateRole::Leader {
                        // Error not leader
                        let _ = raft_cmd.notifier.send(Ok(RaftCommandResult::None));
                    } else {
                        // When removed?
                        // Assign next nodeId
                        self.next_peer_id += 1;

                        let mut cc = ConfChangeV2::default();
                        let mut change = ConfChangeSingle::new();
                        change.change_type = ConfChangeType::AddNode;
                        change.node_id = self.next_peer_id;
                        cc.changes = RepeatedField::from_vec(vec![change]);
                        // Check this
                        cc.context = addr.to_string().into();

                        // Notify event to message proxy to add or remove node will be done in handle_entries.
                        // message() method deprecated by this method. Raft Commands.
                        if let Err(err) = self.group.propose_conf_change(vec![], cc) {
                            let _ = raft_cmd.notifier.send(Err(err.into()));
                        } else {
                            // Add log
                            let result = RaftCommandResult::ClusterJoin {
                                own_node_id: self.next_peer_id,
                                leader_node_id: self.group.raft.id,
                                leader_addr: server_handle.socket_addr(),
                            };
                            let _ = raft_cmd.notifier.send(Ok(result));
                        }
                    }
                }
                // Received message over the network. Notify?
                RaftCommand::Raw(msg) => {
                    if let Err(err) = self.group.step(msg) {
                        let _ = raft_cmd.notifier.send(Err(err.into()));
                    }
                }
                RaftCommand::Stop => {
                    self.op_handler.stop();
                    server_handle.stop().await;
                    let _ = raft_cmd.notifier.send(Ok(RaftCommandResult::None));
                }
            }
        }
    }

    // Could be polled per each iteration without blocking
    async fn on_write(
        &mut self,
        mut write_buffer: Vec<AwaitableWriteOp<SM>>,
    ) -> Option<ForwardWrites<SM>> {
        if write_buffer.is_empty() {
            return None;
        }

        if self.group.raft.state != StateRole::Leader {
            let mut operation_bucket = OperationBucket::default();
            for awaitable_operation in write_buffer.iter() {
                let op_buffer = awaitable_operation.op.serialize();
                operation_bucket.add(op_buffer);
            }

            let client = self.client.clone();
            let fut = tokio::spawn(async move { client.write(operation_bucket).await });
            return Some(ForwardWrites::<SM>::new(write_buffer, fut));
        } else {
            for w_op in write_buffer.drain(..) {
                let op_buffer = w_op.op.serialize();
                let message_id = self.callbacks.next();
                let context = message_id.to_be_bytes().to_vec();
                if let Err(err) = self.group.propose(context, op_buffer) {
                    // Add log
                    println!("Error: {err}");
                    let _ = w_op.notifier.send(Err(err.into())).await;
                } else {
                    // If remote notify now. This no matter if Progress Tracker or not, in the moment we have proposed and with messageId, the remote call must return.
                    if w_op.forwarded == true {
                        let _ = w_op.notifier.send(Ok(message_id)).await;
                    }
                    // If go from remote request maintain callback in leader too to avoid one deserialization. But converge to avoid deadlock.
                    self.callbacks.add(message_id, w_op);
                }
            }
        }

        None
    }

    // Limit batch size of op's
    async fn drain_ops<'a>(
        &mut self,
        timeout: Duration,
    ) -> (
        Vec<ReadOpFn<SM>>,
        Vec<AwaitableWriteOp<SM>>,
        Vec<AwaitableRaftCommand>,
    ) {
        let max_drain = 1000;

        let mut read_buffer = Vec::new();
        let mut write_buffer = Vec::new();
        let mut raft_msg_buffer = Vec::new();
        let (read_mut, write_mut, raft_msg_mut) = self.op_handler.as_mut();
        tokio::select! {
            biased;
            _ = time::sleep(timeout) => {
                return (read_buffer, write_buffer, raft_msg_buffer);
            }
            // Read operation received
            Some(r_op) = read_mut.recv() => {
                read_buffer.push(r_op);
            }

            Some(raft_msg) = raft_msg_mut.recv() => {
                raft_msg_buffer.push(raft_msg);
            }

            // Write operation received
            Some(w_op) = write_mut.recv() => {
                write_buffer.push(w_op);
            }
            // Both channels closed
            else => {
                return (read_buffer, write_buffer, raft_msg_buffer);
            }
        }

        for _ in 0..max_drain {
            match self.op_handler.try_recv_read_op() {
                Ok(r_op) => read_buffer.push(r_op),
                Err(_) => break,
            }
        }

        for _ in 0..max_drain {
            match self.op_handler.try_recv_write_op() {
                Ok(w_op) => write_buffer.push(w_op),
                Err(_) => break,
            }
        }

        for _ in 0..max_drain {
            match self.op_handler.try_recv_raft_msg() {
                Ok(raft_msg) => raft_msg_buffer.push(raft_msg),
                Err(_) => break,
            }
        }

        (read_buffer, write_buffer, raft_msg_buffer)
    }
}
