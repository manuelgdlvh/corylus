use crate::callback::CallbackHolder;
use crate::handle::{AwaitableWriteOp, RaftNodeHandle, ReadOpFn};
use crate::operation::{AwaitableRaftCommand, RaftCommand, RaftCommandResult, WriteOperation};
use crate::peer::{
    MessageId, OpDeserializer, RaftPeerClientProxy, RaftPeerServerProxy, ServerHandle,
};
use crate::raft_log::RaftLog;
use crate::state_machine::RaftStateMachine;
use futures::future::join_all;
use protobuf::{Message as ProtobufMessage, RepeatedField};
use raft::eraftpb::{ConfChangeSingle, ConfChangeType};
use raft::prelude::{ConfChangeV2, Entry, EntryType, Message};
use raft::{Config, RawNode, StateRole, Storage};
use std::collections::HashMap;
use std::error::Error;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::time;

pub type GenericError = Box<dyn Error + Send + Sync>;
// As trait
struct OperationHandler<S, D>
where
    S: RaftStateMachine,
    D: OpDeserializer<S>,
{
    r_rx: mpsc::UnboundedReceiver<ReadOpFn<S>>,
    w_rx: mpsc::UnboundedReceiver<AwaitableWriteOp<S>>,
    raft_msg_rx: mpsc::UnboundedReceiver<AwaitableRaftCommand>,
    deserializer: D,
    handle: Option<RaftNodeHandle<S>>,
}

impl<S, D> OperationHandler<S, D>
where
    S: RaftStateMachine,
    D: OpDeserializer<S>,
{
    fn new(deserializer: D) -> Self {
        let (r_tx, r_rx) = mpsc::unbounded_channel();
        let (w_tx, w_rx) = mpsc::unbounded_channel();
        let (raft_msg_tx, raft_msg_rx) = mpsc::unbounded_channel();

        Self {
            r_rx,
            w_rx,
            raft_msg_rx,
            deserializer,
            handle: Some(RaftNodeHandle::new(r_tx, w_tx, raft_msg_tx)),
        }
    }

    fn as_mut(
        &mut self,
    ) -> (
        &mut mpsc::UnboundedReceiver<ReadOpFn<S>>,
        &mut mpsc::UnboundedReceiver<AwaitableWriteOp<S>>,
        &mut mpsc::UnboundedReceiver<AwaitableRaftCommand>,
    ) {
        (&mut self.r_rx, &mut self.w_rx, &mut self.raft_msg_rx)
    }

    fn try_recv_write_op(&mut self) -> Result<AwaitableWriteOp<S>, TryRecvError> {
        self.w_rx.try_recv()
    }

    fn try_recv_read_op(&mut self) -> Result<ReadOpFn<S>, TryRecvError> {
        self.r_rx.try_recv()
    }

    fn try_recv_raft_msg(&mut self) -> Result<AwaitableRaftCommand, TryRecvError> {
        self.raft_msg_rx.try_recv()
    }

    fn stop(&mut self) {
        self.r_rx.close();
        self.w_rx.close();
    }

    fn is_stopped(&self) -> bool {
        self.r_rx.is_closed() || self.w_rx.is_closed()
    }
}

pub struct RaftNode<SM, D, L, C>
where
    SM: RaftStateMachine,
    D: OpDeserializer<SM>,
    L: RaftLog,
    C: RaftPeerClientProxy<SM>,
{
    // Wrap peer id generator / updater to proxy
    next_peer_id: u64,
    group: RawNode<L>,
    op_handler: OperationHandler<SM, D>,
    state: SM,
    client_peer_proxy: C,
    callbacks: CallbackHolder<SM>,
}

impl<SM, D, L, C> Drop for RaftNode<SM, D, L, C>
where
    SM: RaftStateMachine,
    D: OpDeserializer<SM>,
    L: RaftLog,
    C: RaftPeerClientProxy<SM>,
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
    C: RaftPeerClientProxy<SM>,
{
    pub fn new(
        state: SM,
        deserializer: D,
        storage: L,
        client_peer_proxy: C,
    ) -> Result<Self, GenericError> {
        Self::with_config(
            state,
            deserializer,
            storage,
            client_peer_proxy,
            Default::default(),
        )
    }

    pub fn with_config(
        state: SM,
        deserializer: D,
        storage: L,
        client_peer_proxy: C,
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
            client_peer_proxy,
            callbacks: CallbackHolder::new(),
        };

        Ok(self_)
    }

    pub fn start<S, H>(
        mut self,
        server_proxy: S,
        deserializer: D,
    ) -> Result<Arc<RaftNodeHandle<SM>>, GenericError>
    where
        H: ServerHandle,
        S: RaftPeerServerProxy<SM, D, H>,
    {
        let runtime = Builder::new_current_thread()
            .thread_name("raft-worker")
            .enable_all()
            .build()
            .unwrap();
        let handle = Arc::new(self.op_handler.handle.take().unwrap());
        let (start_signal_tx, start_signal_rx) =
            std::sync::mpsc::sync_channel::<Result<(), GenericError>>(1);
        {
            let handle = Arc::clone(&handle);
            thread::spawn(move || {
                let handle = Arc::clone(&handle);
                runtime.block_on(async move {
                    let server_handle = match self.on_init(handle, deserializer, server_proxy).await
                    {
                        Ok(server_handle) => {
                            start_signal_tx.send(Ok(())).unwrap();
                            server_handle
                        }
                        Err(err) => {
                            start_signal_tx.send(Err(err)).unwrap();
                            return;
                        }
                    };

                    let timeout = Duration::from_millis(100);
                    let mut w_buffer = Vec::new();
                    let mut raft_command_buffer = Vec::new();

                    loop {
                        let mut r_buffer = Vec::new();
                        let (new_w_buffer, new_r_buffer, new_raft_command_buffer) = self
                            .drain_ops(w_buffer, r_buffer, raft_command_buffer, timeout)
                            .await;

                        w_buffer = new_w_buffer;
                        r_buffer = new_r_buffer;
                        raft_command_buffer = new_raft_command_buffer;

                        self.on_raft_command(&mut raft_command_buffer).await;
                        if self.op_handler.is_stopped() {
                            server_handle.stop().await;
                            break;
                        }

                        self.on_read(r_buffer).await;
                        self.on_write(&mut w_buffer).await;
                        self.tick().await;
                    }
                });
            });
        }

        match start_signal_rx.recv().unwrap() {
            Ok(_) => Ok(handle),
            Err(err) => Err(err),
        }
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
                    let buffer = entry.context.as_ref();
                    if buffer.is_empty() {
                        println!("Ignoring entry with empty context buffer");
                        continue;
                    }
                    let buffer: [u8; 16] =
                        buffer.try_into().expect("Slice should be exactly 16 bytes");
                    let message_id = MessageId::from_be_bytes(buffer);

                    // Callbacks to notify that execution was end to the caller. If not exists messages must be rebuilt from serialized form.

                    if let Some(callback) = self.callbacks.remove(message_id) {
                        callback.op.execute(&mut self.state);
                        // If write remote we let a callback to avoid deserialization, but not must to be notified because was notified in proposing step
                        if !callback.remote {
                            let _ = callback.notifier.send(Ok(message_id)).await;
                        }
                    } else {
                        let op_buffer = entry.data.as_ref();
                        let op: Box<dyn WriteOperation<SM>> =
                            self.op_handler.deserializer.deserialize(op_buffer);
                        op.execute(&mut self.state);
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

                    let context = cc.context.as_ref();

                    let addr: SocketAddr = String::from_utf8_lossy(context)
                        .as_ref()
                        .parse()
                        .expect("Invalid socket address");
                    self.client_peer_proxy
                        .upsert_peer(peer_node_id, addr, false);

                    println!("Adding {} node id with {} addr", peer_node_id, addr);

                    // Get SocketAddr from context

                    // Notify event to message proxy to add or remove node.
                    // Update next peer Id to node_id received.
                    // Need to distinct if is add or removal.
                }
                _ => {}
            };
        }
    }

    async fn handle_messages(&mut self, messages: Vec<Message>) {
        if messages.is_empty() {
            return;
        }

        let mut pushable_messages = HashMap::new();
        for msg in messages {
            // My Own node id
            if msg.to == self.group.raft.id {
                if let Err(err) = self.group.step(msg) {
                    println!("Error: {err}");
                }
            } else if msg.from == self.group.raft.id {
                if !pushable_messages.contains_key(&msg.to) {
                    pushable_messages.insert(msg.to, Vec::new());
                }

                pushable_messages.get_mut(&msg.to).unwrap().push(msg);
            }
        }

        if !pushable_messages.is_empty() {
            if let Err(err) = self.client_peer_proxy.message(pushable_messages).await {
                println!("Error: {err}");
            }
        }
    }

    async fn on_init<S, H>(
        &mut self,
        handle: Arc<RaftNodeHandle<SM>>,
        deserializer: D,
        server_proxy: S,
    ) -> Result<H, GenericError>
    where
        H: ServerHandle,
        S: RaftPeerServerProxy<SM, D, H>,
    {
        let server_handle = server_proxy.listen(handle, deserializer)?;
        let result = self
            .client_peer_proxy
            .join(server_handle.socket_addr())
            .await?;

        if let Some(RaftCommandResult::ClusterJoin(node_id)) = result {
            self.group.raft.id = node_id;

            // Check this must to be returned by the leader
            self.client_peer_proxy.upsert_peer(
                1,
                SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 8080)),
                true,
            );

            println!("Successfully joined with {} id", node_id);
        }

        self.group.campaign()?;
        println!("Successfully joined as master");
        Ok(server_handle)
    }

    // Add if no Leader depends on configuration, read from master or not (Proxied)
    async fn on_read(&self, r_buffer: Vec<ReadOpFn<SM>>) {
        if !r_buffer.is_empty() {
            let futures = r_buffer.into_iter().map(|op| op(&self.state));
            join_all(futures).await;
        }
    }

    // Internal Raft Commands, return when enqueued. Must return response
    async fn on_raft_command(&mut self, r_buffer: &mut Vec<AwaitableRaftCommand>) {
        for command in r_buffer.drain(..) {
            match command.command {
                // Received joint consensus over the network. If not leader reject.
                RaftCommand::ClusterJoin(addr) => {
                    if self.group.raft.state != StateRole::Leader {
                        // Error not leader
                        let _ = command.notifier.send(Ok(RaftCommandResult::None));
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
                            let _ = command.notifier.send(Err(err.into()));
                        } else {
                            // Add log
                            let _ = command
                                .notifier
                                .send(Ok(RaftCommandResult::ClusterJoin(self.next_peer_id)));
                        }
                    }
                }
                // Received message over the network. Notify?
                RaftCommand::Raw(msg) => {
                    if let Err(err) = self.group.step(msg) {
                        let _ = command.notifier.send(Err(err.into()));
                    }
                }
                RaftCommand::Stop => {
                    self.op_handler.stop();
                }
            }
        }
    }

    // Generalize this to handle other operations like EntryConfChange?. propose_conf_change. Two more channels for messaging and conf change. Wrap this two in one channel.
    // Idempotence must to be handle at implementation level.
    // Deadlock of peer waiting leader, and leader waiting peer.
    async fn on_write(&mut self, w_buffer: &mut Vec<AwaitableWriteOp<SM>>) {
        if w_buffer.is_empty() {
            return;
        }

        if self.group.raft.state != StateRole::Leader {
            println!("Node is not leader. Sending writes over the network");
            let mut ops_buffer = Vec::new();
            // Avoid networking
            for w_op in w_buffer.iter() {
                let op_buffer = w_op.op.serialize();
                ops_buffer.push(op_buffer);
            }

            // Must return message id.
            // Allow batching.
            // Track Rejections to remove dirty callbacks.
            match self.client_peer_proxy.write(ops_buffer).await {
                Ok(mut results) => {
                    let mut updated = false;

                    for (idx, w_op) in w_buffer.drain(..).enumerate().rev() {
                        let message_id = results.swap_remove(idx);

                        // To avoid loss of callbacks if reelection.
                        if !updated {
                            self.callbacks.update_next(message_id);
                            updated = true;
                        }

                        self.callbacks.add(message_id, w_op);
                    }
                }
                Err(err) => {
                    for w_op in w_buffer.drain(..) {
                        let _ = w_op.notifier.send(Err(err.to_string().into())).await;
                    }
                }
            };
        } else {
            for w_op in w_buffer.drain(..) {
                let op_buffer = w_op.op.serialize();
                let message_id = self.callbacks.next();
                let context = message_id.to_be_bytes().to_vec();
                if let Err(err) = self.group.propose(context, op_buffer) {
                    // Add log
                    println!("Error: {err}");
                    let _ = w_op.notifier.send(Err(err.into())).await;
                } else {
                    // If remote notify now. This no matter if Progress Tracker or not, in the moment we have proposed and with messageId, the remote call must return.
                    if w_op.remote == true {
                        let _ = w_op.notifier.send(Ok(message_id)).await;
                    }
                    // If go from remote request maintain callback in leader too to avoid one deserialization. But converge to avoid deadlock.
                    self.callbacks.add(message_id, w_op);
                }
            }
        }
    }

    // Limit batch size of op's
    async fn drain_ops<'a>(
        &mut self,
        mut w_buffer: Vec<AwaitableWriteOp<SM>>,
        mut r_buffer: Vec<ReadOpFn<SM>>,
        mut raft_msg_buffer: Vec<AwaitableRaftCommand>,
        timeout: Duration,
    ) -> (
        Vec<AwaitableWriteOp<SM>>,
        Vec<ReadOpFn<SM>>,
        Vec<AwaitableRaftCommand>,
    ) {
        let max_drain = 256;
        let (read_mut, write_mut, raft_msg_mut) = self.op_handler.as_mut();
        tokio::select! {
            biased;
            _ = time::sleep(timeout) => {
                return (w_buffer, r_buffer, raft_msg_buffer);
            }
            // Read operation received
            Some(r_op) = read_mut.recv() => {
                r_buffer.push(r_op);
            }

            Some(raft_msg) = raft_msg_mut.recv() => {
                raft_msg_buffer.push(raft_msg);
            }

            // Write operation received
            Some(w_op) = write_mut.recv() => {
                w_buffer.push(w_op);
            }
            // Both channels closed
            else => {
                return (w_buffer, r_buffer, raft_msg_buffer);
            }
        }

        for _ in 0..max_drain {
            match self.op_handler.try_recv_read_op() {
                Ok(r_op) => r_buffer.push(r_op),
                Err(_) => break,
            }
        }

        for _ in 0..max_drain {
            match self.op_handler.try_recv_write_op() {
                Ok(w_op) => w_buffer.push(w_op),
                Err(_) => break,
            }
        }

        for _ in 0..max_drain {
            match self.op_handler.try_recv_raft_msg() {
                Ok(raft_msg) => raft_msg_buffer.push(raft_msg),
                Err(_) => break,
            }
        }
        (w_buffer, r_buffer, raft_msg_buffer)
    }
}
