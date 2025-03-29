use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::thread;
use std::time::{Duration, Instant};

use protobuf::Message as ProtobufMessage;
use raft::{RawNode, StateRole};
use raft::eraftpb::{ConfChangeV2, Entry, EntryType, Message, Snapshot};
use raft::storage::MemStorage;

use crate::message::{AwaitableMessage, MessageType, RemoteMessage};
use crate::network::NetworkHandle;

pub struct Node<N>
where
    N: NetworkHandle,
{
    raft_group: RawNode<MemStorage>,
    network_handle: N,
    message_channel: Receiver<AwaitableMessage>,
}

impl<N> Node<N>
where
    N: NetworkHandle,
{
    fn build_leader(network_handle: N, message_channel: Receiver<AwaitableMessage>) -> Self {
        let mut s = Snapshot::default();
        s.mut_metadata().index = 1;
        s.mut_metadata().term = 1;
        s.mut_metadata().mut_conf_state().voters = vec![1];

        let storage = MemStorage::new();
        storage.wl().apply_snapshot(s).unwrap();

        let mut raft_config: raft::Config = raft::Config::default();
        raft_config.id = 1;
        let raft_group = RawNode::with_default_logger(&raft_config, storage).expect("Raft node built successfully");

        Self {
            network_handle,
            raft_group,
            message_channel,
        }
    }

    fn build_follower(node_id: u64, network_handle: N, message_channel: Receiver<AwaitableMessage>) -> Self {
        let storage = MemStorage::new();
        let mut raft_config: raft::Config = raft::Config::default();
        raft_config.id = node_id;
        let raft_group = RawNode::with_default_logger(&raft_config, storage).expect("Raft node built successfully");

        Self {
            network_handle,
            raft_group,
            message_channel,
        }
    }

    pub async fn build(network_handle: N) -> anyhow::Result<Self> {
        let tick_timeout = Duration::from_millis(100);
        let (msg_tx, msg_rx) = mpsc::sync_channel::<AwaitableMessage>(1024);

        network_handle.start_server(msg_tx).await?;
        let cluster_joint = network_handle.discover_leader().await?;
        let node = match cluster_joint.own_node_id() {
            None => {
                let mut node = Self::build_leader(network_handle, msg_rx);
                node.await_become_leader(tick_timeout).await;
                node
            }
            Some(node_id) => {
                Self::build_follower(node_id, network_handle, msg_rx)
            }
        };

        Ok(node)
    }

    pub async fn start(mut self) {
        let mut tick_timeout = Duration::from_millis(100);
        // Callbacks
        loop {
            let start = Instant::now();
            if let Ok(msg) = self.message_channel.recv_timeout(tick_timeout) {
                self.handle_msg(msg);
            }

            self.process().await;

            let elapsed = start.elapsed();
            if elapsed < tick_timeout {
                tick_timeout -= elapsed;
            } else {
                tick_timeout = Duration::from_millis(100);
                self.raft_group.tick();
                println!("I am {:?}", self.raft_group.raft.state);
            }
        }
    }


    fn handle_msg(&mut self, msg: AwaitableMessage) {
        let result = match msg.message().type_() {
            MessageType::RawMessage => {
                let mut message = Message::new();
                message.merge_from_bytes(msg.message().data()).unwrap();
                self.raft_group.raft.step(message).unwrap();
                true
            }
            MessageType::Proposal => {
                if self.raft_group.raft.state == StateRole::Leader {
                    self.raft_group.propose(vec![], msg.message().data().to_vec()).unwrap();
                }
                true
            }
            MessageType::ConfChange => {
                let mut config_change = ConfChangeV2::new();
                config_change.merge_from_bytes(msg.message().data()).unwrap();
                self.raft_group.propose_conf_change(vec![], config_change).unwrap();
                true
            }
        };

        msg.notifier().send(result).unwrap();
    }

    async fn process(&mut self) {
        if !self.raft_group.has_ready() {
            return;
        }

        let mut ready = self.raft_group.ready();
        self.handle_messages(ready.take_messages()).await;
        if !ready.snapshot().is_empty() {
            self.raft_group.store().wl()
                .apply_snapshot(ready.snapshot().clone())
                .unwrap();
        }

        // To be persisted in store machine because were commited
        self.handle_entries(ready.take_committed_entries());

        // New entries to be appended to the log
        if !ready.entries().is_empty() {
            self.raft_group.store().wl().append(ready.entries()).unwrap();
        }

        if let Some(hs) = ready.hs() {
            self.raft_group.store().wl().set_hardstate(hs.clone());
        }

        self.handle_messages(ready.take_persisted_messages()).await;

        let mut light_rd = self.raft_group.advance(ready);
        self.handle_messages(light_rd.take_messages()).await;
        self.handle_entries(light_rd.take_committed_entries());
        self.raft_group.advance_apply();
    }

    async fn await_become_leader(&mut self, tick_timeout: Duration) {
        loop {
            self.process().await;
            self.raft_group.tick();
            if self.raft_group.raft.state == StateRole::Leader {
                println!("Node became leader!");
                break;
            }

            thread::park_timeout(tick_timeout);
        }
    }

    fn handle_entries(&mut self, entries: Vec<Entry>) {
        println!("Entries: {entries:?}");

        let mut _last_apply_index = 0;
        for entry in entries {
            _last_apply_index = entry.index;

            if entry.data.is_empty() {
                continue;
            }

            match entry.get_entry_type() {
                EntryType::EntryNormal => {}
                EntryType::EntryConfChangeV2 => {
                    let mut cc = ConfChangeV2::default();
                    cc.merge_from_bytes(&entry.data).unwrap();
                    let cs = self.raft_group.apply_conf_change(&cc).unwrap();
                    self.raft_group.store().wl().set_conf_state(cs);
                }
                _ => {}
            };
        }
    }

    async fn handle_messages(&self, messages: Vec<Message>) {
        println!("Messages: {messages:?}");

        for msg in messages {
            let result = self.network_handle.send(msg.to, RemoteMessage::raw_message(msg)).await;
            if result.is_err() {
                // Do stuff
            }
        }
    }
}


