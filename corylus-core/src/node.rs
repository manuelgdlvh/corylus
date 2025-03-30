use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::thread;
use std::time::{Duration, Instant};

use protobuf::Message as ProtobufMessage;
use raft::{RawNode, StateRole};
use raft::eraftpb::{ConfChangeV2, Entry, EntryType, Message, Snapshot};

use crate::message::{AwaitableMessage, MessageServer, MessageType, RemoteMessage};
use crate::network::NetworkClient;
use crate::raft_log::RaftLog;
use crate::state_machine::StateMachine;

pub struct Node<NC, RL, SM>
where
    NC: NetworkClient,
    RL: RaftLog,
    SM: StateMachine,
{
    raft_group: RawNode<RL>,
    network_client: NC,
    state_machine: SM,
    message_channel: Receiver<AwaitableMessage>,
}

impl<NC, RL, SM> Node<NC, RL, SM>
where
    NC: NetworkClient,
    RL: RaftLog,
    SM: StateMachine,
{
    fn build_leader(network_client: NC, mut raft_log: RL, state_machine: SM, message_channel: Receiver<AwaitableMessage>) -> Self {
        let mut s = Snapshot::default();
        s.mut_metadata().index = 1;
        s.mut_metadata().term = 1;
        s.mut_metadata().mut_conf_state().voters = vec![1];

        raft_log.apply_snapshot(s).unwrap();

        let mut raft_config: raft::Config = raft::Config::default();
        raft_config.id = 1;

        let raft_group = RawNode::with_default_logger(&raft_config, raft_log).expect("Raft node built successfully");

        Self {
            network_client,
            raft_group,
            message_channel,
            state_machine,
        }
    }

    fn build_follower(node_id: u64, network_client: NC, raft_log: RL, state_machine: SM, message_channel: Receiver<AwaitableMessage>) -> Self {
        let mut raft_config: raft::Config = raft::Config::default();
        raft_config.id = node_id;
        let raft_group = RawNode::with_default_logger(&raft_config, raft_log).expect("Raft node built successfully");

        Self {
            network_client,
            raft_group,
            message_channel,
            state_machine,
        }
    }

    pub async fn build<MS: MessageServer>(network_client: NC, message_server: MS, raft_log: RL, state_machine: SM) -> anyhow::Result<Self> {
        let tick_timeout = Duration::from_millis(100);
        let (msg_tx, msg_rx) = mpsc::sync_channel::<AwaitableMessage>(1024);

        tokio::spawn(async move {
            let _ = message_server.start(msg_tx).await;
            // Add signaling if stopped
        });

        let cluster_joint = network_client.discover_leader().await?;
        let node = match cluster_joint.own_node_id() {
            None => {
                let mut node = Self::build_leader(network_client, raft_log, state_machine, msg_rx);
                node.await_become_leader(tick_timeout).await;
                node
            }
            Some(node_id) => {
                Self::build_follower(node_id, network_client, raft_log, state_machine, msg_rx)
            }
        };

        Ok(node)
    }

    pub async fn start(mut self) -> anyhow::Result<()> {
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

        // async
        msg.notifier().send(result).unwrap();
    }

    async fn process(&mut self) {
        if !self.raft_group.has_ready() {
            return;
        }

        let mut ready = self.raft_group.ready();
        self.handle_messages(ready.take_messages()).await;
        if !ready.snapshot().is_empty() {
            self.raft_group.store().apply_snapshot(ready.snapshot().clone()).unwrap();
        }

        // To be persisted in store machine because were commited
        self.handle_entries(ready.take_committed_entries());

        // New entries to be appended to the log
        if !ready.entries().is_empty() {
            self.raft_group.store().append(ready.entries()).unwrap();
        }

        if let Some(hs) = ready.hs() {
            self.raft_group.store().set_hard_state(hs.clone());
        }

        self.handle_messages(ready.take_persisted_messages()).await;

        let mut light_rd = self.raft_group.advance(ready);
        self.handle_messages(light_rd.take_messages()).await;
        self.handle_entries(light_rd.take_committed_entries());
        self.raft_group.advance_apply();

        // Check signals
    }

    async fn await_become_leader(&mut self, tick_timeout: Duration) {
        while self.raft_group.raft.state != StateRole::Leader {
            self.process().await;
            self.raft_group.tick();
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
                    self.raft_group.store().set_conf_state(cs);
                }
                _ => {}
            };
        }
    }

    async fn handle_messages(&self, messages: Vec<Message>) {
        println!("Messages: {messages:?}");

        for msg in messages {
            let result = self.network_client.send(msg.to, RemoteMessage::raw_message(msg)).await;
            if result.is_err() {
                // Do stuff
            }
        }
    }
}


