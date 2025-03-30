use std::sync::mpsc;
use std::sync::mpsc::{Receiver, SyncSender};

use protobuf::Message as ProtobufMessage;
use raft::eraftpb::Message;

pub trait MessageServer: Send + 'static {
    fn start(self, tx: SyncSender<AwaitableMessage>) -> impl Future<Output=anyhow::Result<()>> + Send;
}

pub struct RemoteMessage {
    type_: MessageType,
    data: Vec<u8>,
}

impl RemoteMessage {
    pub fn type_(&self) -> &MessageType {
        &self.type_
    }

    pub fn data(&self) -> &Vec<u8> {
        &self.data
    }
}

impl RemoteMessage
{
    pub fn raw_message(message: Message) -> Self {
        Self {
            type_: MessageType::RawMessage,
            data: message.write_to_bytes().unwrap(),
        }
    }
}

pub enum MessageType {
    RawMessage,
    Proposal,
    ConfChange,
}

pub struct AwaitableMessage {
    message: RemoteMessage,
    notifier: SyncSender<bool>,
}

impl AwaitableMessage {
    pub fn message(&self) -> &RemoteMessage {
        &self.message
    }

    pub fn notifier(&self) -> &SyncSender<bool> {
        &self.notifier
    }

    pub fn new(message: RemoteMessage) -> (Self, Receiver<bool>) {
        let (tx, rx) = mpsc::sync_channel(0);
        let self_ = Self { message, notifier: tx };
        (self_, rx)
    }
}
