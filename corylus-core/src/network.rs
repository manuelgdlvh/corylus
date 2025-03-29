use std::sync::mpsc::SyncSender;

use crate::message::{AwaitableMessage, RemoteMessage};

pub trait NetworkHandle {
    async fn start_server(&self, tx: SyncSender<AwaitableMessage>) -> anyhow::Result<()>;
    async fn discover_leader(&self) -> anyhow::Result<ClusterJoint>;
    async fn send(&self, node_id: u64, request: RemoteMessage) -> anyhow::Result<()>;
}


pub struct ClusterJoint {
    own_node_id: Option<u64>,
}

impl ClusterJoint {
    pub fn own_node_id(&self) -> Option<u64> {
        self.own_node_id
    }
}