use std::net::SocketAddr;

use crate::message::RemoteMessage;

pub trait NetworkClient {
    async fn join(&self) -> anyhow::Result<ClusterJoin>;
    async fn send_to_peer(&self, node_id: u64, message: RemoteMessage) -> anyhow::Result<()>;
    async fn send(&self, socket_addr: SocketAddr, message: RemoteMessage) -> anyhow::Result<()>;
    fn add_peer(&mut self, id: u64, socket_addr: SocketAddr);
    fn remove_peer(&mut self, id: u64);
}

pub struct ClusterJoin {
    assigned_id: Option<u64>,
}

impl ClusterJoin {
    pub fn assigned_id(&self) -> Option<u64> {
        self.assigned_id
    }

    pub fn with(node_id: u64) -> Self {
        Self {
            assigned_id: Some(node_id),
        }
    }

    pub fn none() -> Self {
        Self { assigned_id: None }
    }
}
