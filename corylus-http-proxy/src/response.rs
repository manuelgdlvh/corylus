use corylus_core::operation::NodeId;
use corylus_core::peer::MessageId;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Serialize, Deserialize)]
pub struct ClusterJoinResponse {
    pub(crate) own_node_id: NodeId,
    pub(crate) leader_node_id: NodeId,
    pub(crate) leader_addr: SocketAddr,
}

#[derive(Serialize, Deserialize)]
pub struct WritesResponse {
    pub(crate) message_ids: Vec<MessageId>,
}
