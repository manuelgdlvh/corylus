use corylus_core::operation::NodeId;
use corylus_core::peer::MessageId;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct ClusterJoinResponse {
    pub(crate) node_id: NodeId,
}

#[derive(Serialize, Deserialize)]
pub struct WritesResponse {
    pub(crate) message_ids: Vec<MessageId>,
}
