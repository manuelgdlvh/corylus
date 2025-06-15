use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct MessagesRequest {
    pub(crate) data: Vec<Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
pub struct WritesRequest {
    pub(crate) data: Vec<Vec<u8>>,
}
#[derive(Serialize, Deserialize)]
pub struct ClusterJoinRequest {
    pub(crate) socket_addr: String,
}
