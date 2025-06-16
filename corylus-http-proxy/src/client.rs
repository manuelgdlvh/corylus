use crate::request::{ClusterJoinRequest, MessagesRequest, WritesRequest};
use crate::response::{ClusterJoinResponse, WritesResponse};
use corylus_core::node::GenericError;
use corylus_core::operation::RaftCommandResult;
use corylus_core::peer::{MessageId, OperationBucket, PeerId, RaftPeerClient};
use corylus_core::state_machine::RaftStateMachine;
use dashmap::{DashMap, DashSet};
use protobuf::Message as ProtobufMessage;
use raft::eraftpb::Message;
use reqwest::Client;
use std::net::SocketAddr;

#[derive(Default)]
pub struct ReqwestHttpClient {
    peers: DashMap<PeerId, SocketAddr>,
    leader_id: DashSet<PeerId>,
}

impl<SM> RaftPeerClient<SM> for ReqwestHttpClient
where
    SM: RaftStateMachine,
{
    async fn message(
        &self,
        peer_id: PeerId,
        messages: Vec<Message>,
    ) -> Result<RaftCommandResult, GenericError> {
        let client = Client::new();
        if let Some(addr) = self.peers.get(&peer_id) {
            let url = format!("http://{}/messages", addr.value());

            let mut msgs_buffer = Vec::new();
            for msg in messages {
                let msg_buffer = msg.write_to_bytes().unwrap();
                msgs_buffer.push(msg_buffer);
            }

            let request = MessagesRequest { data: msgs_buffer };
            let _ = client.post(&url).json(&request).send().await;
        } else {
            println!("Peer not found in map");
        }
        Ok(RaftCommandResult::None)
    }

    // Split logic by move to inner implementation
    // Logic to find (Must be changed). Localhost
    async fn join(
        &self,
        socket_addr: SocketAddr,
    ) -> Result<Option<RaftCommandResult>, GenericError> {
        let client = Client::new();

        let join_request = ClusterJoinRequest {
            socket_addr: socket_addr.to_string(),
        };

        for port in 8080..=8082 {
            if socket_addr.port() == port {
                continue;
            }

            let url = format!("http://localhost:{port}/join");

            let response = match client.post(&url).json(&join_request).send().await {
                Ok(resp) => resp,
                Err(_) => continue,
            };

            if !response.status().is_success() {
                continue;
            }

            match response.json::<ClusterJoinResponse>().await {
                Ok(response) => {
                    let result = RaftCommandResult::ClusterJoin {
                        own_node_id: response.own_node_id,
                        leader_node_id: response.leader_node_id,
                        leader_addr: response.leader_addr,
                    };

                    return Ok(Some(result));
                }
                Err(_) => continue,
            }
        }
        Ok(None)
    }

    // Decouple this peer handling for client proxy implementation. Always must be the same. PeerHandler
    fn upsert_peer(&self, node_id: u64, addr: SocketAddr, is_leader: bool) {
        self.peers.insert(node_id, addr);
        if is_leader {
            self.leader_id.clear();
            self.leader_id.insert(node_id);
        }
    }

    async fn write(&self, mut bucket: OperationBucket) -> Result<Vec<MessageId>, GenericError> {
        let leader_id_ref = &self.leader_id.iter().find(|_| true).unwrap();

        if let Some(addr) = self.peers.get(&leader_id_ref) {
            let client = Client::new();
            let url = format!("http://{}/write", addr.value());

            let request = WritesRequest {
                data: bucket.take_messages(),
            };
            let response = client.post(&url).json(&request).send().await?;

            if !response.status().is_success() {
                return Err("response is not success".into());
            }

            match response.json::<WritesResponse>().await {
                Ok(result) => Ok(result.message_ids),
                Err(err) => Err(err.into()),
            }
        } else {
            println!("Leader Peer not found in map");
            Err("Leader peer does not exist".into())
        }
    }
}
