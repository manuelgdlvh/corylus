use crate::request::{ClusterJoinRequest, MessagesRequest, WritesRequest};
use crate::response::{ClusterJoinResponse, WritesResponse};
use corylus_core::node::GenericError;
use corylus_core::operation::{RaftCommand, RaftCommandResult};
use corylus_core::peer::{MessageId, PeerId, RaftPeerClientProxy};
use corylus_core::state_machine::RaftStateMachine;
use protobuf::Message as ProtobufMessage;
use raft::eraftpb::Message;
use reqwest::Client;
use std::collections::HashMap;
use std::net::SocketAddr;

#[derive(Default)]
pub struct ReqwestHttpClient {
    peers: HashMap<PeerId, SocketAddr>,
    leader_peer_id: PeerId,
}

impl<SM> RaftPeerClientProxy<SM> for ReqwestHttpClient
where
    SM: RaftStateMachine,
{
    async fn message(
        &mut self,
        messages: HashMap<PeerId, Vec<Message>>,
    ) -> Result<RaftCommandResult, GenericError> {
        let client = Client::new();
        for (peer_id, messages) in messages {
            if let Some(addr) = self.peers.get(&peer_id) {
                let url = format!("http://{addr}/messages");

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
        }

        Ok(RaftCommandResult::None)
    }

    // Split logic by move to inner implementation
    // Logic to find (Must be changed). Localhost
    async fn join(
        &mut self,
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
                Ok(result) => {
                    return Ok(Some(RaftCommandResult::ClusterJoin(result.node_id)));
                }
                Err(_) => continue,
            }
        }
        Ok(None)
    }

    async fn write(&mut self, writes: Vec<Vec<u8>>) -> Result<Vec<MessageId>, GenericError> {
        if let Some(addr) = self.peers.get(&self.leader_peer_id) {
            let client = Client::new();
            let url = format!("http://{addr}/write");

            let request = WritesRequest { data: writes };
            let response = client.post(&url).json(&request).send().await?;

            if !response.status().is_success() {
                return Err("response is not success".into());
            }

            match response.json::<WritesResponse>().await {
                Ok(result) => {
                    Ok(result.message_ids)
                }
                Err(err) => Err(err.into()),
            }
        } else {
            println!("Leader Peer not found in map");
            Err("Leader peer does not exist".into())
        }
    }

    // Decouple this peer handling for client proxy implementation. Always must be the same. PeerHandler
    fn upsert_peer(&mut self, node_id: u64, addr: SocketAddr, is_leader: bool) {
        self.peers.insert(node_id, addr);
        // Change this
        if node_id == 1 {
            self.leader_peer_id = node_id;
        }
    }

    fn generate_peer_id(&mut self) -> PeerId {
        todo!()
    }
}
