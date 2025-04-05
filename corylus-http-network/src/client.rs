use std::collections::HashMap;
use std::net::SocketAddr;

use ipnetwork::IpNetwork;
use reqwest::Client;

use corylus_core::message::{MessageType, RemoteMessage};
use corylus_core::network::{ClusterJoin, NetworkClient};

use crate::config::NetworkConfig;
use crate::server::{EXECUTE_PATH, InputMessage, InputMessageType};

pub struct HttpClient {
    config: NetworkConfig,
    peers: HashMap<u64, SocketAddr>,
    listening_addr: SocketAddr,
}

impl HttpClient {
    pub fn new(config: NetworkConfig, listening_addr: SocketAddr) -> Self {
        Self {
            config,
            peers: Default::default(),
            listening_addr,
        }
    }
}

impl NetworkClient for HttpClient {
    async fn join(&self) -> anyhow::Result<ClusterJoin> {
        let network = IpNetwork::new(self.config.net_interface(), self.config.mask())?;
        let first_port = self.config.port();
        let last_port = self.config.port() + self.config.port_count();

        for ip in network.iter() {
            for port in first_port..=last_port {
                let current_addr = SocketAddr::new(ip, port);
                if self.listening_addr.eq(&current_addr) {
                    continue;
                }
            }
        }

        Ok(ClusterJoin::none())
    }
    async fn send_to_peer(&self, node_id: u64, message: RemoteMessage) -> anyhow::Result<()> {
        if let Some(peer) = self.peers.get(&node_id) {
            return self.send(*peer, message).await;
        }
        Ok(())
    }

    async fn send(&self, socket_addr: SocketAddr, message: RemoteMessage) -> anyhow::Result<()> {
        let client = Client::new();

        let msg_type = match message.type_() {
            MessageType::RawMessage => InputMessageType::RawMessage,
            MessageType::Proposal => InputMessageType::Proposal,
            MessageType::ConfChange => InputMessageType::ConfChange,
        };

        let input_message = InputMessage::new(msg_type, message.take_data());
        let response = client
            .post(format!("http://{}/{}", socket_addr, EXECUTE_PATH))
            .json(&input_message)
            .send()
            .await?;

        // Return error
        if response.status().is_success() {
            return Ok(());
        }

        Ok(())
    }

    fn add_peer(&mut self, id: u64, socket_addr: SocketAddr) {
        self.peers.insert(id, socket_addr);
    }
    fn remove_peer(&mut self, id: u64) {
        self.peers.remove(&id);
    }
}
