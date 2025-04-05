use std::net::SocketAddr;
use std::sync::mpsc::RecvError;
use std::sync::mpsc::SyncSender;

use anyhow::bail;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::serve::Listener;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

use corylus_core::message::{AwaitableMessage, MessageServer, MessageType, RemoteMessage};
use corylus_core::network::NetworkClient;

use crate::client::HttpClient;
use crate::config::NetworkConfig;

pub const EXECUTE_PATH: &str = "/execute";
pub struct HttpMessageServer {
    config: NetworkConfig,
    tcp_listener: TcpListener,
}
impl HttpMessageServer {
    pub async fn execute(
        State(channel): State<SyncSender<AwaitableMessage>>,
        Json(input_msg): Json<InputMessage>,
    ) -> StatusCode {
        let msg_type = match input_msg.type_ {
            InputMessageType::RawMessage => MessageType::RawMessage,
            InputMessageType::Proposal => MessageType::Proposal,
            InputMessageType::ConfChange => MessageType::ConfChange,
        };

        let awaitable_msg = AwaitableMessage::new(RemoteMessage::new(msg_type, input_msg.data));
        if let Err(_err) = channel.send(awaitable_msg.0) {
            return StatusCode::INTERNAL_SERVER_ERROR;
        }

        match awaitable_msg.1.recv() {
            Ok(result) => {
                if result {
                    StatusCode::OK
                } else {
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            }
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub async fn new(config: NetworkConfig) -> anyhow::Result<Self> {
        let first_port = config.port();
        let last_port = config.port() + config.port_count();
        for port in first_port..=last_port {
            let socket_address = SocketAddr::new(config.outbound_net_interface(), port);
            if let Ok(listener) = TcpListener::bind(socket_address).await {
                let self_ = Self {
                    config,
                    tcp_listener: listener,
                };
                return Ok(self_);
            }
        }

        bail!("ERROR");
    }
}
impl MessageServer for HttpMessageServer {
    type Client = HttpClient;
    fn build_client(&self) -> Self::Client {
        HttpClient::new(self.config, self.tcp_listener.local_addr().unwrap())
    }
    async fn start(self, tx: SyncSender<AwaitableMessage>) -> anyhow::Result<()> {
        let app = Router::new()
            .route(EXECUTE_PATH, post(Self::execute))
            .with_state(tx);

        axum::serve(self.tcp_listener, app)
            .await
            .map_err(anyhow::Error::from)
    }
}

#[derive(Serialize, Deserialize)]
pub struct InputMessage {
    type_: InputMessageType,
    data: Vec<u8>,
}

impl InputMessage {
    pub fn new(type_: InputMessageType, data: Vec<u8>) -> Self {
        Self { type_, data }
    }
}
#[derive(Serialize, Deserialize)]
pub enum InputMessageType {
    RawMessage,
    Proposal,
    ConfChange,
}
