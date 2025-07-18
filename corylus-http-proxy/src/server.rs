use crate::request::{ClusterJoinRequest, MessagesRequest, WritesRequest};
use crate::response::{ClusterJoinResponse, WritesResponse};
use actix_web::web::{Data, Json};
use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use corylus_core::handle::RaftNodeHandle;
use corylus_core::node::GenericError;
use corylus_core::operation::{RaftCommand, RaftCommandResult};
use corylus_core::peer::{OpDeserializer, RaftPeerServer, RaftPeerServerHandle};
use corylus_core::state_machine::RaftStateMachine;
use protobuf::Message as ProtobufMessage;
use raft::prelude::Message;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, mpsc};
use std::thread;
use tokio::runtime::Builder;

pub struct ActixHttpServer {
    net_interface: IpAddr,
    port: u16,
}

impl ActixHttpServer {
    pub fn new(net_interface: IpAddr, port: u16) -> Self {
        Self {
            net_interface,
            port,
        }
    }
}

pub struct ActixRaftPeerServerHandle {
    handle: actix_web::dev::ServerHandle,
    socket_addr: SocketAddr,
}
impl RaftPeerServerHandle for ActixRaftPeerServerHandle {
    fn socket_addr(&self) -> SocketAddr {
        self.socket_addr
    }

    async fn stop(&self) {
        self.handle.stop(true).await;
    }
}

impl<SM, D> RaftPeerServer<SM, D, ActixRaftPeerServerHandle> for ActixHttpServer
where
    SM: RaftStateMachine,
    D: OpDeserializer<SM>,
{
    fn listen(
        self,
        handle: Arc<RaftNodeHandle<SM>>,
        deserializer: D,
    ) -> Result<ActixRaftPeerServerHandle, GenericError> {
        let runtime = Builder::new_multi_thread()
            .thread_name("actix-http-server")
            .enable_all()
            .build()
            .unwrap();

        let (started_signal_tx, started_signal_rx) =
            mpsc::sync_channel::<Result<ActixRaftPeerServerHandle, GenericError>>(1);

        thread::spawn(move || {
            runtime.block_on(async move {
                let app_data = (deserializer, handle);
                match HttpServer::new(move || {
                    App::new()
                        .app_data(Data::new(app_data.clone()))
                        .route("/join", web::post().to(join::<SM, D>))
                        .route("/messages", web::post().to(messages::<SM, D>))
                        .route("/write", web::post().to(write::<SM, D>))
                })
                .bind((self.net_interface, self.port))
                {
                    Ok(server) => {
                        let socket_addr = server
                            .addrs()
                            .first()
                            .expect("Socket Address should be initialized")
                            .clone();
                        let server = server.run();
                        let handle = ActixRaftPeerServerHandle {
                            handle: server.handle(),
                            socket_addr,
                        };

                        started_signal_tx.send(Ok(handle)).expect("");
                        let _ = server.await;
                    }
                    Err(err) => {
                        started_signal_tx.send(Err(err.into())).expect("");
                    }
                }
            })
        });

        started_signal_rx
            .recv()
            .expect("Server should start successfully")
    }
}

async fn join<SM, D>(
    request: Json<ClusterJoinRequest>,
    state: Data<(D, Arc<RaftNodeHandle<SM>>)>,
) -> impl Responder
where
    SM: RaftStateMachine,
    D: OpDeserializer<SM>,
{
    let socket_addr: SocketAddr = request.socket_addr.parse().unwrap();

    // Thread blocked waiting response, change to async
    if let Ok(result) = state
        .1
        .raft_command(RaftCommand::ClusterJoin(socket_addr))
        .recv()
    {
        match result {
            Ok(response) => match response {
                RaftCommandResult::ClusterJoin {
                    own_node_id,
                    leader_node_id,
                    leader_addr,
                } => HttpResponse::Ok().json(ClusterJoinResponse {
                    own_node_id,
                    leader_node_id,
                    leader_addr,
                }),
                RaftCommandResult::None => HttpResponse::UnprocessableEntity().finish(),
            },
            Err(_) => HttpResponse::InternalServerError().finish(),
        }
    } else {
        HttpResponse::RequestTimeout().finish()
    }
}

async fn messages<SM, D>(
    request: Json<MessagesRequest>,
    state: Data<(D, Arc<RaftNodeHandle<SM>>)>,
) -> impl Responder
where
    SM: RaftStateMachine,
    D: OpDeserializer<SM>,
{
    for msg_buff in request.data.iter() {
        let mut message = Message::default();
        message.merge_from_bytes(msg_buff).unwrap();
        state.1.raft_command(RaftCommand::Raw(message));
    }

    HttpResponse::Ok().finish()
}

async fn write<SM, D>(
    request: Json<WritesRequest>,
    state: Data<(D, Arc<RaftNodeHandle<SM>>)>,
) -> impl Responder
where
    SM: RaftStateMachine,
    D: OpDeserializer<SM>,
{
    let mut waiters = Vec::new();
    for msg_buff in request.data.iter() {
        let operation = state.0.deserialize(msg_buff);
        waiters.push(state.1.write_forwarded(operation));
    }

    let mut message_ids = Vec::new();
    for mut waiter in waiters {
        // Check possible errors of one item.
        if let Ok(result) = waiter.recv().await.unwrap() {
            message_ids.push(result);
        }
    }

    HttpResponse::Ok().json(&WritesResponse { message_ids })
}
