use actix_web::web::{Data, Json};
use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use corylus_core::handle::RaftNodeHandle;
use corylus_core::node::GenericError;
use corylus_core::operation::{NodeId, RaftCommand, RaftCommandResult};
use corylus_core::peer::RaftPeerServerProxy;
use corylus_core::state_machine::RaftStateMachine;
use protobuf::{Message as ProtobufMessage, RepeatedField};
use raft::prelude::Message;
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
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

impl<SM> RaftPeerServerProxy<SM> for ActixHttpServer
where
    SM: RaftStateMachine,
{
    fn listen(self, handle: Arc<RaftNodeHandle<SM>>) -> Result<SocketAddr, GenericError> {
        let runtime = Builder::new_multi_thread()
            .thread_name("actix-http-server")
            .enable_all()
            .build()
            .unwrap();

        let (tx, rx) = mpsc::sync_channel::<Result<SocketAddr, GenericError>>(1);
        thread::spawn(move || {
            runtime.block_on(async move {
                // Signal if failed binding
                match HttpServer::new(move || {
                    App::new()
                        .app_data(Data::new(handle.clone()))
                        .route("/join", web::post().to(join::<SM>))
                        .route("/messages", web::post().to(messages::<SM>))
                })
                .bind((self.net_interface, self.port))
                {
                    Ok(server) => {
                        let result = Ok(SocketAddr::V4(SocketAddrV4::new(
                            Ipv4Addr::new(127, 0, 0, 1),
                            self.port,
                        )));
                        tx.send(result).unwrap();
                        let _ = server.run().await;
                    }
                    Err(err) => {
                        tx.send(Err(err.into())).unwrap();
                    }
                }
            })
        });
        rx.recv().unwrap()
    }
}

#[derive(Serialize, Deserialize)]
pub struct ClusterJoinRequest {
    pub(crate) socket_addr: String,
}

#[derive(Serialize, Deserialize)]
pub struct ClusterJoinResponse {
    pub(crate) node_id: NodeId,
}

#[derive(Serialize, Deserialize)]
pub struct MessagesRequest {
    pub(crate) data: Vec<Vec<u8>>,
}

async fn join<SM>(
    request: Json<ClusterJoinRequest>,
    handle: Data<Arc<RaftNodeHandle<SM>>>,
) -> impl Responder
where
    SM: RaftStateMachine,
{
    println!("request join received");

    let socket_addr: SocketAddr = request.socket_addr.parse().unwrap();

    // Thread blocked waiting response, change to async
    if let Ok(result) = handle
        .raft_command(RaftCommand::ClusterJoin(socket_addr))
        .recv()
    {
        match result {
            Ok(response) => match response {
                RaftCommandResult::ClusterJoin(id) => {
                    HttpResponse::Ok().json(ClusterJoinResponse { node_id: id })
                }
                RaftCommandResult::None => HttpResponse::UnprocessableEntity().finish(),
            },
            Err(_) => HttpResponse::InternalServerError().finish(),
        }
    } else {
        HttpResponse::RequestTimeout().finish()
    }
}

async fn messages<SM>(
    request: Json<MessagesRequest>,
    handle: Data<Arc<RaftNodeHandle<SM>>>,
) -> impl Responder
where
    SM: RaftStateMachine,
{

    for msg_buff in request.data.iter() {
        let mut message = Message::default();
        message.merge_from_bytes(msg_buff).unwrap();
        handle.raft_command(RaftCommand::Raw(message));
    }

    HttpResponse::Ok().finish()
}
