use std::sync::mpsc::SyncSender;

use corylus_core::message::{AwaitableMessage, MessageServer, RemoteMessage};
use corylus_core::network::{ClusterJoint, NetworkClient};

pub struct GrpcMessageServer;


impl MessageServer for GrpcMessageServer {
    fn start(self, tx: SyncSender<AwaitableMessage>) -> impl Future<Output=anyhow::Result<()>> + Send {
        async {
            Ok(())
        }
    }
}


pub struct GrpcClient;


impl NetworkClient for GrpcClient {
    async fn discover_leader(&self) -> anyhow::Result<ClusterJoint> {
        todo!()
    }

    async fn send(&self, node_id: u64, message: RemoteMessage) -> anyhow::Result<()> {
        todo!()
    }
}