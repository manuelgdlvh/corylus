/*
Network module must define how will be sent the read/write over the network in this two cases:
   * Writes: When reach to node that is not the leader must be forwarded to leader node
   * Reads: When is configured with strong consistency model

The basic operations are send_write_op() and send_read_op(). With the DeserializerOp trait can be rebuilt and forwarded to the local node.
The read operation will return some response through the network layer and the write operation will return OK when successfully proposed.
The caller of write operation will be notified when fully commited.
*/
use crate::handle::RaftNodeHandle;
use crate::node::GenericError;
use crate::operation::{RaftCommandResult, WriteOperation};
use crate::state_machine::RaftStateMachine;
use raft::prelude::Message;
use std::collections::HashMap;
use std::error::Error;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;

pub type MessageId = u128;
pub type PeerId = u64;

pub trait OpDeserializer<SM>: Send + Sync + Clone + 'static
where
    SM: RaftStateMachine,
{
    fn deserialize(&self, data: &[u8]) -> Box<dyn WriteOperation<SM>>;
}

pub trait RaftPeerClientProxy<S>: Send + 'static
where
    S: RaftStateMachine,
{
    // The implementation must ensure to always be forwarded to Leader Node in other case always will fail in propose().

    fn message(
        &mut self,
        message: HashMap<PeerId, Vec<Message>>,
    ) -> impl Future<Output = Result<RaftCommandResult, GenericError>>;
    fn join(
        &mut self,
        socket_addr: SocketAddr,
    ) -> impl Future<Output = Result<Option<RaftCommandResult>, GenericError>>;

    fn write(
        &mut self,
        data: Vec<Vec<u8>>,
    ) -> impl Future<Output = Result<Vec<MessageId>, GenericError>>;

    fn upsert_peer(&mut self, node_id: u64, addr: SocketAddr, is_leader: bool);
    fn generate_peer_id(&mut self) -> PeerId;

    /*
    This clash with ReadOpFn. At the moment only eventual consistency.
        async fn read<R: ReadOperation<S>>(&self) -> Result<Option<>, GenericError>;
    */
}

pub trait ServerHandle {
    fn socket_addr(&self) -> SocketAddr;
    async fn stop(&self);
}

pub trait RaftPeerServerProxy<SM, D, H>: Send + 'static
where
    SM: RaftStateMachine,
    D: OpDeserializer<SM>,
    H: ServerHandle,
{
    fn listen(self, handle: Arc<RaftNodeHandle<SM>>, deserializer: D) -> Result<H, GenericError>;
}
