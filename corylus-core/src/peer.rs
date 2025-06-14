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
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;

pub type MessageId = u128;
pub type PeerId = u64;

pub trait OpDeserializer<SM>: Send + 'static
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

    fn write(&mut self, data: &[u8]) -> impl Future<Output = Result<MessageId, GenericError>>;

    fn upsert_peer(&mut self, node_id: u64, addr: SocketAddr, is_leader: bool);
    fn generate_peer_id(&mut self) -> PeerId;

    /*
    This clash with ReadOpFn. At the moment only eventual consistency.
        async fn read<R: ReadOperation<S>>(&self) -> Result<Option<>, GenericError>;
    */
}

pub trait RaftPeerServerProxy<SM>: Send + 'static
where
    SM: RaftStateMachine,
{
    fn listen(self, handle: Arc<RaftNodeHandle<SM>>) -> Result<SocketAddr, GenericError>;
}

#[derive(Default)]
pub struct NoOpRaftPeerClientProxy {
    next_id: MessageId,
    next_peer_id: PeerId,
}
impl<S> RaftPeerClientProxy<S> for NoOpRaftPeerClientProxy
where
    S: RaftStateMachine,
{
    async fn write(&mut self, _: &[u8]) -> Result<MessageId, GenericError> {
        self.next_id += 1;
        Ok(self.next_id)
    }
    
    // Todo handle the batch internally, expose only how to send a batch of message. Message must to be exposed to use the ser/des of your convenience.
    async fn message(
        &mut self,
        messages: HashMap<PeerId, Vec<Message>>,
    ) -> Result<RaftCommandResult, GenericError> {
        todo!()
    }
    async fn join(
        &mut self,
        message: SocketAddr,
    ) -> Result<Option<RaftCommandResult>, GenericError> {
        Ok(None)
    }

    fn upsert_peer(&mut self, node_id: u64, addr: SocketAddr, is_leader: bool) {
        todo!()
    }

    // check if method to update next peer id pointer
    fn generate_peer_id(&mut self) -> PeerId {
        self.next_peer_id += 1;
        self.next_peer_id
    }
}

#[derive(Default)]
pub struct NoOpRaftPeerServerProxy {}

impl<S> RaftPeerServerProxy<S> for NoOpRaftPeerServerProxy
where
    S: RaftStateMachine,
{
    fn listen(self, handle: Arc<RaftNodeHandle<S>>) -> Result<SocketAddr, GenericError> {
        Ok(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(127, 0, 0, 1),
            80,
        )))
    }
}
