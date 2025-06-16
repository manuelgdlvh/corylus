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
use std::net::SocketAddr;
use std::sync::Arc;

pub type MessageId = u128;
pub type PeerId = u64;

#[derive(Default)]
pub struct OperationBucket {
    pub(crate) messages: Vec<Vec<u8>>,
}

impl OperationBucket {
    pub fn add(&mut self, msg: Vec<u8>) {
        self.messages.push(msg);
    }

    pub fn take_messages(&mut self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.messages)
    }
}

pub trait OpDeserializer<SM>: Send + Sync + 'static
where
    SM: RaftStateMachine,
{
    fn deserialize(&self, data: &[u8]) -> Box<dyn WriteOperation<SM>>;
}

pub trait RaftPeerClient<S>: Send + Sync + 'static
where
    S: RaftStateMachine,
{
    // The implementation must ensure to always be forwarded to Leader Node in other case always will fail in propose().

    fn message(
        &self,
        peer_id: PeerId,
        messages: Vec<Message>,
    ) -> impl Future<Output = Result<RaftCommandResult, GenericError>>;
    fn join(
        &self,
        socket_addr: SocketAddr,
    ) -> impl Future<Output = Result<Option<RaftCommandResult>, GenericError>>;

    fn write(
        &self,
        bucket: OperationBucket,
    ) -> impl Future<Output = Result<Vec<MessageId>, GenericError>> + Send;

    fn upsert_peer(&self, node_id: PeerId, addr: SocketAddr, is_leader: bool);

    /*
    This clash with ReadOpFn. At the moment only eventual consistency.
        async fn read<R: ReadOperation<S>>(&self) -> Result<Option<>, GenericError>;
    */
}

pub trait RaftPeerServerHandle {
    fn socket_addr(&self) -> SocketAddr;
    async fn stop(&self);
}

pub trait RaftPeerServer<SM, D, H>: Send + 'static
where
    SM: RaftStateMachine,
    D: OpDeserializer<SM>,
    H: RaftPeerServerHandle,
{
    fn listen(
        self,
        handle: Arc<RaftNodeHandle<SM>>,
        deserializer: Arc<D>,
    ) -> Result<H, GenericError>;
}
