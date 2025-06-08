/*
Network module must define how will be sent the read/write over the network in this two cases:
   * Writes: When reach to node that is not the leader must be forwarded to leader node
   * Reads: When is configured with strong consistency model

The basic operations are send_write_op() and send_read_op(). With the DeserializerOp trait can be rebuilt and forwarded to the local node.
The read operation will return some response through the network layer and the write operation will return OK when successfully proposed.
The caller of write operation will be notified when fully commited.
*/
use crate::node::GenericError;
use crate::operation::{ReadOperation, WriteOperation};
use crate::state_machine::StateMachine;
use std::net::SocketAddr;

pub trait OpDeserializer<S>
where
    S: StateMachine,
{
    type Output: WriteOperation<S>;

    // Deserialize useful when travelling to network
    fn deserialize(data: Vec<u8>) -> Self::Output;
}

pub trait RaftLeaderProxy<S>: Send + 'static
where
    S: StateMachine,
{
    // The implementation must ensure to always be forwarded to Leader Node in other case always will fail in propose().
    fn write(&mut self, data: &[u8]) -> impl Future<Output = Result<u128, GenericError>>;

    fn update_leader(&mut self, node_id: u64, addr: SocketAddr);

    /*
    This clash with ReadOpFn. At the moment only eventual consistency.
        async fn read<R: ReadOperation<S>>(&self) -> Result<Option<>, GenericError>;
    */
}

#[derive(Default)]
pub struct NoOpRaftLeaderProxy {
    next_id: u128,
}
impl<S> RaftLeaderProxy<S> for NoOpRaftLeaderProxy
where
    S: StateMachine,
{
    async fn write(&mut self, _: &[u8]) -> Result<u128, GenericError> {
        self.next_id += 1;
        Ok(self.next_id)
    }

    fn update_leader(&mut self, node_id: u64, addr: SocketAddr) {
        todo!()
    }
}
