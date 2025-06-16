use crate::handle::AwaitableWriteOp;
use crate::node::{GenericError, RaftNode};
use crate::peer::{MessageId, OpDeserializer, RaftPeerClient};
use crate::raft_log::RaftLog;
use crate::state_machine::RaftStateMachine;
use async_trait::async_trait;
use std::fmt::Pointer;
use tokio::task::JoinHandle;

#[async_trait]
pub trait AsyncRaftRunnable<SM, D, L, C>: 'static
where
    SM: RaftStateMachine,
    D: OpDeserializer<SM>,
    L: RaftLog,
    C: RaftPeerClient<SM>,
{
    fn is_done(&self) -> bool;
    async fn callback(&mut self, raft_node: &mut RaftNode<SM, D, L, C>);
}

pub struct ForwardWrites<SM>
where
    SM: RaftStateMachine,
{
    write_buffer: Vec<AwaitableWriteOp<SM>>,
    future: Option<JoinHandle<Result<Vec<MessageId>, GenericError>>>,
}

impl<SM> ForwardWrites<SM>
where
    SM: RaftStateMachine,
{
    pub fn new(
        write_buffer: Vec<AwaitableWriteOp<SM>>,
        future: JoinHandle<Result<Vec<MessageId>, GenericError>>,
    ) -> Self {
        Self {
            write_buffer,
            future: Some(future),
        }
    }
}

#[async_trait]
impl<SM, D, L, C> AsyncRaftRunnable<SM, D, L, C> for ForwardWrites<SM>
where
    SM: RaftStateMachine,
    D: OpDeserializer<SM>,
    L: RaftLog,
    C: RaftPeerClient<SM>,
{
    fn is_done(&self) -> bool {
        if let Some(_) = self.future.as_ref() {
            true
        } else {
            false
        }
    }

    async fn callback(&mut self, raft_node: &mut RaftNode<SM, D, L, C>) {
        let future = self.future.take().unwrap();
        match future.await.unwrap() {
            Ok(mut results) => {
                let mut updated = false;

                for (idx, w_op) in self.write_buffer.drain(..).enumerate().rev() {
                    let message_id = results.swap_remove(idx);
                    if !updated {
                        raft_node.callbacks.update_next(message_id);
                        updated = true;
                    }

                    raft_node.callbacks.add(message_id, w_op);
                }
            }
            Err(err) => {
                for w_op in self.write_buffer.drain(..) {
                    let _ = w_op.notifier.send(Err(err.to_string().into())).await;
                }
            }
        }
    }
}

pub struct AsyncRunnableExecutor<SM, D, L, C>
where
    SM: RaftStateMachine,
    D: OpDeserializer<SM>,
    L: RaftLog,
    C: RaftPeerClient<SM>,
{
    futures: Vec<Box<dyn AsyncRaftRunnable<SM, D, L, C>>>,
}

impl<SM, D, L, C> AsyncRunnableExecutor<SM, D, L, C>
where
    SM: RaftStateMachine,
    D: OpDeserializer<SM>,
    L: RaftLog,
    C: RaftPeerClient<SM>,
{
    pub fn new() -> Self {
        Self {
            futures: Vec::new(),
        }
    }
    pub async fn process(&mut self, raft_node: &mut RaftNode<SM, D, L, C>) {
        let mut futures_pending = Vec::new();
        for mut future in self.futures.drain(..) {
            if future.is_done() {
                future.callback(raft_node).await;
            } else {
                futures_pending.push(future);
            }
        }

        self.futures = futures_pending;
    }

    pub fn add<R>(&mut self, future: R)
    where
        R: AsyncRaftRunnable<SM, D, L, C>,
    {
        self.futures.push(Box::new(future));
    }
}
