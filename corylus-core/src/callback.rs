use crate::handle::AwaitableWriteOp;
use crate::peer::MessageId;
use crate::state_machine::RaftStateMachine;
use std::collections::HashMap;

pub struct CallbackHolder<S>
where
    S: RaftStateMachine,
{
    // Use Min Binary Heap to remove fast dirty callbacks
    callbacks: HashMap<MessageId, AwaitableWriteOp<S>>,
    next: MessageId,
    last_applied: MessageId,
}

impl<S> CallbackHolder<S>
where
    S: RaftStateMachine,
{
    pub fn new() -> Self {
        Self {
            callbacks: Default::default(),
            next: 0,
            last_applied: 0,
        }
    }

    // Could be modelled using erased type like Runnable to allow more than on Callback type
    pub fn add(&mut self, id: MessageId, op: AwaitableWriteOp<S>) {
        self.callbacks.insert(id, op);
    }

    pub fn update_next(&mut self, message_id: MessageId) {
        if message_id > self.next {
            self.next = message_id;
        }
    }

    // Last applied to remove dirty callbacks
    pub fn remove(&mut self, id: MessageId) -> Option<AwaitableWriteOp<S>> {
        let result = self.callbacks.remove(&id);
        if result.is_some() {
            self.last_applied = id;
        }

        result
    }

    pub fn next(&mut self) -> MessageId {
        self.next += 1;
        self.next
    }
}
