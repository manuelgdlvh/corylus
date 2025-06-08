use crate::handle::AwaitableWriteOp;
use crate::state_machine::StateMachine;
use std::collections::HashMap;



pub struct CallbackHolder<S>
where
    S: StateMachine,
{
    callbacks: HashMap<u128, AwaitableWriteOp<S>>,
    next: u128,
}

impl<S> CallbackHolder<S>
where
    S: StateMachine,
{
    pub fn new() -> Self {
        Self {
            callbacks: Default::default(),
            next: 0,
        }
    }

    // Could be modelled using erased type like Runnable to allow more than on Callback type
    pub fn add(&mut self, id: u128, op: AwaitableWriteOp<S>) {
        self.callbacks.insert(id, op);
    }

    pub fn remove(&mut self, id: u128) -> Option<AwaitableWriteOp<S>> {
        self.callbacks.remove(&id)
    }

    pub fn next(&mut self) -> u128 {
        self.next += 1;
        self.next
    }
}
