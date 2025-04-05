use std::time::Duration;

pub struct Config {
    message_capacity: usize,
    tick_duration_ms: u64,
    consensus_config: raft::Config,
}

impl Config {
    pub fn message_capacity(&self) -> usize {
        self.message_capacity
    }

    pub fn tick_duration_ms(&self) -> Duration {
        Duration::from_millis(self.tick_duration_ms)
    }

    pub fn consensus_config(&mut self) -> &mut raft::Config {
        &mut self.consensus_config
    }
}
