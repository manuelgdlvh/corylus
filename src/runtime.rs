use std::thread::{self};

pub struct Context<S: Spawner, L: Logger> {
    spawner: S,
    logger: L,
}

impl<S: Spawner, L: Logger> Context<S, L> {
    pub fn new(spawner: S, logger: L) -> Self {
        Self { spawner, logger }
    }
}

pub trait JoinHandle<O> {
    fn join(self) -> Result<O, ()>;
}

pub trait Spawner: Send + Sync + Clone + 'static {
    type Handle<O>: JoinHandle<O>;

    fn spawn<F, O>(&self, f: F) -> Self::Handle<O>
    where
        F: FnOnce() -> O + Send + 'static,
        O: Send + 'static;
}

pub trait Logger: Send + Sync + Clone + 'static {
    fn log(&self, message: &str, level: LogLevel);
}

pub enum LogLevel {
    Info,
    Debug,
    Trace,
    Warn,
    Error,
}

// --- Spawners ---

#[derive(Copy, Clone, Default)]
pub struct ThreadSpawner {}

impl<O> JoinHandle<O> for std::thread::JoinHandle<O> {
    fn join(self) -> Result<O, ()> {
        thread::JoinHandle::join(self).map_err(|_| ())
    }
}

impl Spawner for ThreadSpawner {
    type Handle<O> = std::thread::JoinHandle<O>;
    fn spawn<F, O>(&self, f: F) -> Self::Handle<O>
    where
        F: FnOnce() -> O + Send + 'static,
        O: Send + 'static,
    {
        thread::spawn(f)
    }
}
