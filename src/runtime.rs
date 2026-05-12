use std::{io, net::ToSocketAddrs};

#[derive(Clone)]
pub struct Options {
    pub name: String,
    pub threads: usize,
}

pub trait Timer {}

pub trait Io: Clone + 'static {
    type Listener: TcpListener<StreamRead = Self::StreamRead, StreamWrite = Self::StreamWrite>;
    type StreamRead: TcpRead;
    type StreamWrite: TcpWrite;

    fn listener<A: ToSocketAddrs>(addr: A) -> impl Future<Output = io::Result<Self::Listener>>;
    fn stream<A: ToSocketAddrs>(
        addr: A,
    ) -> impl Future<Output = io::Result<(Self::StreamRead, Self::StreamWrite)>>;
}

pub trait TcpRead: Send {
    fn read_exact(&mut self, buffer: &mut [u8]) -> impl Future<Output = io::Result<()>> + Send;
}
pub trait TcpWrite: Send {
    fn write_all(&mut self, buffer: &[u8]) -> impl Future<Output = io::Result<()>>;
}

pub trait TcpListener: Send {
    type StreamRead: TcpRead;
    type StreamWrite: TcpWrite;

    fn accept(
        &self,
    ) -> impl Future<Output = io::Result<(Self::StreamRead, Self::StreamWrite)>> + Send;
}

pub trait Spawner: Send + Sync + Clone + 'static {
    fn build(opts: Options) -> io::Result<Self>
    where
        Self: Sized;

    fn spawn<F>(&self, f: F)
    where
        F: Future<Output = ()> + Send + 'static;

    fn scoped_spawn<F>(&self, f: F)
    where
        F: Future<Output = ()> + 'static;
}
