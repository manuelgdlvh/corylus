use std::{
    array,
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    io::{self, Read},
    net::{SocketAddr, TcpStream},
    sync::{
        Arc, Mutex, RwLock, RwLockReadGuard,
        atomic::{AtomicU64, Ordering},
        mpsc::SyncSender,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use uuid::Uuid;

use crate::{
    instance::Shutdown,
    network::{
        self, Message,
        packet::{self, Event, PACKET_LENGTH, Packet},
    },
    runtime::{TcpRead, TcpWrite},
};
use crate::{network::packet::Inbound, runtime};

const CONN_STRIPES_LEN: usize = 8;
struct Limiter {
    stripes: [Mutex<()>; CONN_STRIPES_LEN],
}

impl Limiter {
    pub fn new() -> Self {
        Self {
            stripes: array::from_fn(|_| Mutex::new(())),
        }
    }

    pub async fn execute<F: Future<Output = io::Result<()>>>(
        &self,
        addr: &SocketAddr,
        f: F,
    ) -> io::Result<()> {
        let mut hasher = DefaultHasher::new();
        addr.hash(&mut hasher);
        let id = (hasher.finish() as usize) & (CONN_STRIPES_LEN - 1);

        let _guard = &self.stripes[id]
            .lock()
            .expect("connection limiter stripe mutex poisoned");
        f.await
    }
}

// TODO: Make just the conversion from TCPStream and sched threads sync into async with Runtime abstractions. Next step will be reduce the lock's and isolate receiver.
#[derive(Clone)]
pub(crate) struct Registry<S: runtime::Spawner, I: runtime::Io> {
    inner: Arc<Inner<S, I>>,
}

pub(crate) struct Inner<S: runtime::Spawner, I: runtime::Io> {
    pub(crate) id: Uuid,
    pub(crate) spawner: S,
    pub(crate) config: network::Config,
    pub(crate) tx_msg: SyncSender<Message>,
    pub(crate) sigterm: Shutdown,
    addrs: Mutex<HashMap<Uuid, SocketAddr>>,
    writers: RwLock<HashMap<Uuid, PeerWrite<I>>>,
    limiter: Limiter,
}

impl<S: runtime::Spawner, I: runtime::Io> AsRef<Inner<S, I>> for Registry<S, I> {
    fn as_ref(&self) -> &Inner<S, I> {
        &self.inner
    }
}

impl<S: runtime::Spawner, I: runtime::Io> Registry<S, I> {
    pub fn new(
        id: Uuid,
        spawner: S,
        config: network::Config,
        tx_msg: SyncSender<Message>,
        sigterm: Shutdown,
    ) -> Self {
        let inner = Arc::new(Inner {
            id,
            spawner,
            config,
            tx_msg,
            sigterm,
            addrs: Mutex::new(HashMap::new()),
            writers: RwLock::new(HashMap::new()),
            limiter: Limiter::new(),
        });
        Self { inner }
    }

    // Listener accepts connection single threaded, so we always have linearizability here.
    pub fn register(&self, peer_id: Uuid, peer_addr: &SocketAddr, writer: PeerWrite<I>) {
        let old_addr = self
            .as_ref()
            .addrs
            .lock()
            .expect("peer addrs mutex poisoned")
            .insert(peer_id, *peer_addr);
        self.as_ref()
            .writers
            .write()
            .expect("peer writers RwLock poisoned")
            .insert(peer_id, writer);
        if old_addr.is_none()
            && let Err(err) = self
                .as_ref()
                .tx_msg
                .send(Message::Event(Event::PeerAdded { id: peer_id }))
        {
            log::error!("Peer added event enqueue failed. Err: {}.", err);
        }
    }

    pub fn unregister(&self, peer_id: Uuid, v: u64) {
        match self
            .as_ref()
            .writers
            .write()
            .expect("peer writers RwLock poisoned")
            .entry(peer_id)
        {
            std::collections::hash_map::Entry::Occupied(entry) => {
                if entry.get().v != v {
                    return;
                }

                entry.remove_entry();
            }
            std::collections::hash_map::Entry::Vacant(_) => {
                return;
            }
        }

        self.as_ref()
            .addrs
            .lock()
            .expect("peer addrs mutex poisoned")
            .remove_entry(&peer_id);
        let _ = self
            .as_ref()
            .tx_msg
            .send(Message::Event(Event::PeerRemoved { id: peer_id }));

        log::info!(
            "Peer disconnected successfully. Id: {}. Peer id: {}.",
            self.as_ref().id,
            peer_id
        );
    }

    pub fn version(&self, id: Uuid) -> Option<u64> {
        self.as_ref()
            .writers
            .read()
            .expect("peer writers RwLock poisoned")
            .get(&id)
            .map(|w| w.v)
    }

    pub async fn connect_with_id(&self, id: Uuid, v: Option<u64>) -> io::Result<()> {
        if let Some(addr) = self.addr(id) {
            self.connect(&addr, v).await
        } else {
            Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "No peer connection found",
            ))
        }
    }

    pub async fn connect(&self, peer_addr: &SocketAddr, v: Option<u64>) -> io::Result<()> {
        self.as_ref()
            .limiter
            .execute(peer_addr, async move {
                if let Some(peer_id) = self.peer_id_from_addr(peer_addr)
                    && let Some(current_v) = self.version(peer_id)
                {
                    match v {
                        Some(val) => {
                            if val != current_v {
                                return Ok(());
                            }
                        }

                        None => {
                            return Ok(());
                        }
                    }
                }

                let (r, w) = I::stream(peer_addr).await?;
                // TcpStream::connect_timeout(peer_addr, self.as_ref().config.timeout.connect)?;
                let mut r = PeerRead::new(r);
                let w = PeerWrite::new(w);

                w.write(
                    &Packet::Request(packet::Request::WhoIs {
                        id: self.as_ref().id,
                        addr: self.as_ref().config.addr,
                    }),
                    Some(self.as_ref().config.timeout.write),
                )
                .await?;

                let packet_raw = r.read(Some(self.as_ref().config.timeout.read)).await?;
                match packet::Reply::try_from(&packet_raw)? {
                    packet::Reply::WhoIs { id } => {
                        let v = w.v;
                        self.register(id, peer_addr, w);
                        // match r.start(self.clone(), id, v) {
                        //     Ok(h) => {
                        //         self.as_ref().sigterm.register(h);
                        //         Ok(())
                        //     }
                        //     Err(err) => {
                        //         self.unregister(id, v);
                        //         Err(err)
                        //     }
                        // }

                        r.start(self.clone(), id, v);
                        Ok(())

                    }
                    _ => Err(io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        "Who Is reply not received",
                    )),
                }
            })
            .await
    }

    fn peer_id_from_addr(&self, addr: &SocketAddr) -> Option<Uuid> {
        let addrs = self
            .as_ref()
            .addrs
            .lock()
            .expect("peer addrs mutex poisoned");
        for (id, peer_addr) in addrs.iter() {
            if addr.eq(peer_addr) {
                return Some(*id);
            }
        }

        None
    }

    pub(crate) fn is_connected(&self, addr: &SocketAddr) -> bool {
        if let Some(peer_id) = self.peer_id_from_addr(addr) {
            self.with_writers_read(|writers| writers.contains_key(&peer_id))
        } else {
            false
        }
    }

    pub(crate) fn addr(&self, id: Uuid) -> Option<SocketAddr> {
        self.as_ref()
            .addrs
            .lock()
            .expect("peer addrs mutex poisoned")
            .get(&id)
            .copied()
    }

    pub(crate) fn connected_peers(&self) -> Vec<Uuid> {
        self.with_writers_read(|writers| writers.keys().copied().collect())
    }

    pub(crate) fn update_hb(&self, id: Uuid) {
        self.with_writers_read(|writers| {
            if let Some(writer) = writers.get(&id) {
                writer.update_hb();
            }
        })
    }

    pub(crate) fn hb(&self, id: Uuid) -> Option<Instant> {
        self.with_writers_read(|writers| writers.get(&id).map(|writer| writer.hb()))
    }

    pub(crate) async fn with_writers_read<F, O>(&self, f: F) -> O
    where
        F: Fn(RwLockReadGuard<'_, HashMap<Uuid, PeerWrite<I>>>) -> impl Future<Output = O>,
    {
        let guard = self
            .as_ref()
            .writers
            .read()
            .expect("peer writers RwLock poisoned");
        f(guard).await
    }
}

pub(crate) struct PeerWrite<I: runtime::Io> {
    stream: Mutex<I::StreamWrite>,
    hb: Mutex<Instant>,
    pub(crate) v: u64,
}

impl<I: runtime::Io> PeerWrite<I> {
    pub fn new(stream: I::StreamWrite) -> Self {
        static VERSION: AtomicU64 = AtomicU64::new(0);

        Self {
            stream: Mutex::new(stream),
            hb: Mutex::new(Instant::now()),
            v: VERSION.fetch_add(1, Ordering::Relaxed),
        }
    }

    pub async fn write(&self, packet: &Packet<'_>, timeout: Option<Duration>) -> io::Result<()> {
        let raw: Vec<u8> = packet.into();
        let mut stream = self
            .stream
            .lock()
            .expect("PeerWrite TcpStream mutex poisoned");
        // stream.set_write_timeout(timeout)?;
        //
        stream.write_all(raw.as_slice()).await
    }

    pub fn update_hb(&self) {
        let mut hb = self.hb.lock().expect("PeerWrite heartbeat mutex poisoned");
        *hb = Instant::now();
    }

    pub fn hb(&self) -> Instant {
        *self.hb.lock().expect("PeerWrite heartbeat mutex poisoned")
    }
}

pub(super) struct PeerRead<I: runtime::Io> {
    stream: I::StreamRead,
}

impl<I: runtime::Io> PeerRead<I> {
    pub fn new(stream: I::StreamRead) -> Self {
        Self { stream }
    }

    pub async fn read(&mut self, timeout: Option<Duration>) -> io::Result<packet::Raw> {
        // self.stream.set_read_timeout(timeout)?;
        let mut len_buffer: [u8; PACKET_LENGTH] = [0; PACKET_LENGTH];
        self.stream.read_exact(len_buffer.as_mut_slice()).await?;
        let len: u32 = u32::from_le_bytes(len_buffer);

        let mut payload_buffer = vec![0u8; len as usize];
        self.stream
            .read_exact(payload_buffer.as_mut_slice())
            .await?;
        Ok(packet::Raw::new(payload_buffer))
    }

    pub fn start<S: runtime::Spawner>(
        mut self,
        registry: Registry<S, I>,
        peer_id: Uuid,
        version: u64,
    ) {
        // ) -> io::Result<JoinHandle<()>> {
        //

        let spawner = registry.as_ref().spawner.clone();
        spawner.spawn(async move {

        log::info!(
            "TCP connection initialized. Id: {}. Peer id: {}. V: {}.",
            registry.as_ref().id,
            peer_id,
            version
        );

        loop {
            match registry.version(peer_id) {
                Some(current) if current == version => {}
                _ => break,
            }

            if !registry.as_ref().sigterm.checkpoint(None) {
                break;
            }

            match self.read(Some(registry.as_ref().config.timeout.read)).await {
                Ok(packet) => {
                    let kind = match packet.try_kind() {
                        Ok(k) => k,
                        Err(err) => {
                            log::error!(
                                "Invalid packet kind. Id: {}. Peer id: {}. V: {}. Err: {}.",
                                registry.as_ref().id,
                                peer_id,
                                version,
                                err
                            );
                            continue;
                        }
                    };
                    if matches!(kind, packet::Kind::HeartBeatRequest) {
                        log::info!(
                            "Heartbeat packet received. Id: {}. Peer id: {}. V: {}.",
                            registry.as_ref().id,
                            peer_id,
                            version
                        );
                        registry.update_hb(peer_id);
                    } else if let Err(err) = registry
                        .as_ref()
                        .tx_msg
                        .send(Message::Packet(Inbound::new(peer_id, packet)))
                    {
                        log::error!(
                            "Packet enqueue failed. Id: {}. Peer id: {}. V: {}. Kind: {}. Err: {}.",
                            registry.as_ref().id,
                            peer_id,
                            version,
                            kind,
                            err
                        );
                    }
                }
                Err(err)
                    if matches!(
                        err.kind(),
                        io::ErrorKind::BrokenPipe
                            | io::ErrorKind::ConnectionReset
                            | io::ErrorKind::ConnectionAborted
                            | io::ErrorKind::NotConnected
                            | io::ErrorKind::UnexpectedEof
                    ) =>
                {
                    break;
                }
                Err(err) => {
                    if !matches!(err.kind(), io::ErrorKind::WouldBlock) {
                        log::error!(
                            "Packet read failed. Id: {}. Peer id: {}. V: {}. Kind: {:?}. Err: {}.",
                            registry.as_ref().id,
                            peer_id,
                            version,
                            err.kind(),
                            err
                        );
                    }
                }
            }
        }

        log::info!(
            "TCP connection destroyed. Id: {}. Peer id: {}. V: {}.",
            registry.as_ref().id,
            peer_id,
            version
        );
            
    });
} }
