use std::{
    array,
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    io::{self, Read, Write},
    net::{SocketAddr, TcpStream},
    sync::{
        Arc, Mutex, RwLock, RwLockReadGuard,
        atomic::{AtomicU64, Ordering},
        mpsc::{self, SyncSender},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use tracing::{error, info};
use uuid::Uuid;

use crate::{
    instance::Shutdown,
    network::{
        self, Message,
        packet::{self, Event, InboundPacket, PACKET_LENGTH, Packet},
    },
};

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

    pub fn execute<F: Fn() -> io::Result<()>>(&self, addr: &SocketAddr, f: F) -> io::Result<()> {
        let mut hasher = DefaultHasher::new();
        addr.hash(&mut hasher);
        let id = (hasher.finish() as usize) & (CONN_STRIPES_LEN - 1);

        let _guard = &self.stripes[id].lock().expect("Cannot be poisoned");
        f()
    }
}

pub struct Response<'a> {
    corr_id: Uuid,
    reg: &'a Registry,
    receiver: mpsc::Receiver<packet::Reply>,
}

impl<'a> Response<'a> {
    fn new(corr_id: Uuid, reg: &'a Registry, receiver: mpsc::Receiver<packet::Reply>) -> Self {
        Self {
            corr_id,
            reg,
            receiver,
        }
    }

    pub fn get(&self, timeout: Duration) -> Result<packet::Reply, io::Error> {
        self.receiver
            .recv_timeout(timeout)
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Timeout"))
    }
}

impl Drop for Response<'_> {
    fn drop(&mut self) {
        self.reg.unregister_ack(self.corr_id);
    }
}

#[derive(Clone)]
pub(crate) struct Registry {
    inner: Arc<Inner>,
}

pub(crate) struct Inner {
    pub(crate) id: Uuid,
    pub(crate) config: network::Config,
    pub(crate) tx_msg: SyncSender<Message>,
    pub(crate) sigterm: Shutdown,
    addrs: Mutex<HashMap<Uuid, SocketAddr>>,
    writers: RwLock<HashMap<Uuid, PeerWrite>>,
    acks: Mutex<HashMap<Uuid, SyncSender<packet::Reply>>>,
    limiter: Limiter,
}

impl AsRef<Inner> for Registry {
    fn as_ref(&self) -> &Inner {
        &self.inner
    }
}

impl Registry {
    pub fn new(
        id: Uuid,
        config: network::Config,
        tx_msg: SyncSender<Message>,
        sigterm: Shutdown,
    ) -> Self {
        let inner = Arc::new(Inner {
            id,
            config,
            tx_msg,
            sigterm,
            addrs: Mutex::new(HashMap::new()),
            writers: RwLock::new(HashMap::new()),
            acks: Mutex::new(HashMap::new()),
            limiter: Limiter::new(),
        });
        Self { inner }
    }

    // Listener accepts connection single threaded, so we always have linearizability here.
    pub fn register(&self, peer_id: Uuid, peer_addr: &SocketAddr, writer: PeerWrite) {
        let old_addr = self
            .as_ref()
            .addrs
            .lock()
            .expect("Cannot be poisoned")
            .insert(peer_id, *peer_addr);
        self.as_ref()
            .writers
            .write()
            .expect("Cannot be poisoned")
            .insert(peer_id, writer);
        if old_addr.is_none()
            && let Err(err) = self.as_ref().tx_msg.send(Message::Event {
                val: Event::PeerAdded { id: peer_id },
            })
        {
            error!(err = %err, "Peer added event enqueue failed");
        }
    }

    pub fn unregister(&self, peer_id: Uuid, v: u64) {
        match self
            .as_ref()
            .writers
            .write()
            .expect("Cannot be poisoned")
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
            .expect("Cannot be poisoned")
            .remove_entry(&peer_id);
        let _ = self.as_ref().tx_msg.send(Message::Event {
            val: Event::PeerRemoved { id: peer_id },
        });

        info!(id = %self.as_ref().id, peer_id = %peer_id, "Peer disconnected successfully");
    }

    pub fn version(&self, id: Uuid) -> Option<u64> {
        self.as_ref()
            .writers
            .read()
            .expect("Cannot be poisoned")
            .get(&id)
            .map(|w| w.v)
    }

    pub fn connect_with_id(&self, id: Uuid, v: Option<u64>) -> io::Result<()> {
        if let Some(addr) = self.addr(id) {
            self.connect(&addr, v)
        } else {
            Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "No peer connection found",
            ))
        }
    }

    pub fn connect(&self, peer_addr: &SocketAddr, v: Option<u64>) -> io::Result<()> {
        self.as_ref().limiter.execute(peer_addr, || {
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

            let stream =
                TcpStream::connect_timeout(peer_addr, self.as_ref().config.timeout.connect)?;
            let mut r = PeerRead::new(stream.try_clone()?);
            let w = PeerWrite::new(stream);

            w.write(
                &Packet::Request(packet::Request::Read(packet::Read::WhoIs {
                    id: self.as_ref().id,
                    addr: self.as_ref().config.addr,
                })),
                Some(self.as_ref().config.timeout.write),
            )?;

            match r.read(Some(self.as_ref().config.timeout.read))? {
                Packet::Reply(packet::Reply::WhoIs { id }) => {
                    let v = w.v;
                    self.register(id, peer_addr, w);
                    match r.start(self.clone(), id, v) {
                        Ok(h) => {
                            self.as_ref().sigterm.register(h);
                            Ok(())
                        }
                        Err(err) => {
                            self.unregister(id, v);
                            Err(err)
                        }
                    }
                }
                _ => Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "Who Is reply not received",
                )),
            }
        })
    }

    fn peer_id_from_addr(&self, addr: &SocketAddr) -> Option<Uuid> {
        let addrs = self.as_ref().addrs.lock().expect("Cannot be poisoned");
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
            .expect("Cannot be poisoned")
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

    pub(crate) fn with_writers_read<F, O>(&self, f: F) -> O
    where
        F: Fn(RwLockReadGuard<'_, HashMap<Uuid, PeerWrite>>) -> O,
    {
        let guard = self.as_ref().writers.read().expect("Cannot be poisoned");
        f(guard)
    }

    pub(crate) fn register_ack(&self, corr_id: Uuid) -> Response<'_> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.as_ref()
            .acks
            .lock()
            .expect("Cannot be poisoned")
            .insert(corr_id, tx);

        Response::new(corr_id, self, rx)
    }

    pub(crate) fn unregister_ack(&self, corr_id: Uuid) -> Option<mpsc::SyncSender<packet::Reply>> {
        self.as_ref()
            .acks
            .lock()
            .expect("Cannot be poisoned")
            .remove(&corr_id)
    }
}

pub(crate) struct PeerWrite {
    stream: Mutex<TcpStream>,
    hb: Mutex<Instant>,
    pub(crate) v: u64,
}

impl PeerWrite {
    pub fn new(stream: TcpStream) -> Self {
        static VERSION: AtomicU64 = AtomicU64::new(0);

        Self {
            stream: Mutex::new(stream),
            hb: Mutex::new(Instant::now()),
            v: VERSION.fetch_add(1, Ordering::Relaxed),
        }
    }

    pub fn write(&self, packet: &Packet, timeout: Option<Duration>) -> io::Result<()> {
        let raw: Vec<u8> = packet.into();
        let mut stream = self.stream.lock().expect("Cannot be poisoned");
        stream.set_write_timeout(timeout)?;
        stream.write_all(raw.as_slice())
    }

    pub fn update_hb(&self) {
        let mut hb = self.hb.lock().expect("Cannot be poisoned");
        *hb = Instant::now();
    }

    pub fn hb(&self) -> Instant {
        *self.hb.lock().expect("Cannot be poisoned")
    }
}

pub(crate) struct PeerRead {
    stream: TcpStream,
}

impl PeerRead {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    pub fn read(&mut self, timeout: Option<Duration>) -> io::Result<Packet> {
        self.stream.set_read_timeout(timeout)?;
        let mut len_buffer: [u8; PACKET_LENGTH] = [0; PACKET_LENGTH];
        self.stream.read_exact(&mut len_buffer)?;
        let len: u32 = u32::from_le_bytes(len_buffer);

        let mut payload_buffer = vec![0u8; len as usize];
        self.stream.read_exact(&mut payload_buffer)?;

        Ok(Packet::from(payload_buffer.as_slice()))
    }

    pub fn start(
        mut self,
        registry: Registry,
        peer_id: Uuid,
        version: u64,
    ) -> io::Result<JoinHandle<()>> {
        thread::Builder::new()
            .name(format!("tcp-{}-{}", peer_id, version))
            .stack_size(128 * 1024)
            .spawn(move || {
                info!(id = %registry.as_ref().id, peer_id = %peer_id, v = %version, "tcp connection initialized");

                loop {
                    match registry.version(peer_id) {
                        Some(current) if current == version => {}
                        _ => break,
                    }

                    if !registry.as_ref().sigterm.checkpoint(None) {
                        break;
                    }

                    match self.read(Some(registry.as_ref().config.timeout.read)) {
                        Ok(packet) => {
                            let kind = packet.kind();
                            if matches!(packet, Packet::Request(packet::Request::Write(packet::Write::HeartBeat))) {
                                info!(id = %registry.as_ref().id, peer_id = %peer_id, v = %version, "Heartbeat packet received");
                                registry.update_hb(peer_id);
                            } else if let Err(err) =
                                registry.as_ref().tx_msg.send(Message::Packet { val: InboundPacket::new(peer_id, packet) })
                            {
                                error!(id = %registry.as_ref().id, peer_id = %peer_id, v = %version, kind = %kind, err = %err, "Packet enqueue failed");
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
                            if !matches!(
                                err.kind(),
                                io::ErrorKind::WouldBlock) {
                                error!(id = %registry.as_ref().id, peer_id = %peer_id, v = %version, kind = %err.kind(), err = %err, "Packet read failed");
                            }
                        }
                    }
                }

                info!(id = %registry.as_ref().id, peer_id = %peer_id, v = %version, "tcp connection destroyed");
            })
    }
}
