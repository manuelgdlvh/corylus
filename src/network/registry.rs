use std::{
    array,
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    io::{self, Read, Write},
    net::{SocketAddr, TcpStream},
    sync::{
        Arc, Mutex, RwLock, RwLockReadGuard,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::SyncSender,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
    u64,
};

use tracing::{error, info};
use uuid::Uuid;

use crate::network::{
    self,
    packet::{Event, InboundPacket, PACKET_LENGTH, Packet},
};

const CONN_STRIPES_LEN: usize = 8;

pub(crate) struct Registry {
    pub(crate) config: network::Config,
    tx_pckt: SyncSender<InboundPacket>,
    tx_event: SyncSender<Event>,
    pub(crate) sigterm: Arc<AtomicBool>,
    addrs: Mutex<HashMap<Uuid, SocketAddr>>,
    writers: RwLock<HashMap<Uuid, PeerWrite>>,
    conn_stripes: [Mutex<()>; CONN_STRIPES_LEN],
}

impl Registry {
    pub fn new(
        config: network::Config,
        tx_pckt: SyncSender<InboundPacket>,
        tx_event: SyncSender<Event>,
        sigterm: Arc<AtomicBool>,
    ) -> Self {
        Self {
            config,
            tx_pckt,
            tx_event,
            sigterm,
            addrs: Mutex::new(HashMap::new()),
            writers: RwLock::new(HashMap::new()),
            conn_stripes: array::from_fn(|_| Mutex::new(())),
        }
    }

    // Listener accepts connection single threaded, so we always have linearizability here.
    pub fn register(&self, peer_id: Uuid, peer_addr: &SocketAddr, writer: PeerWrite) {
        let old_addr = self
            .addrs
            .lock()
            .expect("Cannot be poisoned")
            .insert(peer_id, *peer_addr);
        self.writers
            .write()
            .expect("Cannot be poisoned")
            .insert(peer_id, writer);
        if old_addr.is_none()
            && let Err(err) = self.tx_event.send(Event::PeerAdded { id: peer_id }) {
                error!(err = %err, "Peer added event enqueue failed");
            }
    }

    pub fn unregister(&self, peer_id: Uuid, v: u64) {
        match self
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
            std::collections::hash_map::Entry::Vacant(_) => {}
        }

        self.addrs
            .lock()
            .expect("Cannot be poisoned")
            .remove_entry(&peer_id);
        let _ = self.tx_event.send(Event::PeerRemoved { id: peer_id });

        info!(id = %self.config.id, peer_id = %peer_id, "Peer disconnected successfully");
    }

    pub fn version(&self, id: Uuid) -> Option<u64> {
        self.writers
            .read()
            .expect("Cannot be poisoned")
            .get(&id)
            .map(|w| w.v)
    }

    pub fn connect_from_id(self: &Arc<Self>, id: Uuid, v: Option<u64>) -> io::Result<()> {
        if let Some(addr) = self.addr(id) {
            self.connect(&addr, v)
        } else {
            Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "No peer connection found",
            ))
        }
    }

    pub fn connect(self: &Arc<Self>, peer_addr: &SocketAddr, v: Option<u64>) -> io::Result<()> {
        let mut hasher = DefaultHasher::new();
        peer_addr.hash(&mut hasher);
        let id = (hasher.finish() as usize) & (CONN_STRIPES_LEN - 1);

        let conn_lock = &self.conn_stripes[id];
        let _guard = conn_lock.lock().expect("Cannot be poisoned");
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

        let stream = TcpStream::connect_timeout(peer_addr, self.config.timeout.connect)?;
        let mut r = PeerRead::new(stream.try_clone()?);
        let w = PeerWrite::new(stream);
        w.write(
            &Packet::WhoIs {
                id: self.config.id,
                addr: self.config.addr,
            },
            Some(self.config.timeout.write),
        )?;

        match r.read(Some(self.config.timeout.read))? {
            Packet::WhoIsReply { id } => {
                let v = w.v;
                self.register(id, peer_addr, w);
                r.start(Arc::clone(self), id, v).map(|_| ())
            }
            _ => Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "Who Is reply don't received",
            )),
        }
    }

    fn peer_id_from_addr(&self, addr: &SocketAddr) -> Option<Uuid> {
        let addrs = self.addrs.lock().expect("Cannot be poisoned");
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
        self.addrs
            .lock()
            .expect("Cannot be poisoned")
            .get(&id).copied()
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
        self.with_writers_read(|writers| {
            writers.get(&id).map(|writer| writer.hb())
        })
    }

    pub(crate) fn with_writers_read<
        O,
        F: Fn(RwLockReadGuard<'_, HashMap<Uuid, PeerWrite>>) -> O,
    >(
        &self,
        f: F,
    ) -> O {
        let guard = self.writers.read().expect("Cannot be poisoned");
        f(guard)
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
        registry: Arc<Registry>,
        peer_id: Uuid,
        version: u64,
    ) -> io::Result<JoinHandle<()>> {
        thread::Builder::new()
            .name(format!("tcp-{}-{}", peer_id, version))
            .stack_size(128 * 1024)
            .spawn(move || {
                info!(id = %registry.config.id, peer_id = %peer_id, v = %version, "tcp connection initialized");

                loop {
                    match registry.version(peer_id) {
                        Some(current) if current == version => {}
                        _ => break,
                    }

                    if registry.sigterm.load(Ordering::Acquire) {
                        break;
                    }

                    match self.read(Some(registry.config.timeout.read)) {
                        Ok(packet) => {
                            let kind = packet.kind();
                            if matches!(packet, Packet::HeartBeat) {
                                info!(id = %registry.config.id, peer_id = %peer_id, v = %version, "Heartbeat packet received");
                                registry.update_hb(peer_id);
                            } else if let Err(err) =
                                registry.tx_pckt.send(InboundPacket::new(peer_id, packet))
                            {
                            error!(id = %registry.config.id, peer_id = %peer_id, v = %version, kind = %kind, err = %err, "Packet enqueue failed");
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
                            error!(id = %registry.config.id, peer_id = %peer_id, v = %version, err = %err, "Packet read failed");
                        }
                    }
                }

                info!(id = %registry.config.id, peer_id = %peer_id, v = %version, "tcp connection destroyed");
            })
    }
}
