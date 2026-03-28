use std::{
    collections::HashMap,
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, SyncSender},
    },
    thread::{self},
    time::Duration,
};

use uuid::Uuid;

// TODO: Add auth

pub struct NetSender {
    registry: Arc<Registry>,
}

impl NetSender {
    pub fn send(&self, id: Uuid, packet: Packet, timeout: Option<Duration>) -> io::Result<()> {
        // TODO: Detected if removed or stale to just replace or remove
        {
            let writers = self.registry.writers.read().expect("Cannot be poisoned");
            if let Some(writer) = writers.get(&id) {
                return writer.write(packet, timeout);
            }
        }

        let peer_addr = {
            let addrs = self.registry.addrs.lock().expect("Cannot be poisoned");
            match addrs.get(&id) {
                Some(addr) => addr.clone(),
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::NotConnected,
                        "No peer connection found",
                    ));
                }
            }
        };

        self.registry.connect(&peer_addr)?;

        let writers = self.registry.writers.read().expect("Cannot be poisoned");
        match writers.get(&id) {
            Some(w) => w.write(packet, timeout),
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "No peer connection found",
                ));
            }
        }
    }
}

pub struct NetReceiver {
    rx: mpsc::Receiver<Packet>,
    sigterm: Arc<AtomicBool>,
}

impl NetReceiver {
    pub fn recv(&self) -> Packet {
        self.rx.recv().expect("There is always a sender")
    }
}

impl Drop for NetReceiver {
    fn drop(&mut self) {
        self.sigterm.store(true, Ordering::Release);
    }
}

pub fn handle(
    id: Uuid,
    addr: SocketAddr,
    d: Discovery,
    buffer_len: usize,
) -> io::Result<(NetSender, NetReceiver)> {
    let (tx, rx) = mpsc::sync_channel(buffer_len);

    let sigterm = Arc::new(AtomicBool::new(false));
    let registry = Arc::new(Registry::new(id, addr, tx.clone(), Arc::clone(&sigterm)));

    listener(Arc::clone(&registry))?;
    discovery(d, Arc::clone(&registry));

    let net_tx = NetSender { registry };
    let net_rx = NetReceiver { rx, sigterm };

    Ok((net_tx, net_rx))
}

fn discovery(d: Discovery, registry: Arc<Registry>) {
    thread::spawn(move || {
        loop {
            if registry.sigterm.load(Ordering::Relaxed) {
                break;
            }

            let addrs = match &d {
                Discovery::DNS { host } => Vec::new(),
                Discovery::FIXED { addresses } => addresses.clone(),
            }
            .into_iter()
            .filter(|peer_addr| !peer_addr.eq(&registry.addr))
            .filter(|peer_addr| registry.peer_id_from_addr(peer_addr).is_none())
            .collect::<Vec<_>>();

            for addr in addrs.iter() {
                if let Err(err) = registry.connect(addr) {
                    eprintln!("{:?}", err);
                }
            }

            thread::sleep(Duration::from_secs(3));
        }
    });
}

fn listener(registry: Arc<Registry>) -> io::Result<()> {
    let listener = TcpListener::bind(registry.addr)?;
    thread::spawn(move || {
        loop {
            if registry.sigterm.load(Ordering::Acquire) {
                break;
            }

            if let Ok((stream, _)) = listener.accept() {
                let read_stream = match stream.try_clone() {
                    Ok(v) => v,
                    Err(err) => {
                        eprintln!("{:?}", err);
                        continue;
                    }
                };
                let mut r = PeerRead::new(registry.tx.clone(), read_stream);
                let w = PeerWrite::new(stream);

                // Wait to receive peer node id
                let who_is_req = match r.read(Some(Duration::from_secs(3))) {
                    Ok(Some(packet)) => packet,
                    Ok(None) => {
                        eprintln!("No received WhoIsReply packet");
                        continue;
                    }
                    Err(err) => {
                        eprintln!("{:?}", err);
                        continue;
                    }
                };

                let (peer_id, peer_addr) = match who_is_req {
                    Packet::WhoIs { id, addr } => (id, addr),
                    _ => {
                        eprintln!("WhoIs packet don't received");
                        continue;
                    }
                };

                // Only higher id are allowed to establish connections.
                if registry.id >= peer_id {
                    eprintln!(
                        "Connection don't allowed. Origin: {}, Local: {}",
                        peer_id, registry.id
                    );
                    continue;
                }

                eprintln!(
                    "Connection allowed. Origin: {}, Local: {}",
                    peer_id, registry.id
                );

                if let Err(err) = w.write(
                    Packet::WhoIsReply { id: registry.id },
                    Some(Duration::from_secs(3)),
                ) {
                    eprintln!("{:?}", err);
                    continue;
                }

                match registry.register(peer_id, &peer_addr, w) {
                    Ok(v) => {
                        r.start(Arc::clone(&registry), peer_id, v);
                    }
                    Err(err) => {
                        eprintln!("{:?}", err);
                    }
                }
            }
        }
    });

    Ok(())
}

pub enum Discovery {
    DNS { host: String },
    FIXED { addresses: Vec<SocketAddr> },
}

struct Registry {
    id: Uuid,
    addr: SocketAddr,
    tx: SyncSender<Packet>,
    sigterm: Arc<AtomicBool>,
    addrs: Mutex<HashMap<Uuid, SocketAddr>>,
    writers: RwLock<HashMap<Uuid, PeerWrite>>,
    versions: RwLock<HashMap<Uuid, u64>>,
}

impl Registry {
    pub fn new(
        id: Uuid,
        addr: SocketAddr,
        tx: SyncSender<Packet>,
        sigterm: Arc<AtomicBool>,
    ) -> Self {
        Self {
            id,
            addr,
            tx,
            sigterm,
            addrs: Mutex::new(HashMap::new()),
            writers: RwLock::new(HashMap::new()),
            versions: RwLock::new(HashMap::new()),
        }
    }

    pub fn register(
        &self,
        peer_id: Uuid,
        peer_addr: &SocketAddr,
        writer: PeerWrite,
    ) -> io::Result<u64> {
        self.addrs
            .lock()
            .expect("Cannot be poisoned")
            .insert(peer_id, *peer_addr);
        let v = match self
            .versions
            .write()
            .expect("Cannot be poisoned")
            .entry(peer_id)
        {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let v = *entry.get() + 1;
                entry.insert(v);
                v
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(1);
                1
            }
        };

        self.writers
            .write()
            .expect("Cannot be poisoned")
            .insert(peer_id, writer);
        Ok(v)
    }

    pub fn unregister(&self, peer_id: Uuid) {
        self.addrs
            .lock()
            .expect("Cannot be poisoned")
            .remove(&peer_id);
        self.writers
            .write()
            .expect("Cannot be poisoned")
            .remove(&peer_id);
        self.versions
            .write()
            .expect("Cannot be poisoned")
            .remove(&peer_id);
    }

    pub fn version(&self, id: Uuid) -> Option<u64> {
        self.versions
            .read()
            .expect("Cannot be poisoned")
            .get(&id)
            .map(|v| *v)
    }

    pub fn connect(self: &Arc<Self>, peer_addr: &SocketAddr) -> io::Result<()> {
        eprintln!("{} trying to connect to {} addr", self.id, peer_addr);
        let stream = TcpStream::connect(peer_addr)?;
        let mut r = PeerRead::new(self.tx.clone(), stream.try_clone()?);
        let w = PeerWrite::new(stream);
        w.write(
            Packet::WhoIs {
                id: self.id,
                addr: self.addr,
            },
            Some(Duration::from_secs(3)),
        )?;

        if let Some(reply) = r.read(Some(Duration::from_secs(3)))? {
            match reply {
                Packet::WhoIsReply { id } => {
                    let version = self.register(id, peer_addr, w)?;
                    r.start(Arc::clone(self), id, version);
                    Ok(())
                }
                _ => Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "Who Is reply don't received",
                )),
            }
        } else {
            Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "Who Is reply don't received",
            ))
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
}

// TODO: Setup timeout in all interactions.

struct PeerWrite {
    stream: Mutex<TcpStream>,
}

impl PeerWrite {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: Mutex::new(stream),
        }
    }

    pub fn write(&self, packet: Packet, timeout: Option<Duration>) -> io::Result<()> {
        let raw: Vec<u8> = packet.into();
        let mut stream = self.stream.lock().expect("Cannot be poisoned");
        stream.set_write_timeout(timeout)?;
        stream.write_all(raw.as_slice())
    }
}

struct PeerRead {
    tx: SyncSender<Packet>,
    stream: TcpStream,
}

const DISCRIMINANT_LENGTH: usize = 1;
const PACKET_LENGTH: usize = 4;

impl PeerRead {
    pub fn new(tx: SyncSender<Packet>, stream: TcpStream) -> Self {
        Self { tx, stream }
    }

    pub fn read(&mut self, timeout: Option<Duration>) -> io::Result<Option<Packet>> {
        self.stream.set_read_timeout(timeout)?;
        let mut len_buffer: [u8; PACKET_LENGTH] = [0; PACKET_LENGTH];
        self.stream.read_exact(&mut len_buffer)?;
        let len: u32 = u32::from_le_bytes(len_buffer);

        let mut payload_buffer = vec![0u8; len as usize];
        self.stream.read_exact(&mut payload_buffer)?;

        Ok(Some(Packet::from(payload_buffer.as_slice())))
    }

    pub fn start(mut self, registry: Arc<Registry>, peer_id: Uuid, version: u64) {
        thread::spawn(move || {
            loop {
                match registry.version(peer_id) {
                    Some(current) if current == version => {}
                    _ => return,
                }

                if registry.sigterm.load(Ordering::Acquire) {
                    return;
                }

                match self.read(None) {
                    Ok(Some(packet)) => {
                        if let Err(err) = self.tx.send(packet) {
                            eprintln!("{:?}", err);
                        }
                    }
                    _ => {}
                }
            }
        });
    }
}

// TODO: Use Buffer pool for read/write packets

// Request packets
#[derive(Debug, Eq, PartialEq)]
pub enum Packet {
    WhoIs { id: Uuid, addr: SocketAddr },
    WhoIsReply { id: Uuid },
    HeartBeat,
    HeartBeatReply,
}

impl Packet {
    pub fn kind(&self) -> u8 {
        match self {
            Self::WhoIs { .. } => 1,
            Self::WhoIsReply { .. } => 2,
            Self::HeartBeat => 3,
            Self::HeartBeatReply => 4,
        }
    }

    pub fn len(discriminant: u8) -> Option<usize> {
        match discriminant {
            1 => None,
            2 => Some(16),
            3 => Some(0),
            4 => Some(0),
            _ => None,
        }
    }

    pub fn reserve_buffer(discriminant: u8) -> Vec<u8> {
        const DISCRIMINANT: usize = 1;
        const DEFAULT_LEN: usize = 256;
        Vec::with_capacity(
            PACKET_LENGTH + DISCRIMINANT + Packet::len(discriminant).unwrap_or(DEFAULT_LEN),
        )
    }
}

impl From<Packet> for Vec<u8> {
    fn from(packet: Packet) -> Self {
        let discriminant = packet.kind();
        let mut buffer = Packet::reserve_buffer(packet.kind());

        let mut filled = false;
        if let Some(len) = Packet::len(discriminant) {
            buffer.extend_from_slice(&((len + DISCRIMINANT_LENGTH) as u32).to_le_bytes());
            filled = true;
        }

        buffer.push(discriminant);
        match packet {
            Packet::WhoIs { id, addr } => {
                buffer.extend_from_slice(id.as_bytes());
                let addr = addr.to_string().into_bytes();
                buffer.extend_from_slice(addr.as_slice());
            }
            Packet::WhoIsReply { id } => {
                buffer.extend_from_slice(id.as_bytes());
            }
            Packet::HeartBeat | Packet::HeartBeatReply => {}
        }

        if filled {
            buffer
        } else {
            let mut new_buffer = Vec::with_capacity(PACKET_LENGTH + buffer.len());
            new_buffer.extend_from_slice(&(buffer.len() as u32).to_le_bytes());
            new_buffer.append(&mut buffer);
            new_buffer
        }
    }
}

impl From<&[u8]> for Packet {
    fn from(value: &[u8]) -> Self {
        let discriminant = value.get(0).unwrap();
        match *discriminant {
            1 => {
                let id = Uuid::from_slice(&value[1..=16]).unwrap();
                let addr: SocketAddr = str::from_utf8(&value[17..]).unwrap().parse().unwrap();
                Packet::WhoIs { id, addr }
            }
            2 => Packet::WhoIsReply {
                id: Uuid::from_slice(&value[1..]).unwrap(),
            },
            3 => Packet::HeartBeat,
            4 => Packet::HeartBeatReply,
            _ => {
                panic!("Unknown packet");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        net::{Ipv4Addr, SocketAddr},
        thread,
        time::{Duration, Instant},
    };

    use uuid::Uuid;

    use crate::network::{Discovery, Packet, handle};

    #[test]
    pub fn members_connect_successfully() -> io::Result<()> {
        let (tx, rx) = handle(
            Uuid::from_u128(1),
            SocketAddr::from((Ipv4Addr::LOCALHOST, 8080)),
            Discovery::FIXED {
                addresses: vec![
                    SocketAddr::from((Ipv4Addr::LOCALHOST, 8080)),
                    SocketAddr::from((Ipv4Addr::LOCALHOST, 8081)),
                ],
            },
            1024,
        )?;

        let (tx_2, rx_2) = handle(
            Uuid::from_u128(2),
            SocketAddr::from((Ipv4Addr::LOCALHOST, 8081)),
            Discovery::FIXED {
                addresses: vec![
                    SocketAddr::from((Ipv4Addr::LOCALHOST, 8080)),
                    SocketAddr::from((Ipv4Addr::LOCALHOST, 8081)),
                ],
            },
            1024,
        )?;

        wait_until_ready(Duration::from_millis(100), Duration::from_secs(10), || {
            let addr_registered = tx
                .registry
                .peer_id_from_addr(&SocketAddr::from((Ipv4Addr::LOCALHOST, 8081)))
                .is_some_and(|id| Uuid::from_u128(2).eq(&id));
            let connected = tx
                .registry
                .writers
                .read()
                .expect("Cannot be poisoned")
                .contains_key(&Uuid::from_u128(2));
            addr_registered && connected
        });

        wait_until_ready(Duration::from_millis(100), Duration::from_secs(10), || {
            let addr_registered = tx_2
                .registry
                .peer_id_from_addr(&SocketAddr::from((Ipv4Addr::LOCALHOST, 8080)))
                .is_some_and(|id| Uuid::from_u128(1).eq(&id));

            let connected = tx_2
                .registry
                .writers
                .read()
                .expect("Cannot be poisoned")
                .contains_key(&Uuid::from_u128(1));
            addr_registered && connected
        });

        tx.send(Uuid::from_u128(2), Packet::HeartBeat, None)?;
        assert_eq!(Packet::HeartBeat, rx_2.recv());

        tx_2.send(Uuid::from_u128(1), Packet::HeartBeat, None)?;
        assert_eq!(Packet::HeartBeat, rx.recv());
        Ok(())
    }

    fn wait_until_ready<F: Fn() -> bool>(poll_interval: Duration, max_wait: Duration, is_ready: F) {
        let start = Instant::now();

        loop {
            if is_ready() {
                return;
            }

            if start.elapsed() >= max_wait {
                assert!(false, "timeout waiting for ready");
            }

            thread::sleep(poll_interval);
        }
    }
}
