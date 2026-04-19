use crate::network::packet::{Kind, Reply};
use crate::{
    instance::{
        self, Shutdown,
        task::{self, Task},
    },
    network::{
        self,
        packet::{Event, Packet},
        registry::Registry,
    },
};
use std::collections::HashMap;
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex};
use std::{
    io::{self},
    net::{Ipv4Addr, SocketAddr},
    sync::mpsc::{self},
    thread::{self, JoinHandle},
    time::Duration,
};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

pub mod packet;
pub mod registry;
mod sched;

pub enum Discovery {
    Dns { host: String },
    List { addresses: Vec<SocketAddr> },
}

#[derive(Clone)]
pub struct AckHolder {
    entries: Arc<Mutex<HashMap<Uuid, SyncSender<packet::Raw>>>>,
}

impl Default for AckHolder {
    fn default() -> Self {
        Self::new()
    }
}

impl AckHolder {
    pub fn new() -> Self {
        Self {
            entries: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn register(&self, corr_id: Uuid, recv_timeout: Duration) -> Response<'_> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.entries
            .lock()
            .expect("ack correlation map mutex poisoned")
            .insert(corr_id, tx);

        Response::new(corr_id, self, rx, recv_timeout)
    }

    pub(crate) fn unregister(&self, corr_id: Uuid) -> Option<SyncSender<packet::Raw>> {
        self.entries
            .lock()
            .expect("ack correlation map mutex poisoned")
            .remove(&corr_id)
    }
}

pub struct Response<'a> {
    corr_id: Uuid,
    reg: &'a AckHolder,
    receiver: mpsc::Receiver<packet::Raw>,
    recv_timeout: Duration,
}

impl<'a> Response<'a> {
    pub(crate) fn new(
        corr_id: Uuid,
        reg: &'a AckHolder,
        receiver: mpsc::Receiver<packet::Raw>,
        recv_timeout: Duration,
    ) -> Self {
        Self {
            corr_id,
            reg,
            receiver,
            recv_timeout,
        }
    }

    pub fn get(&self) -> Result<packet::Raw, io::Error> {
        self.get_with_timeout(self.recv_timeout)
    }

    pub fn get_with_timeout(&self, timeout: Duration) -> Result<packet::Raw, io::Error> {
        self.receiver
            .recv_timeout(timeout)
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Timeout"))
    }
}

impl Drop for Response<'_> {
    fn drop(&mut self) {
        self.reg.unregister(self.corr_id);
    }
}

#[derive(Clone)]
pub struct Sender {
    registry: Registry,
    ack_holder: AckHolder,
}

impl Sender {
    pub(crate) fn read_timeout(&self) -> Duration {
        self.registry.as_ref().config.timeout.read
    }

    fn write_timeout(&self) -> Option<Duration> {
        Some(self.registry.as_ref().config.timeout.write)
    }

    pub fn request_sync(&self, id: Uuid, packet: packet::Request) -> io::Result<Response<'_>> {
        if let Some(corr_id) = packet.correlation_id() {
            let recv_timeout = self.read_timeout();
            let response = self.ack_holder.register(corr_id, recv_timeout);
            self.send_internal(id, Packet::Request(packet))?;
            Ok(response)
        } else {
            unreachable!("All packets must have informed correlation_id")
        }
    }

    pub fn request(&self, id: Uuid, packet: packet::Request) -> io::Result<()> {
        self.send_internal(id, Packet::Request(packet))
    }

    pub fn reply(&self, id: Uuid, packet: packet::Reply) -> io::Result<()> {
        self.send_internal(id, Packet::Reply(packet))
    }

    fn send_internal(&self, id: Uuid, packet: Packet) -> io::Result<()> {
        let timeout = self.write_timeout();
        let (result, v) = self.registry.with_writers_read(|writers| {
            if let Some(writer) = writers.get(&id) {
                match writer.write(&packet, timeout) {
                    Ok(()) => (Some(Ok(())), None),
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
                        (None, Some(writer.v))
                    }
                    Err(err) => (Some(Err(err)), None),
                }
            } else {
                (None, None)
            }
        });

        match result {
            Some(Err(err)) => {
                error!(id = %self.registry.as_ref().id, peer_id = %id, kind = %packet.kind(), err = %err, "Packet send failed");
                Err(err)
            }
            Some(Ok(_)) => {
                debug!(id = %self.registry.as_ref().id, peer_id = %id, kind = %packet.kind(),  "Packet send successfully");
                Ok(())
            }
            None => {
                let addr = match self.registry.addr(id) {
                    Some(addr) => addr,
                    None => {
                        warn!(id = %self.registry.as_ref().id, peer_id = %id, "Connection to peer failed due to no addr found");
                        return Err(io::Error::new(
                            io::ErrorKind::AddrNotAvailable,
                            "No peer connection found",
                        ));
                    }
                };

                if let Err(err) = self.registry.connect(&addr, v) {
                    error!(id = %self.registry.as_ref().id, peer_id= %id, err = %err, "Connection to peer failed");
                    return Err(err);
                }

                match self
                    .registry
                    .with_writers_read(|writers| match writers.get(&id) {
                        Some(w) => w.write(&packet, timeout),
                        None => Err(io::Error::new(
                            io::ErrorKind::NotConnected,
                            "No peer connection found",
                        )),
                    }) {
                    Err(err) => {
                        error!(id = %self.registry.as_ref().id, peer_id = %id, kind = %packet.kind(), err = %err, "Packet send failed");
                        Err(err)
                    }
                    Ok(_) => {
                        debug!(id = %self.registry.as_ref().id, peer_id = %id, kind = %packet.kind(),  "Packet send successfully");
                        Ok(())
                    }
                }
            }
        }
    }
}

#[derive(Eq, PartialEq, Debug)]
pub enum Message {
    Packet(packet::Inbound),
    Event(Event),
}

pub struct Receiver {
    rx_msg: mpsc::Receiver<Message>,
    ack_holder: AckHolder,
    registry: Registry,
}

impl Receiver {
    pub fn recv(&self, timeout: Option<Duration>) -> Option<Message> {
        match timeout {
            Some(val) => self.rx_msg.recv_timeout(val).ok(),
            None => Some(
                self.rx_msg
                    .recv()
                    .expect("network message channel closed (sender dropped before receiver)"),
            ),
        }
    }

    pub fn start(self, instance: instance::Weak) -> io::Result<JoinHandle<()>> {
        let inner = instance
            .as_ref()
            .upgrade()
            .expect("Instance dropped before packet receiver thread started");
        let id = inner.id;
        let task_executor = task::Executor::new(inner.config.task);

        thread::Builder::new()
            .name(format!("pckt-receiver-{}", id))
            .spawn(move || {
                info!(id = %id, "pckt-receiver started");
                loop {
                    if !self.registry.as_ref().sigterm.checkpoint(None) {
                        break;
                    }

                    if let Some(message) = self.recv(Some(Duration::from_secs(1))) {
                        match message {
                            Message::Packet(inbound) => match inbound.p.try_kind() {
                                Ok(kind) => match kind {
                                    Kind::WhoIsRequest
                                    | Kind::GetOpRequest
                                    | Kind::FetchObjectRequest => {
                                        task_executor.spawn(instance.clone(), Task::Read(inbound));
                                    }
                                    Kind::HeartBeatRequest
                                    | Kind::WriteOpRequest
                                    | Kind::PartitionFetchCompletionRequest => {
                                        task_executor.spawn(instance.clone(), Task::Write(inbound));
                                    }
                                    Kind::PartitionFetchCompletionReply
                                    | Kind::FetchObjectReply
                                    | Kind::GetOpReply
                                    | Kind::WriteOpReply
                                    | Kind::WhoIsReply => match Reply::try_from(&inbound.p) {
                                        Ok(packet) => {
                                            if let Some(corr_id) = packet.correlation_id()
                                                && let Some(entry) =
                                                    self.ack_holder.unregister(corr_id)
                                            {
                                                let _ = entry.try_send(inbound.p);
                                            }
                                        }
                                        Err(err) => {
                                            warn!(err = %err, "Invalid reply packet");
                                        }
                                    },
                                },
                                Err(err) => {
                                    warn!(err = %err, "Invalid packet discriminant");
                                }
                            },
                            Message::Event(event) => match event {
                                Event::PeerAdded { id } => {
                                    if let Some(ref_) = instance.as_ref().upgrade() {
                                        ref_.membership.add(id);
                                    }
                                }
                                Event::PeerRemoved { id } => {
                                    if let Some(ref_) = instance.as_ref().upgrade() {
                                        ref_.membership.remove(id);
                                    }
                                }
                                Event::Checkpoint => {
                                    task_executor.spawn(instance.clone(), Task::PartitionRebalance);
                                }
                            },
                        }
                    }
                }

                info!(id = %id, "pckt-receiver destroyed");
            })
    }
}

#[derive(Clone, Copy)]
pub struct Config {
    pub addr: SocketAddr,
    pub msg_buf_len: usize,
    pub timeout: TimeoutConfig,
    pub hb: HeartbeatConfig,
}

#[derive(Clone, Copy)]
pub struct TimeoutConfig {
    pub read: Duration,
    pub write: Duration,
    pub connect: Duration,
}

#[derive(Clone, Copy)]
pub struct HeartbeatConfig {
    pub poll_interval: Duration,
    pub tolerance: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 8080)),
            msg_buf_len: 1024,
            timeout: Default::default(),
            hb: Default::default(),
        }
    }
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            read: Duration::from_secs(2),
            write: Duration::from_secs(2),
            connect: Duration::from_secs(3),
        }
    }
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(5),
            tolerance: Duration::from_secs(30),
        }
    }
}

pub fn handle(
    id: Uuid,
    d: Discovery,
    shutdown: Shutdown,
    c: network::Config,
) -> io::Result<(Sender, Receiver)> {
    let (tx_msg, rx_msg) = mpsc::sync_channel(c.msg_buf_len);

    let registry = Registry::new(id, c, tx_msg, shutdown.clone());

    shutdown.register(sched::listener(c, registry.clone())?);
    shutdown.register(sched::hb(c, d, registry.clone())?);

    let ack_holder = AckHolder::new();

    let net_tx = Sender {
        registry: registry.clone(),
        ack_holder: ack_holder.clone(),
    };
    let net_rx = Receiver {
        rx_msg,
        ack_holder,
        registry,
    };

    Ok((net_tx, net_rx))
}
