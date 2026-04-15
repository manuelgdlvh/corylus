use std::{
    io::{self},
    net::{Ipv4Addr, SocketAddr},
    sync::mpsc::{self},
    thread::{self, JoinHandle},
    time::Duration,
};

use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::{
    instance::{
        self, Shutdown,
        task::{self, Task},
    },
    network::{
        self,
        packet::{Event, InboundPacket, Packet},
        registry::{Registry, Response},
    },
};

pub mod packet;
pub mod registry;
mod sched;

pub enum Discovery {
    Dns { host: String },
    List { addresses: Vec<SocketAddr> },
}

#[derive(Clone)]
pub struct Sender {
    registry: Registry,
}

impl Sender {
    pub fn sync_send(
        &self,
        id: Uuid,
        packet: Packet,
        timeout: Option<Duration>,
    ) -> io::Result<Response<'_>> {
        if let Some(corr_id) = packet.correlation_id() {
            let response = self.registry.register_ack(corr_id);
            self.send(id, packet, timeout)?;
            Ok(response)
        } else {
            unreachable!("All packets must have informed correlation_id")
        }
    }

    pub fn send(&self, id: Uuid, packet: Packet, timeout: Option<Duration>) -> io::Result<()> {
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
    Packet { val: InboundPacket },
    Event { val: Event },
}

pub struct Receiver {
    rx_msg: mpsc::Receiver<Message>,
    registry: Registry,
}

impl Receiver {
    pub fn recv(&self, timeout: Option<Duration>) -> Option<Message> {
        match timeout {
            Some(val) => self.rx_msg.recv_timeout(val).ok(),
            None => Some(
                self.rx_msg
                    .recv()
                    .expect("There is always a sender available"),
            ),
        }
    }

    pub fn start(self, instance: instance::Weak) -> io::Result<JoinHandle<()>> {
        let task_executor = task::Executor::new();

        let id = instance
            .as_ref()
            .upgrade()
            .expect("No possibility to be destroyed yet")
            .id;

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
                            Message::Packet { val } => match val.p {
                                Packet::Request(req) => match req {
                                    packet::Request::Read(r) => {
                                        task_executor.spawn(
                                            instance.clone(),
                                            Task::Read {
                                                from: val.from,
                                                packet: r,
                                            },
                                        );
                                    }
                                    packet::Request::Write(w) => {
                                        task_executor.spawn(
                                            instance.clone(),
                                            Task::Write {
                                                from: val.from,
                                                packet: w,
                                            },
                                        );
                                    }
                                },
                                Packet::Reply(reply) => {
                                    if let Some(corr_id) = reply.correlation_id()
                                        && let Some(entry) = self.registry.unregister_ack(corr_id)
                                    {
                                        let _ = entry.try_send(reply);
                                    }
                                }
                            },
                            Message::Event { val } => match val {
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

    let net_tx = Sender {
        registry: registry.clone(),
    };
    let net_rx = Receiver { rx_msg, registry };

    Ok((net_tx, net_rx))
}
