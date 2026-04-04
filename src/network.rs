use std::{
    io::{self},
    net::{Ipv4Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::{
    instance::{
        self,
        task::{self, Task},
    },
    network::{
        self,
        packet::{Event, InboundPacket, Packet},
        registry::{Registry, Response},
    },
};

pub mod packet;
mod registry;
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
            todo!("Must return error if not informed correlation_id")
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
    sigterm: Arc<AtomicBool>,
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
                    if self.sigterm.load(Ordering::Acquire) {
                        break;
                    }

                    if let Some(message) = self.recv(Some(Duration::from_secs(1))) {
                        match message {
                            Message::Packet { val } => {
                                if val.p.is_read() {
                                    task_executor
                                        .spawn(instance.clone(), Task::Read { packet: val });
                                } else if val.p.is_write() {
                                    task_executor
                                        .spawn(instance.clone(), Task::Write { packet: val });
                                } else {
                                    match val.p {
                                        Packet::WhoIs { .. }
                                        | Packet::WhoIsReply { .. }
                                        | Packet::HeartBeat
                                        | Packet::WriteOp { .. }
                                        | Packet::GetOp { .. } => {}

                                        Packet::WriteOpReply { corr_id, .. }
                                        | Packet::GetOpReply { corr_id, .. } => {
                                            if let Some(entry) =
                                                self.registry.unregister_ack(corr_id)
                                            {
                                                let _ = entry.try_send(val.p);
                                            }
                                        }
                                    }
                                }
                            }
                            Message::Event { val } => match val {
                                Event::PeerAdded { id } => {
                                    if let Some(ref_) = instance.as_ref().upgrade() {
                                        ref_.add_member(id);
                                        task_executor
                                            .spawn(instance.clone(), Task::PartitionRebalance);
                                    }
                                }
                                Event::PeerRemoved { id } => {
                                    if let Some(ref_) = instance.as_ref().upgrade() {
                                        ref_.remove_member(id);
                                        task_executor
                                            .spawn(instance.clone(), Task::PartitionRebalance);
                                    }
                                }
                            },
                        }
                    }
                }

                info!(id = %id, "pckt-receiver destroyed");
            })
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        self.sigterm.store(true, Ordering::Release);
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

pub fn handle(id: Uuid, d: Discovery, c: network::Config) -> io::Result<(Sender, Receiver)> {
    let (tx_msg, rx_msg) = mpsc::sync_channel(c.msg_buf_len);

    let sigterm = Arc::new(AtomicBool::new(false));
    let registry = Registry::new(id, c, tx_msg, Arc::clone(&sigterm));

    sched::listener(c, registry.clone())?;
    sched::hb(c, d, registry.clone())?;

    let net_tx = Sender {
        registry: registry.clone(),
    };
    let net_rx = Receiver {
        rx_msg,
        registry,
        sigterm,
    };

    Ok((net_tx, net_rx))
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        net::{Ipv4Addr, SocketAddr},
        time::Duration,
    };

    use tracing_subscriber::FmtSubscriber;
    use uuid::Uuid;

    use crate::network::{self, Discovery, Event, Message, Receiver, Sender, handle};

    #[test]
    pub fn peers_dis_connect_successfully() -> io::Result<()> {
        let (net_1, net_2) = setup()?;

        drop(net_2);
        assert_eq!(
            Some(Message::Event {
                val: Event::PeerRemoved {
                    id: Uuid::from_u128(2)
                }
            }),
            net_1.1.recv(None)
        );

        Ok(())
    }

    fn setup() -> io::Result<((Sender, Receiver), (Sender, Receiver))> {
        let subscriber = FmtSubscriber::new();
        let _ = tracing::subscriber::set_global_default(subscriber);

        let id = Uuid::from_u128(1);
        let id_2 = Uuid::from_u128(2);

        let config = network::Config {
            hb: network::HeartbeatConfig {
                poll_interval: Duration::from_secs(1),
                tolerance: Duration::from_secs(3),
            },
            ..Default::default()
        };

        let config_2 = network::Config {
            addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 8081)),
            hb: network::HeartbeatConfig {
                poll_interval: Duration::from_secs(1),
                tolerance: Duration::from_secs(3),
            },
            ..Default::default()
        };

        let (tx, rx) = handle(
            id,
            Discovery::List {
                addresses: vec![
                    SocketAddr::from((Ipv4Addr::LOCALHOST, 8080)),
                    SocketAddr::from((Ipv4Addr::LOCALHOST, 8081)),
                ],
            },
            config,
        )?;

        let (tx_2, rx_2) = handle(
            id_2,
            Discovery::List {
                addresses: vec![
                    SocketAddr::from((Ipv4Addr::LOCALHOST, 8080)),
                    SocketAddr::from((Ipv4Addr::LOCALHOST, 8081)),
                ],
            },
            config_2,
        )?;

        assert_eq!(
            Some(Message::Event {
                val: Event::PeerAdded { id: id_2 }
            }),
            rx.recv(None)
        );

        assert_eq!(
            Some(Message::Event {
                val: Event::PeerAdded { id }
            }),
            rx_2.recv(None)
        );

        Ok(((tx, rx), (tx_2, rx_2)))
    }
}
