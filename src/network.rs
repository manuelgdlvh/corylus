use std::{
    io::{self},
    net::{Ipv4Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self},
    },
    time::Duration,
};

use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::network::{
    packet::{Event, InboundPacket, Packet},
    registry::Registry,
    sched::Discovery,
};

pub mod packet;
mod registry;
mod sched;

#[derive(Clone)]
pub struct Sender {
    registry: Arc<Registry>,
}

impl Sender {
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
                error!(id = %self.registry.config.id, peer_id = %id, kind = %packet.kind(), err = %err, "Packet send failed");
                Err(err)
            }
            Some(Ok(_)) => {
                debug!(id = %self.registry.config.id, peer_id = %id, kind = %packet.kind(),  "Packet send successfully");
                Ok(())
            }
            None => {
                let addr = match self.registry.addr(id) {
                    Some(addr) => addr,
                    None => {
                        warn!(id = %self.registry.config.id, peer_id = %id, "Connection to peer failed due to no addr found");
                        return Err(io::Error::new(
                            io::ErrorKind::AddrNotAvailable,
                            "No peer connection found",
                        ));
                    }
                };

                if let Err(err) = self.registry.connect(&addr, v) {
                    error!(id = %self.registry.config.id, peer_id= %id, err = %err, "Connection to peer failed");
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
                        error!(id = %self.registry.config.id, peer_id = %id, kind = %packet.kind(), err = %err, "Packet send failed");
                        Err(err)
                    }
                    Ok(_) => {
                        debug!(id = %self.registry.config.id, peer_id = %id, kind = %packet.kind(),  "Packet send successfully");
                        Ok(())
                    }
                }
            }
        }
    }
}

pub struct Receiver {
    rx_pckt: mpsc::Receiver<InboundPacket>,
    rx_event: mpsc::Receiver<Event>,
    sigterm: Arc<AtomicBool>,
}

impl Receiver {
    pub fn recv_packet(&self, timeout: Option<Duration>) -> Option<InboundPacket> {
        match timeout {
            Some(val) => self.rx_pckt.recv_timeout(val).ok(),
            None => Some(
                self.rx_pckt
                    .recv()
                    .expect("There is always a sender available"),
            ),
        }
    }

    pub fn recv_event(&self, timeout: Option<Duration>) -> Option<Event> {
        match timeout {
            Some(val) => self.rx_event.recv_timeout(val).ok(),
            None => Some(
                self.rx_event
                    .recv()
                    .expect("There is always a sender available"),
            ),
        }
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        self.sigterm.store(true, Ordering::Release);
    }
}

#[derive(Clone, Copy)]
pub struct Config {
    pub id: Uuid,
    pub addr: SocketAddr,
    pub pckt_buf_len: usize,
    pub event_buf_len: usize,
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
            id: Uuid::from_u128(1),
            addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 8080)),
            pckt_buf_len: 1024,
            event_buf_len: 1024,
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

pub fn handle(config: Config, d: Discovery) -> io::Result<(Sender, Receiver)> {
    let (tx_pckt, rx_pckt) = mpsc::sync_channel(config.pckt_buf_len);
    let (tx_event, rx_event) = mpsc::sync_channel(config.event_buf_len);

    let sigterm = Arc::new(AtomicBool::new(false));
    let registry = Arc::new(Registry::new(
        config,
        tx_pckt,
        tx_event,
        Arc::clone(&sigterm),
    ));

    sched::listener(config, Arc::clone(&registry))?;
    sched::hb(config, d, Arc::clone(&registry))?;

    let net_tx = Sender { registry };
    let net_rx = Receiver {
        rx_pckt,
        rx_event,
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

    use crate::network::{self, Discovery, Event, handle};

    #[test]
    pub fn peers_dis_connect_successfully() -> io::Result<()> {
        let subscriber = FmtSubscriber::new();
        tracing::subscriber::set_global_default(subscriber)
            .expect("Failed to set global subscriber");

        let config = network::Config {
            hb: network::HeartbeatConfig {
                poll_interval: Duration::from_secs(1),
                tolerance: Duration::from_secs(3),
            },
            ..Default::default()
        };

        let config_2 = network::Config {
            id: Uuid::from_u128(2),
            addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 8081)),
            hb: network::HeartbeatConfig {
                poll_interval: Duration::from_secs(1),
                tolerance: Duration::from_secs(3),
            },
            ..Default::default()
        };

        let (tx, rx) = handle(
            config,
            Discovery::List {
                addresses: vec![
                    SocketAddr::from((Ipv4Addr::LOCALHOST, 8080)),
                    SocketAddr::from((Ipv4Addr::LOCALHOST, 8081)),
                ],
            },
        )?;

        {
            let (tx_2, rx_2) = handle(
                config_2,
                Discovery::List {
                    addresses: vec![
                        SocketAddr::from((Ipv4Addr::LOCALHOST, 8080)),
                        SocketAddr::from((Ipv4Addr::LOCALHOST, 8081)),
                    ],
                },
            )?;

            assert_eq!(
                Some(Event::PeerAdded { id: config_2.id }),
                rx.recv_event(None)
            );
            assert_eq!(
                Some(Event::PeerAdded { id: config.id }),
                rx_2.recv_event(None)
            );
        }

        assert_eq!(
            Some(Event::PeerRemoved { id: config_2.id }),
            rx.recv_event(None)
        );

        Ok(())
    }
}
