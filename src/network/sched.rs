use std::{
    borrow::Cow,
    collections::HashMap,
    io,
    net::TcpListener,
    sync::atomic::Ordering,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use rand::RngExt;
use tracing::{error, info};

use crate::network::{
    self, Discovery,
    packet::Packet,
    registry::{PeerRead, PeerWrite, Registry},
};

pub(crate) fn hb(
    config: network::Config,
    d: Discovery,
    registry: Registry,
) -> io::Result<JoinHandle<()>> {
    thread::Builder::new()
        .name("heartbeat".to_string())
        .spawn(move || {
            let poll_interval = config.hb.poll_interval;
            let hb_tolerance = config.hb.tolerance;

            info!(id = %registry.as_ref().id, "Heartbeat scheduler initialized");
            loop {
                if registry.as_ref().sigterm.load(Ordering::Acquire) {
                    break;
                }

                let rng = rand::rng().random_range(0.75..=1.0);
                let millis = poll_interval.as_millis() as f64;
                let jitter = millis * rng;
                thread::sleep(Duration::from_millis(jitter as u64));

                match &d {
                    Discovery::Dns { .. } => {
                        todo!()
                    }
                    Discovery::List { addresses } => Cow::Borrowed(addresses.as_slice()),
                }
                .iter()
                .filter(|addr| !registry.as_ref().config.addr.eq(addr))
                .filter(|addr| !registry.is_connected(addr))
                .for_each(|addr| {
                    if let Err(err) = registry.connect(addr, None) {
                        error!(id = %registry.as_ref().id, addr = %addr, err = %err, "Connection to peer failed");
                    }
                });

                let connected_peers = registry.connected_peers();
                let mut peer_v = HashMap::new();
                connected_peers.iter().for_each(|id| {
                    let (v, reconnect) = registry.with_writers_read(|writers| {
                        let writer = writers.get(id).expect("Checked existence before");
                        let reconnect =
                            match writer.write(&Packet::HeartBeat, Some(config.timeout.write)) {
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
                                    true
                                }
                                Err(err) => {
                                    error!(id = %registry.as_ref().id, peer_id = %id, err = %err, "Packet send failed");
                                    false
                                }
                                Ok(_) => false,
                            };

                        (writer.v, reconnect)
                    });

                    peer_v.insert(id, v);
                    if reconnect {
                        let _ = registry.connect_with_id(*id, Some(v));
                    }
                });

                connected_peers.iter().for_each(|id| {
                    let hb = registry.hb(*id).expect("Checked existence before");
                    if hb
                        .checked_add(hb_tolerance)
                        .expect("Always Instant representable")
                        < Instant::now()
                    {
                        let v = peer_v.get(id).expect("Checked existence before");
                        registry.unregister(*id, *v);
                    }
                });

                info!(id = %registry.as_ref().id, "Heartbeat tick finished");
            }

            info!(id = %registry.as_ref().id, "Heartbeat scheduler destroyed");
        })
}

pub(crate) fn listener(config: network::Config, registry: Registry) -> io::Result<JoinHandle<()>> {
    let listener = TcpListener::bind(registry.as_ref().config.addr)?;
    listener.set_nonblocking(true)?;

    thread::Builder::new()
        .name("listener".to_string())
        .spawn(move || {

            info!(id = %registry.as_ref().id, "Listener scheduler initialized");
            loop {
                if registry.as_ref().sigterm.load(Ordering::Acquire) {
                    break;
                }

                match listener.accept() {
                    Ok((stream, _)) => {
                        let read_stream = match stream.try_clone() {
                            Ok(v) => v,
                            Err(err) => {
                                error!(id = %registry.as_ref().id, err = %err ,"Peer connection accept failed");
                                continue;
                            }
                        };
                        let mut r = PeerRead::new(read_stream);
                        let w = PeerWrite::new(stream);

                        // Wait to receive peer node id
                        let who_is_req = match r.read(Some(config.timeout.read)) {
                            Ok(packet) => packet,
                            Err(err) => {
                                error!(id = %registry.as_ref().id, err = %err ,"Peer connection accept failed waiting identity discovery");
                                continue;
                            }
                        };

                        let (peer_id, peer_addr) = match who_is_req {
                            Packet::WhoIs { id, addr } => (id, addr),
                            _ => {
                                continue;
                            }
                        };

                        if let Err(err) = w.write(
                            &Packet::WhoIsReply {
                                id: registry.as_ref().id,
                            },
                            Some(config.timeout.write)) {
                            error!(id = %registry.as_ref().id, peer_id = %peer_id, err = %err ,"Peer connection accept failed sending own identity");
                            continue;
                        }


                        let v = w.v;
                        registry.register(peer_id, &peer_addr, w);
                        let _ = r.start(registry.clone(), peer_id, v);

                        info!(id = %registry.as_ref().id, peer_id = %peer_id, v = %v, "Peer connection accept successfully");
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(50));
                    }
                    Err(err) => {
                        error!(id = %registry.as_ref().id, err = %err, "Listener threw an error");
                    }
                }
            }

            info!(id = %registry.as_ref().id, "Listener scheduler destroyed");
        })
}
