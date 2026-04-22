use std::{
    borrow::Cow,
    collections::HashMap,
    convert::TryFrom,
    io,
    net::TcpListener,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use rand::RngExt;

use crate::{
    network::{
        self, Discovery, Message,
        packet::{self, Event, Packet},
        registry::{PeerRead, PeerWrite, Registry},
    },
    runtime::Logger,
};

pub(crate) fn hb<L: Logger>(
    config: network::Config,
    d: Discovery,
    registry: Registry<L>,
) -> io::Result<JoinHandle<()>> {
    thread::Builder::new()
        .name("heartbeat".to_string())
        .spawn(move || {
            let poll_interval = config.hb.poll_interval;
            let hb_tolerance = config.hb.tolerance;

            registry.as_ref().logger.info(format_args!(
                "Heartbeat scheduler initialized. Id: {}.",
                registry.as_ref().id
            ));
            loop {
                let rng = rand::rng().random_range(0.75..=1.0);
                let millis = poll_interval.as_millis() as f64;
                let jitter = millis * rng;
                if !registry
                    .as_ref()
                    .sigterm
                    .checkpoint(Some(Duration::from_millis(jitter as u64)))
                {
                    break;
                }

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
                        registry.as_ref().logger.error(format_args!(
                            "Connection to peer failed. Id: {}. Addr: {}. Err: {}.",
                            registry.as_ref().id,
                            addr,
                            err
                        ));
                    }
                });

                let connected_peers = registry.connected_peers();
                let mut peer_v = HashMap::new();
                connected_peers.iter().for_each(|id| {
                    let (v, reconnect) = registry.with_writers_read(|writers| {
                        let writer = writers
                            .get(id)
                            .expect("writer must exist for each id returned by connected_peers");
                        let reconnect = match writer.write(
                            &Packet::Request(packet::Request::HeartBeat),
                            Some(config.timeout.write),
                        ) {
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
                                registry.as_ref().logger.error(format_args!(
                                    "Packet send failed. Id: {}. Peer id: {}. Err: {}.",
                                    registry.as_ref().id,
                                    id,
                                    err
                                ));
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
                    let hb = registry
                        .hb(*id)
                        .expect("heartbeat Instant must exist for each id in connected_peers");
                    if hb
                        .checked_add(hb_tolerance)
                        .expect("heartbeat deadline overflow (last_seen + tolerance)")
                        < Instant::now()
                    {
                        let v = peer_v
                            .get(id)
                            .expect("connection version must exist for each id in connected_peers");
                        registry.unregister(*id, *v);
                    }
                });

                let _ = registry
                    .as_ref()
                    .tx_msg
                    .send(Message::Event(Event::Checkpoint));
                registry.as_ref().logger.info(format_args!(
                    "Heartbeat tick finished. Id: {}.",
                    registry.as_ref().id
                ));
            }

            registry.as_ref().logger.info(format_args!(
                "Heartbeat scheduler destroyed. Id: {}.",
                registry.as_ref().id
            ));
        })
}

pub(crate) fn listener<L: Logger>(
    config: network::Config,
    registry: Registry<L>,
) -> io::Result<JoinHandle<()>> {
    let listener = TcpListener::bind(registry.as_ref().config.addr)?;
    listener.set_nonblocking(true)?;

    thread::Builder::new()
        .name("listener".to_string())
        .spawn(move || {
            registry.as_ref().logger.info(format_args!(
                "Listener scheduler initialized. Id: {}.",
                registry.as_ref().id
            ));
            loop {
                if !registry.as_ref().sigterm.checkpoint(Some(Duration::from_millis(50))) {
                    break;
                }

                match listener.accept() {
                    Ok((stream, _)) => {
                        let read_stream = match stream.try_clone() {
                            Ok(v) => v,
                            Err(err) => {
                                registry.as_ref().logger.error(format_args!(
                                    "Peer connection accept failed. Id: {}. Err: {}.",
                                    registry.as_ref().id,
                                    err
                                ));
                                continue;
                            }
                        };
                        let mut r = PeerRead::new(read_stream);
                        let w = PeerWrite::new(stream);

                        // Wait to receive peer node id
                        let who_is_req = match r.read(Some(config.timeout.read)) {
                            Ok(packet) => packet,
                            Err(err) => {
                                registry.as_ref().logger.error(format_args!(
                                    "Peer connection accept failed waiting identity discovery. Id: {}. Err: {}.",
                                    registry.as_ref().id,
                                    err
                                ));
                                continue;
                            }
                        };

                        let (peer_id, peer_addr) = match Packet::try_from(&who_is_req) {
                            Ok(Packet::Request(packet::Request::WhoIs { id, addr })) => {
                                (id, addr)
                            }
                            Ok(_) | Err(_) => {
                                registry.as_ref().logger.error(format_args!(
                                    "Peer connection accept failed: expected WhoIs request. Id: {}.",
                                    registry.as_ref().id
                                ));
                                continue;
                            }
                        };

                        if let Err(err) = w.write(&Packet::Reply(packet::Reply::WhoIs { id: registry.as_ref().id })
                                                  , Some(config.timeout.write), ) {
                            registry.as_ref().logger.error(format_args!(
                                "Peer connection accept failed sending own identity. Id: {}. Peer id: {}. Err: {}.",
                                registry.as_ref().id,
                                peer_id,
                                err
                            ));
                            continue;
                        }

                        let v = w.v;
                        registry.register(peer_id, &peer_addr, w);

                        match r.start(registry.clone(), peer_id, v) {
                            Ok(h) => {
                                registry.as_ref().sigterm.register(h);
                            }
                            Err(err) => {
                                registry.unregister(peer_id, v);

                                registry.as_ref().logger.error(format_args!(
                                    "Peer connection accept failed. Id: {}. Err: {}.",
                                    registry.as_ref().id,
                                    err
                                ));
                            }
                        }

                        registry.as_ref().logger.info(format_args!(
                            "Peer connection accepted. Id: {}. Peer id: {}. V: {}.",
                            registry.as_ref().id,
                            peer_id,
                            v
                        ));
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(err) => {
                        registry.as_ref().logger.error(format_args!(
                            "Listener threw an error. Id: {}. Err: {}.",
                            registry.as_ref().id,
                            err
                        ));
                    }
                }
            }

            registry.as_ref().logger.info(format_args!(
                "Listener scheduler destroyed. Id: {}.",
                registry.as_ref().id
            ));
        })
}
