use std::{
    borrow::Cow,
    collections::HashMap,
    convert::TryFrom,
    io,
    time::{Duration, Instant},
};

use rand::RngExt;

use crate::{
    network::{
        self, Discovery, Message,
        packet::{self, Event, Packet},
        registry::{PeerRead, PeerWrite, Registry},
    },
    runtime::{self, TcpListener },
};

pub(crate) fn hb<S: runtime::Spawner, I: runtime::Io>(
    config: network::Config,
    d: Discovery,
    registry: Registry<S, I>,
) {

    let spawner = registry.as_ref().spawner.clone();

    spawner.spawn(async move {

            let poll_interval = config.hb.poll_interval;
            let hb_tolerance = config.hb.tolerance;

            log::info!(
                "Heartbeat scheduler initialized. Id: {}.",
                registry.as_ref().id
            );
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

                let futs = match &d {
                    Discovery::Dns { .. } => {
                        todo!()
                    }
                    Discovery::List { addresses } => Cow::Borrowed(addresses.as_slice()),
                }
                .iter()
                .filter(|addr| !registry.as_ref().config.addr.eq(addr))
                .filter(|addr| !registry.is_connected(addr))
                .map(|addr| async move {
                    if let Err(err) = registry.connect(addr, None).await {
                        log::error!(
                            "Connection to peer failed. Id: {}. Addr: {}. Err: {}.",
                            registry.as_ref().id,
                            addr,
                            err
                        );
                    }
                }).collect::<Vec<_>>();

               futures::future::join_all(futs).await;

                let connected_peers = registry.connected_peers();
                let mut peer_v = HashMap::new();
                connected_peers.iter().for_each(|id| {
                    let (v, reconnect) = registry.with_writers_read(|writers|async  {
                        let writer = writers
                            .get(id)
                            .expect("writer must exist for each id returned by connected_peers");
                        let reconnect = match writer.write(
                            &Packet::Request(packet::Request::HeartBeat),
                            Some(config.timeout.write),
                        ).await {
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
                                log::error!(
                                    "Packet send failed. Id: {}. Peer id: {}. Err: {}.",
                                    registry.as_ref().id,
                                    id,
                                    err
                                );
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
                log::info!("Heartbeat tick finished. Id: {}.", registry.as_ref().id);
            }

            log::info!(
                "Heartbeat scheduler destroyed. Id: {}.",
                registry.as_ref().id
            );
        
    });
}

pub(crate) async fn listener<S: runtime::Spawner, I: runtime::Io>(
    config: network::Config,
    registry: Registry<S, I>,
) -> io::Result<()> {

    let listener = I::listener(config.addr).await?;

    let spawner = registry.as_ref().spawner.clone();
    spawner.spawn(async move {


    log::info!(
        "Listener scheduler initialized. Id: {}.",
        registry.as_ref().id
    );
    loop {
        if !registry
            .as_ref()
            .sigterm
            .checkpoint(Some(Duration::from_millis(50)))
        {
            break;
        }


        match listener.accept().await {
            Ok((r, w)) => {
                let mut r = PeerRead::new(r);
                let w = PeerWrite::new(w);

                // Wait to receive peer node id
                let who_is_req = match r.read(Some(config.timeout.read)).await {
                    Ok(packet) => packet,
                    Err(err) => {
                        log::error!(
                            "Peer connection accept failed waiting identity discovery. Id: {}. Err: {}.",
                            registry.as_ref().id,
                            err
                        );
                        continue;
                    }
                };

                let (peer_id, peer_addr) = match Packet::try_from(&who_is_req) {
                    Ok(Packet::Request(packet::Request::WhoIs { id, addr })) => (id, addr),
                    Ok(_) | Err(_) => {
                        log::error!(
                            "Peer connection accept failed: expected WhoIs request. Id: {}.",
                            registry.as_ref().id
                        );
                        continue;
                    }
                };

                if let Err(err) = w.write(
                    &Packet::Reply(packet::Reply::WhoIs {
                        id: registry.as_ref().id,
                    }),
                    Some(config.timeout.write),
                ).await {
                    log::error!(
                        "Peer connection accept failed sending own identity. Id: {}. Peer id: {}. Err: {}.",
                        registry.as_ref().id,
                        peer_id,
                        err
                    );
                    continue;
                }

                let v = w.v;
                registry.register(peer_id, &peer_addr, w);

                r.start(registry.clone(), peer_id, v);


                // match r.start(registry.clone(), peer_id, v) {
                //     Ok(h) => {
                //         registry.as_ref().sigterm.register(h);
                //     }
                //     Err(err) => {
                //         registry.unregister(peer_id, v);

                //         log::error!(
                //             "Peer connection accept failed. Id: {}. Err: {}.",
                //             registry.as_ref().id,
                //             err
                //         );
                //     }
                // }

                log::info!(
                    "Peer connection accepted. Id: {}. Peer id: {}. V: {}.",
                    registry.as_ref().id,
                    peer_id,
                    v
                );
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
            Err(err) => {
                log::error!(
                    "Listener threw an error. Id: {}. Err: {}.",
                    registry.as_ref().id,
                    err
                );
            }
        }
    }

    log::info!(
        "Listener scheduler destroyed. Id: {}.",
        registry.as_ref().id
    );


        
    });

    Ok(())

}


// Define how many runtimes we will use and if some are needed to be non-Send. (We can use runtime enum flavors to allow non-send futures panicking if called in multithreaded) (We can use runtime enum flavors to allow non-send futures panicking if called in multithreaded). Get mutex and then .await its not allowed for multithreaded. (Implement pckt-sender). If we remove the mutex wrapping tcp stream, the futures can be sent safely to other threads.
// Define the communication model of commands for pck_sender. (Send request, Connect, Clear, ...etc)
// TODO: Before continue, define how will looks like the task async shutdowns (and runtime tracking, to avoid drop them if created in local function (for current thread we can just spawn os thread )) and tick intervals for schedulers.

pub(crate) async fn pckt_sender<S: runtime::Spawner, I: runtime::Io>(
    config: network::Config,
    registry: Registry<S, I>,
) {
}

