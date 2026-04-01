use std::{net::SocketAddr, u64};

use uuid::Uuid;

#[derive(Debug, Eq, PartialEq)]
pub enum Event {
    PeerAdded { id: Uuid },
    PeerRemoved { id: Uuid },
}

#[derive(Debug, Eq, PartialEq)]
pub struct InboundPacket {
    pub(crate) from: Uuid,
    pub(crate) p: Packet,
}

impl InboundPacket {
    pub fn new(id: Uuid, p: Packet) -> Self {
        Self { from: id, p }
    }
}

// Request packets
#[derive(Debug, Eq, PartialEq)]
pub enum Packet {
    WhoIs { id: Uuid, addr: SocketAddr },
    WhoIsReply { id: Uuid },
    HeartBeat,
    NoOp { corr_id: u64 },
    NoOpReply { corr_id: u64 },
}

pub(crate) const DISCRIMINANT: usize = 1;
pub(crate) const PACKET_LENGTH: usize = 4;

impl Packet {
    pub fn kind(&self) -> u8 {
        match self {
            Self::WhoIs { .. } => 1,
            Self::WhoIsReply { .. } => 2,
            Self::HeartBeat => 3,
            Self::NoOp { .. } => 4,
            Self::NoOpReply { .. } => 5,
        }
    }

    pub fn is_request(&self) -> bool {
        match self {
            Self::WhoIs { .. }
            | Self::WhoIsReply { .. }
            | Self::HeartBeat
            | Self::NoOpReply { .. } => false,
            Self::NoOp { .. } => true,
        }
    }

    pub fn correlation_id(&self) -> Option<u64> {
        match self {
            Self::WhoIs { .. } | Self::WhoIsReply { .. } | Self::HeartBeat => None,
            Self::NoOp { corr_id } | Self::NoOpReply { corr_id } => Some(*corr_id),
        }
    }

    pub fn len(discriminant: u8) -> Option<usize> {
        match discriminant {
            1 => None,
            2 => Some(16),
            3 => Some(0),
            4 => Some(8),
            5 => Some(8),
            _ => None,
        }
    }

    pub fn reserve_buffer(discriminant: u8) -> Vec<u8> {
        const DEFAULT_LEN: usize = 256;
        Vec::with_capacity(
            PACKET_LENGTH + DISCRIMINANT + Packet::len(discriminant).unwrap_or(DEFAULT_LEN),
        )
    }
}

impl From<&Packet> for Vec<u8> {
    fn from(packet: &Packet) -> Self {
        let discriminant = packet.kind();
        let mut buffer = Packet::reserve_buffer(packet.kind());

        let mut filled = false;
        if let Some(len) = Packet::len(discriminant) {
            buffer.extend_from_slice(&((len + DISCRIMINANT) as u32).to_le_bytes());
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
            Packet::HeartBeat => {}
            Packet::NoOp { corr_id } => {
                buffer.extend_from_slice(corr_id.to_le_bytes().as_slice());
            }
            Packet::NoOpReply { corr_id } => {
                buffer.extend_from_slice(corr_id.to_le_bytes().as_slice());
            }
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
        let discriminant = value
            .first()
            .expect("First byte must contains discriminant");
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
            4 => {
                let buff: [u8; 8] = value[1..].try_into().unwrap();
                Packet::NoOp {
                    corr_id: u64::from_le_bytes(buff),
                }
            }
            5 => {
                let buff: [u8; 8] = value[1..].try_into().unwrap();
                Packet::NoOpReply {
                    corr_id: u64::from_le_bytes(buff),
                }
            }
            _ => {
                panic!("Unknown packet");
            }
        }
    }
}
