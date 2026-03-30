use std::net::SocketAddr;

use uuid::Uuid;

#[derive(Debug, Eq, PartialEq)]
pub enum Event {
    PeerAdded { id: Uuid },
    PeerRemoved { id: Uuid },
}

#[derive(Debug, Eq, PartialEq)]
pub struct InboundPacket {
    from: Uuid,
    p: Packet,
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
}

pub(crate) const DISCRIMINANT: usize = 1;
pub(crate) const PACKET_LENGTH: usize = 4;

impl Packet {
    pub fn kind(&self) -> u8 {
        match self {
            Self::WhoIs { .. } => 1,
            Self::WhoIsReply { .. } => 2,
            Self::HeartBeat => 3,
        }
    }

    pub fn len(discriminant: u8) -> Option<usize> {
        match discriminant {
            1 => None,
            2 => Some(16),
            3 => Some(0),
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
        let discriminant = value.first().unwrap();
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
            _ => {
                panic!("Unknown packet");
            }
        }
    }
}
