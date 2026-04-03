use std::net::SocketAddr;

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
    WhoIs {
        id: Uuid,
        addr: SocketAddr,
    },
    WhoIsReply {
        id: Uuid,
    },
    HeartBeat,
    WriteOp {
        corr_id: Uuid,
        // Just to check inconsistences in the partition tables to reject
        partition_id: u16,
        segment_id: String,
        op_id: String,
        raw_op: Vec<u8>,
    },
    WriteOpReply {
        corr_id: Uuid,
        ok: bool,
    },
    GetOp {
        corr_id: Uuid,
        // Just to check inconsistences in the partition tables to reject
        partition_id: u16,
        segment_id: String,
        op_id: String,
        raw_op: Vec<u8>,
    },
    GetOpReply {
        corr_id: Uuid,
        ok: bool,
        result: Vec<u8>,
    },
}

pub(crate) const DISCRIMINANT: usize = 1;
pub(crate) const PACKET_LENGTH: usize = 4;

impl Packet {
    pub fn kind(&self) -> u8 {
        match self {
            Self::WhoIs { .. } => 1,
            Self::WhoIsReply { .. } => 2,
            Self::HeartBeat => 3,
            Self::WriteOp { .. } => 6,
            Self::WriteOpReply { .. } => 7,
            Self::GetOp { .. } => 8,
            Self::GetOpReply { .. } => 9,
        }
    }

    pub fn is_read(&self) -> bool {
        match self {
            Self::WhoIs { .. }
            | Self::WhoIsReply { .. }
            | Self::HeartBeat
            | Self::GetOpReply { .. }
            | Self::WriteOpReply { .. }
            | Self::WriteOp { .. } => false,
            Self::GetOp { .. } => true,
        }
    }

    pub fn is_write(&self) -> bool {
        match self {
            Self::WhoIs { .. }
            | Self::WhoIsReply { .. }
            | Self::HeartBeat
            | Self::GetOpReply { .. }
            | Self::WriteOpReply { .. }
            | Self::GetOp { .. } => false,
            Self::WriteOp { .. } => true,
        }
    }

    pub fn correlation_id(&self) -> Option<Uuid> {
        match self {
            Self::WhoIs { .. } | Self::WhoIsReply { .. } | Self::HeartBeat => None,
            Self::GetOp { corr_id, .. }
            | Self::GetOpReply { corr_id, .. }
            | Self::WriteOp { corr_id, .. }
            | Self::WriteOpReply { corr_id, .. } => Some(*corr_id),
        }
    }

    pub fn len(discriminant: u8) -> Option<usize> {
        match discriminant {
            1 => None,
            2 => Some(16),
            3 => Some(0),
            6 => None,
            7 => Some(17),
            8 => None,
            9 => None,
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
            Packet::WriteOp {
                corr_id,
                partition_id,
                segment_id,
                op_id,
                raw_op,
            } => {
                buffer.extend_from_slice(corr_id.as_bytes());
                buffer.extend_from_slice(partition_id.to_le_bytes().as_slice());

                let segment_id_len: u16 = segment_id.len() as u16;
                buffer.extend_from_slice(segment_id_len.to_le_bytes().as_slice());
                buffer.extend_from_slice(segment_id.as_bytes());

                let op_id_len: u16 = op_id.len() as u16;
                buffer.extend_from_slice(op_id_len.to_le_bytes().as_slice());
                buffer.extend_from_slice(op_id.as_bytes());

                if !raw_op.is_empty() {
                    buffer.extend_from_slice(raw_op.as_slice());
                }
            }
            Packet::WriteOpReply { corr_id, ok } => {
                buffer.extend_from_slice(corr_id.as_bytes().as_slice());
                buffer.push(*ok as u8);
            }

            Packet::GetOp {
                corr_id,
                partition_id,
                segment_id,
                op_id,
                raw_op,
            } => {
                buffer.extend_from_slice(corr_id.as_bytes().as_slice());
                buffer.extend_from_slice(partition_id.to_le_bytes().as_slice());

                let segment_id_len: u16 = segment_id.len() as u16;
                buffer.extend_from_slice(segment_id_len.to_le_bytes().as_slice());
                buffer.extend_from_slice(segment_id.as_bytes());

                let op_id_len: u16 = op_id.len() as u16;
                buffer.extend_from_slice(op_id_len.to_le_bytes().as_slice());
                buffer.extend_from_slice(op_id.as_bytes());

                if !raw_op.is_empty() {
                    buffer.extend_from_slice(raw_op.as_slice());
                }
            }

            Packet::GetOpReply {
                corr_id,
                ok,
                result,
            } => {
                buffer.extend_from_slice(corr_id.as_bytes().as_slice());
                buffer.push(*ok as u8);

                if !result.is_empty() {
                    buffer.extend_from_slice(result.as_slice());
                }
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
            6 => {
                let mut offset = 1;

                // corr_id (Uuid)
                let corr_id = Uuid::from_slice(&value[offset..offset + 16]).unwrap();
                offset += 16;

                // partition_id (u16)
                let partition_id =
                    u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap());
                offset += 2;

                // segment_id length
                let segment_id_len =
                    u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap()) as usize;
                offset += 2;

                // segment_id bytes
                let segment_id = std::str::from_utf8(&value[offset..offset + segment_id_len])
                    .unwrap()
                    .to_string();

                offset += segment_id_len;

                // op_id length
                let op_id_len =
                    u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap()) as usize;
                offset += 2;

                // op_id bytes
                let op_id = std::str::from_utf8(&value[offset..offset + op_id_len])
                    .unwrap()
                    .to_string();

                offset += op_id_len;

                // raw_op (no extra extend needed)
                let raw_op = if offset >= value.len() {
                    vec![]
                } else {
                    value[offset..].to_vec()
                };

                Packet::WriteOp {
                    corr_id,
                    partition_id,
                    segment_id,
                    op_id,
                    raw_op,
                }
            }
            7 => {
                let mut offset = 1;

                // corr_id (Uuid)
                let corr_id = Uuid::from_slice(&value[offset..offset + 16]).unwrap();
                offset += 16;

                let ok = match value[offset] {
                    0 => false,
                    _ => true,
                };

                Packet::WriteOpReply { corr_id, ok }
            }
            8 => {
                let mut offset = 1;

                // corr_id (Uuid)
                let corr_id = Uuid::from_slice(&value[offset..offset + 16]).unwrap();
                offset += 16;

                // partition_id (u16)
                let partition_id =
                    u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap());
                offset += 2;

                // segment_id length
                let segment_id_len =
                    u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap()) as usize;
                offset += 2;

                // segment_id bytes
                let segment_id = std::str::from_utf8(&value[offset..offset + segment_id_len])
                    .unwrap()
                    .to_string();

                offset += segment_id_len;

                // op_id length
                let op_id_len =
                    u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap()) as usize;
                offset += 2;

                // op_id bytes
                let op_id = std::str::from_utf8(&value[offset..offset + op_id_len])
                    .unwrap()
                    .to_string();

                offset += op_id_len;

                // raw_op (no extra extend needed)
                let raw_op = if offset >= value.len() {
                    vec![]
                } else {
                    value[offset..].to_vec()
                };

                Packet::GetOp {
                    corr_id,
                    partition_id,
                    segment_id,
                    op_id,
                    raw_op,
                }
            }
            9 => {
                let mut offset = 1;

                // corr_id (Uuid)
                let corr_id = Uuid::from_slice(&value[offset..offset + 16]).unwrap();
                offset += 16;

                let ok = match value[offset] {
                    0 => false,
                    _ => true,
                };

                offset += 1;

                let result = if offset >= value.len() {
                    vec![]
                } else {
                    value[offset..].to_vec()
                };

                Packet::GetOpReply {
                    corr_id,
                    ok,
                    result,
                }
            }
            _ => {
                panic!("Unknown packet");
            }
        }
    }
}
