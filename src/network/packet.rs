use std::net::SocketAddr;

use uuid::Uuid;

use crate::{CorylusError, instance::operation, partition, serde};

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

#[derive(Debug, Eq, PartialEq)]
pub enum Packet {
    Request(Request),
    Reply(Reply),
}

#[derive(Debug, Eq, PartialEq)]
pub enum Request {
    Read(Read),
    Write(Write),
}

#[derive(Debug, Eq, PartialEq)]
pub enum Write {
    HeartBeat,
    WriteOp {
        v: u128,
        corr_id: Uuid,
        // Just to check inconsistences in the partition tables to reject
        partition_id: u16,
        segment_id: String,
        op_id: String,
        raw_op: Vec<u8>,
    },
}

#[derive(Debug, Eq, PartialEq)]
pub enum Read {
    WhoIs {
        id: Uuid,
        addr: SocketAddr,
    },
    GetOp {
        v: u128,
        corr_id: Uuid,
        // Just to check inconsistences in the partition tables to reject
        partition_id: u16,
        segment_id: String,
        op_id: String,
        raw_op: Vec<u8>,
    },
}

#[repr(u8)]
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum Status {
    Success = 0,
    InvalidPartitionVersion = 1,
    OperationNotFound = 2,
    PartitionNotFound = 3,
    SegmentNotFound = 4,
    IoError = 5,
    SerdeError = 6,
}

// TODO: Fix these mappings
impl From<CorylusError> for Status {
    fn from(value: CorylusError) -> Self {
        match value {
            CorylusError::Io(_) => Status::IoError,
            CorylusError::Partition(err) => match err {
                partition::Error::PartitionNotFound => Status::PartitionNotFound,
                partition::Error::SegmentNotFound => Status::SegmentNotFound,
                partition::Error::InvalidVersion => Status::InvalidPartitionVersion,
            },
            CorylusError::Operation(err) => match err {
                operation::Error::OperationNotFound => Status::OperationNotFound,
            },
            CorylusError::Serde(_) => Status::SerdeError,
        }
    }
}

use std::convert::TryFrom;

impl TryFrom<Status> for CorylusError {
    type Error = ();

    fn try_from(value: Status) -> Result<Self, Self::Error> {
        match value {
            Status::Success => Err(()),
            Status::InvalidPartitionVersion => {
                Ok(CorylusError::Partition(partition::Error::InvalidVersion))
            }
            Status::OperationNotFound => {
                Ok(CorylusError::Operation(operation::Error::OperationNotFound))
            }
            Status::PartitionNotFound => {
                Ok(CorylusError::Partition(partition::Error::PartitionNotFound))
            }
            Status::SegmentNotFound => {
                Ok(CorylusError::Partition(partition::Error::SegmentNotFound))
            }
            Status::IoError => Ok(CorylusError::Io(std::io::Error::other("IO error"))),
            Status::SerdeError => Ok(CorylusError::Serde(serde::Error::Unknown)),
        }
    }
}

impl TryFrom<u8> for Status {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Status::Success),
            1 => Ok(Status::Success),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Reply {
    WhoIs {
        id: Uuid,
    },
    WriteOp {
        corr_id: Uuid,
        status: Status,
    },
    GetOp {
        corr_id: Uuid,
        status: Status,
        result: Vec<u8>,
    },
}

pub(crate) const DISCRIMINANT: usize = std::mem::size_of::<u8>();
pub(crate) const PACKET_LENGTH: usize = std::mem::size_of::<u32>();

pub enum Len {
    Hint(usize),
    Exact(usize),
}

impl Len {
    pub fn value(&self) -> usize {
        match self {
            Self::Hint(val) | Self::Exact(val) => *val,
        }
    }
}

impl Packet {
    pub fn kind(&self) -> u8 {
        match self {
            Self::Request(val) => match val {
                Request::Read(val) => match val {
                    Read::WhoIs { .. } => 1,
                    Read::GetOp { .. } => 8,
                },
                Request::Write(val) => match val {
                    Write::WriteOp { .. } => 6,
                    Write::HeartBeat => 3,
                },
            },
            Self::Reply(val) => match val {
                Reply::WhoIs { .. } => 2,
                Reply::WriteOp { .. } => 7,
                Reply::GetOp { .. } => 9,
            },
        }
    }

    pub fn correlation_id(&self) -> Option<Uuid> {
        match self {
            Self::Request(val) => match val {
                Request::Read(val) => match val {
                    Read::WhoIs { .. } => None,
                    Read::GetOp { corr_id, .. } => Some(*corr_id),
                },
                Request::Write(val) => match val {
                    Write::WriteOp { corr_id, .. } => Some(*corr_id),
                    Write::HeartBeat => None,
                },
            },
            Self::Reply(val) => match val {
                Reply::WhoIs { .. } => None,
                Reply::WriteOp { corr_id, .. } => Some(*corr_id),
                Reply::GetOp { corr_id, .. } => Some(*corr_id),
            },
        }
    }

    pub fn len(discriminant: u8) -> Len {
        match discriminant {
            1 => Len::Hint(128),
            2 => Len::Exact(16),
            3 => Len::Exact(0),
            6 => Len::Hint(256),
            7 => Len::Exact(17),
            8 => Len::Hint(512),
            9 => Len::Hint(512),
            _ => Len::Hint(256),
        }
    }

    pub fn reserve_buffer(discriminant: u8) -> Vec<u8> {
        Vec::with_capacity(PACKET_LENGTH + DISCRIMINANT + Packet::len(discriminant).value())
    }
}

impl From<&Packet> for Vec<u8> {
    fn from(packet: &Packet) -> Self {
        let discriminant = packet.kind();
        let mut buffer = Packet::reserve_buffer(packet.kind());

        match Packet::len(discriminant) {
            Len::Hint(_) => {
                buffer.extend_from_slice(&0u32.to_le_bytes());
            }
            Len::Exact(val) => {
                buffer.extend_from_slice(&((val + DISCRIMINANT) as u32).to_le_bytes());
            }
        }

        buffer.push(discriminant);

        match packet {
            Packet::Request(req) => match req {
                Request::Read(read) => match read {
                    Read::WhoIs { id, addr } => {
                        buffer.extend_from_slice(id.as_bytes());

                        let addr = addr.to_string().into_bytes();
                        buffer.extend_from_slice(addr.as_slice());
                    }

                    Read::GetOp {
                        v,
                        corr_id,
                        partition_id,
                        segment_id,
                        op_id,
                        raw_op,
                    } => {
                        buffer.extend_from_slice(&v.to_le_bytes());
                        buffer.extend_from_slice(corr_id.as_bytes());
                        buffer.extend_from_slice(&partition_id.to_le_bytes());

                        let segment_id_len: u16 = segment_id.len() as u16;
                        buffer.extend_from_slice(&segment_id_len.to_le_bytes());
                        buffer.extend_from_slice(segment_id.as_bytes());

                        let op_id_len: u16 = op_id.len() as u16;
                        buffer.extend_from_slice(&op_id_len.to_le_bytes());
                        buffer.extend_from_slice(op_id.as_bytes());

                        if !raw_op.is_empty() {
                            buffer.extend_from_slice(raw_op.as_slice());
                        }
                    }
                },

                Request::Write(write) => match write {
                    Write::HeartBeat => {}

                    Write::WriteOp {
                        v,
                        corr_id,
                        partition_id,
                        segment_id,
                        op_id,
                        raw_op,
                    } => {
                        buffer.extend_from_slice(&v.to_le_bytes());
                        buffer.extend_from_slice(corr_id.as_bytes());
                        buffer.extend_from_slice(&partition_id.to_le_bytes());

                        let segment_id_len: u16 = segment_id.len() as u16;
                        buffer.extend_from_slice(&segment_id_len.to_le_bytes());
                        buffer.extend_from_slice(segment_id.as_bytes());

                        let op_id_len: u16 = op_id.len() as u16;
                        buffer.extend_from_slice(&op_id_len.to_le_bytes());
                        buffer.extend_from_slice(op_id.as_bytes());

                        if !raw_op.is_empty() {
                            buffer.extend_from_slice(raw_op.as_slice());
                        }
                    }
                },
            },

            Packet::Reply(rep) => match rep {
                Reply::WhoIs { id } => {
                    buffer.extend_from_slice(id.as_bytes());
                }

                Reply::WriteOp { corr_id, status } => {
                    buffer.extend_from_slice(corr_id.as_bytes());
                    buffer.push(*status as u8);
                }
                Reply::GetOp {
                    corr_id,
                    status,
                    result,
                } => {
                    buffer.extend_from_slice(corr_id.as_bytes());
                    buffer.push(*status as u8);

                    if !result.is_empty() {
                        buffer.extend_from_slice(result.as_slice());
                    }
                }
            },
        }

        if matches!(Packet::len(discriminant), Len::Hint(_)) {
            let len = buffer[4..].len() as u32;
            buffer[0..4].copy_from_slice(&len.to_le_bytes());
        }

        buffer
    }
}

impl From<&[u8]> for Packet {
    fn from(value: &[u8]) -> Self {
        let discriminant = value.first().expect("First byte must contain discriminant");

        match *discriminant {
            // WhoIs (Request::Read)
            1 => {
                let id = Uuid::from_slice(&value[1..=16]).unwrap();
                let addr: SocketAddr = std::str::from_utf8(&value[17..]).unwrap().parse().unwrap();

                Packet::Request(Request::Read(Read::WhoIs { id, addr }))
            }

            // WhoIsReply (Reply)
            2 => Packet::Reply(Reply::WhoIs {
                id: Uuid::from_slice(&value[1..]).unwrap(),
            }),

            // HeartBeat (Request::Write)
            3 => Packet::Request(Request::Write(Write::HeartBeat)),

            // WriteOp (Request::Write)
            6 => {
                let mut offset = 1;

                let v = u128::from_le_bytes(value[offset..offset + 16].try_into().unwrap());
                offset += 16;

                let corr_id = Uuid::from_slice(&value[offset..offset + 16]).unwrap();
                offset += 16;

                let partition_id =
                    u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap());
                offset += 2;

                let segment_id_len =
                    u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap()) as usize;
                offset += 2;

                let segment_id = std::str::from_utf8(&value[offset..offset + segment_id_len])
                    .unwrap()
                    .to_string();

                offset += segment_id_len;

                let op_id_len =
                    u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap()) as usize;
                offset += 2;

                let op_id = std::str::from_utf8(&value[offset..offset + op_id_len])
                    .unwrap()
                    .to_string();

                offset += op_id_len;

                let raw_op = if offset >= value.len() {
                    vec![]
                } else {
                    value[offset..].to_vec()
                };

                Packet::Request(Request::Write(Write::WriteOp {
                    v,
                    corr_id,
                    partition_id,
                    segment_id,
                    op_id,
                    raw_op,
                }))
            }

            // WriteOpReply (Reply)
            7 => {
                let mut offset = 1;

                let corr_id = Uuid::from_slice(&value[offset..offset + 16]).unwrap();
                offset += 16;

                let status = Status::try_from(value[offset]).unwrap();

                Packet::Reply(Reply::WriteOp { corr_id, status })
            }

            // GetOp (Request::Read)
            8 => {
                let mut offset = 1;

                let v = u128::from_le_bytes(value[offset..offset + 16].try_into().unwrap());
                offset += 16;

                let corr_id = Uuid::from_slice(&value[offset..offset + 16]).unwrap();
                offset += 16;

                let partition_id =
                    u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap());
                offset += 2;

                let segment_id_len =
                    u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap()) as usize;
                offset += 2;

                let segment_id = std::str::from_utf8(&value[offset..offset + segment_id_len])
                    .unwrap()
                    .to_string();

                offset += segment_id_len;

                let op_id_len =
                    u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap()) as usize;
                offset += 2;

                let op_id = std::str::from_utf8(&value[offset..offset + op_id_len])
                    .unwrap()
                    .to_string();

                offset += op_id_len;

                let raw_op = if offset >= value.len() {
                    vec![]
                } else {
                    value[offset..].to_vec()
                };

                Packet::Request(Request::Read(Read::GetOp {
                    v,
                    corr_id,
                    partition_id,
                    segment_id,
                    op_id,
                    raw_op,
                }))
            }

            // GetOpReply (Reply)
            9 => {
                let mut offset = 1;

                let corr_id = Uuid::from_slice(&value[offset..offset + 16]).unwrap();
                offset += 16;

                let status = Status::try_from(value[offset]).unwrap();
                offset += 1;

                let result = if offset >= value.len() {
                    vec![]
                } else {
                    value[offset..].to_vec()
                };

                Packet::Reply(Reply::GetOp {
                    corr_id,
                    status,
                    result,
                })
            }

            _ => panic!("Unknown packet"),
        }
    }
}
