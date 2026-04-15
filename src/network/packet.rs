use std::net::SocketAddr;

use uuid::Uuid;

use crate::{CorylusError, object, partition, serde};

#[derive(Debug, Eq, PartialEq)]
pub enum Event {
    PeerAdded { id: Uuid },
    PeerRemoved { id: Uuid },
    Checkpoint,
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
        part_id: u16,
        obj_id: String,
        opart_id: String,
        raw_op: Vec<u8>,
    },
    PartitionFetchCompletion {
        v: u128,
        corr_id: Uuid,
        part_id: u16,
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
        part_id: u16,
        obj_id: String,
        opart_id: String,
        raw_op: Vec<u8>,
    },
    FetchObject {
        v: u128,
        corr_id: Uuid,
        // Just to check inconsistences in the partition tables to reject
        part_id: u16,
        obj_id: String,
    },
}

#[repr(u8)]
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum Status {
    Success = 0,
    Rebalance = 1,
    OperationNotFound = 2,
    PartitionNotFound = 3,
    ObjectNotFound = 4,
    IoError = 5,
    SerdeInvalidBufferSize = 6,
    SerdeUnknown = 7,
    SerdeInvalidUtf8 = 8,
    NotEnoughReplicas = 9,
    PartitionNotReady = 10,
}
impl From<CorylusError> for Status {
    fn from(value: CorylusError) -> Self {
        match value {
            CorylusError::Io(_) => Status::IoError,
            CorylusError::Partition(err) => match err {
                partition::Error::PartitionNotFound => Status::PartitionNotFound,
                partition::Error::Rebalance => Status::Rebalance,
                partition::Error::NotEnoughReplicas => Status::NotEnoughReplicas,
                partition::Error::PartitionNotReady => Status::PartitionNotReady,
            },
            CorylusError::Object(err) => match err {
                Error::OperationNotFound => Status::OperationNotFound,
                Error::ObjectNotFound => Status::ObjectNotFound,
            },
            CorylusError::Serde(err) => match err {
                serde::Error::InvalidBufferSize => Status::SerdeInvalidBufferSize,
                serde::Error::InvalidUtf8 => Status::SerdeInvalidUtf8,
                serde::Error::Unknown => Status::SerdeUnknown,
            },
        }
    }
}

use crate::object::Error;
use std::convert::TryFrom;

impl TryFrom<Status> for CorylusError {
    type Error = ();

    fn try_from(value: Status) -> Result<Self, Self::Error> {
        match value {
            Status::Success => Err(()),
            Status::Rebalance => Ok(CorylusError::Partition(partition::Error::Rebalance)),
            Status::OperationNotFound => Ok(CorylusError::Object(Error::OperationNotFound)),
            Status::PartitionNotFound => {
                Ok(CorylusError::Partition(partition::Error::PartitionNotFound))
            }
            Status::ObjectNotFound => Ok(CorylusError::Object(object::Error::ObjectNotFound)),
            Status::IoError => Ok(CorylusError::Io(std::io::Error::other("IO error"))),
            Status::SerdeUnknown => Ok(CorylusError::Serde(serde::Error::Unknown)),
            Status::SerdeInvalidBufferSize => {
                Ok(CorylusError::Serde(serde::Error::InvalidBufferSize))
            }
            Status::SerdeInvalidUtf8 => Ok(CorylusError::Serde(serde::Error::InvalidUtf8)),
            Status::NotEnoughReplicas => {
                Ok(CorylusError::Partition(partition::Error::NotEnoughReplicas))
            }
            Status::PartitionNotReady => {
                Ok(CorylusError::Partition(partition::Error::PartitionNotReady))
            }
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
    FetchObject {
        corr_id: Uuid,
        status: Status,
        result: Vec<u8>,
    },

    PartitionFetchCompletion {
        corr_id: Uuid,
        status: Status,
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
                    Read::FetchObject { .. } => 10,
                },
                Request::Write(val) => match val {
                    Write::WriteOp { .. } => 6,
                    Write::HeartBeat => 3,
                    Write::PartitionFetchCompletion { .. } => 12,
                },
            },
            Self::Reply(val) => match val {
                Reply::WhoIs { .. } => 2,
                Reply::WriteOp { .. } => 7,
                Reply::GetOp { .. } => 9,
                Reply::FetchObject { .. } => 11,
                Reply::PartitionFetchCompletion { .. } => 13,
            },
        }
    }

    pub fn correlation_id(&self) -> Option<Uuid> {
        match self {
            Self::Request(val) => match val {
                Request::Read(val) => match val {
                    Read::WhoIs { .. } => None,
                    Read::GetOp { corr_id, .. } => Some(*corr_id),
                    Read::FetchObject { corr_id, .. } => Some(*corr_id),
                },
                Request::Write(val) => match val {
                    Write::WriteOp { corr_id, .. } => Some(*corr_id),
                    Write::HeartBeat => None,
                    Write::PartitionFetchCompletion { corr_id, .. } => Some(*corr_id),
                },
            },
            Self::Reply(val) => match val {
                Reply::WhoIs { .. } => None,
                Reply::WriteOp { corr_id, .. } => Some(*corr_id),
                Reply::GetOp { corr_id, .. } => Some(*corr_id),
                Reply::FetchObject { corr_id, .. } => Some(*corr_id),
                Reply::PartitionFetchCompletion { corr_id, .. } => Some(*corr_id),
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
            10 => Len::Hint(128),
            11 => Len::Hint(4096),
            12 => Len::Exact(34),
            13 => Len::Exact(17),
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
                        part_id,
                        obj_id,
                        opart_id,
                        raw_op,
                    } => {
                        buffer.extend_from_slice(&v.to_le_bytes());
                        buffer.extend_from_slice(corr_id.as_bytes());
                        buffer.extend_from_slice(&part_id.to_le_bytes());

                        let obj_id_len: u16 = obj_id.len() as u16;
                        buffer.extend_from_slice(&obj_id_len.to_le_bytes());
                        buffer.extend_from_slice(obj_id.as_bytes());

                        let opart_id_len: u16 = opart_id.len() as u16;
                        buffer.extend_from_slice(&opart_id_len.to_le_bytes());
                        buffer.extend_from_slice(opart_id.as_bytes());

                        if !raw_op.is_empty() {
                            buffer.extend_from_slice(raw_op.as_slice());
                        }
                    }
                    Read::FetchObject {
                        v,
                        corr_id,
                        part_id,
                        obj_id,
                    } => {
                        buffer.extend_from_slice(&v.to_le_bytes());
                        buffer.extend_from_slice(corr_id.as_bytes());
                        buffer.extend_from_slice(&part_id.to_le_bytes());

                        let obj_id_len: u16 = obj_id.len() as u16;
                        buffer.extend_from_slice(&obj_id_len.to_le_bytes());
                        buffer.extend_from_slice(obj_id.as_bytes());
                    }
                },

                Request::Write(write) => match write {
                    Write::HeartBeat => {}

                    Write::WriteOp {
                        v,
                        corr_id,
                        part_id,
                        obj_id,
                        opart_id,
                        raw_op,
                    } => {
                        buffer.extend_from_slice(&v.to_le_bytes());
                        buffer.extend_from_slice(corr_id.as_bytes());
                        buffer.extend_from_slice(&part_id.to_le_bytes());

                        let obj_id_len: u16 = obj_id.len() as u16;
                        buffer.extend_from_slice(&obj_id_len.to_le_bytes());
                        buffer.extend_from_slice(obj_id.as_bytes());

                        let opart_id_len: u16 = opart_id.len() as u16;
                        buffer.extend_from_slice(&opart_id_len.to_le_bytes());
                        buffer.extend_from_slice(opart_id.as_bytes());

                        if !raw_op.is_empty() {
                            buffer.extend_from_slice(raw_op.as_slice());
                        }
                    }

                    Write::PartitionFetchCompletion {
                        v,
                        corr_id,
                        part_id,
                    } => {
                        buffer.extend_from_slice(&v.to_le_bytes());
                        buffer.extend_from_slice(corr_id.as_bytes());
                        buffer.extend_from_slice(&part_id.to_le_bytes());
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
                Reply::FetchObject {
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

                Reply::PartitionFetchCompletion { corr_id, status } => {
                    buffer.extend_from_slice(corr_id.as_bytes());
                    buffer.push(*status as u8);
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

                let part_id = u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap());
                offset += 2;

                let obj_id_len =
                    u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap()) as usize;
                offset += 2;

                let obj_id = std::str::from_utf8(&value[offset..offset + obj_id_len])
                    .unwrap()
                    .to_string();

                offset += obj_id_len;

                let opart_id_len =
                    u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap()) as usize;
                offset += 2;

                let opart_id = std::str::from_utf8(&value[offset..offset + opart_id_len])
                    .unwrap()
                    .to_string();

                offset += opart_id_len;

                let raw_op = if offset >= value.len() {
                    vec![]
                } else {
                    value[offset..].to_vec()
                };

                Packet::Request(Request::Write(Write::WriteOp {
                    v,
                    corr_id,
                    part_id,
                    obj_id,
                    opart_id,
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

                let part_id = u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap());
                offset += 2;

                let obj_id_len =
                    u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap()) as usize;
                offset += 2;

                let obj_id = std::str::from_utf8(&value[offset..offset + obj_id_len])
                    .unwrap()
                    .to_string();

                offset += obj_id_len;

                let opart_id_len =
                    u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap()) as usize;
                offset += 2;

                let opart_id = std::str::from_utf8(&value[offset..offset + opart_id_len])
                    .unwrap()
                    .to_string();

                offset += opart_id_len;

                let raw_op = if offset >= value.len() {
                    vec![]
                } else {
                    value[offset..].to_vec()
                };

                Packet::Request(Request::Read(Read::GetOp {
                    v,
                    corr_id,
                    part_id,
                    obj_id,
                    opart_id,
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

            10 => {
                let mut offset = 1;

                let v = u128::from_le_bytes(value[offset..offset + 16].try_into().unwrap());
                offset += 16;

                let corr_id = Uuid::from_slice(&value[offset..offset + 16]).unwrap();
                offset += 16;

                let part_id = u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap());
                offset += 2;

                let obj_id_len =
                    u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap()) as usize;
                offset += 2;

                let obj_id = std::str::from_utf8(&value[offset..offset + obj_id_len])
                    .unwrap()
                    .to_string();

                Packet::Request(Request::Read(Read::FetchObject {
                    v,
                    corr_id,
                    part_id,
                    obj_id,
                }))
            }

            11 => {
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

                Packet::Reply(Reply::FetchObject {
                    corr_id,
                    status,
                    result,
                })
            }

            12 => {
                let mut offset = 1;

                let v = u128::from_le_bytes(value[offset..offset + 16].try_into().unwrap());
                offset += 16;

                let corr_id = Uuid::from_slice(&value[offset..offset + 16]).unwrap();
                offset += 16;

                let part_id = u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap());

                Packet::Request(Request::Write(Write::PartitionFetchCompletion {
                    v,
                    corr_id,
                    part_id,
                }))
            }

            13 => {
                let mut offset = 1;

                let corr_id = Uuid::from_slice(&value[offset..offset + 16]).unwrap();
                offset += 16;

                let status = Status::try_from(value[offset]).unwrap();

                Packet::Reply(Reply::PartitionFetchCompletion { corr_id, status })
            }

            _ => panic!("Unknown packet"),
        }
    }
}
