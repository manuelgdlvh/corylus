use std::fmt::Display;
use std::io;
use std::net::SocketAddr;

use uuid::Uuid;

pub(crate) fn wire_err(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

fn take_slice<'a>(v: &'a [u8], off: &mut usize, len: usize) -> io::Result<&'a [u8]> {
    let end = off
        .checked_add(len)
        .ok_or_else(|| wire_err("packet offset overflow"))?;
    let r = v
        .get(*off..end)
        .ok_or_else(|| wire_err("truncated packet"))?;
    *off = end;
    Ok(r)
}

fn parse_uuid(v: &[u8], off: &mut usize) -> io::Result<Uuid> {
    let s = take_slice(v, off, 16)?;
    Uuid::from_slice(s).map_err(|_| wire_err("invalid uuid"))
}

fn u128_le(v: &[u8], off: &mut usize) -> io::Result<u128> {
    let b: [u8; 16] = take_slice(v, off, 16)?
        .try_into()
        .map_err(|_| wire_err("u128 slice"))?;
    Ok(u128::from_le_bytes(b))
}

fn u16_le(v: &[u8], off: &mut usize) -> io::Result<u16> {
    let b: [u8; 2] = take_slice(v, off, 2)?
        .try_into()
        .map_err(|_| wire_err("u16 slice"))?;
    Ok(u16::from_le_bytes(b))
}

fn status_byte(value: &[u8], offset: &mut usize) -> io::Result<Status> {
    let b = take_slice(value, offset, 1)?[0];
    Status::try_from(b).map_err(|_| wire_err("invalid status byte"))
}

use crate::{CorylusError, object, partition, serde};

#[derive(Debug, Eq, PartialEq)]
pub enum Event {
    PeerAdded { id: Uuid },
    PeerRemoved { id: Uuid },
    Checkpoint,
}

#[derive(Debug, Eq, PartialEq)]
pub struct Inbound {
    pub(crate) from: Uuid,
    pub(crate) p: Raw,
}

impl Inbound {
    pub fn new(id: Uuid, raw: Raw) -> Self {
        Self { from: id, p: raw }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Packet<'a> {
    Request(Request<'a>),
    Reply(Reply<'a>),
}

#[derive(Debug, Eq, PartialEq)]
pub enum Request<'a> {
    HeartBeat,
    WriteOp {
        v: u128,
        corr_id: Uuid,
        // Just to check inconsistencies in the partition tables to reject
        part_id: u16,
        obj_id: &'a str,
        op_id: &'a str,
        raw_op: &'a [u8],
    },
    PartitionFetchCompletion {
        v: u128,
        corr_id: Uuid,
        part_id: u16,
    },
    WhoIs {
        id: Uuid,
        addr: SocketAddr,
    },
    GetOp {
        v: u128,
        corr_id: Uuid,
        // Just to check inconsistences in the partition tables to reject
        part_id: u16,
        obj_id: &'a str,
        op_id: &'a str,
        raw_op: &'a [u8],
    },
    FetchObject {
        v: u128,
        corr_id: Uuid,
        // Just to check inconsistences in the partition tables to reject
        part_id: u16,
        obj_id: &'a str,
    },
}

impl Request<'_> {
    pub fn correlation_id(&self) -> Option<Uuid> {
        match self {
            Self::WhoIs { .. } => None,
            Self::GetOp { corr_id, .. } => Some(*corr_id),
            Self::FetchObject { corr_id, .. } => Some(*corr_id),
            Self::WriteOp { corr_id, .. } => Some(*corr_id),
            Self::HeartBeat => None,
            Self::PartitionFetchCompletion { corr_id, .. } => Some(*corr_id),
        }
    }
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
            1 => Ok(Status::Rebalance),
            2 => Ok(Status::OperationNotFound),
            3 => Ok(Status::PartitionNotFound),
            4 => Ok(Status::ObjectNotFound),
            5 => Ok(Status::IoError),
            6 => Ok(Status::SerdeInvalidBufferSize),
            7 => Ok(Status::SerdeUnknown),
            8 => Ok(Status::SerdeInvalidUtf8),
            9 => Ok(Status::NotEnoughReplicas),
            10 => Ok(Status::PartitionNotReady),
            _ => Err(()),
        }
    }
}
#[derive(Debug, Eq, PartialEq)]
pub enum Reply<'a> {
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
        result: &'a [u8],
    },
    FetchObject {
        corr_id: Uuid,
        status: Status,
        result: &'a [u8],
    },

    PartitionFetchCompletion {
        corr_id: Uuid,
        status: Status,
    },
}

impl Reply<'_> {
    pub fn correlation_id(&self) -> Option<Uuid> {
        match self {
            Reply::WhoIs { .. } => None,
            Reply::WriteOp { corr_id, .. } => Some(*corr_id),
            Reply::GetOp { corr_id, .. } => Some(*corr_id),
            Reply::FetchObject { corr_id, .. } => Some(*corr_id),
            Reply::PartitionFetchCompletion { corr_id, .. } => Some(*corr_id),
        }
    }
}

impl Packet<'_> {
    pub fn kind(&self) -> Kind {
        match self {
            Self::Request(val) => match val {
                Request::WhoIs { .. } => Kind::WhoIsRequest,
                Request::GetOp { .. } => Kind::GetOpRequest,
                Request::FetchObject { .. } => Kind::FetchObjectRequest,
                Request::WriteOp { .. } => Kind::WriteOpRequest,
                Request::HeartBeat => Kind::HeartBeatRequest,
                Request::PartitionFetchCompletion { .. } => Kind::PartitionFetchCompletionRequest,
            },
            Self::Reply(val) => match val {
                Reply::WhoIs { .. } => Kind::WhoIsReply,
                Reply::WriteOp { .. } => Kind::WriteOpReply,
                Reply::GetOp { .. } => Kind::GetOpReply,
                Reply::FetchObject { .. } => Kind::FetchObjectReply,
                Reply::PartitionFetchCompletion { .. } => Kind::PartitionFetchCompletionReply,
            },
        }
    }

    pub fn correlation_id(&self) -> Option<Uuid> {
        match self {
            Self::Request(req) => req.correlation_id(),
            Self::Reply(reply) => reply.correlation_id(),
        }
    }
}

impl From<&Packet<'_>> for Vec<u8> {
    fn from(packet: &Packet) -> Self {
        let kind = packet.kind();
        let mut buffer = kind.buffer();

        match kind.len() {
            Len::Hint(_) => {
                buffer.extend_from_slice(&0u32.to_le_bytes());
            }
            Len::Exact(val) => {
                buffer.extend_from_slice(&((val + DISCRIMINANT) as u32).to_le_bytes());
            }
        }

        buffer.push(kind as u8);

        match packet {
            Packet::Request(req) => match req {
                Request::WhoIs { id, addr } => {
                    buffer.extend_from_slice(id.as_bytes());

                    let addr = addr.to_string().into_bytes();
                    buffer.extend_from_slice(addr.as_slice());
                }

                Request::GetOp {
                    v,
                    corr_id,
                    part_id,
                    obj_id,
                    op_id,
                    raw_op,
                } => {
                    buffer.extend_from_slice(&v.to_le_bytes());
                    buffer.extend_from_slice(corr_id.as_bytes());
                    buffer.extend_from_slice(&part_id.to_le_bytes());

                    let obj_id_len: u16 = obj_id.len() as u16;
                    buffer.extend_from_slice(&obj_id_len.to_le_bytes());
                    buffer.extend_from_slice(obj_id.as_bytes());

                    let op_id_len: u16 = op_id.len() as u16;
                    buffer.extend_from_slice(&op_id_len.to_le_bytes());
                    buffer.extend_from_slice(op_id.as_bytes());

                    if !raw_op.is_empty() {
                        buffer.extend_from_slice(raw_op);
                    }
                }
                Request::FetchObject {
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
                Request::HeartBeat => {}
                Request::WriteOp {
                    v,
                    corr_id,
                    part_id,
                    obj_id,
                    op_id,
                    raw_op,
                } => {
                    buffer.extend_from_slice(&v.to_le_bytes());
                    buffer.extend_from_slice(corr_id.as_bytes());
                    buffer.extend_from_slice(&part_id.to_le_bytes());

                    let obj_id_len: u16 = obj_id.len() as u16;
                    buffer.extend_from_slice(&obj_id_len.to_le_bytes());
                    buffer.extend_from_slice(obj_id.as_bytes());

                    let op_id_len: u16 = op_id.len() as u16;
                    buffer.extend_from_slice(&op_id_len.to_le_bytes());
                    buffer.extend_from_slice(op_id.as_bytes());

                    if !raw_op.is_empty() {
                        buffer.extend_from_slice(raw_op);
                    }
                }
                Request::PartitionFetchCompletion {
                    v,
                    corr_id,
                    part_id,
                } => {
                    buffer.extend_from_slice(&v.to_le_bytes());
                    buffer.extend_from_slice(corr_id.as_bytes());
                    buffer.extend_from_slice(&part_id.to_le_bytes());
                }
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
                        buffer.extend_from_slice(result);
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
                        buffer.extend_from_slice(result);
                    }
                }

                Reply::PartitionFetchCompletion { corr_id, status } => {
                    buffer.extend_from_slice(corr_id.as_bytes());
                    buffer.push(*status as u8);
                }
            },
        }

        if matches!(kind.len(), Len::Hint(_)) {
            let len = buffer[4..].len() as u32;
            buffer[0..4].copy_from_slice(&len.to_le_bytes());
        }

        buffer
    }
}

impl<'a> TryFrom<&'a Raw> for Packet<'a> {
    type Error = io::Error;

    fn try_from(value: &'a Raw) -> Result<Self, Self::Error> {
        match value.try_kind()? {
            Kind::PartitionFetchCompletionReply
            | Kind::FetchObjectReply
            | Kind::GetOpReply
            | Kind::WriteOpReply
            | Kind::WhoIsReply => Ok(Packet::Reply(Reply::try_from(value)?)),
            Kind::WhoIsRequest
            | Kind::HeartBeatRequest
            | Kind::WriteOpRequest
            | Kind::GetOpRequest
            | Kind::FetchObjectRequest
            | Kind::PartitionFetchCompletionRequest => {
                Ok(Packet::Request(Request::try_from(value)?))
            }
        }
    }
}

const DISCRIMINANT: usize = std::mem::size_of::<u8>();
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

#[repr(u8)]
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum Kind {
    WhoIsRequest = 1,
    WhoIsReply = 2,
    HeartBeatRequest = 3,
    WriteOpRequest = 6,
    WriteOpReply = 7,
    GetOpRequest = 8,
    GetOpReply = 9,
    FetchObjectRequest = 10,
    FetchObjectReply = 11,
    PartitionFetchCompletionRequest = 12,
    PartitionFetchCompletionReply = 13,
}
impl Display for Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Kind::WhoIsRequest => "WhoIsRequest",
            Kind::WhoIsReply => "WhoIsReply",
            Kind::HeartBeatRequest => "HeartBeatRequest",
            Kind::WriteOpRequest => "WriteOpRequest",
            Kind::WriteOpReply => "WriteOpReply",
            Kind::GetOpRequest => "GetOpRequest",
            Kind::GetOpReply => "GetOpReply",
            Kind::FetchObjectRequest => "FetchObjectRequest",
            Kind::FetchObjectReply => "FetchObjectReply",
            Kind::PartitionFetchCompletionRequest => "PartitionFetchCompletionRequest",
            Kind::PartitionFetchCompletionReply => "PartitionFetchCompletionReply",
        };

        f.write_str(s)
    }
}

impl Kind {
    pub fn len(&self) -> Len {
        match self {
            Kind::WhoIsRequest => Len::Hint(128),
            Kind::WhoIsReply => Len::Exact(16),
            Kind::HeartBeatRequest => Len::Exact(0),
            Kind::WriteOpRequest => Len::Hint(256),
            Kind::WriteOpReply => Len::Exact(17),
            Kind::GetOpRequest => Len::Hint(512),
            Kind::GetOpReply => Len::Hint(512),
            Kind::FetchObjectRequest => Len::Hint(128),
            Kind::FetchObjectReply => Len::Hint(4096),
            Kind::PartitionFetchCompletionRequest => Len::Exact(34),
            Kind::PartitionFetchCompletionReply => Len::Exact(17),
        }
    }

    pub fn buffer(&self) -> Vec<u8> {
        Vec::with_capacity(PACKET_LENGTH + DISCRIMINANT + self.len().value())
    }
}

impl<'a> TryFrom<&'a Raw> for Request<'a> {
    type Error = io::Error;

    fn try_from(value: &'a Raw) -> Result<Self, Self::Error> {
        let mut offset = size_of::<u8>();

        match value.try_kind()? {
            Kind::WhoIsRequest => {
                let id = parse_uuid(value, &mut offset)?;
                let rest = &value[offset..];
                let addr: SocketAddr = std::str::from_utf8(rest)
                    .map_err(|_| wire_err("invalid utf8 in WhoIs address"))?
                    .parse()
                    .map_err(|_| wire_err("invalid socket address in WhoIs"))?;

                Ok(Request::WhoIs { id, addr })
            }
            Kind::HeartBeatRequest => Ok(Request::HeartBeat),
            Kind::WriteOpRequest => {
                let v = u128_le(value, &mut offset)?;
                let corr_id = parse_uuid(value, &mut offset)?;
                let part_id = u16_le(value, &mut offset)?;
                let obj_id_len = u16_le(value, &mut offset)? as usize;
                let obj_id = std::str::from_utf8(take_slice(value, &mut offset, obj_id_len)?)
                    .map_err(|_| wire_err("invalid utf8 in WriteOp obj_id"))?;
                let op_id_len = u16_le(value, &mut offset)? as usize;
                let op_id = std::str::from_utf8(take_slice(value, &mut offset, op_id_len)?)
                    .map_err(|_| wire_err("invalid utf8 in WriteOp op_id"))?;
                let raw_op = if offset >= value.len() {
                    &[][..]
                } else {
                    &value[offset..]
                };

                Ok(Request::WriteOp {
                    v,
                    corr_id,
                    part_id,
                    obj_id,
                    op_id,
                    raw_op,
                })
            }
            Kind::GetOpRequest => {
                let v = u128_le(value, &mut offset)?;
                let corr_id = parse_uuid(value, &mut offset)?;
                let part_id = u16_le(value, &mut offset)?;
                let obj_id_len = u16_le(value, &mut offset)? as usize;
                let obj_id = std::str::from_utf8(take_slice(value, &mut offset, obj_id_len)?)
                    .map_err(|_| wire_err("invalid utf8 in GetOp obj_id"))?;
                let op_id_len = u16_le(value, &mut offset)? as usize;
                let op_id = std::str::from_utf8(take_slice(value, &mut offset, op_id_len)?)
                    .map_err(|_| wire_err("invalid utf8 in GetOp op_id"))?;
                let raw_op = if offset >= value.len() {
                    &[][..]
                } else {
                    &value[offset..]
                };

                Ok(Request::GetOp {
                    v,
                    corr_id,
                    part_id,
                    obj_id,
                    op_id,
                    raw_op,
                })
            }
            Kind::FetchObjectRequest => {
                let v = u128_le(value, &mut offset)?;
                let corr_id = parse_uuid(value, &mut offset)?;
                let part_id = u16_le(value, &mut offset)?;
                let obj_id_len = u16_le(value, &mut offset)? as usize;
                let obj_id = std::str::from_utf8(take_slice(value, &mut offset, obj_id_len)?)
                    .map_err(|_| wire_err("invalid utf8 in FetchObject obj_id"))?;

                Ok(Request::FetchObject {
                    v,
                    corr_id,
                    part_id,
                    obj_id,
                })
            }
            Kind::PartitionFetchCompletionRequest => {
                let v = u128_le(value, &mut offset)?;
                let corr_id = parse_uuid(value, &mut offset)?;
                let part_id = u16_le(value, &mut offset)?;

                Ok(Request::PartitionFetchCompletion {
                    v,
                    corr_id,
                    part_id,
                })
            }

            Kind::WhoIsReply
            | Kind::WriteOpReply
            | Kind::GetOpReply
            | Kind::FetchObjectReply
            | Kind::PartitionFetchCompletionReply => Err(wire_err("expected request packet")),
        }
    }
}
impl<'a> TryFrom<&'a Raw> for Reply<'a> {
    type Error = io::Error;

    fn try_from(value: &'a Raw) -> Result<Self, Self::Error> {
        let mut offset = size_of::<u8>();

        match value.try_kind()? {
            Kind::WhoIsReply => Ok(Reply::WhoIs {
                id: parse_uuid(value, &mut offset)?,
            }),
            Kind::WriteOpReply => {
                let corr_id = parse_uuid(value, &mut offset)?;
                let status = status_byte(value, &mut offset)?;

                Ok(Reply::WriteOp { corr_id, status })
            }
            Kind::GetOpReply => {
                let corr_id = parse_uuid(value, &mut offset)?;
                let status = status_byte(value, &mut offset)?;

                let result = if offset >= value.len() {
                    &[][..]
                } else {
                    &value[offset..]
                };

                Ok(Reply::GetOp {
                    corr_id,
                    status,
                    result,
                })
            }
            Kind::FetchObjectReply => {
                let corr_id = parse_uuid(value, &mut offset)?;
                let status = status_byte(value, &mut offset)?;

                let result = if offset >= value.len() {
                    &[][..]
                } else {
                    &value[offset..]
                };

                Ok(Reply::FetchObject {
                    corr_id,
                    status,
                    result,
                })
            }
            Kind::PartitionFetchCompletionReply => {
                let corr_id = parse_uuid(value, &mut offset)?;
                let status = status_byte(value, &mut offset)?;

                Ok(Reply::PartitionFetchCompletion { corr_id, status })
            }
            Kind::WhoIsRequest
            | Kind::HeartBeatRequest
            | Kind::WriteOpRequest
            | Kind::GetOpRequest
            | Kind::FetchObjectRequest
            | Kind::PartitionFetchCompletionRequest => Err(wire_err("expected reply packet")),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Raw(Vec<u8>);

impl Raw {
    pub fn new(buffer: Vec<u8>) -> Self {
        Self(buffer)
    }

    pub fn try_kind(&self) -> io::Result<Kind> {
        let discriminant = self
            .0
            .first()
            .copied()
            .ok_or_else(|| wire_err("empty packet buffer"))?;
        match discriminant {
            1 => Ok(Kind::WhoIsRequest),
            2 => Ok(Kind::WhoIsReply),
            3 => Ok(Kind::HeartBeatRequest),
            6 => Ok(Kind::WriteOpRequest),
            7 => Ok(Kind::WriteOpReply),
            8 => Ok(Kind::GetOpRequest),
            9 => Ok(Kind::GetOpReply),
            10 => Ok(Kind::FetchObjectRequest),
            11 => Ok(Kind::FetchObjectReply),
            12 => Ok(Kind::PartitionFetchCompletionRequest),
            13 => Ok(Kind::PartitionFetchCompletionReply),
            _ => Err(wire_err("unknown packet discriminant")),
        }
    }
}

impl std::ops::Deref for Raw {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}
