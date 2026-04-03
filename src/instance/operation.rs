use std::{collections::HashMap, io, sync::Arc};

use crate::partition;

#[derive(thiserror::Error, Debug)]
pub enum DeserializeError {
    #[error("Invalid buffer size: expected {expected} bytes, got {got}")]
    InvalidBufferSize { expected: usize, got: usize },
    #[error("Invalid UTF-8: {0}")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),
    #[error("{0}")]
    Unknown(String),
}

pub trait Serializer {
    fn serialize(&self) -> Vec<u8>;
}

pub trait Deserializer: Sized {
    type Error: std::error::Error + Send + Sync + 'static;
    fn deserialize(buffer: &[u8]) -> Result<Self, Self::Error>;
}

pub trait Base: Serializer {
    fn static_id() -> &'static str
    where
        Self: Sized;
    fn id(&self) -> &str;
    fn partition_key(&self) -> Vec<u8>;
}

pub trait Write: Base {
    fn execute(&mut self, segment: &mut dyn partition::RawSegment);
    fn deserialize(val: &[u8]) -> Result<GenericWrite, io::Error>
    where
        Self: Sized;
}

pub trait Read: Base {
    fn execute(&self, segment: &dyn partition::RawSegment) -> Vec<u8>;
    fn deserialize(val: &[u8]) -> Result<GenericRead, io::Error>
    where
        Self: Sized;
}

// --- Adapter ---

pub struct GenericRead {
    pub(crate) inner: Box<dyn Read>,
}

impl Serializer for GenericRead {
    fn serialize(&self) -> Vec<u8> {
        self.inner.serialize()
    }
}

impl Base for GenericRead {
    fn static_id() -> &'static str
    where
        Self: Sized,
    {
        unreachable!("Generic type must not call associated type function")
    }

    fn id(&self) -> &str {
        self.inner.id()
    }

    fn partition_key(&self) -> Vec<u8> {
        self.inner.partition_key()
    }
}

impl Read for GenericRead {
    fn execute(&self, segment: &dyn partition::RawSegment) -> Vec<u8> {
        self.inner.execute(segment)
    }

    fn deserialize(_: &[u8]) -> Result<GenericRead, io::Error>
    where
        Self: Sized,
    {
        unreachable!("Generic type must not call associated type function")
    }
}

pub struct GenericWrite {
    pub(crate) inner: Box<dyn Write>,
}

impl Serializer for GenericWrite {
    fn serialize(&self) -> Vec<u8> {
        self.inner.serialize()
    }
}

impl Base for GenericWrite {
    fn static_id() -> &'static str
    where
        Self: Sized,
    {
        unreachable!("Generic type must not call associated type function")
    }

    fn id(&self) -> &str {
        self.inner.id()
    }

    fn partition_key(&self) -> Vec<u8> {
        self.inner.partition_key()
    }
}

impl Write for GenericWrite {
    fn execute(&mut self, segment: &mut dyn partition::RawSegment) {
        self.inner.execute(segment);
    }

    fn deserialize(_: &[u8]) -> Result<GenericWrite, io::Error>
    where
        Self: Sized,
    {
        unreachable!("Generic type must not call associated type function")
    }
}

// --- Core ---

pub type ReadBuilder = fn(&[u8]) -> Result<GenericRead, io::Error>;
pub type WriteBuilder = fn(&[u8]) -> Result<GenericWrite, io::Error>;
pub type OpId = &'static str;

pub enum Type {
    Read,
    Write,
}

#[derive(Clone)]
pub struct Registry {
    inner: Arc<Inner>,
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

impl Registry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Default::default()),
        }
    }
}

#[derive(Default)]
pub struct Inner {
    types: HashMap<OpId, Type>,
    read_fns: HashMap<OpId, ReadBuilder>,
    write_fns: HashMap<OpId, WriteBuilder>,
}

impl Registry {
    pub fn with_read_op<O: Read>(mut self) -> Self {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.types.insert(O::static_id(), Type::Read);
            inner.read_fns.insert(O::static_id(), O::deserialize);
        }
        self
    }
    pub fn with_write_op<O: Write>(mut self) -> Self {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.types.insert(O::static_id(), Type::Write);
            inner.write_fns.insert(O::static_id(), O::deserialize);
        }
        self
    }

    pub fn read_fn(&self, op_id: &str) -> Option<ReadBuilder> {
        self.inner.read_fns.get(op_id).copied()
    }

    pub fn write_fn(&self, op_id: &str) -> Option<WriteBuilder> {
        self.inner.write_fns.get(op_id).copied()
    }
}
