use std::{collections::HashMap, io};

use crate::{object, serde};

pub trait Base: serde::Serializer + Send {
    fn static_id() -> &'static str
    where
        Self: Sized;
    fn id(&self) -> &str;
    fn partition_key(&self) -> Vec<u8>;
}

pub trait Write: Base {
    fn execute(&mut self, segment: &mut dyn object::Raw);
    fn deserialize(val: &[u8]) -> Result<GenericWrite, io::Error>
    where
        Self: Sized;
}

pub trait Read: Base {
    fn execute(&self, segment: &dyn object::Raw) -> Vec<u8>;
    fn deserialize(val: &[u8]) -> Result<GenericRead, io::Error>
    where
        Self: Sized;
}

// --- Adapter ---

pub struct GenericRead {
    pub(crate) inner: Box<dyn Read>,
}

impl serde::Serializer for GenericRead {
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
    fn execute(&self, segment: &dyn object::Raw) -> Vec<u8> {
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

impl serde::Serializer for GenericWrite {
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
    fn execute(&mut self, segment: &mut dyn object::Raw) {
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

#[derive(Default)]
pub struct Registry {
    read_fns: HashMap<OpId, ReadBuilder>,
    write_fns: HashMap<OpId, WriteBuilder>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Registry {
    pub fn with_read_op<O: Read>(mut self) -> Self {
        self.read_fns.insert(O::static_id(), O::deserialize);
        self
    }
    pub fn with_write_op<O: Write>(mut self) -> Self {
        self.write_fns.insert(O::static_id(), O::deserialize);
        self
    }

    pub fn read_fn(&self, opart_id: &str) -> Result<ReadBuilder, object::Error> {
        self.read_fns
            .get(opart_id)
            .ok_or(object::Error::OperationNotFound)
            .copied()
    }

    pub fn write_fn(&self, opart_id: &str) -> Result<WriteBuilder, object::Error> {
        self.write_fns
            .get(opart_id)
            .ok_or(object::Error::OperationNotFound)
            .copied()
    }
}
