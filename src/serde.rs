pub mod object;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid buffer size: expected {expected} bytes, got {got}")]
    InvalidBufferSize { expected: usize, got: usize },
    #[error("Invalid UTF-8: {0}")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),
    #[error("Unknown error")]
    Unknown,
}

pub trait Serializer {
    fn serialize(&self) -> Vec<u8>;
}

pub trait Deserializer: Sized {
    fn deserialize(buffer: &[u8]) -> Result<Self, Error>;
}
