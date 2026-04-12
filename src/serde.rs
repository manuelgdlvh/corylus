pub mod object;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid buffer size")]
    InvalidBufferSize,
    #[error("Invalid UTF-8")]
    InvalidUtf8,
    #[error("Unknown error")]
    Unknown,
}

pub trait Serializer {
    fn serialize(&self) -> Vec<u8>;
}

pub trait Deserializer: Sized {
    fn deserialize(buffer: &[u8]) -> Result<Self, Error>;
}
