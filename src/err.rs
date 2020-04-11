use failure::Fail;
use std::io;

#[derive(Debug, Fail)]
pub enum KvError {
    #[fail(display = "io error occur: {}", _0)]
    IO(#[cause] io::Error),
    #[fail(display = "key not found")]
    KeyNotFound,
    #[fail(display = "unknown command")]
    UnKnownCommand,
    #[fail(display = "serde err: {}", _0)]
    Serde(#[cause] serde_json::Error)
}

impl From<io::Error> for KvError {
    fn from(e: io::Error) -> KvError {
        KvError::IO(e)
    }
}
impl From<serde_json::Error> for KvError {
    fn from(e: serde_json::Error) -> KvError {
        KvError::Serde(e)
    }
}


pub type Result<T> = std::result::Result<T, KvError>;