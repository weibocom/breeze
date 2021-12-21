use std::time::Duration;

#[derive(Debug)]
pub enum Error {
    ReadEof,
    QueueClosed,
    NotInit,
    Closed,
    QueueFull,
    RequestProtocolNotValid,
    ResponseProtocolNotValid,
    ProtocolNotSupported,
    IndexOutofBound,
    Inner,
    TopChanged,
    WriteResponseErr,
    NoResponseFound,
    Quit,
    Timeout((Duration, u32)),
    Pending, // 在连接退出时，仍然有请求在队列中没有发送。
    Waiting, // 连接退出时，有请求已发送，但未接收到response
    IO(std::io::Error),
}

impl From<std::io::Error> for Error {
    #[inline(always)]
    fn from(err: std::io::Error) -> Self {
        Self::IO(err)
    }
}

impl std::error::Error for Error {}
use std::fmt::{self, Display, Formatter};
impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "error: {:?}", self)
    }
}
