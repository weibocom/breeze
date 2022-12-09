use ds::time::Duration;

#[derive(Debug)]
pub enum Error {
    Eof,
    QueueClosed,
    NotInit,
    Closed,
    QueueFull,
    ChanFull,
    ChanDisabled,
    ChanClosed,
    ProtocolIncomplete,
    RequestProtocolInvalid(&'static str),
    RequestProtocolInvalidNumber(&'static str),
    RequestProtocolInvalidStar(&'static str),
    RequestProtocolInvalidNumberZero(&'static str),
    RequestProtocolInvalidDigit(&'static str),
    RequestProtocolInvalidNoReturn(&'static str),
    ResponseProtocolInvalid,
    ProtocolNotSupported,
    //IndexOutofBound,
    //Inner,
    TopChanged,
    WriteResponseErr,
    NoResponseFound,
    // CommandNotSupported,
    BufferFull,
    Quit,
    Timeout(Duration),
    Pending, // 在连接退出时，仍然有请求在队列中没有发送。
    Waiting, // 连接退出时，有请求已发送，但未接收到response
    IO(std::io::Error),
}

impl From<std::io::Error> for Error {
    #[inline]
    fn from(err: std::io::Error) -> Self {
        Self::IO(err)
    }
}
impl From<Duration> for Error {
    #[inline]
    fn from(to: Duration) -> Self {
        Self::Timeout(to)
    }
}

impl std::error::Error for Error {}
use std::fmt::{self, Display, Formatter};
impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Error::RequestProtocolInvalid(desc) => write!(f, "{}", desc),
            Error::RequestProtocolInvalidNumber(desc) => write!(f, "{}", desc),
            Error::RequestProtocolInvalidStar(desc) => write!(f, "{}", desc),
            // Error::RequestProtocolInvalidNumberZero(desc) => write!(f, "{}", desc),
            // Error::RequestProtocolInvalidDigit(desc) => write!(f, "{}", desc),
            Error::RequestProtocolInvalidNoReturn(desc) => write!(f, "{}", desc),
            _ => write!(f, "error: {:?}", self),
        }
    }
}

#[allow(dead_code)]
pub enum ProtocolType {
    Request,
    Response,
}

impl Error {
    #[inline]
    pub fn proto_not_support(&self) -> bool {
        match self {
            Self::ProtocolNotSupported => true,
            _ => false,
        }
    }
}
