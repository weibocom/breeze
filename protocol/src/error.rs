use crate::msgque::mcq::text::error::McqError;
#[derive(Debug)]
#[repr(u8)]
pub enum Error {
    // Redis 的扩展Error目前都是FlushOnClose
    // Redis(RedisError),
    Mcq(McqError),
    // 关闭连接前需要把异常消息发出去
    FlushOnClose(&'static [u8]),
    // TODO: 先临时用这个打通，后续优化
    MysqlError,
    Eof,
    UnexpectedData,
    QueueClosed,
    NotInit,
    Closed,
    QueueFull,
    ChanFull,
    ChanDisabled,
    ChanClosed,
    ProtocolIncomplete,
    RequestInvalidMagic,
    ResponseInvalidMagic,
    RequestProtocolInvalid,
    ResponseQuiet, // mc的response返回了quite请求
    //RequestProtocolInvalidNumber(&'static str),
    //RequestProtocolInvalidStar(&'static str),
    //RequestProtocolInvalidNumberZero(&'static str),
    //RequestProtocolInvalidDigit(&'static str),
    //RequestProtocolInvalidNoReturn(&'static str),
    ResponseProtocolInvalid,
    ProtocolNotSupported,
    //IndexOutofBound,
    //Inner,
    TopChanged,
    WriteResponseErr,
    NoResponseFound,
    OpCodeNotSupported(u16),
    // CommandNotSupported,
    BufferFull,
    Quit,
    Timeout(u16),
    Pending, // 在连接退出时，仍然有请求在队列中没有发送。
    Waiting, // 连接退出时，有请求已发送，但未接收到response
    IO(std::io::ErrorKind),
    AuthFailed,
}

impl From<std::io::Error> for Error {
    #[inline]
    fn from(err: std::io::Error) -> Self {
        Self::IO(err.kind())
    }
}
impl From<u64> for Error {
    #[inline]
    fn from(to: u64) -> Self {
        Self::Timeout(to.min(u16::MAX as u64) as u16)
    }
}

impl std::error::Error for Error {}
use std::fmt::{self, Display, Formatter};
impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            //Error::RequestProtocolInvalid(desc) => write!(f, "{}", desc),
            //Error::RequestProtocolInvalidNumber(desc) => write!(f, "{}", desc),
            //Error::RequestProtocolInvalidStar(desc) => write!(f, "{}", desc),
            // Error::RequestProtocolInvalidNumberZero(desc) => write!(f, "{}", desc),
            //// Error::RequestProtocolInvalidDigit(desc) => write!(f, "{}", desc),
            //Error::RequestProtocolInvalidNoReturn(desc) => write!(f, "{}", desc),
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
