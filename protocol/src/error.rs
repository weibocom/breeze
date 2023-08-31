use crate::msgque::mcq::text::error::McqError;

#[derive(Debug)]
#[repr(u8)]
pub enum Error {
    // Redis 的扩展Error目前都是FlushOnClose
    // Redis(RedisError),
    Mcq(McqError),
    // 关闭连接前需要把（固定的）异常消息发出去
    FlushOnClose(&'static [u8]),
    // 关闭连接前需要把（动态的）异常消息发出去
    FlushDynOnClose(Vec<u8>),
    // 目前用这个表示预期内的异常响应，KV对这种异常会直接返回sdk
    ResponseCommonError(Vec<u8>),
    // 用这个异常表示不知如何处理的响应，通常处理方式：关闭后端连接
    ResponseUnexpected(Vec<u8>),

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

/// 将str转为vec，方便Error内部信息的转换
#[inline(always)]
pub(crate) fn str_to_vec(s: &str) -> Vec<u8> {
    let mut msg = Vec::with_capacity(s.len());
    msg.extend(s.as_bytes());
    msg
}
