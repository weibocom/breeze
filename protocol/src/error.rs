use crate::msgque::mcq::text::error::McqError;

#[derive(Debug)]
#[repr(u8)]
pub enum Error {
    Mcq(McqError),
    // 注意，当前仅在paser_req出错时才会发送错误，关闭连接前需要把（静态/动态）异常消息发出去
    FlushOnClose(ToVec),
    // TODO: 暂时保留，等endpoint merge完毕后再清理，避免merge冲突导致的ci测试问题
    MysqlError(Vec<u8>),
    Eof,
    UnexpectedData,
    NotInit,
    Closed,
    ChanFull,
    ChanDisabled,
    ChanWriteClosed,
    ChanReadClosed,
    ProtocolIncomplete,
    RequestInvalidMagic,
    ResponseInvalidMagic,
    RequestProtocolInvalid,
    ResponseQuiet, // mc的response返回了quite请求
    ResponseProtocolInvalid,
    ProtocolNotSupported,
    TopChanged,
    TopInvalid,
    WriteResponseErr,
    OpCodeNotSupported(u16),
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
        write!(f, "{:?}", self)
    }
}

#[derive(Debug)]
pub enum ToVec {
    Slice(&'static [u8]),
    Vec(Vec<u8>),
}

impl From<&'static [u8]> for ToVec {
    #[inline]
    fn from(s: &'static [u8]) -> Self {
        Self::Slice(s)
    }
}
impl From<Vec<u8>> for ToVec {
    #[inline]
    fn from(v: Vec<u8>) -> Self {
        Self::Vec(v)
    }
}
impl From<String> for ToVec {
    #[inline]
    fn from(v: String) -> Self {
        Self::Vec(v.into())
    }
}
use std::ops::Deref;
impl Deref for ToVec {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &Self::Target {
        match &self {
            Self::Slice(s) => s,
            Self::Vec(v) => v.as_slice(),
        }
    }
}
