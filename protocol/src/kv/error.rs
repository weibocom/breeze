use std::{fmt::Display, io::ErrorKind};

use ds::Utf8;

// /// 按照mc标准error进行设置异常内容
// // pub(super) const REQ_INVALID: &str = "CLIENT_ERROR request is invalid";
pub(super) const REQ_INVALID_KEY: &str = "CLIENT_ERROR request key is invalid";

/// 在mysql解析过程中，统统返回Error类型，最后解析处理完毕后，再转为crate::Error
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    // IO异常；  预期处理：断连接
    AuthInvalid(Vec<u8>),
    IO(ErrorKind),      // mesh无法处理的响应异常； 预期处理： 发现后端异常，直接返回sdk
    ProtocolIncomplete, // 连接校验异常； 预期处理：断后端连接；
    RequestInvalid(Vec<u8>), // 非法请求异常； 预期处理：发给client异常响应，并断连；
    RequestInvalidKey(Vec<u8>), // 非法key请求异常； 预期处理：发给client异常响应，并断连；
    UnhandleResponseError(Vec<u8>), // 协议未读完； 预期处理：继续读取
}

/// 将Error转为FlushOnClose的通用error，从而
impl Into<crate::Error> for Error {
    fn into(self) -> crate::Error {
        match self {
            Self::IO(e) => crate::Error::IO(e),
            Self::AuthInvalid(_) => crate::Error::AuthFailed,
            Self::RequestInvalid(packet) => crate::Error::FlushOnClose(packet.into()),
            Self::RequestInvalidKey(packet) => crate::Error::FlushOnClose(packet.into()),
            Self::UnhandleResponseError(packet) => {
                // auth时，如果有这种异常，需要上抛异常断连接
                log::warn!("found unhandle response: {}", packet.utf8());
                crate::Error::ResponseProtocolInvalid
            }
            Self::ProtocolIncomplete => crate::Error::ProtocolIncomplete,
        }
    }
}

/// 实现std的Error trait，方便kv内部统一使用Error
impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Error: {:?}", self)
    }
}

impl From<std::io::Error> for Error {
    #[inline]
    fn from(err: std::io::Error) -> Self {
        Self::IO(err.kind())
    }
}
