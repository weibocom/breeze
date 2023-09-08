use std::{fmt::Display, io::ErrorKind};

use crate::Error;

// /// 按照mc标准error进行设置异常内容
// // pub(super) const REQ_INVALID: &str = "CLIENT_ERROR request is invalid";
pub(super) const REQ_INVALID_KEY: &str = "CLIENT_ERROR request key is invalid";

/// 在mysql解析过程中，统统返回KVError类型，最后解析处理完毕后，再转为crate::Error
pub type KVResult<T> = std::result::Result<T, KVError>;

#[derive(Debug)]
pub enum KVError {
    IO(ErrorKind),                  // IO异常；  预期处理：断连接
    AuthInvalid(Vec<u8>),           // 连接校验异常； 预期处理：断后端连接；
    RequestInvalid(Vec<u8>),        // 非法请求异常； 预期处理：发给client异常响应，并断连；
    RequestInvalidKey(Vec<u8>),     // 非法key请求异常； 预期处理：发给client异常响应，并断连；
    UnhandleResponseError(Vec<u8>), // mesh无法处理的响应异常； 预期处理： 发现后端异常，直接返回sdk
    ProtocolIncomplete,             // 协议未读完； 预期处理：继续读取
}

/// 将KVError转为FlushOnClose的通用error，从而
impl Into<Error> for KVError {
    fn into(self) -> Error {
        match self {
            Self::IO(e) => Error::IO(e),
            Self::AuthInvalid(_) => Error::AuthFailed,
            Self::RequestInvalid(packet) => Error::FlushOnClose(packet),
            Self::RequestInvalidKey(packet) => Error::FlushOnClose(packet),
            Self::UnhandleResponseError(packet) => {
                // 该异常需要构建成response，不能转为error传出
                panic!("kv unhanlde rsp err: {:?}", packet);
            }
            Self::ProtocolIncomplete => Error::ProtocolIncomplete,
        }
    }
}

/// 实现std的Error trait，方便kv内部统一使用KVError
impl std::error::Error for KVError {}

impl Display for KVError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KVError: {:?}", self)
    }
}

impl From<std::io::Error> for KVError {
    #[inline]
    fn from(err: std::io::Error) -> Self {
        Self::IO(err.kind())
    }
}
