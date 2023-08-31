use std::fmt::Display;

use crate::Error;

/// 对于mesh protocol error体系的思考：
/// 1 各种协议内部，支持各自的协议相关的xxxError，emsg内容可能只是简单的错误说明；
/// 2 在paser request、response的最外层，根据协议将错误说明转成对应的标准协议格式；
/// 3 在外层，根据Error的类型，分别进行不同的处理动作。
/// TODO 有待统一整合处理 fishermen

/// 按照mc标准error进行设置异常内容
// pub(super) const REQ_INVALID: &str = "CLIENT_ERROR request is invalid";
pub(super) const REQ_INVALID_KEY: &str = "CLIENT_ERROR request key is invalid";

/// KV 的error分为5类，对应分类及预期处理方案：
///     1. 连接校验异常；    预期处理：断后端连接；
///     2. 非法请求异常；    预期处理：发给client异常响应，并断连；
///     3. 非法key请求异常； 预期处理：发给client异常响应，并断连；
///     4. 普通响应异常；    预期处理：发给client异常信息，不做其他处理；
///     5. 非预期响应异常；  预期处理：
#[derive(Debug)]
pub enum KVError {
    AuthInvalid(Vec<u8>),
    RequestInvalid(Vec<u8>),
    RequestInvalidKey(Vec<u8>),
    ResponseCommonError(Vec<u8>),
    ResponseUnexpectedError(Vec<u8>),
}

/// 将KVError转为FlushOnClose的通用error，从而
impl Into<Error> for KVError {
    fn into(self) -> Error {
        match self {
            Self::AuthInvalid(_) => Error::AuthFailed,
            Self::RequestInvalid(packet) => Error::FlushDynOnClose(packet),
            Self::RequestInvalidKey(packet) => Error::FlushDynOnClose(packet),
            Self::ResponseCommonError(packet) => Error::ResponseCommonError(packet),
            Self::ResponseUnexpectedError(packet) => Error::ResponseUnexpected(packet),
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
