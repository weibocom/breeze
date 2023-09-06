use crate::Error;

/// 用于处理Redis协议解析中的异常，用于在关闭client连接前，返回特定的异常响应
#[derive(Debug)]
pub enum RedisError {
    ReqInvalid,
    ReqInvalidStar,
    ReqInvalidNum,
    ReqInvalidNoReturn,
    ReqInvalidBulkNum,
    ReqNotSupported,
    RespInvalid,
    // ReqInvalidNumZero,
    // ReqInvalidDigit,
}

lazy_static! {
    static ref REQ_INVALID: Vec<u8> = str_to_vec("-ERR invalid request\r\n");
    static ref REQ_INVALID_STAR: Vec<u8> = str_to_vec("-ERR invalid star\r\n");
    static ref REQ_INVALID_NUM: Vec<u8> = str_to_vec("-ERR invalid num\r\n");
    static ref REQ_INVALID_NO_RETURN: Vec<u8> = str_to_vec("-ERR invalid no return char\r\n");
    static ref REQ_INVALID_BULK_NUM: Vec<u8> = str_to_vec("-ERR invalid bulk num\r\n");
    static ref REQ_NOT_SUPPORTED: Vec<u8> = str_to_vec("-ERR unsupport cmd\r\n");
    static ref RESP_INVALID: Vec<u8> = str_to_vec("-ERR  mesh bug for parsing resp\r\n");
}

/// 将Redis error转为通用可flush的Error，保留Error细节
impl Into<Error> for RedisError {
    #[inline]
    fn into(self) -> Error {
        match self {
            Self::ReqInvalid => Error::FlushOnClose(REQ_INVALID.clone()),
            Self::ReqInvalidStar => Error::FlushOnClose(REQ_INVALID_STAR.clone()),
            Self::ReqInvalidNum => Error::FlushOnClose(REQ_INVALID_NUM.clone()),
            Self::ReqInvalidNoReturn => Error::FlushOnClose(REQ_INVALID_NO_RETURN.clone()),
            Self::ReqInvalidBulkNum => Error::FlushOnClose(REQ_INVALID_BULK_NUM.clone()),
            Self::ReqNotSupported => Error::FlushOnClose(REQ_NOT_SUPPORTED.clone()),
            Self::RespInvalid => Error::FlushOnClose(RESP_INVALID.clone()),
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
