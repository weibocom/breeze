use crate::Error;

/// 用于处理kvector/redis协议解析中的异常，用于在关闭client连接前，返回特定的异常响应
#[derive(Debug)]
pub enum KvectorError {
    // ReqInvalid,
    ReqInvalidStar,
    // ReqInvalidNum,
    // ReqInvalidNoReturn,
    ReqInvalidBulkNum,
    ReqNotSupported,
    // RespInvalid,
    // ReqInvalidNumZero,
    // ReqInvalidDigit,
}

// const REQ_INVALID: &'static [u8] = b"-ERR kv invalid request\r\n";
const REQ_INVALID_STAR: &'static [u8] = b"-ERR kv invalid star\r\n";
// const REQ_INVALID_NUM: &'static [u8] = b"-ERR kv invalid num\r\n";
// const REQ_INVALID_NO_RETURN: &'static [u8] = b"-ERR kv invalid no return char\r\n";
const REQ_INVALID_BULK_NUM: &'static [u8] = b"-ERR kv invalid bulk num\r\n";
const REQ_NOT_SUPPORTED: &'static [u8] = b"-ERR kv unsupport cmd\r\n";
// const RESP_INVALID: &'static [u8] = b"-ERR kv mesh bug for parsing resp\r\n";

/// 将Redis error转为通用可flush的Error，保留Error细节
impl Into<Error> for KvectorError {
    #[inline]
    fn into(self) -> Error {
        match self {
            // Self::ReqInvalid => Error::FlushOnClose(REQ_INVALID.into()),
            Self::ReqInvalidStar => Error::FlushOnClose(REQ_INVALID_STAR.into()),
            // Self::ReqInvalidNum => Error::FlushOnClose(REQ_INVALID_NUM.into()),
            // Self::ReqInvalidNoReturn => Error::FlushOnClose(REQ_INVALID_NO_RETURN.into()),
            Self::ReqInvalidBulkNum => Error::FlushOnClose(REQ_INVALID_BULK_NUM.into()),
            Self::ReqNotSupported => Error::FlushOnClose(REQ_NOT_SUPPORTED.into()),
            // Self::RespInvalid => Error::FlushOnClose(RESP_INVALID.into()),
        }
    }
}
