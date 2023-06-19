use crate::Error;

/// 用于处理Redis请求解析中的异常，返回特定的异常响应
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

// impl Into<Error> for RedisError {
//     #[inline]
//     fn into(self) -> Error {
//         Error::Redis(self)
//     }
// }

/// 将Redis error转为通用可flush的Error，保留Error细节
impl Into<Error> for RedisError {
    #[inline]
    fn into(self) -> Error {
        let mut emsg = Vec::with_capacity(32);
        match self {
            Self::ReqInvalid => {
                emsg.extend("-ERR malformed request\r\n".as_bytes());
                Error::FlushOnClose(emsg)
            }
            Self::ReqInvalidStar => {
                emsg.extend("-ERR invalid star\r\n".as_bytes());
                Error::FlushOnClose(emsg)
            }
            Self::ReqInvalidNum => {
                emsg.extend("-ERR invalid num\r\n".as_bytes());
                Error::FlushOnClose(emsg)
            }
            Self::ReqInvalidNoReturn => {
                emsg.extend("-ERR invalid no return char\r\n".as_bytes());
                Error::FlushOnClose(emsg)
            }
            Self::ReqInvalidBulkNum => {
                emsg.extend("-ERR invalid bulk num\r\n".as_bytes());
                Error::FlushOnClose(emsg)
            }
            Self::ReqNotSupported => {
                emsg.extend("-ERR unsupport cmd\r\n".as_bytes());
                Error::FlushOnClose(emsg)
            }
            Self::RespInvalid => {
                emsg.extend("-ERR response invalid\r\n".as_bytes());
                Error::FlushOnClose(emsg)
            }
        }
    }
}
