use crate::Error;

pub(super) enum McqError {
    ReqInvalid,
    RspInvalid,
}
impl McqError {
    pub(super) fn error(&self) -> Error {
        match self {
            McqError::ReqInvalid => Error::RequestProtocolInvalid("-ERR request invalid\r\n"),
            McqError::RspInvalid => Error::RequestProtocolInvalid("-ERR response invalid\r\n"),
            // RedisError::ReqInvalidNumZero => {
            //     Error::RequestProtocolInvalidNumberZero("-ERR request invalid num zero\r\n")
            // }
            // RedisError::ReqInvalidDigit => {
            //     Error::RequestProtocolInvalidDigit("-ERR request invalid digit\r\n")
            // }
        }
    }
}
