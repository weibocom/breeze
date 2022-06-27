use crate::Error;

pub(super) enum RedisError {
    ReqInvalid,
    ReqInvalidStar,
    ReqInvalidNum,
    ReqInvalidNoReturn,
    // ReqInvalidNumZero,
    // ReqInvalidDigit,
}
impl RedisError {
    pub(super) fn error(&self) -> Error {
        match self {
            RedisError::ReqInvalid => Error::RequestProtocolInvalid("-ERR request invalid\r\n"),
            RedisError::ReqInvalidStar => {
                Error::RequestProtocolInvalidStar("-ERR request invalid star\r\n")
            }
            RedisError::ReqInvalidNum => {
                Error::RequestProtocolInvalidNumber("-ERR request invalid num\r\n")
            }
            RedisError::ReqInvalidNoReturn => {
                Error::RequestProtocolInvalidNoReturn("-ERR request missing return\r\n")
            } // RedisError::ReqInvalidNumZero => {
              //     Error::RequestProtocolInvalidNumberZero("-ERR request invalid num zero\r\n")
              // }
              // RedisError::ReqInvalidDigit => {
              //     Error::RequestProtocolInvalidDigit("-ERR request invalid digit\r\n")
              // }
        }
    }
}
