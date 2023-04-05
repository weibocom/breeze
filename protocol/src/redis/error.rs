use crate::Error;

#[derive(Debug)]
pub enum RedisError {
    ReqInvalid,
    ReqInvalidStar,
    ReqInvalidNum,
    ReqInvalidNoReturn,
    ReqInvalidBulkNum,
    // ReqInvalidNumZero,
    // ReqInvalidDigit,
}

impl Into<Error> for RedisError {
    #[inline]
    fn into(self) -> Error {
        Error::Redis(self)
    }
}
