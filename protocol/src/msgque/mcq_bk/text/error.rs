use crate::Error;

#[derive(Debug)]
pub enum McqError {
    ReqInvalid,
    RspInvalid,
}

impl Into<Error> for McqError {
    #[inline]
    fn into(self) -> Error {
        Error::Mcq(self)
    }
}
