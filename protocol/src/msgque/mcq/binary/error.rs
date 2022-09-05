use crate::Error;

pub(super) enum MemcacheError {
    ReqInvalid,
}
impl MemcacheError {
    pub(super) fn error(&self) -> Error {
        match self {
            MemcacheError::ReqInvalid => {
                Error::RequestProtocolInvalid("CLIENT_ERROR request malformed\r\n")
            }
        }
    }
}
