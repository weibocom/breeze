use crate::Error;

pub(super) enum PtError {
    ReqInvalid,
    ReqInvalidNum,
    ReqInvalidNoReturn,
    // ReqInvalidStar,
    // ReqInvalidNumZero,
    // ReqInvalidDigit,
}
impl PtError {
    pub(super) fn error(&self) -> Error {
        match self {
            PtError::ReqInvalid => Error::RequestProtocolInvalid("-ERR request invalid\r\n"),

            PtError::ReqInvalidNum => {
                Error::RequestProtocolInvalidNumber("-ERR request invalid num\r\n")
            }
            PtError::ReqInvalidNoReturn => {
                Error::RequestProtocolInvalidNoReturn("-ERR request missing return\r\n")
            } // PtError::ReqInvalidStar => {
              //     Error::RequestProtocolInvalidStar("-ERR request invalid star\r\n")
              // }
              // PtError::ReqInvalidNumZero => {
              //     Error::RequestProtocolInvalidNumberZero("-ERR request invalid num zero\r\n")
              // }
              // PtError::ReqInvalidDigit => {
              //     Error::RequestProtocolInvalidDigit("-ERR request invalid digit\r\n")
              // }
        }
    }
}
