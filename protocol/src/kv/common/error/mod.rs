// Copyright (c) 2020 rust-mysql-simple contributors
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use super::{
    named_params::MixedParamsError, packets, params::MissingNamedParameterError,
    proto::codec::error::PacketCodecError, row::convert::FromRowError,
    value::convert::FromValueError,
};
use url::ParseError;

use std::{error, fmt, io, sync};

use crate::kv::common::{row::Row, value::Value};

pub mod tls;

impl From<packets::ServerError> for MySqlError {
    fn from(x: packets::ServerError) -> MySqlError {
        MySqlError {
            state: x.sql_state_str().into_owned(),
            code: x.error_code(),
            // message: x.message_str().into_owned(),
            message: x.message_str(),
        }
    }
}

#[derive(Eq, PartialEq, Clone)]
pub struct MySqlError {
    pub state: String,
    pub message: String,
    pub code: u16,
}

impl fmt::Display for MySqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ERROR {} ({}): {}", self.code, self.state, self.message)
    }
}

impl fmt::Debug for MySqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl error::Error for MySqlError {
    fn description(&self) -> &str {
        "Error returned by a server"
    }
}

pub enum Error {
    IoError(io::Error),
    CodecError(super::proto::codec::error::PacketCodecError),
    MySqlError(MySqlError),
    DriverError(DriverError),
    UrlError(UrlError),
    #[cfg(any(feature = "native-tls", feature = "rustls"))]
    TlsError(tls::TlsError),
    FromValueError(Value),
    FromRowError(Row),
}

impl Error {
    // #[doc(hidden)]
    // pub fn is_connectivity_error(&self) -> bool {
    //     match self {
    //         #[cfg(any(feature = "native-tls", feature = "rustls"))]
    //         Error::TlsError(_) => true,
    //         Error::IoError(_) | Error::DriverError(_) | Error::CodecError(_) => true,
    //         Error::MySqlError(_)
    //         | Error::UrlError(_)
    //         | Error::FromValueError(_)
    //         | Error::FromRowError(_) => false,
    //     }
    // }

    // #[doc(hidden)]
    // pub fn server_disconnected() -> Self {
    //     Error::IoError(io::Error::new(
    //         io::ErrorKind::BrokenPipe,
    //         "server disconnected",
    //     ))
    // }

    /// 将mysql内部解析、编码的Error映射到Error，目前仅根据当前使用情况进行转换，
    /// 后续如果Error发生变动，需要关注这里是否需要调整
    #[inline]
    pub(crate) fn error(&self) -> crate::kv::Error {
        let msg = format!("{}", self).as_bytes().to_vec();
        // let content = RingSlice::from_vec(&msg.as_bytes().to_vec());
        log::error!("kv found error: {:?}", self);
        match self {
            Error::IoError(_e) => crate::kv::Error::RequestInvalid(msg), // io异常
            Error::DriverError(_e) => crate::kv::Error::AuthInvalid(msg), // driver 异常，意味着无法正常完成连接
            Error::CodecError(_e) => crate::kv::Error::RequestInvalid(msg), // codec 异常，一般为请求异常
            _ => crate::kv::Error::UnhandleResponseError(msg), // 其他mysql异常，直接返回给调用房
        }
    }
}

impl error::Error for Error {
    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            Error::IoError(ref err) => Some(err),
            Error::DriverError(ref err) => Some(err),
            Error::MySqlError(ref err) => Some(err),
            Error::UrlError(ref err) => Some(err),
            #[cfg(any(feature = "native-tls", feature = "rustls"))]
            Error::TlsError(ref err) => Some(err),
            _ => None,
        }
    }
}

impl From<FromValueError> for Error {
    fn from(FromValueError(value): FromValueError) -> Error {
        Error::FromValueError(value)
    }
}

impl From<FromRowError> for Error {
    fn from(FromRowError(row): FromRowError) -> Error {
        Error::FromRowError(row)
    }
}

impl From<MissingNamedParameterError> for Error {
    fn from(MissingNamedParameterError(name): MissingNamedParameterError) -> Error {
        Error::DriverError(DriverError::MissingNamedParameter(
            String::from_utf8_lossy(&name).into_owned(),
        ))
    }
}

impl From<MixedParamsError> for Error {
    fn from(_: MixedParamsError) -> Error {
        Error::DriverError(DriverError::MixedParams)
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IoError(err)
    }
}

impl From<DriverError> for Error {
    fn from(err: DriverError) -> Error {
        Error::DriverError(err)
    }
}

impl From<MySqlError> for Error {
    fn from(x: MySqlError) -> Error {
        Error::MySqlError(x)
    }
}

impl From<PacketCodecError> for Error {
    fn from(err: PacketCodecError) -> Self {
        Error::CodecError(err)
    }
}

impl From<std::convert::Infallible> for Error {
    fn from(err: std::convert::Infallible) -> Self {
        match err {}
    }
}

impl From<UrlError> for Error {
    fn from(err: UrlError) -> Error {
        Error::UrlError(err)
    }
}

impl<T> From<sync::PoisonError<T>> for Error {
    fn from(_: sync::PoisonError<T>) -> Error {
        Error::DriverError(DriverError::PoisonedPoolMutex)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Error::IoError(ref err) => write!(f, "IoError {{ {} }}", err),
            Error::CodecError(ref err) => write!(f, "CodecError {{ {} }}", err),
            Error::MySqlError(ref err) => write!(f, "{}", err.message),
            Error::DriverError(ref err) => write!(f, "{{ {} }}", err),
            Error::UrlError(ref err) => write!(f, "UrlError {{ {} }}", err),
            #[cfg(any(feature = "native-tls", feature = "rustls"))]
            Error::TlsError(ref err) => write!(f, "TlsError {{ {} }}", err),
            Error::FromRowError(_) => "from row conversion error".fmt(f),
            Error::FromValueError(_) => "from value conversion error".fmt(f),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[derive(Eq, PartialEq, Clone)]
pub enum DriverError {
    // ConnectTimeout,
    // (address, description)
    // CouldNotConnect(Option<(String, String, io::ErrorKind)>),
    UnsupportedProtocol(u8),
    // PacketOutOfSync,
    // PacketTooLarge,
    Protocol41NotSet,
    UnexpectedPacket,
    // MismatchedStmtParams(u16, usize),
    // InvalidPoolConstraints,
    // SetupError,
    // TlsNotSupported,
    // CouldNotParseVersion,
    // ReadOnlyTransNotSupported,
    PoisonedPoolMutex,
    // Timeout,
    MissingNamedParameter(String),
    // NamedParamsForPositionalQuery,
    MixedParams,
    UnknownAuthPlugin(String),
    // OldMysqlPasswordDisabled,
}

impl error::Error for DriverError {
    fn description(&self) -> &str {
        "MySql driver error"
    }
}

impl DriverError {
    // Driver error 用于处理auth中的异常，目前全部转为kv中的AuthInvalid fishermen
    #[inline]
    pub(crate) fn error(&self) -> crate::kv::Error {
        let msg = format!("mysql Driver Error: {}", self);
        // let content = RingSlice::from_vec(&msg.as_bytes().to_vec());
        match self {
            _ => crate::kv::Error::AuthInvalid(msg.as_bytes().to_vec()),
        }
    }
}

impl fmt::Display for DriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            // DriverError::ConnectTimeout => write!(f, "Could not connect: connection timeout"),
            // DriverError::CouldNotConnect(None) => {
            //     write!(f, "Could not connect: address not specified")
            // }
            // DriverError::CouldNotConnect(Some((ref addr, ref desc, _))) => {
            //     write!(f, "Could not connect to address `{}': {}", addr, desc)
            // }
            DriverError::UnsupportedProtocol(proto_version) => {
                write!(f, "Unsupported protocol version {}", proto_version)
            }
            // DriverError::PacketOutOfSync => write!(f, "Packet out of sync"),
            // DriverError::PacketTooLarge => write!(f, "Packet too large"),
            DriverError::Protocol41NotSet => write!(f, "Server must set CLIENT_PROTOCOL_41 flag"),
            DriverError::UnexpectedPacket => write!(f, "Unexpected packet"),
            // DriverError::MismatchedStmtParams(exp, prov) => write!(
            //     f,
            //     "Statement takes {} parameters but {} was supplied",
            //     exp, prov
            // ),
            // DriverError::InvalidPoolConstraints => write!(f, "Invalid pool constraints"),
            // DriverError::SetupError => write!(f, "Could not setup connection"),
            // DriverError::TlsNotSupported => write!(
            //     f,
            //     "Client requires secure connection but server \
            //      does not have this capability"
            // ),
            // DriverError::CouldNotParseVersion => write!(f, "Could not parse MySQL version"),
            // DriverError::ReadOnlyTransNotSupported => write!(
            //     f,
            //     "Read-only transactions does not supported in your MySQL version"
            // ),
            DriverError::PoisonedPoolMutex => write!(f, "Poisoned pool mutex"),
            // DriverError::Timeout => write!(f, "Operation timed out"),
            DriverError::MissingNamedParameter(ref name) => {
                write!(f, "Missing named parameter `{}' for statement", name)
            }
            // DriverError::NamedParamsForPositionalQuery => {
            //     write!(f, "Can not pass named parameters to positional query")
            // }
            DriverError::MixedParams => write!(
                f,
                "Can not mix named and positional parameters in one statement"
            ),
            DriverError::UnknownAuthPlugin(ref name) => {
                write!(f, "Unknown authentication protocol: `{}`", name)
            } // DriverError::OldMysqlPasswordDisabled => {
              //     write!(
              //         f,
              //         "`old_mysql_password` plugin is insecure and disabled by default",
              //     )
              // }
        }
    }
}

impl fmt::Debug for DriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[allow(dead_code)]
#[derive(Eq, PartialEq, Clone)]
pub enum UrlError {
    ParseError(ParseError),
    UnsupportedScheme(String),
    // /// (feature_name, parameter_name)
    // FeatureRequired(String, String),
    /// (feature_name, value)
    InvalidValue(String, String),
    UnknownParameter(String),
    BadUrl,
}

impl error::Error for UrlError {
    fn description(&self) -> &str {
        "Database connection URL error"
    }
}

impl fmt::Display for UrlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            UrlError::ParseError(ref err) => write!(f, "URL ParseError {{ {} }}", err),
            UrlError::UnsupportedScheme(ref s) => write!(f, "URL scheme `{}' is not supported", s),
            // UrlError::FeatureRequired(ref feature, ref parameter) => write!(
            //     f,
            //     "Url parameter `{}' requires {} feature",
            //     parameter, feature
            // ),
            UrlError::InvalidValue(ref parameter, ref value) => write!(
                f,
                "Invalid value `{}' for URL parameter `{}'",
                value, parameter
            ),
            UrlError::UnknownParameter(ref parameter) => {
                write!(f, "Unknown URL parameter `{}'", parameter)
            }
            UrlError::BadUrl => write!(f, "Invalid or incomplete connection URL"),
        }
    }
}

impl fmt::Debug for UrlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl From<ParseError> for UrlError {
    fn from(x: ParseError) -> UrlError {
        UrlError::ParseError(x)
    }
}
