use std::io::Error;

use serde::{Deserialize, Serialize};

const STATUS_OK: u32 = 200;
const STATUS_SERVER_FAILED: u32 = 500;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    status: u32,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    result: String,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    error: String,
}

impl Response {
    pub fn from_result(result: String) -> Self {
        Self {
            status: STATUS_OK,
            result: result,
            error: Default::default(),
        }
    }

    pub fn from_error(err: &Error) -> Self {
        Self {
            status: STATUS_SERVER_FAILED,
            result: Default::default(),
            error: err.to_string(),
        }
    }

    pub fn from_illegal_user() -> Self {
        Self {
            status: STATUS_SERVER_FAILED,
            result: Default::default(),
            error: "illegal user".to_string(),
        }
    }
}
