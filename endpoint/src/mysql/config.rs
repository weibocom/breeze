use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct MysqlNamespace {
    // TODO speed up, ref: https://git.intra.weibo.com/platform/resportal/-/issues/548
    pub(crate) basic: Basic,
    pub(crate) backends: Vec<String>,
    pub(crate) archive: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Basic {
    #[serde(default)]
    pub(crate) hash: String,
    #[serde(default)]
    pub(crate) distribution: String,
    #[serde(default)]
    pub(crate) listen: String,
    #[serde(default)]
    resource_type: String,
    #[serde(default)]
    pub(crate) selector: String,
    #[serde(default)]
    pub(crate) timeout_ms_master: u32,
    #[serde(default)]
    pub(crate) timeout_ms_slave: u32,
    #[serde(default)]
    pub(crate) min_pool_size: u16,
    // TODO speed up
}
