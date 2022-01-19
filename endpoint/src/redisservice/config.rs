use std::collections::{HashMap, HashSet};
use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct RedisNamespace {
    pub(crate) basic: Basic,
    pub(crate) backends: Vec<String>,
    #[serde(default)]
    pub(crate) master: Vec<String>,
    // 可能是域名，也可能是ip，调用者确认
    #[serde(default)]
    pub(crate) slaves: Vec<Vec<String>>,

    // TODO 下面这几个稍后抽取到更高一层次 fishermen
    #[serde(default)]
    pub(crate) host_addrs: HashMap<String, HashSet<String>>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Basic {
    #[serde(default)]
    pub(crate) access_mod: String,
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
}

impl RedisNamespace {
    pub(super) fn try_from(cfg: &str) -> Option<Self> {
        serde_yaml::from_str::<RedisNamespace>(cfg)
            .map_err(|e| {
                log::info!("failed to parse redis config:{}", cfg);
                e
            })
            .ok()
    }
    pub(super) fn timeout_master(&self) -> Duration {
        Duration::from_millis(250.max(self.basic.timeout_ms_master as u64))
    }
    pub(super) fn timeout_slave(&self) -> Duration {
        Duration::from_millis(100.max(self.basic.timeout_ms_slave as u64))
    }
}
