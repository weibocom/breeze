use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct MysqlNamespace {
    // TODO speed up, ref: https://git/platform/resportal/-/issues/548
    pub(crate) basic: Basic,
    pub(crate) sql: HashMap<String, String>,
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

    #[serde(default)]
    pub(crate) max_idle_time: u32,
    #[serde(default)]
    pub(crate) db_prefix: String,
    #[serde(default)]
    pub(crate) table_prefix: String,
    #[serde(default)]
    pub(crate) table_postfix: String,
    #[serde(default)]
    pub(crate) db_count: u32,
    #[serde(default)]
    pub(crate) table_count: u32,
    #[serde(default)]
    pub(crate) hierarchy: bool,
    #[serde(default)]
    pub(crate) password: String,
    #[serde(default)]
    pub(crate) user: String,
}

impl MysqlNamespace {
    pub(super) fn try_from(cfg: &str) -> Option<Self> {
        let nso = serde_yaml::from_str::<MysqlNamespace>(cfg)
            .map_err(|e| {
                log::info!("failed to parse mysql config:{}", cfg);
                e
            })
            .ok();
        if let Some(ns) = nso {
            return Some(ns);
        }
        nso
    }
}
