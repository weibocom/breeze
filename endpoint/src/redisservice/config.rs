//use ds::time::Duration;

use serde::{Deserialize, Serialize};
//use sharding::distribution::{DIST_ABS_MODULA, DIST_MODULA};

use crate::{Timeout, TO_REDIS_M, TO_REDIS_S};

#[derive(Debug, Clone, Default, Deserialize)]
pub struct RedisNamespace {
    pub(crate) basic: Basic,
    pub(crate) backends: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Basic {
    #[serde(default)]
    pub(crate) access_mod: String,
    #[serde(default)]
    pub(crate) hash: String,
    #[serde(default)]
    pub(crate) distribution: String,
    //#[serde(default)]
    //pub(crate) listen: String,
    #[serde(default)]
    resource_type: String,
    #[serde(default)]
    pub(crate) selector: String,
    #[serde(default)]
    pub(crate) timeout_ms_master: u32,
    #[serde(default)]
    pub(crate) timeout_ms_slave: u32,
    // master是否参与读
    #[serde(default)]
    pub(crate) master_read: bool,
}

impl RedisNamespace {
    pub(super) fn try_from(cfg: &str) -> Option<Self> {
        let ns = serde_yaml::from_str::<RedisNamespace>(cfg)
            .map_err(|e| {
                log::info!("failed to parse redis config:{}", cfg);
                e
            })
            .ok()?;
        if ns.backends.len() == 0 {
            log::warn!("cfg invalid:{:?}", ns);
            return None;
        }
        // check backend size，对于range/modrange类型的dist需要限制后端数量为2^n
        let dist = &ns.basic.distribution;
        if dist.starts_with(sharding::distribution::DIST_RANGE)
            || dist.starts_with(sharding::distribution::DIST_MOD_RANGE)
        {
            let len = ns.backends.len();
            let power_two = len > 0 && ((len & len - 1) == 0);
            if !power_two {
                log::error!("shard num {} is not power of two: {}", len, cfg);
                return None;
            }
        }
        Some(ns)
    }
    pub(super) fn timeout_master(&self) -> Timeout {
        let mut to = TO_REDIS_M;
        if self.basic.timeout_ms_master > 0 {
            to.adjust(self.basic.timeout_ms_master);
        }
        to
    }
    pub(super) fn timeout_slave(&self) -> Timeout {
        let mut to = TO_REDIS_S;
        if self.basic.timeout_ms_slave > 0 {
            to.adjust(self.basic.timeout_ms_master);
        }
        to
    }
}
