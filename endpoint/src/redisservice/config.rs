//use ds::time::Duration;

use std::fmt::Debug;

use serde::{Deserialize, Serialize};
//use sharding::distribution::{DIST_ABS_MODULA, DIST_MODULA};

use crate::{Timeout, TO_REDIS_M, TO_REDIS_S};

// range/modrange 对应的distribution配置项如果有此后缀，不进行后端数量的校验
const NO_CHECK_SUFFIX: &str = "-nocheck";

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
    pub(crate) resource_type: String,
    #[serde(default)]
    pub(crate) selector: String,
    #[serde(default)]
    pub(crate) region_enabled: bool,
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
        let mut ns = serde_yaml::from_str::<RedisNamespace>(cfg)
            .map_err(|e| log::info!("failed to parse redis config:{} => {e:?}", cfg))
            .ok()?;
        if ns.backends.len() == 0 {
            log::warn!("cfg invalid:{:?}", ns);
            return None;
        }

        if !ns.validate_and_correct() {
            log::error!("shards {} is not power of two: {}", ns.backends.len(), cfg);
            return None;
        }

        log::debug!("parsed redis config:{}/{}", ns.basic.distribution, cfg);
        return Some(ns);
    }

    #[inline]
    pub(super) fn timeout_master(&self) -> Timeout {
        let mut to = TO_REDIS_M;
        if self.basic.timeout_ms_master > 0 {
            to.adjust(self.basic.timeout_ms_master);
        }
        to
    }

    #[inline]
    pub(super) fn timeout_slave(&self) -> Timeout {
        let mut to = TO_REDIS_S;
        if self.basic.timeout_ms_slave > 0 {
            to.adjust(self.basic.timeout_ms_master);
        }
        to
    }

    /// 对配置进行合法性校验，当前只检验部分dist的后端数量
    #[inline(always)]
    fn validate_and_correct(&mut self) -> bool {
        let dist = &self.basic.distribution;
        // 需要检测dist时（默认场景），对于range/modrange类型的dist需要限制后端数量为2^n

        if dist.starts_with(sharding::distribution::DIST_RANGE)
            || dist.starts_with(sharding::distribution::DIST_MOD_RANGE)
        {
            // 对于range、morange，如果后有-nocheck后缀，不进行后端数量检测，并将该后缀清理掉
            if dist.ends_with(NO_CHECK_SUFFIX) {
                self.basic.distribution = dist.trim_end_matches(NO_CHECK_SUFFIX).to_string();
                return true;
            }
            let len = self.backends.len();
            let power_two = len > 0 && ((len & len - 1) == 0);
            if !power_two {
                return false;
            }
        }

        true
    }
}
