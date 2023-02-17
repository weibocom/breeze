use std::collections::{HashMap, HashSet};
//use ds::time::Duration;

use serde::{Deserialize, Serialize};
//use sharding::distribution::{DIST_ABS_MODULA, DIST_MODULA};

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
    pub(super) fn local(&self) -> String {
        match std::env::var("BREEZE_LOCAL")
            .unwrap_or("".to_string())
            .as_str()
        {
            "distance" => "distance",
            _ => self.basic.selector.as_str(),
        }
        .to_string()
    }

    pub(super) fn try_from(cfg: &str) -> Option<Self> {
        let nso = serde_yaml::from_str::<RedisNamespace>(cfg)
            .map_err(|e| {
                log::info!("failed to parse redis config:{}", cfg);
                e
            })
            .ok();
        if let Some(ns) = nso {
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
            return Some(ns);
        }
        nso
    }
    //pub(super) fn timeout_master(&self) -> Duration {
    //    if self.basic.timeout_ms_master > 0 {
    //        Duration::from_millis(100.max(self.basic.timeout_ms_master as u64))
    //    } else {
    //        Duration::from_millis(500)
    //    }
    //}
    //pub(super) fn timeout_slave(&self) -> Duration {
    //    Duration::from_millis(80.max(self.basic.timeout_ms_slave as u64))
    //}
}
