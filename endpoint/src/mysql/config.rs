use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize)]
pub struct MysqlNamespace {
    // TODO speed up, ref: https://git/platform/resportal/-/issues/548
    pub(crate) basic: Basic,
    // pub(crate) sql: HashMap<String, String>,
    // pub(crate) backends: Vec<String>,
    pub(crate) backends: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Basic {
    // #[serde(default)]
    // pub(crate) hash: String,
    // #[serde(default)]
    // pub(crate) distribution: String,
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
    pub(crate) db_name: String,

    // #[serde(default)]
    // pub(crate) min_pool_size: u16,

    // #[serde(default)]
    // pub(crate) max_idle_time: u32,
    // #[serde(default)]
    // pub(crate) db_prefix: String,
    // #[serde(default)]
    // pub(crate) table_prefix: String,
    // #[serde(default)]
    // pub(crate) table_postfix: String,
    #[serde(default)]
    pub(crate) db_count: u32,
    #[serde(default)]
    pub(crate) strategy: String,
    // #[serde(default)]
    // pub(crate) table_count: u32,
    // #[serde(default)]
    // pub(crate) hierarchy: bool,
    #[serde(default)]
    pub(crate) password: String,
    #[serde(default)]
    pub(crate) user: String,
}
pub const ARCHIVE_DEFAULT_KEY: &str = "__default__";

impl MysqlNamespace {
    pub(super) fn get_backends(&self) -> Vec<String> {
        let mut backends: Vec<String> = Vec::new();
        for vec in self.backends.values() {
            backends.extend(vec.iter().cloned());
        }
        backends
    }
    pub(super) fn try_from(cfg: &str) -> Option<Self> {
        let nso = serde_yaml::from_str::<MysqlNamespace>(cfg)
            .map_err(|e| {
                log::info!("failed to parse mysql  e:{} config:{}", e, cfg);
                e
            })
            .ok();

        if let Some(mut ns) = nso {
            // archive shard 处理
            // 2009-2012 ,[111xxx.com:111,222xxx.com:222]
            // 2013 ,[112xxx.com:112,223xxx.com:223]
            let mut archive: HashMap<String, Vec<String>> = HashMap::new();
            for (key, val) in ns.backends.iter() {
                //处理当前库
                if ARCHIVE_DEFAULT_KEY == key {
                    archive.insert(key.to_string(), val.to_vec());
                    continue;
                }
                //适配N年共用一个组shard情况，例如2009-2012共用
                let years: Vec<&str> = key.split("-").collect();
                let min: u16 = years[0].parse().unwrap();
                if years.len() > 1 {
                    // 2009-2012 包括2012,故max需要加1
                    let max = years[1].parse::<u16>().expect("malformed mysql cfg") + 1_u16;
                    for i in min..max {
                        archive.insert(i.to_string(), val.to_vec());
                    }
                } else {
                    archive.insert(min.to_string(), val.to_vec());
                }
            }
            ns.backends = archive;
            return Some(ns);
        }
        nso
    }
}
