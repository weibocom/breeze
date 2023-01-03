//use ds::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhantomNamespace {
    pub(crate) basic: Basic,
    pub(crate) backends: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Basic {
    #[serde(default)]
    pub(crate) hash: String,
    #[serde(default)]
    pub(crate) distribution: String,
    #[serde(default)]
    pub(crate) timeout_ms: u32,
}

impl PhantomNamespace {
    #[inline]
    pub fn try_from(cfg: &str) -> Option<PhantomNamespace> {
        match serde_yaml::from_str::<PhantomNamespace>(cfg) {
            Ok(ns) => {
                if ns.backends.len() < 1 {
                    log::warn!("found malfromed phantome cfg:{}", cfg);
                    return None;
                }
                return Some(ns);
            }
            Err(_err) => {
                log::warn!("parse phantome cfg failed:{:?}, cfg:{}", _err, cfg);
                return None;
            }
        }
    }
}
