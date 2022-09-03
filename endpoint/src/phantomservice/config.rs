//use std::time::Duration;

use serde::{Deserialize, Serialize};

pub(crate) const ACCESS_NONE: &str = "none";
pub(crate) const ACCESS_WRITE: &str = "w";
pub(crate) const ACCESS_READ: &str = "r";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhantomNamespace {
    pub(crate) basic: Basic,
    pub(crate) backends: Vec<Backend>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Basic {
    // #[serde(default)]
    // pub(crate) access_mod: String,
    #[serde(default)]
    pub(crate) hash: String,
    // #[serde(default)]
    // pub(crate) distribution: String,
    #[serde(default)]
    pub(crate) listen: String,
    #[serde(default)]
    resource_type: String,
    #[serde(default)]
    pub(crate) timeout: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Backend {
    #[serde(default)]
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) access_mod: String,
    #[serde(default)]
    pub(crate) distribution: String,
    #[serde(default)]
    pub(crate) servers: Vec<String>,
}

impl PhantomNamespace {
    #[inline]
    pub fn try_from(cfg: &str) -> Option<PhantomNamespace> {
        match serde_yaml::from_str::<PhantomNamespace>(cfg) {
            Ok(mut ns) => {
                if ns.backends.len() < 1 {
                    log::warn!("found malfromed phantome cfg:{}", cfg);
                    return None;
                }
                ns.backends
                    .retain(|b| !ACCESS_NONE.eq_ignore_ascii_case(b.access_mod.as_str()));

                return Some(ns);
            }
            Err(_err) => {
                log::warn!("parse phantome cfg failed:{:?}, cfg:{}", _err, cfg);
                return None;
            }
        }
    }

    //pub(super) fn timeout(&self) -> Duration {
    //    Duration::from_millis(200.max(self.basic.timeout as u64))
    //}
}
