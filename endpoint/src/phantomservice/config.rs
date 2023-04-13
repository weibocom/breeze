//use ds::time::Duration;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct PhantomNamespace {
    pub(crate) basic: Basic,
    pub(crate) backends: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Basic {
    //#[serde(default)]
    //pub(crate) hash: String,
    #[serde(default)]
    pub(crate) distribution: String,
    //#[serde(default)]
    //pub(crate) listen: String,
    //#[serde(default)]
    //resource_type: String,
    #[serde(default)]
    pub(crate) timeout_ms: u32,
}

impl PhantomNamespace {
    #[inline]
    pub fn try_from(cfg: &str) -> Option<PhantomNamespace> {
        let ns = serde_yaml::from_str::<PhantomNamespace>(cfg)
            .map_err(|_err| {
                log::warn!("parse phantome cfg failed:{:?}, cfg:{}", _err, cfg);
            })
            .ok()?;
        if ns.backends.len() < 1 {
            log::warn!("found malfromed phantome cfg:{}", cfg);
            return None;
        }
        Some(ns)
    }

    pub(super) fn timeout(&self) -> crate::Timeout {
        let mut to = crate::TO_PHANTOM_M;
        if self.basic.timeout_ms > 0 {
            to.adjust(self.basic.timeout_ms);
        }
        to
    }
}
