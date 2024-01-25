use crate::{Timeout, TO_UUID};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize)]
pub struct UuidNamespace {
    pub(crate) basic: Basic,
    pub(crate) backends: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Basic {
    #[serde(default)]
    pub(crate) region_enabled: bool,
    #[serde(default)]
    pub(crate) timeout: u32,
    #[serde(default)]
    pub(crate) selector: String,
}

impl UuidNamespace {
    pub(super) fn try_from(cfg: &str) -> Option<Self> {
        let ns = serde_yaml::from_str::<UuidNamespace>(cfg)
            .map_err(|e| log::info!("parse uuid:{cfg} => {e:?}"))
            .ok()?;
        (ns.backends.len() > 0).then_some(ns)
    }
    pub(super) fn timeout(&self) -> Timeout {
        let mut to = TO_UUID;
        if self.basic.timeout > 0 {
            to.adjust(self.basic.timeout);
        }
        to
    }
}
