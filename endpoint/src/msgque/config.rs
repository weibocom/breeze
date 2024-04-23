use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Namespace {
    #[serde(default)]
    pub(crate) basic: Basic,

    #[serde(default)]
    pub(crate) backends: HashMap<usize, String>,

    // TODO 作为非主路径，实时构建更轻便？先和其他ns保持一致 fishermen
    #[serde(skip)]
    pub(crate) backends_flatten: Vec<String>,
    #[serde(skip)]
    pub(crate) backends_qsize: Vec<usize>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Basic {
    /// msgque 可以读写的mq的名字
    #[serde(default)]
    pub(crate) keys: Vec<String>,
    ///eg: mcq2,mcq3
    #[serde(default)]
    pub(crate) resource_type: String,
    #[serde(default)]
    pub(crate) timeout_read: u32,
    #[serde(default)]
    pub(crate) timeout_write: u32,
}

impl Namespace {
    pub(crate) fn try_from(cfg: &str, _namespace: &str) -> Option<Self> {
        log::debug!("mq/{} parsing - cfg: {}", _namespace, cfg);
        match serde_yaml::from_str::<Namespace>(cfg) {
            Ok(mut ns) => {
                let bkends = ns.parse_and_sort_backends();
                if bkends.is_none() {
                    log::warn!("+++ mq malformed cfg:{}/{}", _namespace, cfg);
                    return None;
                }
                let bkends = bkends.expect("mq");
                bkends.into_iter().for_each(|(qsize, domain)| {
                    ns.backends_qsize.push(qsize);
                    ns.backends_flatten.push(domain);
                });
                return Some(ns);
            }
            Err(_e) => {
                log::warn!("parse ns/{} failed: {:?}, cfg: {}", _namespace, _e, cfg);
                return None;
            }
        }
    }

    /// 解析backends，对新的backends进行按qsize递增排序；
    #[inline]
    fn parse_and_sort_backends(&mut self) -> Option<Vec<(usize, String)>> {
        let mut bkends: Vec<(usize, String)> = Vec::with_capacity(self.backends.len());
        self.backends
            .iter()
            .for_each(|(qsize, domains)| bkends.push((qsize.clone(), domains.to_string())));
        bkends.sort_by(|a, b| a.0.cmp(&b.0));
        Some(bkends)
    }
}
