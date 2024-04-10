use serde::{Deserialize, Serialize};

const QSIZE_DOMAIN_DELIMITER: &str = "=";

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Namespace {
    #[serde(default)]
    pub(crate) basic: Basic,

    #[serde(default)]
    pub(crate) backends: Vec<String>,

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
    pub(crate) keys: String,
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

    /// 解析backends，分离出qsize，并对新的backends进行按qsize递增排序；
    /// 注意：不允许qsize重复，否则认为配置错误
    #[inline]
    fn parse_and_sort_backends(&mut self) -> Option<Vec<(usize, String)>> {
        let mut bkends: Vec<(usize, String)> = Vec::with_capacity(self.backends.len());
        for sd in self.backends.iter() {
            let size_domain = sd.split_once(QSIZE_DOMAIN_DELIMITER);
            if size_domain.is_none() {
                return None;
            }
            let (s, d) = size_domain.expect("mq");
            let qsize = s.parse::<usize>().unwrap_or(0);
            let domain = d.to_string();
            if qsize == 0 || domain.len() == 0 {
                return None;
            }
            // 排重，不能有相同size的mq
            for (qs, _) in bkends.iter() {
                if qs.eq(&qsize) {
                    return None;
                }
            }
            bkends.push((qsize, domain));
        }

        bkends.sort_by(|a, b| a.0.cmp(&b.0));
        Some(bkends)
    }
}
