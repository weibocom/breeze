use std::collections::HashMap;

use crate::path::{GetNamespace, ToName, ToPath};
use crate::sig::Sig;
use crate::Discover;

pub(crate) trait Cache {
    // 有更新，并且成功获取则返回对应的值
    async fn get<G: crate::TopologyWrite + Send + Sync>(
        &mut self,
        name: &str,
        sig: &Sig,
        p: &G,
    ) -> Option<(String, Sig)>;
}

impl<D> DiscoveryCache<D> {
    pub(crate) fn new(d: D) -> Self {
        Self {
            discovery: d,
            cache: Default::default(),
            sigs: Default::default(),
        }
    }
}

pub(crate) struct DiscoveryCache<D> {
    discovery: D,
    cache: HashMap<String, (String, Sig)>, // 这里面的key存储的是ServiceId::name。
    sigs: HashMap<String, String>,         // 这里面存储的key是Service::path
}

impl<D: Discover + Send + Sync> Cache for DiscoveryCache<D> {
    async fn get<G: crate::TopologyWrite + Send + Sync>(
        &mut self,
        name: &str,
        sig: &Sig,
        p: &G,
    ) -> Option<(String, Sig)> {
        let path = name.path();
        let sig = sig.group_sig();
        // 从discovery获取。
        if !self.sigs.contains_key(&path) {
            // 说明当前是第一次从snapshot获取到的值。
            self.sigs.insert(path.clone(), sig.to_string());
        }
        // 先检查 sig 是否相等
        let path_sig = self.sigs.get_mut(&path).expect("not init");
        if path_sig.len() > 0 && *path_sig == sig {
            log::debug!("cache hit group not changed. {}", name);
            return None;
        }
        // 一个服务只会访问一次。直接删除即可。
        if let Some((cfg, s)) = self.cache.remove(name) {
            log::debug!("cache hit. name:{} sig:{:?}", name, s);
            return Some((cfg, s));
        }
        if let Ok(crate::Config::Config(s, c)) =
            self.discovery.get_service::<String>(&path, sig).await
        {
            *path_sig = s;
            // 一个path可能会对应于多个name。一次解析，后续直接缓存。
            if sig != *path_sig {
                // 有更新
                let groups = p.disgroup(&path, &c);
                log::info!("service {} updated from '{}' to {}", path, sig, path_sig);
                for (n, v) in groups {
                    // n: 是namespace。 v: 对应的配置
                    let name = path.cat(n).name();
                    let hash = Sig::new(md5::compute(v).into(), path_sig.clone());
                    self.cache.insert(name, (v.to_string(), hash));
                }
                return self.cache.remove(name).or_else(|| {
                    log::warn!("group cfg found, but namespace missed:{}", name);
                    let empty_sig = Sig::new(md5::compute(&[]).into(), path_sig.clone());
                    Some((String::new(), empty_sig))
                });
            }
            log::warn!("this should not happen. cfg not found for {}", name);
        }
        if path_sig != sig {
            *path_sig = sig.to_string();
        }
        log::debug!("{} not changed by discovery", name);
        None
    }
}
impl<D: Discover + Send + Sync> DiscoveryCache<D> {
    // 清空缓存
    pub(crate) fn clear(&mut self) {
        for (_path, sig) in self.sigs.iter_mut() {
            sig.clear();
        }
    }
    pub(crate) fn inner(&self) -> &D {
        &self.discovery
    }
}
