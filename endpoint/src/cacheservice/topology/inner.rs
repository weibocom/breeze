use std::collections::HashMap;
use std::sync::Arc;

use super::VisitAddress;
use protocol::{Protocol, Resource};
use stream::{Addressed, BackendBuilder, BackendStream, LayerRole};

#[derive(Clone, Default)]
pub(crate) struct Inner<T> {
    // noreply场景下，不占用逻辑连接。
    fack_cid: bool,
    pub(crate) addrs: T,
    streams: HashMap<String, Arc<BackendBuilder>>,
}

impl<T> Inner<T> {
    pub(crate) fn with<F: FnMut(&mut T)>(&mut self, mut f: F) {
        f(&mut self.addrs);
    }
    pub(crate) fn set(&mut self, addrs: T) {
        self.addrs = addrs;
    }
    pub(crate) fn update<P>(&mut self, namespace: &str, parser: &P, c: usize)
    where
        T: VisitAddress,
        P: Send + Sync + Protocol + 'static + Clone,
    {
        let mut all = HashMap::with_capacity(64);
        let mut news = HashMap::with_capacity(16);
        // 新增
        self.addrs.visit(|addr| {
            all.insert(addr.to_string(), ());
            if !self.streams.contains_key(addr) {
                news.insert(addr.to_owned(), ());
            }
        });
        for (addr, _) in news {
            // 添加新的builder
            self.streams.insert(
                addr.to_string(),
                Arc::new(BackendBuilder::from(
                    parser.clone(),
                    &addr,
                    c,
                    Resource::Memcache,
                    namespace,
                )),
            );
        }
        //删除
        self.streams.retain(|addr, _| all.contains_key(addr));
    }

    pub(crate) fn select(
        &self,
        share: Option<&HashMap<String, Arc<BackendBuilder>>>,
    ) -> Vec<(LayerRole, Vec<BackendStream>)>
    where
        T: VisitAddress,
    {
        let builders = share.unwrap_or(&self.streams);
        let mut streams = HashMap::with_capacity(4);

        self.addrs.select(|role, pool, addr| {
            if !streams.contains_key(&pool) {
                streams.insert(pool, (role, Vec::with_capacity(8)));
            }
            streams.get_mut(&pool).expect("pools").1.push(
                builders
                    .get(addr)
                    .expect("address match stream")
                    .build(self.fack_cid),
            )
        });
        // 按照pool排序。
        let mut sorted = streams
            .into_iter()
            .map(|(k, (role, v))| (role, k, v))
            .collect::<Vec<(LayerRole, usize, Vec<BackendStream>)>>();
        sorted.sort_by(|a, b| a.1.cmp(&b.1));
        let mut layers = Vec::with_capacity(sorted.len());
        for (role, pool, streams) in sorted {
            log::debug!(
                "builded for layer:{:?}, pool:{}, addr:{:?}",
                role,
                pool,
                streams.addr()
            );
            layers.push((role, streams));
        }
        layers
    }

    pub(crate) fn inited(&self) -> bool {
        self.streams
            .iter()
            .fold(true, |inited, (_, s)| inited && s.inited())
    }
    pub(crate) fn enable_fake_cid(&mut self) {
        self.fack_cid = true;
    }
    pub(crate) fn streams(&self) -> &HashMap<String, Arc<BackendBuilder>> {
        &self.streams
    }
    pub(crate) fn take(&mut self) {
        if self.streams.len() > 0 {
            log::info!("streams taken:{:?}", self.streams.keys());
            self.streams.clear();
        }
    }
}

impl<T> std::ops::Deref for Inner<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.addrs
    }
}
