use std::collections::HashMap;
use std::sync::Arc;

use super::VisitAddress;
use protocol::{Protocol, Resource};
use stream::{BackendBuilder, BackendStream};

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
    pub(crate) fn update<P>(&mut self, namespace: &str, parser: &P)
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
                    stream::MAX_CONNECTIONS,
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
    ) -> Vec<Vec<BackendStream>>
    where
        T: VisitAddress,
    {
        let builders = share.unwrap_or(&self.streams);
        let mut streams = HashMap::with_capacity(4);

        self.addrs.select(|layer, addr| {
            if !streams.contains_key(&layer) {
                streams.insert(layer, Vec::with_capacity(8));
            }
            streams.get_mut(&layer).expect("layers").push(
                builders
                    .get(addr)
                    .expect("address match stream")
                    .build(self.fack_cid),
            )
        });
        // 按照layer排序。
        let mut sorted = streams
            .into_iter()
            .map(|(k, v)| (k, v))
            .collect::<Vec<(usize, Vec<BackendStream>)>>();
        sorted.sort_by(|a, b| a.0.cmp(&b.0));
        sorted.into_iter().map(|(_layer, s)| s).collect()
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
