use std::collections::HashMap;
use std::sync::Arc;

use protocol::Protocol;
use protocol::Resource;
use stream::{BackendBuilder, BackendStream, LayerRole};

use crate::topology::*;

use super::MemcacheNamespace;

// master: Vec1<Vec2<String>>: Vec2<String> 代表一组完整的资源， Vec1负责负载均衡策略
#[derive(Clone)]
pub struct MemcacheTopology<P> {
    namespace: String,
    hash: String,         // hash策略
    distribution: String, //distribution策略
    master: Inner<Vec<String>>,
    // slaves: Inner<Vec<(LayerRole, Vec<String>)>>,
    get: Inner<Layer>,
    mget: Inner<Layer>,
    noreply: Inner<Vec<(LayerRole, Vec<String>)>>,
    parser: P,
    // 在没有master_l1与slave_l1时。所有的command共用一个物理连接
    share: Inner<Vec<(LayerRole, Vec<String>)>>,
    shared: bool,
}

impl<P> MemcacheTopology<P>
where
    P: Protocol,
{
    fn with_write_back(
        &self,
        streams: Vec<(LayerRole, Vec<BackendStream>)>,
    ) -> (
        Vec<(LayerRole, Vec<BackendStream>)>,
        Vec<(LayerRole, Vec<BackendStream>)>,
    ) {
        let mut write_back = Vec::with_capacity(streams.len());
        for (idx, l_vec) in streams.iter() {
            write_back.push((idx.clone(), l_vec.iter().map(|s| s.faked_clone()).collect()));
        }
        (streams, write_back)
    }
    fn shared(&self) -> Option<&HashMap<String, Arc<BackendBuilder>>> {
        if self.shared {
            Some(self.share.streams())
        } else {
            None
        }
    }

    pub(crate) fn hash(&self) -> &str {
        &self.hash
    }
    pub(crate) fn distribution(&self) -> &str {
        &self.distribution
    }

    pub(crate) fn master(&self) -> Vec<BackendStream> {
        self.master
            .select(Some(self.share.streams()))
            .pop()
            .expect("master empty")
            .1
    }
    // // 所有slave连接
    // fn slaves(&self) -> Vec<(LayerRole, Vec<BackendStream>)> {
    //     // self.slaves.select(Some(self.share.streams())).split_off(1)
    //     self.slaves.select(Some(self.share.streams()))
    // }

    // 第一个元素是master，去掉
    pub(crate) fn followers(&self) -> Vec<(LayerRole, Vec<BackendStream>)> {
        self.noreply.select(Some(self.share.streams())).split_off(1)
    }

    pub(crate) fn get(
        &self,
    ) -> (
        Vec<(LayerRole, Vec<BackendStream>)>,
        Vec<(LayerRole, Vec<BackendStream>)>,
    ) {
        self.with_write_back(self.get.select(self.shared()))
    }

    pub(crate) fn mget(
        &self,
    ) -> (
        Vec<(LayerRole, Vec<BackendStream>)>,
        Vec<(LayerRole, Vec<BackendStream>)>,
    ) {
        self.with_write_back(self.mget.select(self.shared()))
    }
}

impl<P> discovery::TopologyWrite for MemcacheTopology<P>
where
    P: Send + Sync + Protocol,
{
    fn resource(&self) -> Resource {
        Resource::Memcache
    }
    fn update(&mut self, name: &str, cfg: &str, _hosts: &HashMap<String, Vec<String>>) {
        let idx = name.find(':').unwrap_or(name.len());
        if idx == 0 || idx >= name.len() - 1 {
            log::info!("not a valid cache service name:{} no namespace found", name);
            return;
        }
        let namespace = &name[idx + 1..];
        self.namespace = namespace.to_string();

        match MemcacheNamespace::parse(cfg, namespace) {
            Err(e) => {
                log::warn!(
                    "parse mc config failed. error:{} name:{} cfg:{}",
                    e,
                    name,
                    cfg.len()
                );
            }
            Ok(ns) => {
                if ns.master.len() == 0 {
                    log::info!("cacheservice empty. {} => {}", name, cfg);
                } else {
                    self.hash = ns.hash.to_owned();
                    self.distribution = ns.distribution.to_owned();
                    let p = &self.parser;

                    self.share.set(ns.uniq_all());
                    self.share.update(namespace, p);

                    // 更新配置
                    self.master.set(ns.master.clone());
                    // self.slaves.set(ns.writers());
                    self.get.with(|t| t.update_for_memcache(&ns));
                    self.mget.with(|t| t.update_for_memcache(&ns));
                    self.noreply.set(ns.writers());

                    // 如果配置中包不含有master_l1,
                    // 则所有的请求共用一个物理连接。否则每一种op使用独立的连接
                    if ns.master_l1.len() == 0 {
                        self.shared = true;
                    } else {
                        self.shared = false;
                        self.get.update(namespace, p);
                        self.mget.update(namespace, p);
                    };
                }
            }
        }
    }
    fn gc(&mut self) {
        if self.shared {
            self.get.take();
            self.mget.take();
        }
    }
}

impl<P> From<P> for MemcacheTopology<P>
where
    P: Protocol,
{
    fn from(parser: P) -> Self {
        let mut me = Self {
            namespace: Default::default(),
            parser: parser,
            hash: Default::default(),
            distribution: Default::default(),
            master: Default::default(),
            // slaves: Default::default(),
            get: Default::default(),
            mget: Default::default(),
            noreply: Default::default(),
            share: Default::default(),
            shared: false,
        };
        me.noreply.enable_fake_cid();
        me
    }
}

// 所有的stream都初始化完成
impl<P> discovery::Inited for MemcacheTopology<P> {
    fn inited(&self) -> bool {
        self.master.len() > 0
            && self.master.inited()
            && self.get.inited()
            && self.mget.inited()
            && self.noreply.inited()
    }
}
