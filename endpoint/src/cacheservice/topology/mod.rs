use std::collections::HashMap;
use std::sync::Arc;

use protocol::Protocol;
use stream::{BackendBuilder, BackendStream};

mod inner;
use inner::*;
mod layer;
use layer::*;
mod config;
use config::*;

#[derive(Clone)]
pub struct Topology<P> {
    hash: String,         // hash策略
    distribution: String, //distribution策略
    master: Inner<Vec<String>>,
    get: Inner<Layer>,
    mget: Inner<Layer>,
    noreply: Inner<Vec<Vec<String>>>,
    parser: P,
    // 在没有master_l1与slave_l1时。所有的command共用一个物理连接
    share: Inner<Vec<Vec<String>>>,
    shared: bool,
}

impl<P> Topology<P> {
    pub(crate) fn hash(&self) -> &str {
        &self.hash
    }
    pub(crate) fn distribution(&self) -> &str {
        &self.distribution
    }
    pub fn master(&self) -> Vec<BackendStream> {
        self.master
            .select(Some(self.share.streams()))
            .pop()
            .expect("master empty")
    }
    // 第一个元素是master，去掉
    pub fn followers(&self) -> Vec<Vec<BackendStream>> {
        self.noreply.select(Some(self.share.streams())).split_off(1)
    }
    pub fn get(&self) -> (Vec<Vec<BackendStream>>, Vec<Vec<BackendStream>>) {
        self.with_write_back(self.get.select(self.shared()))
    }
    pub fn mget(&self) -> (Vec<Vec<BackendStream>>, Vec<Vec<BackendStream>>) {
        self.with_write_back(self.mget.select(self.shared()))
    }
    fn with_write_back(
        &self,
        streams: Vec<Vec<BackendStream>>,
    ) -> (Vec<Vec<BackendStream>>, Vec<Vec<BackendStream>>) {
        let write_back = streams
            .iter()
            .map(|layer| layer.iter().map(|s| s.faked_clone()).collect())
            .collect();
        (streams, write_back)
    }
    fn shared(&self) -> Option<&HashMap<String, Arc<BackendBuilder>>> {
        if self.shared {
            Some(self.share.streams())
        } else {
            None
        }
    }
}

impl<P> discovery::TopologyWrite for Topology<P>
where
    P: Send + Sync + Protocol,
{
    fn update(&mut self, name: &str, cfg: &str) {
        let idx = name.find(':').unwrap_or(name.len());
        if idx == 0 || idx >= name.len() - 1 {
            log::info!("not a valid cache service name:{} no namespace found", name);
            return;
        }
        let namespace = &name[idx + 1..];

        match Namespace::parse(cfg, namespace) {
            Err(e) => {
                log::info!("parse config. error:{} name:{} cfg:{}", e, name, cfg.len());
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
                    self.get.with(|t| t.update(&ns));
                    self.mget.with(|t| t.update(&ns));
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

impl<P> From<P> for Topology<P> {
    fn from(parser: P) -> Self {
        let mut me = Self {
            parser: parser,
            hash: Default::default(),
            distribution: Default::default(),
            master: Default::default(),
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
impl<P> discovery::Inited for Topology<P> {
    fn inited(&self) -> bool {
        self.master.len() > 0
            && self.master.inited()
            && self.get.inited()
            && self.mget.inited()
            && self.noreply.inited()
    }
}

pub(crate) trait VisitAddress {
    fn visit<F: FnMut(&str)>(&self, f: F);
    fn select<F: FnMut(usize, &str)>(&self, f: F);
}

impl VisitAddress for Vec<String> {
    fn visit<F: FnMut(&str)>(&self, mut f: F) {
        for addr in self.iter() {
            f(addr)
        }
    }
    fn select<F: FnMut(usize, &str)>(&self, mut f: F) {
        for (_i, addr) in self.iter().enumerate() {
            f(0, addr);
        }
    }
}
impl VisitAddress for Vec<Vec<String>> {
    fn visit<F: FnMut(&str)>(&self, mut f: F) {
        for layers in self.iter() {
            for addr in layers.iter() {
                f(addr)
            }
        }
    }
    fn select<F: FnMut(usize, &str)>(&self, mut f: F) {
        for (i, layers) in self.iter().enumerate() {
            for addr in layers.iter() {
                f(i, addr);
            }
        }
    }
}
