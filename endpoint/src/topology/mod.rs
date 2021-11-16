use std::collections::HashMap;
use std::sync::Arc;

use protocol::Protocol;
use stream::{BackendBuilder, BackendStream, LayerRole};

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
    noreply: Inner<Vec<(LayerRole, Vec<String>)>>,
    parser: P,
    // 在没有master_l1与slave_l1时。所有的command共用一个物理连接
    share: Inner<Vec<(LayerRole, Vec<String>)>>,
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
        // <<<<<<< HEAD
        //         // self.master.select().pop().unwrap_or_default()
        //         let master = self.master.select().pop();
        //         return master.unwrap().1;
        //     }
        //     // 第一个元素是master，去掉
        //     pub fn followers(&self) -> Vec<(LayerRole, Vec<BackendStream>)> {
        //         self.noreply.select().split_off(1)
        //     }
        //     pub fn noreply(&self) -> Vec<(LayerRole, Vec<BackendStream>)> {
        //         self.noreply.select()
        //     }
        //     pub fn get(
        //         &self,
        //     ) -> (
        //         Vec<(LayerRole, Vec<BackendStream>)>,
        //         Vec<(LayerRole, Vec<BackendStream>)>,
        //     ) {
        //         self.with_write_back(self.get.select())
        //     }
        //     pub fn mget(
        //         &self,
        //     ) -> (
        //         Vec<(LayerRole, Vec<BackendStream>)>,
        //         Vec<(LayerRole, Vec<BackendStream>)>,
        //     ) {
        //         self.with_write_back(self.mget.select())
        // =======
        self.master
            .select(Some(self.share.streams()))
            .pop()
            .expect("master empty")
            .1
    }
    // 第一个元素是master，去掉
    pub fn followers(&self) -> Vec<(LayerRole, Vec<BackendStream>)> {
        self.noreply.select(Some(self.share.streams())).split_off(1)
    }
    pub fn get(
        &self,
    ) -> (
        Vec<(LayerRole, Vec<BackendStream>)>,
        Vec<(LayerRole, Vec<BackendStream>)>,
    ) {
        self.with_write_back(self.get.select(self.shared()))
    }
    pub fn mget(
        &self,
    ) -> (
        Vec<(LayerRole, Vec<BackendStream>)>,
        Vec<(LayerRole, Vec<BackendStream>)>,
    ) {
        self.with_write_back(self.mget.select(self.shared()))
    }
    fn with_write_back(
        &self,
        streams: Vec<(LayerRole, Vec<BackendStream>)>,
    ) -> (
        Vec<(LayerRole, Vec<BackendStream>)>,
        Vec<(LayerRole, Vec<BackendStream>)>,
    ) {
        // let write_back = streams
        //     .iter()
        //     .map(|(role, streams)| (role.clone(), streams.iter().map(|s|s.faked_clone()))
        //     .collect();
        //.map(|layer| layer.iter().map(|s| s.faked_clone()).collect())
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
    fn select<F: FnMut(LayerRole, usize, &str)>(&self, f: F);
}

impl VisitAddress for Vec<String> {
    fn visit<F: FnMut(&str)>(&self, mut f: F) {
        for addr in self.iter() {
            f(addr)
        }
    }
    // 每一层可能有多个pool，所以usize表示pool编号，新增LayerRole表示层次
    fn select<F: FnMut(LayerRole, usize, &str)>(&self, mut f: F) {
        for (_i, addr) in self.iter().enumerate() {
            f(LayerRole::Unknow, 0, addr);
        }
    }
}
impl VisitAddress for Vec<(LayerRole, Vec<String>)> {
    fn visit<F: FnMut(&str)>(&self, mut f: F) {
        for (_role, layers) in self.iter() {
            for addr in layers.iter() {
                f(addr)
            }
        }
    }
    fn select<F: FnMut(LayerRole, usize, &str)>(&self, mut f: F) {
        // for (i, layers) in self.iter().enumerate() {
        for (i, (role, layers)) in self.iter().enumerate() {
            for addr in layers.iter() {
                f(role.clone(), i, addr);
            }
        }
    }
}
