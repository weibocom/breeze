use std::collections::HashMap;

use protocol::Protocol;
use protocol::Resource;
use stream::{BackendStream, LayerRole};

use crate::topology::*;
use crate::RedisNamespace;

#[derive(Clone)]
pub struct RedisTopology<P> {
    // resource: Resource,
    namespace: String,
    access_mod: String,
    hash: String,         // hash策略
    distribution: String, //distribution策略
    listen_ports: Vec<u16>,
    master: Inner<Vec<String>>,
    slaves: Inner<Vec<(LayerRole, Vec<String>)>>,
    parser: P,
    // 在没有slave时。所有的command共用一个物理连接
    share: Inner<Vec<(LayerRole, Vec<String>)>>,
    shared: bool,
}

impl<P> RedisTopology<P>
where
    P: Protocol,
{
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
    // 所有slave连接
    pub(crate) fn slaves(&self) -> Vec<(LayerRole, Vec<BackendStream>)> {
        // self.slaves.select(Some(self.share.streams())).split_off(1)
        self.slaves.select(Some(self.share.streams()))
    }
}

impl<P> discovery::TopologyWrite for RedisTopology<P>
where
    P: Send + Sync + Protocol,
{
    fn resource(&self) -> Resource {
        Resource::Redis
    }
    fn update(&mut self, name: &str, cfg: &str, hosts: &HashMap<String, Vec<String>>) {
        let idx = name.find(':').unwrap_or(name.len());
        if idx == 0 || idx >= name.len() - 1 {
            log::info!("not a valid cache service name:{} no namespace found", name);
            return;
        }
        let namespace = &name[idx + 1..];
        self.namespace = namespace.to_string();

        match RedisNamespace::parse(cfg, namespace) {
            Err(e) => {
                log::info!(
                    "parse redis config failed. error:{} name:{} cfg:{}",
                    e,
                    name,
                    cfg
                );
            }
            Ok(mut ns) => {
                ns.refresh_backends(hosts);

                if ns.master.len() == 0 {
                    log::warn!("redisservice/{} :{}", name, cfg);
                    return;
                }
                self.access_mod = ns.basic.access_mod.clone();
                self.hash = ns.basic.hash.clone();
                self.distribution = ns.basic.distribution.clone();
                self.listen_ports = ns.parse_listen_ports();

                // 这些需要在解析域名完毕后才能进行
                self.share.set(ns.uniq_all());
                self.share.update(self.namespace.as_str(), &self.parser);
                self.master.set(ns.master.clone());
                self.slaves.set(ns.readers());

                // 如果没有slave，读写共享相同的物理连接
                if ns.slaves.len() == 0 {
                    self.shared = true;
                    self.share.set(vec![(LayerRole::Master, ns.master.clone())]);
                    self.share.update(self.namespace.as_str(), &self.parser);
                } else {
                    self.shared = false;
                    self.master.update(self.namespace.as_str(), &self.parser);
                }
            }
        }
    }
    fn gc(&mut self) {
        // if self.shared {
        //     self.get.take();
        //     self.mget.take();
        // }
    }
}

impl<P> From<P> for RedisTopology<P>
where
    P: Protocol,
{
    fn from(parser: P) -> Self {
        Self {
            namespace: Default::default(),
            parser: parser,
            access_mod: Default::default(),
            hash: Default::default(),
            distribution: Default::default(),
            listen_ports: Default::default(),
            master: Default::default(),
            slaves: Default::default(),
            share: Default::default(),
            shared: false,
        }
    }
}

// 所有的stream都初始化完成
impl<P> discovery::Inited for RedisTopology<P> {
    fn inited(&self) -> bool {
        self.master.len() > 0 && self.master.inited()
    }
}
