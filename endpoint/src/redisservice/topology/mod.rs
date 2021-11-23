// mod config;

// use config::*;

// mod inner;
// use inner::*;
// use protocol::Protocol;
// use stream::{BackendStream, LayerRole};

// use crate::ServiceTopo;

// #[derive(Clone)]
// pub struct Topology<P> {
//     // basic info
//     access_mod: String,
//     hash: String,
//     distribution: String,
//     listen_ports: Vec<u16>,
//     // resource_type: String,
//     // master domain，每个分片一个端口，可能是域名，也可能是ip，每个域名一个ip
//     master: Inner<Vec<String>>,
//     // slave和master 分片的域名，每个域名顺序即分片顺序，每个分片是一个端口，可能是一个域名，也可能是多个ip，每个域名一般有多个ip
//     slave_get: Inner<Vec<String>>,
//     slave_mget: Inner<Vec<String>>,
//     // 备用连接，一般是master,用于slaves在get或者mget的backup
//     standby: Inner<Vec<String>>,

//     parser: P,
//     // 如果没有slave，则所有连接共享master的物流连接
//     share: Inner<Vec<String>>,
//     shared: bool,
// }

// // impl<P> Topology<P> {
// //     fn shared(&self) -> Option<&HashMap<String, Arc<BackendBuilder>>> {
// //         if self.shared {
// //             return Some(self.share.streams());
// //         }
// //         None
// //     }
// // }

// impl<P> ServiceTopo for Topology<P> {
//     fn hash(&self) -> &str {
//         &self.hash
//     }

//     fn distribution(&self) -> &str {
//         &self.distribution
//     }

//     fn listen_ports(&self) -> Vec<u16> {
//         self.listen_ports.clone()
//     }

//     // 返回slave作为common reader，standby作为common reader异常时的backup
//     fn get(
//         &self,
//     ) -> (
//         Vec<(LayerRole, Vec<BackendStream>)>,
//         Vec<(LayerRole, Vec<BackendStream>)>,
//     ) {
//         let streams_slave = self.slave_get.select(Some(self.share.streams()));
//         let streams_standby = self.standby.select(Some(self.share.streams()));

//         let mut readers = Vec::with_capacity(2);
//         if streams_slave.len() > 0 {
//             readers.push((LayerRole::Slave, streams_slave));
//         }
//         readers.push((LayerRole::Master, streams_standby));
//         (readers, vec![])
//     }

//     // 返回slave作为mget的 reader，standby作为common reader异常时的backup
//     fn mget(
//         &self,
//     ) -> (
//         Vec<(LayerRole, Vec<BackendStream>)>,
//         Vec<(LayerRole, Vec<BackendStream>)>,
//     ) {
//         let streams_slave = self.slave_mget.select(Some(self.share.streams()));
//         let streams_standby = self.standby.select(Some(self.share.streams()));

//         let mut readers = Vec::with_capacity(2);
//         if streams_slave.len() > 0 {
//             readers.push((LayerRole::Slave, streams_slave));
//         }
//         readers.push((LayerRole::Master, streams_standby));
//         (readers, vec![])
//     }

//     fn master(&self) -> Vec<BackendStream> {
//         self.master.select(Some(self.share.streams()))
//     }

//     fn topo_inited(&self) -> bool {
//         let rs = self.master.len() > 0
//             && self.master.inited()
//             && self.slave_get.inited()
//             && self.slave_mget.inited();
//         let rs_shared = self.shared && self.share.inited();
//         rs || rs_shared
//     }
// }

// impl<P> discovery::TopologyWrite for Topology<P>
// where
//     P: Send + Sync + Protocol,
// {
//     fn update(&mut self, name: &str, cfg: &str) {
//         let idx = name.find(":").unwrap_or(name.len());
//         if idx == 0 || idx >= name.len() - 1 {
//             log::info!("malformed redis service name:{} not found namespace", name);
//             return;
//         }

//         let namespace = &name[idx + 1..];
//         match Namespace::parse(cfg, namespace) {
//             Err(e) => {
//                 log::warn!(
//                     "parse redisservice config failed for {}, err:{}, cfg: {}",
//                     name,
//                     e,
//                     cfg.len()
//                 );
//             }
//             Ok(ns) => {
//                 let master = ns.master();
//                 if master.len() == 0 {
//                     log::warn!("redisservice/{} no master:{}", name, cfg);
//                     return;
//                 }

//                 self.access_mod = ns.basic.access_mod.clone();
//                 self.hash = ns.basic.hash.clone();
//                 self.distribution = ns.basic.distribution.clone();
//                 self.listen_ports = ns.parse_listen_ports();
//                 self.master.set(ns.master());
//                 self.slave_get.set(ns.slave());
//                 self.slave_mget.set(ns.slave());
//                 self.standby.set(ns.master());

//                 // 如果没有slave，读写共享相同的物理连接
//                 if ns.slave().len() == 0 {
//                     self.shared = true;
//                     self.share.set(ns.master());
//                     self.share.update(namespace, &self.parser);
//                 } else {
//                     self.shared = false;
//                     self.master.update(namespace, &self.parser);
//                     self.slave_get.update(namespace, &self.parser);
//                     self.slave_mget.update(namespace, &self.parser);
//                     self.standby.update(namespace, &self.parser);
//                 }
//             }
//         }
//     }

//     fn gc(&mut self) {
//         if self.shared {
//             self.master.take();
//             self.slave_get.take();
//             self.slave_mget.take();
//             self.standby.take();
//         }
//     }
// }

// impl<P> From<P> for Topology<P> {
//     fn from(parser: P) -> Self {
//         Self {
//             access_mod: Default::default(),
//             hash: Default::default(),
//             distribution: Default::default(),
//             listen_ports: Vec::new(),
//             master: Default::default(),
//             slave_get: Default::default(),
//             slave_mget: Default::default(),
//             standby: Default::default(),
//             parser: parser,
//             share: Default::default(),
//             shared: false,
//         }
//     }
// }

// // 所有的stream都初始化完成
// impl<P> discovery::Inited for Topology<P> {
//     fn inited(&self) -> bool {
//         let rs = self.master.len() > 0
//             && self.master.inited()
//             && self.slave_get.inited()
//             && self.slave_mget.inited();
//         let rs_shared = self.shared && self.share.inited();
//         rs || rs_shared
//     }
// }

// pub(crate) trait VisitAddress {
//     fn visit<F: FnMut(&str)>(&self, f: F);
//     fn select<F: FnMut(&str)>(&self, f: F);
// }

// impl VisitAddress for Vec<String> {
//     fn visit<F: FnMut(&str)>(&self, mut f: F) {
//         for addr in self.iter() {
//             f(addr);
//         }
//     }

//     fn select<F: FnMut(&str)>(&self, mut f: F) {
//         for addr in self.iter() {
//             f(addr);
//         }
//     }
// }
