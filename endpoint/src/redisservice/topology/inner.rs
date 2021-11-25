// use std::{
//     collections::{HashMap, HashSet},
//     net::IpAddr,
//     ops::Deref,
//     sync::{atomic::Ordering, Arc},
// };

// use ds::DnsResolver;
// use protocol::{Protocol, Resource};
// use stream::{BackendBuilder, BackendStream};

// use crate::seq::Seq;

// use super::VisitAddress;

// #[derive(Clone, Default)]
// pub(crate) struct Inner<T> {
//     fack_cid: bool,
//     // addrs 可能是domain，也可能是ip
//     pub(crate) addrs: T,

//     // 域名及对应的ip,ip需要按固定算法排序
//     ips: HashMap<String, Vec<IpAddr>>,
//     // 每个ip对应的streams
//     streams: HashMap<String, Arc<BackendBuilder>>,
//     // 根据这个idx进行域名的ip轮询
//     // idx: usize,
//     idx: Seq,
//     dns_resolver: DnsResolver,
// }

// impl<T> Inner<T> {
//     // pub(crate) fn with<F: FnMut(&mut T)>(&mut self, mut f: F) {
//     //     f(&mut self.addrs);
//     // }

//     pub(crate) fn set(&mut self, addrs: T) {
//         self.addrs = addrs
//     }

//     // 为新的addr获取域名ip，并构建连接，同时清理旧连接
//     pub(crate) fn update<P>(&mut self, namespace: &str, parser: &P)
//     where
//         T: VisitAddress,
//         P: Send + Sync + Protocol + 'static + Clone,
//     {
//         let mut all_addrs = HashSet::with_capacity(16);
//         let mut all_ips = HashSet::with_capacity(64);
//         let mut new_addr_ips = HashMap::with_capacity(64);

//         self.addrs.visit(|addr| {
//             all_addrs.insert(addr.to_string());
//             let new_ips = self.dns_resolver.lookup_ips(addr);
//             // 每次都是用解析出来的新ip
//             if new_ips.len() == 0 {
//                 return;
//             }
//             new_addr_ips.insert(addr.to_string(), new_ips);
//         });

//         // 设置新的ips以及streams
//         self.ips.extend(new_addr_ips.clone());
//         for (_, ips) in new_addr_ips.iter() {
//             for ip in ips.into_iter() {
//                 all_ips.insert(ip.to_string());
//                 // 已有的stream复用
//                 if self.streams.contains_key(&ip.to_string()) {
//                     continue;
//                 }
//                 self.streams.insert(
//                     ip.to_string(),
//                     Arc::new(BackendBuilder::from(
//                         parser.clone(),
//                         ip.to_string().as_str(),
//                         stream::MAX_CONNECTIONS,
//                         Resource::Redis,
//                         namespace,
//                     )),
//                 );
//             }
//         }

//         // 清理不再需要的ips、streams
//         self.ips.retain(|addr, _| all_addrs.contains(addr));
//         self.streams.retain(|ip, _| all_ips.contains(ip));
//     }

//     // 根据设置的addrs轮询ip，然后根据ip返回处理请求的streams
//     pub(crate) fn select(
//         &self,
//         share: Option<&HashMap<String, Arc<BackendBuilder>>>,
//     ) -> Vec<BackendStream>
//     where
//         T: VisitAddress,
//     {
//         let builders = share.unwrap_or(&self.streams);
//         let mut streams = Vec::with_capacity(8);

//         let mut need_loop = false;
//         self.addrs.select(|addr| {
//             let ips = self.ips.get(addr).unwrap();
//             debug_assert!(ips.len() > 0);
//             let mut ip = ips.get(0).unwrap();
//             if ips.len() > 1 {
//                 need_loop = true;
//                 // let idx = self.idx % ips.len();
//                 let idx = self.idx.fetch_and(1, Ordering::AcqRel);
//                 ip = ips.get(idx).unwrap();
//             }

//             streams.push(
//                 builders
//                     .get(&ip.to_string())
//                     .expect("address match streams")
//                     .build(self.fack_cid),
//             )
//         });

//         // if need_loop {
//         //     self.idx += 1;
//         // }

//         streams
//     }

//     pub(crate) fn streams(&self) -> &HashMap<String, Arc<BackendBuilder>> {
//         &self.streams
//     }

//     pub(crate) fn take(&mut self) {
//         if self.streams.len() > 0 {
//             log::info!("clean redis streams taken:{:?}", self.streams.keys());
//             self.streams.clear();
//         }
//     }

//     pub(crate) fn inited(&self) -> bool {
//         self.streams
//             .iter()
//             .fold(true, |inited, (_, s)| inited && s.inited())
//     }
// }

// impl<T> Deref for Inner<T> {
//     type Target = T;
//     fn deref(&self) -> &Self::Target {
//         &self.addrs
//     }
// }
