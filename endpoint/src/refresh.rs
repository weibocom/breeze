// use std::sync::Arc;

// use crate::{Endpoint, Topology, TopologyCheck};
// use discovery::{TopologyRead, TopologyReadGuard};
// use sharding::hash::{Hash, HashKey};

// use protocol::{
//     callback::{Callback, CallbackPtr},
//     request::Request,
// };

// // 支持刷新
// pub struct RefreshTopology<T> {
//     reader: TopologyReadGuard<T>,
//     top: spin::RwLock<SharedTop<T>>,
// }
// impl<T: Clone + 'static + Endpoint<Item = Request>> RefreshTopology<T> {
//     // reader一定是已经初始化过的，否则会UB
//     #[inline]
//     pub fn from(reader: TopologyReadGuard<T>) -> Self {
//         let top = SharedTop::new(&reader).into();
//         Self { top, reader }
//     }
//     pub fn build(self: &Arc<Self>) -> Option<CheckedTopology<T>> {
//         let shared = self.top.try_read()?.clone();
//         return Some(CheckedTopology::new(shared, self.clone()));
//     }
// }

// // 每个connection持有一个CheckedTopology，在refresh时调用check检查是否有更新
// pub struct CheckedTopology<T> {
//     top: SharedTop<T>,
//     global: Arc<RefreshTopology<T>>,
// }
// impl<T> CheckedTopology<T> {
//     fn new(top: SharedTop<T>, inner: Arc<RefreshTopology<T>>) -> Self {
//         Self { top, global: inner }
//     }
// }
// impl<T: Endpoint + Clone + 'static> Endpoint for CheckedTopology<T> {
//     type Item = T::Item;
//     #[inline(always)]
//     fn send(&self, req: T::Item) {
//         self.top.send(req);
//     }

//     #[inline(always)]
//     fn shard_idx(&self, hash: i64) -> usize {
//         self.top.shard_idx(hash)
//     }
// }
// impl<T: Topology + Clone + 'static> Topology for CheckedTopology<T> {
//     #[inline(always)]
//     fn exp_sec(&self) -> u32 {
//         self.top.exp_sec()
//     }
//     #[inline(always)]
//     fn hash<K: HashKey>(&self, k: &K) -> i64 {
//         self.top.hash(k)
//     }
// }
// impl<T: Topology + Clone + 'static + Endpoint<Item = Request>> CheckedTopology<T> {
//     // 检查是否有更新
//     // 一共有三个cycle来控制更新
//     // 1. reader_cycle：TopologyReadGuard.cycle()，每次top下电梯，该值都会+1.
//     // 2. shared_cycle：SharedTop.cycle 当前所有连接共享top的cycle。如果shared_cycle <
//     //    reader_cycle，则需要更新SharedTop
//     // 3. conn_cycle: CheckedTopology.cycle，当前conn持有的top，如果conn_cycle <
//     //    reader_cycle，则需要触发更新SharedTop，并发更新时需要加锁。
//     #[inline]
//     fn update(&mut self) -> Option<()> {
//         let reader_cycle = self.global.reader.cycle();
//         if self.top.cycle() >= reader_cycle {
//             // 当前connection持有的top是最新的。
//             return None;
//         }
//         let shared_top = self.global.top.try_read()?;
//         if shared_top.cycle() >= reader_cycle {
//             // 说明别的conn已经触发了更新，直接获取即可
//             self.top = shared_top.clone();
//             return Some(());
//         }
//         // 释放读锁
//         drop(shared_top);
//         let mut top = self.global.top.try_write()?;
//         // 更新所有conn共享的top
//         *top = SharedTop::new(&self.global.reader);
//         // 更新当前conn持有的top
//         self.top = top.clone();
//         // 释放写锁
//         drop(top);
//         Some(())
//     }
// }
// impl<T: Topology + Clone + 'static + Endpoint<Item = Request>> TopologyCheck
//     for CheckedTopology<T>
// {
//     #[inline]
//     fn refresh(&mut self) -> bool {
//         let _conn_cycle = self.top.cycle();
//         self.update()
//             .map(|_| {
//                 log::info!("topology refreshed {} => {}", _conn_cycle, self.top);
//             })
//             .is_some()
//     }
//     #[inline(always)]
//     fn callback(&self) -> CallbackPtr {
//         self.top.callback.clone()
//     }
// }
// impl<T: Topology + Clone + 'static> Hash for CheckedTopology<T> {
//     #[inline(always)]
//     fn hash<S: HashKey>(&self, key: &S) -> i64 {
//         self.top.hash(key)
//     }
// }

// #[derive(Clone)]
// struct SharedTop<T> {
//     t: Arc<T>,
//     callback: CallbackPtr,
//     cycle: usize,
// }
// impl<T: Clone + Endpoint<Item = Request> + 'static> SharedTop<T> {
//     fn new(reader: &TopologyReadGuard<T>) -> Self {
//         let cycle = reader.cycle();
//         let top = reader.do_with(|t| t.clone());
//         let t = Arc::new(top);
//         let cb_top = t.clone();
//         let send = Box::new(move |req| cb_top.send(req));
//         let callback = Callback::new(send).into();
//         Self { t, callback, cycle }
//     }
//     fn cycle(&self) -> usize {
//         self.cycle
//     }
// }
// use std::ops::Deref;
// impl<T> Deref for SharedTop<T> {
//     type Target = T;
//     #[inline(always)]
//     fn deref(&self) -> &Self::Target {
//         &self.t
//     }
// }

// use std::fmt::Display;
// impl<T> Display for SharedTop<T> {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "cycle:{} ptr:{:p}", self.cycle, self.t.as_ref())
//     }
// }
