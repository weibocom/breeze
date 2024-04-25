// use rand::{seq::SliceRandom, thread_rng};
// use std::{
//     collections::{BTreeMap, HashMap},
//     fmt::{Debug, Display},
//     sync::{
//         atomic::{AtomicU32, AtomicUsize, Ordering},
//         Arc,
//     },
// };

// use super::QID;

// // 命中优先策略
// // 需求：1 滞留时间尽可能短；（即写入量大的ip，写入量大的size queue 列表，对应queue也要读取频率要高）；2 请求负载均衡；
// // 方案：
// //     1 对所有队列ip随机，然后构建N个cursor 以及一个二级cursor；
// //     2 每次请求，通过二级curosr在多个cursor间轮询；
// //     3 如果某个ip命中，cursor会持续读取，直到读完 or 读到1024条msg；
// //     4 如果某个ip miss，则对应cursor向后移动一个位置；

// // 随机cursor数量，用于分拆并发访问
// const PARALLEL_COUNT: usize = 5;
// // 单个ip，如果连续命中，最多一次读取1024条
// const MAX_HITS: u32 = 1024;

// #[derive(Default)]
// pub(crate) struct HitFirstReader {
//     // 对ip随机排序后的组合
//     qnodes: Vec<Node>,
//     qid_idxes: HashMap<QID, usize>,
//     // ip随机组合的访问cursor，每个queue开启多个cursor，方便快速找到有数据的que
//     cursors: Vec<CursorHits>,
//     cursor_current: Arc<AtomicUsize>,
// }

// // 读取的cursor，包括
// #[derive(Debug, Clone)]
// struct CursorHits {
//     node_idx: Arc<AtomicUsize>,
//     hits: Arc<AtomicU32>,
// }

// // 每个节点的id和支持最大写入size
// #[derive(Debug, Clone, Copy)]
// pub struct Node {
//     id: QID,
//     size: usize,
// }

// impl HitFirstReader {
//     // 根据所有的读ip索引构建HitFirst策略>
//     pub(crate) fn from(queues: Vec<Node>) -> Self {
//         assert!(queues.len() > 0);

//         // 初始化mq随机组合
//         let mut rng = thread_rng();
//         let mut qnodes = queues.clone();
//         qnodes.shuffle(&mut rng);

//         // 初始化每个que id所在的位置
//         let mut qid_idxes = HashMap::with_capacity(queues.len());
//         for (i, n) in qnodes.iter().enumerate() {
//             qid_idxes.insert(n.id, i);
//         }

//         // mq 随机组合的cursor
//         let parallel = parallel(qnodes.len());
//         let mut cursors = Vec::with_capacity(parallel);
//         for i in 0..parallel {
//             let cursor = i * queues.len() / parallel;
//             let chits = CursorHits {
//                 node_idx: Arc::new(AtomicUsize::new(cursor)),
//                 hits: Arc::new(AtomicU32::new(0)),
//             };
//             cursors.push(chits);
//         }

//         // 按size聚合queue pools 及 hits
//         let mut sized_pools = BTreeMap::new();
//         let mut sized_pools_hits = BTreeMap::new();
//         for node in queues.iter() {
//             let que_size = node.size;
//             if !sized_pools.contains_key(&que_size) {
//                 sized_pools.insert(que_size, Vec::with_capacity(4));
//                 sized_pools_hits.insert(que_size, Arc::new(AtomicU32::new(0)));
//             }
//             let pool = sized_pools.get_mut(&que_size).unwrap();
//             pool.push(node.id);
//         }

//         let instance = Self {
//             qnodes,
//             qid_idxes,
//             cursors,
//             cursor_current: Arc::new(AtomicUsize::new(0)),
//         };
//         log::debug!("+++  Inited strategy:{:?}", instance);
//         instance
//     }

//     // 获取队列的下一个读取位置
//     pub(crate) fn next_queue_read(&self, last_read: Option<QID>) -> QID {
//         // 如果是重试读，需要先将上一次查询的cursor移位
//         self.try_shift_cursor(last_read);

//         // 获取下一个读取node id
//         let next = self.cursor_current.fetch_add(1, Ordering::Relaxed);
//         let cursor_idx = next % self.cursors.len();
//         let cursor = self.cursors.get(cursor_idx).unwrap();
//         let mut node_idx = cursor.node_idx.load(Ordering::Relaxed);
//         let node_hits = cursor.hits.fetch_add(1, Ordering::Relaxed);

//         // 如果当前位置hits数太大，也偏移一个位置
//         if node_hits > MAX_HITS {
//             let next_idx = (node_idx + 1) % self.qnodes.len();
//             if let Ok(_c) = cursor.node_idx.compare_exchange(
//                 node_idx,
//                 next_idx,
//                 Ordering::Relaxed,
//                 Ordering::Relaxed,
//             ) {
//                 cursor.hits.store(0, Ordering::Relaxed);
//             }

//             // cursor已偏移到新位置，性能
//             node_idx = next_idx;
//         }

//         assert!(
//             node_idx < self.qnodes.len(),
//             "idx:{}, qunodes:{:?}",
//             node_idx,
//             self.qnodes
//         );
//         let node = self.qnodes.get(node_idx).unwrap();
//         log::debug!("+++ use common cursor:{}, {:?}", cursor_idx, self);
//         node.id
//     }

//     // 重试读取的请求，对之前的空读位置cursor进行移位
//     fn try_shift_cursor(&self, last_read_qid: Option<QID>) {
//         // 先尝试获取
//         let lread_op = match last_read_qid {
//             Some(qid) => self.qid_idxes.get(&qid),
//             None => None,
//         };
//         // last_idx 大于qnodes的长度，说明之前没有读取过，直接返回
//         if lread_op == None {
//             return;
//         }

//         // 此处说明已经读过
//         let last_read_idx = *lread_op.unwrap();
//         let mut found = 0;
//         for (_i, ch) in self.cursors.iter().enumerate() {
//             let node_idx = ch.node_idx.load(Ordering::Relaxed);
//             if node_idx == last_read_idx {
//                 let new_node_idx = if found == 0 {
//                     (node_idx + 1) % self.qnodes.len()
//                 } else {
//                     // 一般情况下，cursor重叠的概率不大，所以放在此处计算step
//                     let step = self.qnodes.len() / self.cursors.len();
//                     log::debug!("+++ mcq repeat cursor: {}/{}", node_idx, self.qnodes.len());
//                     (node_idx + found * step) % self.qnodes.len()
//                 };
//                 found += 1;

//                 log::debug!("+++ shift cursor:{}, {} => {}", _i, node_idx, new_node_idx);
//                 // last 读空，需要偏移位置
//                 if let Ok(_c) = ch.node_idx.compare_exchange(
//                     node_idx,
//                     new_node_idx,
//                     Ordering::Relaxed,
//                     Ordering::Relaxed,
//                 ) {
//                     // 谁修改cursor成功，谁也修改hits数
//                     ch.hits.store(0, Ordering::Relaxed);
//                 }
//             }
//         }
//     }
// }

// // 并行查询的cursor 数量，注意并发数不能等于ties 或者 读写阀值
// fn parallel(node_count: usize) -> usize {
//     assert!(PARALLEL_COUNT > 0 && PARALLEL_COUNT <= 10);

//     if node_count < 3 {
//         1
//     } else if node_count < PARALLEL_COUNT {
//         2
//     } else {
//         PARALLEL_COUNT
//     }
// }

// impl Node {
//     pub fn from(qid: QID, que_size: usize) -> Self {
//         Self {
//             id: qid,
//             size: que_size,
//         }
//     }
// }

// impl Display for HitFirstReader {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(
//             f,
//             "[cursor_current={:?}, cursors={:?}, qnodes:{:?}",
//             self.cursor_current, self.cursors, self.qnodes,
//         )
//     }
// }

// impl Debug for HitFirstReader {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         Display::fmt(&self, f)
//     }
// }
