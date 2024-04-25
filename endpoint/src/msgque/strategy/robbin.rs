// use std::{
//     collections::{BTreeMap, HashMap},
//     ops::Bound::{Included, Unbounded},
//     sync::{
//         atomic::{AtomicUsize, Ordering},
//         Arc,
//     },
// };

// use rand::{seq::SliceRandom, thread_rng};

// use super::QID;

// // 按照轮询方式写入，根据讨论，暂时不在支持晋级size，只写对应size的queue pool
// #[derive(Debug, Default)]
// pub(crate) struct RobbinWriter {
//     qnodes: BTreeMap<usize, Vec<QID>>,
//     cursors: HashMap<usize, Arc<AtomicUsize>>,
// }

// impl RobbinWriter {
//     // 根据queue的写队列构建轮询写策略
//     pub(crate) fn from(writers: BTreeMap<usize, Vec<QID>>) -> Self {
//         assert!(writers.len() > 0);

//         let mut qnodes = writers.clone();
//         let mut rng = thread_rng();
//         for v in qnodes.values_mut() {
//             v.shuffle(&mut rng);
//         }

//         let mut cursors = HashMap::with_capacity(writers.len());
//         for k in qnodes.keys() {
//             cursors.insert(*k, Arc::new(AtomicUsize::new(0)));
//         }

//         Self { qnodes, cursors }
//     }

//     // 获取待写入的queue id
//     pub(crate) fn next_queue_write(&self, size: usize) -> (QID, usize) {
//         let mut rsize = size;

//         // 第一次请求，或在配置变更的并发场景下，队列的size发上变更，需要确认存储的que size
//         if !self.qnodes.contains_key(&size) {
//             let mut found = false;
//             for (i, nodes) in self.qnodes.range((Included(&size), Unbounded)) {
//                 assert!(nodes.len() > 0);
//                 rsize = *i;
//                 found = true;
//                 break;
//             }
//             if !found {
//                 rsize = *(self.qnodes.keys().last().unwrap());
//                 log::warn!("mq msg too big: {}, try: {}", size, rsize);
//             } else {
//                 log::debug!("+++ robbing mcq {} => {}", size, rsize);
//             }
//         }

//         assert!(self.cursors.contains_key(&rsize));

//         let nodes = self.qnodes.get(&rsize).unwrap();
//         let idx = self.cursors.get(&rsize).unwrap();
//         let nidx = idx.fetch_add(1, Ordering::Relaxed) % nodes.len();
//         let qid = nodes.get(nidx).unwrap();
//         (*qid, rsize)
//     }
// }
