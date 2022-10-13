use rand::{seq::SliceRandom, thread_rng};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicU32, AtomicUsize, Ordering},
        Arc,
    },
};

use super::QID;

// 命中优先策略
// 需求：1 滞留时间尽可能短；（即写入量大的ip，写入量大的size queue 列表，对应queue也要读取频率要高）；2 请求负载均衡；
// 方案：
//     1 某个ip，如果命中，则继续读取，持续读取N次，如果遇到读空，则跳到下一个ip；
//     2 某1-2个size的ip列表，如果占比明显较高，则增加读取概率；
//     3 提前构建一个ip随机列表，同时构建多个cursor，每次请求轮询选择cursor，避免请求集中到某个ip；

// 随机cursor数量，用于分拆并发访问
const PARALLEL_COUNT: usize = 5;
// 单个ip，如果连续命中，最多一次读取1024条
const MAX_HITS: u32 = 1024;

#[derive(Debug, Default, Clone)]
pub(crate) struct HitFirstReader {
    // 对ip随机排序后的组合
    qnodes: Vec<Node>,
    qid_idxes: HashMap<QID, usize>,
    // ip随机组合的访问cursor，每个queue开启多个cursor，方便快速找到有数据的que
    cursors: Vec<CursorHits>,
    cursor_current: Arc<AtomicUsize>,

    // TODO: BTreeMap 可以换成更高效的Vec，跑通后优化 fishermen
    // size queue pools
    sized_pools: BTreeMap<usize, Vec<QID>>,
    // size queue pool的hits
    sized_pools_hits: BTreeMap<usize, Arc<AtomicU32>>,
    // 优先 queue pool的访问cursor
    priority_pool_cursor: PoolCursor,
}

// 读取的cursor，包括
#[derive(Debug, Clone)]
struct CursorHits {
    node_idx: Arc<AtomicUsize>,
    hits: Arc<AtomicU32>,
}

#[derive(Debug, Default, Clone)]
struct PoolCursor {
    pool_size: usize,
    node_idx: Arc<AtomicUsize>,
}

// 每个节点的id和支持最大写入size
#[derive(Debug, Clone, Copy)]
pub struct Node {
    id: QID,
    size: usize,
}

impl HitFirstReader {
    // 根据所有的读ip索引构建HitFirst策略>
    pub(crate) fn from(queues: Vec<Node>) -> Self {
        assert!(queues.len() > 0);

        // 初始化mq随机组合
        let mut rng = thread_rng();
        let mut qnodes = queues.clone();
        qnodes.shuffle(&mut rng);

        // 初始化每个que id所在的位置
        let mut qid_idxes = HashMap::with_capacity(queues.len());
        for (i, n) in qnodes.iter().enumerate() {
            qid_idxes.insert(n.id, i);
        }

        // mq 随机组合的cursor
        let parallel = parallel(qnodes.len());
        let mut cursors = Vec::with_capacity(parallel);
        for i in 0..parallel {
            let cursor = i * queues.len() / parallel;
            let chits = CursorHits {
                node_idx: Arc::new(AtomicUsize::new(cursor)),
                hits: Arc::new(AtomicU32::new(0)),
            };
            cursors.push(chits);
        }

        // 按size聚合queue pools 及 hits
        let mut sized_pools = BTreeMap::new();
        let mut sized_pools_hits = BTreeMap::new();
        for node in queues.iter() {
            let que_size = node.size;
            if !sized_pools.contains_key(&que_size) {
                sized_pools.insert(que_size, Vec::with_capacity(4));
                sized_pools_hits.insert(que_size, Arc::new(AtomicU32::new(0)));
            }
            let pool = sized_pools.get_mut(&que_size).unwrap();
            pool.push(node.id);
        }

        // pools 初始化访问的 cursor
        let priority_pool_cursor = PoolCursor {
            pool_size: queues.get(0).unwrap().size,
            node_idx: Arc::new(AtomicUsize::new(0)),
        };

        Self {
            qnodes,
            qid_idxes,
            cursors,
            cursor_current: Arc::new(AtomicUsize::new(0)),
            sized_pools,
            sized_pools_hits,
            priority_pool_cursor,
        }
    }

    // 获取队列的下一个读取位置
    pub(crate) fn next_queue_read(&self, last_read: Option<QID>) -> QID {
        // 如果是重试读，需要先将上一次查询的cursor移位
        self.try_shift_cursor(last_read);

        // 获取下一个读取node id
        let next = self.cursor_current.fetch_add(1, Ordering::Relaxed);
        let nidx = next % self.cursors.len();
        let chits = self.cursors.get(nidx).unwrap();
        let mut idx = chits.node_idx.load(Ordering::Relaxed);
        let hits = chits.hits.fetch_add(1, Ordering::Relaxed);

        // 如果当前位置hits数太大，也偏移一个位置
        if hits > MAX_HITS {
            if let Ok(_c) =
                chits
                    .node_idx
                    .compare_exchange(idx, idx + 1, Ordering::Relaxed, Ordering::Relaxed)
            {
                chits.hits.store(0, Ordering::Relaxed);
            }
            // cursor已偏移到新位置
            idx += 1;
        }
        let node = self.qnodes.get(idx).unwrap();
        if self.sized_pools_hits.contains_key(&node.size) {
            let hits = self.sized_pools_hits.get(&node.size).unwrap();
            hits.fetch_add(1, Ordering::Relaxed);
        }
        node.id
    }

    // 获取队列的下一个高优先级队列pool的读取位置
    pub(crate) fn next_priority_queue_read(&self, last_read: Option<QID>) -> QID {
        // 如果高优先级队列还没排出来，降级到获取普通队列读取位置
        if !self
            .sized_pools
            .contains_key(&self.priority_pool_cursor.pool_size)
        {
            log::info!("+++ no priority queue, use common que");
            return self.next_queue_read(last_read);
        }

        // 先移动前一次读取的cursor
        self.try_shift_cursor(last_read);

        // 获取priority queue中的mcq
        let qsize = self.priority_pool_cursor.pool_size;
        let pool_idx = self
            .priority_pool_cursor
            .node_idx
            .fetch_add(1, Ordering::Relaxed);
        if let Some(nodes) = self.sized_pools.get(&qsize) {
            let idx = pool_idx % nodes.len();
            if let Some(qid) = nodes.get(idx) {
                return *qid;
            }
        }

        log::warn!("+++priority que is malfored");
        return self.next_queue_read(last_read);
    }

    // 重试读取的请求，对之前的空读位置cursor进行移位
    fn try_shift_cursor(&self, last_read_qid: Option<QID>) {
        // 先尝试获取
        let lread_op = match last_read_qid {
            Some(qid) => self.qid_idxes.get(&qid),
            None => None,
        };
        // last_idx 大于qnodes的长度，说明之前没有读取过，直接返回
        if lread_op == None {
            return;
        }

        // 此处说明已经读过
        let last_read_idx = *lread_op.unwrap();
        let mut found = 0;
        for ch in self.cursors.iter() {
            let node_idx = ch.node_idx.load(Ordering::Relaxed);
            if node_idx == last_read_idx {
                let new_node_idx = if found == 0 {
                    node_idx + 1
                } else {
                    // 一般情况下，cursor重叠的概率不大，所以放在此处计算step
                    let step = self.qnodes.len() / self.cursors.len();
                    log::debug!("+++ mcq repeat cursor: {}/{}", node_idx, self.qnodes.len());
                    (node_idx + found * step) % self.qnodes.len()
                };
                found += 1;

                // last 读空，需要偏移位置
                if let Ok(_c) = ch.node_idx.compare_exchange(
                    node_idx,
                    new_node_idx,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    // 谁修改cursor成功，谁也修改hits数
                    ch.hits.store(0, Ordering::Relaxed);
                }
            }
        }
    }
}

// 并行查询的cursor 数量
fn parallel(node_count: usize) -> usize {
    if node_count <= PARALLEL_COUNT {
        1
    } else if node_count < 2 * PARALLEL_COUNT {
        2
    } else {
        PARALLEL_COUNT
    }
}

impl Node {
    pub fn from(qid: QID, que_size: usize) -> Self {
        Self {
            id: qid,
            size: que_size,
        }
    }
}
