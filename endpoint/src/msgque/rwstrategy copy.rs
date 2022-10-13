use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU32, AtomicU64, AtomicUsize},
};

use rand::{seq::SliceRandom, thread_rng};

// 命中优先策略
// 需求：1 滞留时间尽可能短；（即写入量大的ip，写入量大的size queue 列表，对应queue也要读取频率要高）；2 请求负载均衡；
// 方案：
//     1 某个ip，如果命中，则继续读取，持续读取N次，如果遇到读空，则跳到下一个ip；
//     2 某1-2个size的ip列表，如果占比明显较高，则增加读取概率；
//     3 提前构建多个ip随机列表，每次请求轮询选择列表，避免请求集中到某个ip；
pub(crate) struct HitFirst {
    // 对ip随机排序后的组合 及 当前位置的hits
    combination_hits: Vec<(Vec<Node>, AtomicU32)>,
    // ip随机组合的访问cursor
    combination_cursor: CombinationCursor,

    // TODO: BTreeMap 可以换成更高效的Vec，跑通后优化 fishermen
    // size queue pools
    sized_pools: BTreeMap<usize, Vec<usize>>,
    // size queue pool的hits
    sized_pools_hits: BTreeMap<usize, AtomicU32>,
    // size queue pool的访问cursor
    sized_pools_cursor: PoolCursor,
}

// 读取的cursor，包括
struct CombinationCursor {
    combination: AtomicUsize,
    cursor: AtomicUsize,
}

struct PoolCursor {
    pool_size: usize,
    cursor: AtomicUsize,
}

// 每个节点的id和支持最大写入size
#[derive(Debug, Clone, Copy)]
struct Node {
    id: usize,
    size: usize,
}

// 随机ip列表数量，用于分拆并发访问
const PARALLEL_COUNT: usize = 5;
// 单个ip，如果连续命中，最多一次读取200条
const MAX_HITS: usize = 200;

impl HitFirst {
    // 根据所有的读ip索引构建HitFirst策略>
    pub(super) fn from(queues: Vec<Node>) -> Self {
        // 初始化mq随机组合
        let combination_hits = Vec::with_capacity(PARALLEL_COUNT);
        let mut rng = thread_rng();
        for i in 0..PARALLEL_COUNT {
            let comb = queues.clone();
            comb.shuffle(&mut rng);
            combination_hits.push((comb, AtomicU32::new(0)));
        }

        // mq 随机组合的cursor
        let combination_cursor = CombinationCursor {
            combination: AtomicUsize::new(0),
            cursor: AtomicUsize::new(0),
        };

        // 按size聚合queue pools 及 hits
        let mut sized_pools = BTreeMap::new();
        let mut sized_pools_hits = BTreeMap::new();
        for node in queues.iter() {
            let que_size = node.size;
            if !sized_pools.contains_key(&que_size) {
                sized_pools.insert(que_size, Vec::with_capacity(4));
                sized_pools_hits.insert(que_size, AtomicU32::new(0));
            }
            let pool = sized_pools.get(&que_size).unwrap();
            pool.push(node.id);
        }

        // pools 初始化访问的 cursor
        let sized_pools_cursor = PoolCursor {
            pool_size: queues.get(0).unwrap().size,
            cursor: AtomicUsize::new(0),
        };

        Self {
            combination_hits,
            combination_cursor,
            sized_pools,
            sized_pools_hits,
            sized_pools_cursor,
        }
    }

    // 获取队列的下一个读取位置，第一次直接读取全队列的cursors，重试读取高频hits的queue
    pub fn next_queue_idx(&self, last_cursor: CombinationCursor) -> usize {
        if first_call {}
    }
}

// 一组读queue的游标信息，包括queue列表和当前位置大概的hits数
#[derive(Debug, Default)]
struct ReaderHits {
    // queue idx 列表
    readers: Vec<usize>,
    // 当前位置的读取hits数
    current_hits: AtomicUsize,
}

impl ReaderCursor {
    // 根据queue列表初始化read cursor，Vec中元素为queue_idx
    pub(super) fn from(queues: &Vec<usize>) -> Self {
        assert!(queues.len() > 0);

        let mut readers = queues.clone();
        let mut rng = thread_rng();
        readers.shuffle(&mut rng);

        let rd = rand::random::<usize>() % queues.len();
        Self {
            readers,
            cursor: AtomicUsize::new(rd),
            hits: AtomicUsize::new(0),
        }
    }
}
