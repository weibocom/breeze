use core::fmt;
use std::sync::atomic::Ordering::Relaxed;
use std::{
    fmt::{Display, Formatter},
    ops::Deref,
};

use crate::{CloneableAtomicUsize, Endpoint};

pub(crate) mod config;
pub mod strategy;
pub mod topo;

// use strategy::QID;

// 0-7: 读/写次数；
const COUNT_BITS: u8 = 8;

// 8-23：读写的位置索引, 数量可能超过256，所以用16位；
const QID_SHIFT: u8 = 0 + COUNT_BITS;
// const QID_BITS: u8 = 16;
// 24-63: 保留

#[repr(transparent)]
struct Context {
    ctx: protocol::Context,
}

impl Context {
    #[inline]
    fn from(ctx: protocol::Context) -> Self {
        Self { ctx }
    }

    // 初始化后，ctx大于0
    #[inline]
    fn inited(&self) -> bool {
        self.ctx > 0
    }

    // 获得已read/write的次数，并对次数加1
    #[inline]
    fn get_and_incr_tried_count(&mut self) -> usize {
        let count = self.ctx as u8;

        const MAX_COUNT: u8 = ((1 << COUNT_BITS as u16) - 1) as u8;
        assert!(count < MAX_COUNT, "ctx count:{}", count);

        self.ctx += 1;

        count as usize
    }

    // read/write 的idx位置相同
    #[inline]
    fn get_last_qid(&self, inited: bool) -> Option<usize> {
        if !inited {
            return None;
        }

        let idx = self.ctx >> QID_SHIFT;
        Some(idx as usize)
    }

    /// 记录本次qid，retry时需要
    #[inline]
    fn update_qid(&mut self, qid: u16) {
        let lower = self.ctx as u8;
        self.ctx = lower as u64 | (qid as u64) << QID_SHIFT;

        // 去掉了高位字段，下面的逻辑不需要了
        // 不直接使用qid后面字段的shit，因为context的字段可能会变
        // let high_shift = QID_SHIFT + QID_BITS;
        // let high = self.ctx >> high_shift << high_shift;
        // self.ctx = lower as u64 | (qid << QID_SHIFT) as u64 | high;
    }
}

pub trait WriteStrategy {
    fn new(que_len: usize, sized_que_infos: Vec<SizedQueueInfo>) -> Self;
    fn get_write_idx(
        &self,
        msg_len: usize,
        last_idx: Option<usize>,
        tried_count: usize,
    ) -> (usize, bool);
}

pub trait ReadStrategy {
    fn new(reader_len: usize) -> Self;
    fn get_read_idx(&self, last_idx: Option<usize>) -> usize;
}

#[derive(Debug, Clone, Default)]
pub struct Shard<E> {
    pub(crate) endpoint: E,
    pub(crate) qsize: usize,
}

impl<E> Shard<E> {
    /// Create a new shard，如果是下线的endpoint，则qsize为0
    #[inline]
    pub fn new(endpoint: E, qsize: usize) -> Self {
        Shard { endpoint, qsize }
    }
}

impl<E> Deref for Shard<E> {
    type Target = E;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.endpoint
    }
}

impl<E> Display for Shard<E>
where
    E: Endpoint,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.endpoint.addr(), self.qsize)
    }
}

/**
 * 某个指定size的queue列表的信息；
 */
#[derive(Debug, Clone, Default)]
pub struct SizedQueueInfo {
    // 当前queue的size大小
    qsize: usize,
    // 当前size的queue在总队列中的起始位置
    start_pos: usize,
    // 当前size的queue的长度
    len: usize,
    // 当前size的queue的访问序号
    pub(crate) sequence: CloneableAtomicUsize,
}

impl SizedQueueInfo {
    pub fn new(qsize: usize, start_pos: usize, len: usize) -> Self {
        Self {
            qsize,
            start_pos,
            len,
            sequence: CloneableAtomicUsize::new(0),
        }
    }

    #[inline]
    pub fn qsize(&self) -> usize {
        self.qsize
    }

    #[inline]
    pub fn start_pos(&self) -> usize {
        self.start_pos
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    // 根据当前的sequence，“轮询”获取本size内下一次应该请求的queue idx
    #[inline]
    pub fn next_idx(&self) -> usize {
        let relative_idx = self.sequence.fetch_add(1, Relaxed) % self.len;
        return self.start_pos + relative_idx;
    }

    // 根据上一次请求的idx，获取本size内下一次应该请求的queue idx
    #[inline]
    pub fn next_retry_idx(&self, last_idx: usize) -> usize {
        assert!(last_idx >= self.start_pos, "{}:{:?}", last_idx, self);
        let idx = last_idx + 1;
        let relative_idx = (idx - self.start_pos) % self.len;
        self.start_pos + relative_idx
    }
}
