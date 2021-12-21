use crate::CacheAligned;
use crossbeam_queue::{ArrayQueue, SegQueue};

use std::cell::Cell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

/// 无锁，支持并发更新offset，按顺序读取offset的数据结构
pub struct SeqOffset {
    l2: ArrayQueue<(usize, usize)>,
    l3: SegQueue<(usize, usize)>,
    // 空跑时，l3.pop也会带来额外的cpu消耗。用l3_num避免
    l3_num: CacheAligned<AtomicUsize>,
    // 只有一个线程访问
    offset: CacheAligned<Cell<usize>>,
    seqs: CacheAligned<Cell<HashMap<usize, usize>>>,
}

impl SeqOffset {
    pub fn new() -> Self {
        Self::with_capacity(32)
    }
    pub fn with_capacity(cap: usize) -> Self {
        debug_assert!(cap >= 1);
        //let cache = (0..cap).map(|_| CacheAligned(Item::new())).collect();
        Self {
            l2: ArrayQueue::new(cap),
            l3: SegQueue::new(),
            l3_num: CacheAligned::new(AtomicUsize::new(0)),
            offset: CacheAligned::new(Cell::new(0)),
            seqs: CacheAligned::new(Cell::new(HashMap::with_capacity(cap))),
        }
    }
    // 插入一个span, [start, end)。
    // 如果start == offset，则直接更新offset,
    // 否则将span临时存储下来。
    // TODO 临时存储空间可能会触发OOM
    // end > start
    #[inline(always)]
    pub fn insert(&self, start: usize, end: usize) {
        log::debug!("{} => {}", start, end);
        debug_assert!(end > start);
        if let Err(_) = self.l2.push((start, end)) {
            log::debug!("l2 missed. start:{} end:{}", start, end);
            self.l3.push((start, end));
            self.l3_num.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    #[inline]
    pub fn span(&self) -> usize {
        let old = self.offset.0.get();
        self.sort_and_flatten() - old
    }

    // 把无序的区间按start排序，并且确保首尾相接. 返回最大的offset。
    #[inline(always)]
    pub fn sort_and_flatten(&self) -> usize {
        let mut offset = self.offset.0.get();
        use std::mem::transmute;
        let seqs: &mut HashMap<usize, usize> = unsafe { transmute(self.seqs.0.as_ptr()) };
        while let Some((start, end)) = self.l2.pop() {
            if offset == start {
                offset = end;
            } else {
                seqs.insert(start, end);
            }
        }
        if self.l3_num.0.load(Ordering::Acquire) > 0 {
            let mut n = 0;
            while let Some((start, end)) = self.l3.pop() {
                if offset == start {
                    offset = end;
                } else {
                    seqs.insert(start, end);
                }
                n += 1;
            }
            self.l3_num.0.fetch_sub(n, Ordering::AcqRel);
        }
        while let Some(end) = seqs.remove(&offset) {
            offset = end;
        }
        self.offset.0.replace(offset);
        log::debug!("offset: loaded = {}", offset);
        offset
    }
    // 调用方确保reset与load不同时访问
    #[inline]
    pub fn reset(&self) {
        while let Some(_) = self.l2.pop() {}
        while let Some(_) = self.l3.pop() {}
        self.offset.0.replace(0);
        use std::mem::transmute;
        let seqs: &mut HashMap<usize, usize> = unsafe { transmute(self.seqs.0.as_ptr()) };
        seqs.clear();
    }
}

unsafe impl Send for SeqOffset {}
unsafe impl Sync for SeqOffset {}
