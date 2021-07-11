//use cache_line_size::CacheAligned;
use crossbeam_queue::{ArrayQueue, SegQueue};

use std::cell::Cell;
use std::collections::HashMap;
//use std::sync::atomic::{AtomicUsize, Ordering};

/// 无锁，支持并发更新offset，按顺序读取offset的数据结构
pub struct SeqOffset {
    // 只有一个线程访问
    offset: Cell<usize>,
    l2: ArrayQueue<(usize, usize)>,
    l3: SegQueue<(usize, usize)>,
    seqs: Cell<HashMap<usize, usize>>,
}

impl SeqOffset {
    pub fn with_capacity(cap: usize) -> Self {
        debug_assert!(cap >= 1);
        //let cache = (0..cap).map(|_| CacheAligned(Item::new())).collect();
        Self {
            offset: Cell::new(0),
            l2: ArrayQueue::new(cap * 4),
            l3: SegQueue::new(),
            seqs: Cell::new(HashMap::with_capacity(cap)),
        }
    }
    // 插入一个span, [start, end)。
    // 如果start == offset，则直接更新offset,
    // 否则将span临时存储下来。
    // TODO 临时存储空间可能会触发OOM
    // end > start
    pub fn insert(&self, start: usize, end: usize) {
        log::debug!("offset: {} => {}", start, end);
        debug_assert!(end > start);
        if let Err(_) = self.l2.push((start, end)) {
            log::debug!("offset: l2 missed. start:{} end:{}", start, end);
            self.l3.push((start, end));
        }
    }

    // load offset, [0.. offset)都已经调用insert被相应的span全部填充
    pub fn load(&self) -> usize {
        log::debug!("offset: loading");
        let mut offset = self.offset.get();
        use std::mem::transmute;
        let seqs: &mut HashMap<usize, usize> = unsafe { transmute(self.seqs.as_ptr()) };
        while let Some((start, end)) = self.l2.pop() {
            if offset == start {
                offset = end;
            } else {
                seqs.insert(start, end);
            }
        }
        while let Some((start, end)) = self.l3.pop() {
            if offset == start {
                offset = end;
            } else {
                seqs.insert(start, end);
            }
        }
        while let Some(end) = seqs.remove(&offset) {
            offset = end;
        }
        self.offset.replace(offset);
        log::debug!("offset: loaded = {}", offset);
        offset
    }
}
