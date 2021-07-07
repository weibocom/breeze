use lockfree::map::Map;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};

/// 无锁，支持并发更新offset，按顺序读取offset的数据结构
pub struct SeqOffset {
    tries: usize,
    offset: AtomicUsize,
    len: AtomicIsize,
    slow_cache: Map<usize, usize>,
}

impl SeqOffset {
    pub fn from(tries: usize) -> Self {
        assert!(tries >= 1);
        Self {
            tries: tries,
            offset: AtomicUsize::new(0),
            len: AtomicIsize::new(0),
            slow_cache: Map::default(),
        }
    }
    // 插入一个span, [start, end)。
    // 如果start == offset，则直接更新offset,
    // 否则将span临时存储下来。
    // TODO 临时存储空间可能会触发OOM
    // end > start
    pub fn insert(&self, start: usize, end: usize) {
        debug_assert!(end > start);
        for _i in 0..self.tries {
            //loop {
            match self
                .offset
                .compare_exchange(start, end, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    return;
                }
                Err(_offset) => {}
            }
        }
        self.len.fetch_add(1, Ordering::Relaxed);
        self.slow_cache.insert(start, end);
    }

    // load offset, [0.. offset)都已经调用insert被相应的span全部填充
    pub fn load(&self) -> usize {
        let mut offset = self.offset.load(Ordering::Acquire);
        let old = offset;
        while let Some(removed) = self.slow_cache.remove(&offset) {
            let len = self.len.fetch_add(-1, Ordering::Relaxed);
            offset = *removed.val();
            log::debug!("offset: read offset loaded by map:{} len:{}", offset, len);
        }
        if offset != old {
            self.offset.store(offset, Ordering::Release);
        }
        offset
    }
}

#[cfg(test)]
mod offset_tests {
    use super::SeqOffset;
    #[test]
    fn test_seq_offset() {
        let offset = SeqOffset::from(8);
        assert_eq!(0, offset.load());
        offset.insert(0, 8);
        assert_eq!(8, offset.load());

        offset.insert(9, 10);
        assert_eq!(8, offset.load());

        offset.insert(20, 40);
        assert_eq!(8, offset.load());
        offset.insert(8, 9);
        assert_eq!(10, offset.load());
        offset.insert(10, 15);
        assert_eq!(15, offset.load());
        offset.insert(15, 20);
        assert_eq!(40, offset.load());
    }
}
