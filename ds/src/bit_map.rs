use std::sync::atomic::{AtomicUsize, Ordering::*};
pub struct BitMap {
    blocks: Vec<AtomicUsize>,
}

const BLK_SIZE: usize = std::mem::size_of::<usize>() * 8;
const BLK_MASK: usize = BLK_SIZE - 1;

impl BitMap {
    pub fn with_capacity(cap: usize) -> Self {
        let blocks = (cap + BLK_MASK) / BLK_SIZE;
        BitMap {
            blocks: (0..blocks).map(|_| AtomicUsize::new(0)).collect(),
        }
    }

    #[inline(always)]
    fn location(&self, pos: usize) -> (usize, usize) {
        let idx = pos / BLK_SIZE;
        let offset = pos - idx * BLK_SIZE;
        (idx, offset)
    }

    #[inline]
    pub fn mark(&self, pos: usize) {
        let (idx, offset) = self.location(pos);
        unsafe {
            self.blocks
                .get_unchecked(idx)
                .fetch_or(1 << offset, Relaxed);
        }
    }
    #[inline]
    pub fn unmark(&self, pos: usize) {
        let (idx, offset) = self.location(pos);
        // mark不需要获取返回值，所以不同的mark之间访问时使用relaxed即可。
        unsafe {
            self.blocks
                .get_unchecked(idx)
                .fetch_and(!(1 << offset), Relaxed);
        }
    }

    #[inline]
    pub fn marked(&self, pos: usize) -> bool {
        let (idx, offset) = self.location(pos);
        unsafe {
            let old = self.blocks.get_unchecked(idx).load(Relaxed);
            old & 1 << offset == 1 << offset
        }
    }

    #[inline]
    pub fn take(&self) -> Vec<usize> {
        //std::sync::atomic::fence(Ordering::Release);
        let mut postions = Vec::with_capacity(64);
        for i in 0..self.blocks.len() {
            let mut one = unsafe { self.blocks.get_unchecked(i).load(Relaxed) };
            let old = one;
            while one > 0 {
                let zeros = one.trailing_zeros() as usize;
                let cid = i * BLK_SIZE + zeros;
                postions.push(cid);
                one = one & !(1 << zeros);
            }
            // unmark
            self.unmarked_one_block(i, old);
        }
        postions
    }

    #[inline(always)]
    fn unmarked_one_block(&self, i: usize, old: usize) {
        unsafe { self.blocks.get_unchecked(i).fetch_and(!old, Relaxed) };
    }
}
