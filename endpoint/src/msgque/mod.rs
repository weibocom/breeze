mod config;
pub mod topo;

// 0-7: 读/写次数；
const COUNT_BITS: u8 = 8;
// 8-23：读写的位置索引
const IDX_SHIFT: u8 = 0 + COUNT_BITS;
const IDX_BITS: u8 = 16;
// 24-31: 写入队列的size的block数，即：size/512；
const WRITE_SIZE_BLOCK_SHIFT: u8 = IDX_SHIFT + IDX_BITS;
const WRITE_SIZE_BLOCK_BITS: u8 = 8;
// 32-63: 保留
const DATA_RESERVE_SHIFT: u8 = WRITE_SIZE_BLOCK_SHIFT + WRITE_SIZE_BLOCK_BITS;

// msgqueue的size必须是512的整数倍
const BLOCK_SIZE: usize = 512;

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
    fn check_inited(&self) -> bool {
        self.ctx > 0
    }

    // 获得已read/write的次数，并对次数加1
    #[inline]
    fn get_and_incr_count(&mut self) -> usize {
        let count = self.ctx as u8;

        const MAX_COUNT: u8 = ((1 << COUNT_BITS as u16) - 1) as u8;
        assert!(count < MAX_COUNT);

        self.ctx += 1;

        count as usize
    }

    // 获取idx，并将原有的idx+1
    #[inline]
    fn get_write_size(&mut self) -> usize {
        // block 占 8 字节
        let block = (self.ctx >> WRITE_SIZE_BLOCK_SHIFT) as u8;
        (block as usize) * BLOCK_SIZE
    }

    // read/write 的idx位置相同
    #[inline]
    fn get_idx(&self) -> usize {
        // idx 占16个字节
        let idx = (self.ctx >> IDX_SHIFT) as u16;
        idx as usize
    }

    #[inline]
    fn update_idx(&mut self, read_idx: u16) {
        self.ctx |= (read_idx << IDX_SHIFT) as u64;
    }

    #[inline]
    fn update_write_size(&mut self, wsize: usize) {
        let lower = self.ctx & (1 << WRITE_SIZE_BLOCK_SHIFT - 1);
        let high = self.ctx >> DATA_RESERVE_SHIFT << DATA_RESERVE_SHIFT;
        let block = wsize / BLOCK_SIZE;
        self.ctx = lower | (block << WRITE_SIZE_BLOCK_SHIFT) as u64 | high;
    }
}
