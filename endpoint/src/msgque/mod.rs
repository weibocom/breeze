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

    // // 获取idx，并将原有的idx+1
    // #[inline]
    // fn get_write_size(&mut self) -> usize {
    //     // block 占 8 字节
    //     let block = (self.ctx >> WRITE_SIZE_BLOCK_SHIFT) as u8;
    //     (block as usize) * BLOCK_SIZE
    // }

    // read/write 的idx位置相同
    #[inline]
    fn get_last_qid(&self, inited: bool) -> Option<usize> {
        if !inited {
            return None;
        }

        let idx = self.ctx >> QID_SHIFT;
        Some(idx as usize)
    }

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

    // #[inline]
    // fn update_write_size(&mut self, wsize: usize) {
    //     let lower = self.ctx & ((1 << WRITE_SIZE_BLOCK_SHIFT) - 1);
    //     let high = self.ctx >> DATA_RESERVE_SHIFT << DATA_RESERVE_SHIFT;
    //     let block = wsize / BLOCK_SIZE;
    //     self.ctx = lower | (block << WRITE_SIZE_BLOCK_SHIFT) as u64 | high;
    // }
}

pub trait WriteStrategy {
    fn new(queue_len: usize, qsize_pos: &Vec<(usize, usize)>) -> Self;
    fn get_write_idx(&self, msg_size: usize, last_idx: Option<usize>) -> usize;
}

pub trait ReadStrategy {
    fn new(reader_len: usize) -> Self;
    fn get_read_idx(&self) -> usize;
}
