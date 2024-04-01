use std::{
    fmt::{Display, Formatter},
    sync::{atomic::AtomicUsize, Arc},
};

use crate::msgque::ReadStrategy;
use std::sync::atomic::Ordering::Relaxed;

/// 依次轮询队列列表，注意整个列表在初始化时需要进行随机乱序处理
#[derive(Debug, Clone, Default)]
pub struct RoundRobbin {
    que_len: usize,
    current_pos: Arc<AtomicUsize>,
}

impl ReadStrategy for RoundRobbin {
    /// 初始化一个轮询读策略，起始位置进行一个随机化处理
    #[inline]
    fn new(reader_len: usize) -> Self {
        let rand: usize = rand::random();
        Self {
            que_len: reader_len,
            current_pos: Arc::new(AtomicUsize::new(rand)),
        }
    }
    /// 实现策略很简单：持续轮询
    #[inline]
    fn get_read_idx(&self) -> usize {
        let pos = self.current_pos.fetch_add(1, Relaxed);
        pos.wrapping_rem(self.que_len)
    }
}

impl Display for RoundRobbin {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "mq round robbin read:{}/{}",
            self.que_len,
            self.current_pos.load(Relaxed)
        )
    }
}
