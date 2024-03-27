use std::sync::{atomic::AtomicU32, Arc};

/// 依次轮询队列列表，注意列表在初始化时或clone时需要进行随机重构
use crate::msgque::ReadStrategy;
use std::sync::atomic::Ordering::AcqRel;

use super::QID;

#[derive(Debug, Clone, Default)]
pub(crate) struct RoundRobbin {
    que_len: u16,
    current_pos: Arc<AtomicU32>,
}

impl ReadStrategy for RoundRobbin {
    #[inline]
    fn new(reader_len: usize) -> Self {
        Self {
            que_len: reader_len as u16,
            current_pos: Arc::new(AtomicU32::new(0)),
        }
    }
    /// 实现策略很简单：持续轮询
    #[inline]
    fn get_read_idx(&self) -> QID {
        let pos = self.current_pos.fetch_add(1, AcqRel);
        pos.wrapping_rem(self.que_len as u32) as u16
    }
}
