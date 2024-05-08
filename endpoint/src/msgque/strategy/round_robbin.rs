use std::fmt::{Display, Formatter};

use crate::{msgque::ReadStrategy, CloneableAtomicUsize};
use std::sync::atomic::Ordering::Relaxed;

// 最大连续命中次数，超过该次数则重置命中次数
const MAX_CONTINUE_HITS: usize = 100;
/// 依次轮询队列列表，注意整个列表在初始化时需要进行随机乱序处理
#[derive(Debug, Clone, Default)]
pub struct RoundRobbin {
    que_len: usize,
    current_pos: CloneableAtomicUsize,
    continue_hits: CloneableAtomicUsize,
}

impl ReadStrategy for RoundRobbin {
    /// 初始化一个轮询读策略，起始位置进行一个随机化处理
    #[inline]
    fn new(reader_len: usize) -> Self {
        let rand: usize = rand::random();
        Self {
            que_len: reader_len,
            // current_pos: Arc::new(AtomicUsize::new(rand)),
            current_pos: CloneableAtomicUsize::new(rand),
            continue_hits: CloneableAtomicUsize::new(0),
        }
    }
    /// 实现策略很简单：持续轮询
    #[inline]
    fn get_read_idx(&self, last_idx: Option<usize>) -> usize {
        let pos = match last_idx {
            None => {
                // 连续hits没有超过阀值，不进行轮询，只增加hits计数，否则重置hits，并将下次请求轮询到下个位置
                let hits = self.continue_hits.load(Relaxed);
                if hits < MAX_CONTINUE_HITS {
                    self.continue_hits.fetch_add(1, Relaxed);
                    self.current_pos.load(Relaxed)
                } else {
                    self.continue_hits.store(0, Relaxed);
                    self.current_pos.fetch_add(1, Relaxed)
                }
            }
            Some(lidx) => {
                // 将pos向后移动一个位置，如果已经被移动了，则不再移动
                let cur_idx = self.current_pos.load(Relaxed).wrapping_rem(self.que_len);
                if cur_idx == lidx {
                    self.current_pos.fetch_add(1, Relaxed);
                }
                // 无论如何，重试的时候都要尝试下一个idx
                lidx + 1
            }
        };
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
