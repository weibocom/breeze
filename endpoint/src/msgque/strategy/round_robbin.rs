use std::fmt::{Display, Formatter};

use crate::{msgque::ReadStrategy, CloneableAtomicUsize};
use std::sync::atomic::Ordering::Relaxed;

const HITS_BITS: u32 = 8;

/// 依次轮询队列列表，注意整个列表在初始化时需要进行随机乱序处理
#[derive(Debug, Clone, Default)]
pub struct RoundRobbin {
    que_len: usize,
    // 低8bits放连续hits次数，其他bits放索引位置
    current_pos: CloneableAtomicUsize,
}

impl ReadStrategy for RoundRobbin {
    /// 初始化一个轮询读策略，起始位置进行一个随机化处理
    #[inline]
    fn new(reader_len: usize) -> Self {
        let rand: usize = rand::random();
        Self {
            que_len: reader_len,
            current_pos: CloneableAtomicUsize::new(rand),
        }
    }
    /// 实现策略很简单：持续轮询
    #[inline]
    fn get_read_idx(&self, last_idx: Option<usize>) -> usize {
        let origin_pos = self.current_pos.fetch_add(1, Relaxed);
        let pos = match last_idx {
            None => origin_pos,
            Some(lidx) => {
                // 将pos向后移动一个位置，如果已经被移动了，则不再移动
                if lidx == origin_pos.que_idx(self.que_len) {
                    let new_pos = (lidx + 1).pos();
                    self.current_pos.store(new_pos, Relaxed);
                    new_pos
                } else {
                    origin_pos
                }
            }
        };

        pos.que_idx(self.que_len)
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

/// pos：低8位为单个idx的持续读取计数，高56位为队列的idx序号
trait Pos {
    fn que_idx(&self, que_len: usize) -> usize;
}

impl Pos for usize {
    fn que_idx(&self, que_len: usize) -> usize {
        self.wrapping_shr(HITS_BITS).wrapping_rem(que_len)
    }
}

/// idx是队列的idx序号，通过将idx左移8位来构建一个新的pos
trait Idx {
    fn pos(&self) -> usize;
}

impl Idx for usize {
    fn pos(&self) -> usize {
        self.wrapping_shl(HITS_BITS)
    }
}
