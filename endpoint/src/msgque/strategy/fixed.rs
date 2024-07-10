use std::fmt::{self, Display, Formatter};

use crate::msgque::SizedQueueInfo;

/// 写策略：对同一个size，总是从固定的队列位置开始访问，但同一个size的队列在初始化时需要进行随机初始化；

#[derive(Debug, Clone, Default)]
pub struct Fixed {
    que_len: usize,
    // 存储的内容：(que_size，起始位置)；按照que size排序，方便查找
    sized_que_infos: Vec<SizedQueueInfo>,
}

impl crate::msgque::WriteStrategy for Fixed {
    #[inline]
    fn new(que_len: usize, sized_que_infos: Vec<SizedQueueInfo>) -> Self {
        Self {
            que_len,
            sized_que_infos,
        }
    }

    /**
     * 第一次总是轮询位置，确保均衡写；
     * 失败后，后续的重复请求，则按照上次的位置继续向后访问，当轮询完本size的queue列表后，进入到下一个size的queue；
     * 返回：(que_idx, try_next)，que_idx:当前队列的位置; try_next: 如果失败是否需要继续轮询；
     */
    #[inline]
    fn get_write_idx(
        &self,
        msg_len: usize,
        last_idx: Option<usize>,
        tried_count: usize,
    ) -> (usize, bool) {
        match last_idx {
            None => {
                let que_info = self.get_que_info(msg_len);
                // 第一次写队列消息，永远使用对应msg size的que list中的队列，且循环使用
                let idx = que_info.next_idx();
                let try_next = self.can_try_next(tried_count, &que_info);
                log::debug!("+++ mcqw/{}, {}/{}/{:?}", msg_len, try_next, idx, que_info);
                (idx, try_next)
            }
            Some(last_idx) => {
                // 重试写队列消息时，首先轮询当前size的queue列表；在当前size的queue已经轮询完后，则进入后续更大size的queue；
                let que_info = self.get_que_info(msg_len);
                let try_next = self.can_try_next(tried_count, &que_info);
                if tried_count < que_info.len() {
                    // 首先重试当前len的所有queues
                    let idx = que_info.next_retry_idx(last_idx);
                    log::debug!("+++ mcqw retry wdix {}:{}/{}", msg_len, idx, last_idx);
                    (idx, try_next)
                } else {
                    let idx = last_idx + 1;
                    (idx.wrapping_rem(self.que_len), try_next)
                }
            }
        }
    }
}

impl Fixed {
    #[inline]
    fn get_que_info(&self, msg_len: usize) -> &SizedQueueInfo {
        // 使用loop原因：短消息是大概率;size小于8时，list loop 性能比hash类算法性能更佳 fishermen
        for qi in self.sized_que_infos.iter() {
            if qi.qsize > msg_len {
                return qi;
            }
        }
        self.sized_que_infos.last().expect("que info")
    }

    /**
     * 判断如果失败，是否可以继续尝试下一个queue。已经尝试的次数只要小于que_len - start_pos - 1，则可以继续尝试下一个queue
     */
    #[inline]
    fn can_try_next(&self, tried_count: usize, sized_que_info: &SizedQueueInfo) -> bool {
        (self.que_len - sized_que_info.start_pos() - 1) > tried_count
    }
}

impl Display for Fixed {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "mq fixed: {}/{:?}", self.que_len, self.sized_que_infos)
    }
}
