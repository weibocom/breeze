use std::fmt::{self, Display, Formatter};

use crate::msgque::SizedQueueInfo;
use std::sync::atomic::Ordering::Relaxed;

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
     */
    #[inline]
    fn get_write_idx(&self, msg_len: usize, last_idx: Option<usize>, tried_count: usize) -> usize {
        match last_idx {
            None => {
                let que_info = self.get_que_info(msg_len);
                // 第一次写队列消息，永远使用对应msg size的que list中的队列，且循环使用
                let relative_idx = que_info.sequence.fetch_add(1, Relaxed) % que_info.len;
                log::debug!("+++ mcqw mlen/{}, {}/{:?}", msg_len, relative_idx, que_info);
                return que_info.start_pos + relative_idx;
            }
            Some(last_idx) => {
                // 重试写队列消息时，首先轮询当前size的queue列表；在当前size的queue已经轮询完后，则进入更大size的queue；
                let idx = last_idx + 1;
                let que_info = self.get_que_info(msg_len);
                if tried_count < que_info.len {
                    // 首先重试当前len的所有queues
                    assert!(idx > que_info.start_pos, "{}:{:?}", idx, que_info);
                    let relative_idx = (idx - que_info.start_pos) % que_info.len;
                    log::debug!("+++ idx:{}, qinfo:{:?}", idx, que_info);
                    que_info.start_pos + relative_idx
                } else {
                    idx.wrapping_rem(self.que_len)
                }
            }
        }
    }
}

impl Fixed {
    fn get_que_info(&self, msg_len: usize) -> &SizedQueueInfo {
        // 使用loop原因：短消息是大概率;size小于8时，list loop 性能比hash类算法性能更佳 fishermen
        for qi in self.sized_que_infos.iter() {
            if qi.qsize > msg_len {
                return qi;
            }
        }
        self.sized_que_infos.last().expect("que info")
    }
}

impl Display for Fixed {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "mq fixed: {}/{:?}", self.que_len, self.sized_que_infos)
    }
}
