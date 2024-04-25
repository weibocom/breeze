use std::fmt::{self, Display, Formatter};

/// 写策略：对同一个size，总是从固定的队列位置开始访问，但同一个size的队列在初始化时需要进行随机初始化；

#[derive(Debug, Clone, Default)]
pub struct Fixed {
    que_len: usize,
    // 存储的内容：(que_size，起始位置)；按照que size排序，方便查找
    qsize_pos: Vec<(usize, usize)>,
}

impl crate::msgque::WriteStrategy for Fixed {
    #[inline]
    fn new(que_len: usize, qsize_pos: &Vec<(usize, usize)>) -> Self {
        Self {
            que_len: que_len,
            qsize_pos: qsize_pos.clone(),
        }
    }
    #[inline]
    fn get_write_idx(&self, msg_len: usize, last_idx: Option<usize>) -> usize {
        match last_idx {
            None => {
                // 使用loop原因：短消息是大概率;size小于8时，list loop 性能比hash类算法性能更佳 fishermen
                for (qsize, idx) in self.qsize_pos.iter() {
                    if *qsize > msg_len {
                        log::debug!("+++ msg len:{}, qsize:{}, idx:{}", msg_len, *qsize, *idx);
                        return *idx;
                    }
                }
                // 如果所有size均小于消息长度，则返回最大size的queue
                log::warn!("+++ msg too big {}", msg_len);
                self.qsize_pos.last().expect("queue").1
            }
            Some(last_idx) => (last_idx + 1) % self.que_len,
        }
    }
}

impl Display for Fixed {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "mq fixed: {}/{:?}", self.que_len, self.qsize_pos)
    }
}
