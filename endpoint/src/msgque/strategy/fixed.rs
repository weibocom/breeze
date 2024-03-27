/// 对同一个size，总是从固定的队列ip位置开始访问，队列在初始化时需要进行随机初始化
use super::QID;

#[derive(Debug, Clone, Default)]
pub(crate) struct Fixed {
    que_len: u16,
    // 存储的内容：(que_size，起始位置)；按照que size排序，方便查找
    writer_pos: Vec<(usize, QID)>,
}

impl crate::msgque::WriteStrategy for Fixed {
    #[inline]
    fn new(que_len: usize, qsize_pos: &Vec<(usize, usize)>) -> Self {
        let writer_pos = qsize_pos
            .iter()
            .map(|(qsize, pos)| (*qsize, (*pos) as QID))
            .collect();
        Self {
            que_len: que_len as u16,
            writer_pos,
        }
    }
    #[inline]
    fn get_write_idx(&self, msg_size: usize, last_idx: Option<QID>) -> QID {
        match last_idx {
            None => {
                for (qsize, idx) in self.writer_pos.iter() {
                    if *qsize > msg_size {
                        return *idx;
                    }
                }
                // 如果所有size均小于消息长度，则返回最大size的queue
                log::warn!("+++ msg too big {}", msg_size);
                self.writer_pos.last().expect("queue").1
            }
            Some(last_idx) => (last_idx + 1) % self.que_len,
        }
    }
}
