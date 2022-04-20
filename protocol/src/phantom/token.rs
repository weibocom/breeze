use ds::RingSlice;

// redis分隔符是\r\n
pub(crate) const REDIS_SPLIT_LEN: usize = 2;

#[derive(Debug, Clone)]
pub struct Token {
    // meta postion in multibulk，对于slice里说，pos=meta_pos+meta_len
    pub meta_pos: usize,
    // meta len，长度的长度，包括\r\n
    pub meta_len: usize,
    // token data position，有效信息位置
    pub pos: usize,
    // token data 的原始len，有效信息的长度，不包括\r\n
    pub bare_len: usize,
}

impl Token {
    pub fn from(meta_pos: usize, meta_len: usize, pos: usize, bare_len: usize) -> Self {
        Self {
            meta_pos,
            meta_len,
            pos,
            bare_len,
        }
    }

    // 包括meta + raw_data,like：$2\r\n12\r\n
    pub fn bulk_data(&self, origin: &RingSlice) -> RingSlice {
        origin.sub_slice(
            self.meta_pos,
            self.meta_len + self.bare_len + REDIS_SPLIT_LEN,
        )
    }

    // 裸数据，即真正的data，不包括meta，不包含\r\n
    pub fn bare_data(&self, origin: &RingSlice) -> RingSlice {
        origin.sub_slice(self.pos, self.bare_len)
    }

    pub fn end_pos(&self) -> usize {
        // \r\n 没有包含在raw_len中
        self.pos + self.bare_len + REDIS_SPLIT_LEN
    }
}
