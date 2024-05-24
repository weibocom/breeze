use bincode::{config, Decode, Encode};

#[derive(Debug, Clone, Encode, Decode)]
pub struct Attachment {
    // 待查询数量，不能超过u16::MAX
    pub left_count: u16,
    // 当前查询的时间游标，一般是年月，like：202405/2405
    //本轮是否有响应
    pub has_rsp: bool,
    // header，*2 + column names
    header: Vec<u8>,
    body: Vec<Vec<u8>>,
    // 查询响应的body中token数量
    body_token_count: u16,
}

impl Attachment {
    #[inline]
    pub fn new(count: u16) -> Self {
        Attachment {
            left_count: count,
            header: Vec::with_capacity(8),
            body: Vec::with_capacity(count as usize),
            body_token_count: 0,
            has_rsp: false,
        }
    }

    #[inline]
    pub fn to_vec(&self) -> Vec<u8> {
        let config = config::standard();
        bincode::encode_to_vec(self, config).expect(format!("attach:{:?}", self).as_str())
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.body.is_empty()
    }

    pub fn attach_header(&mut self, header: Vec<u8>) {
        self.header = header;
    }
    #[inline]
    pub fn attach_body(&mut self, body_data: Vec<u8>, rows: u16, columns: u16) {
        self.body.push(body_data);
        self.body_token_count += rows * columns;
        self.left_count = self.left_count.wrapping_sub(rows);
    }

    #[inline]
    pub fn header(&self) -> &Vec<u8> {
        &self.header
    }

    #[inline]
    pub fn body(&self) -> &Vec<Vec<u8>> {
        &self.body
    }

    #[inline]
    pub fn body_token_count(&self) -> u16 {
        self.body_token_count
    }
}

impl From<&[u8]> for Attachment {
    fn from(bytes: &[u8]) -> Self {
        let config = config::standard();
        let (data, len) = bincode::decode_from_slice(bytes, config).expect("attachment");
        assert_eq!(len, bytes.len());
        data
    }
}
