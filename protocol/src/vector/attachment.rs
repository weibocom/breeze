use bincode::{config, Decode, Encode};

#[derive(Debug, Clone, Encode, Decode)]
pub struct Attachment {
    // 待查询数量，不能超过u16::MAX
    pub left_count: u16,
    // 当前查询的时间游标，一般是年月，like：202405/2405
    //本轮响应是否成功
    pub rsp_ok: bool,
    // header，*2 + column names
    header: Vec<u8>,
    body: Vec<Vec<u8>>,
    // 查询响应的body中token数量
    body_token_count: u16,

    // si表信息
    pub si_count: u16,  // si表中已查询到的数量，等于left_count时，si查询结束
    pub current_si: u8, // 当前使用的si信息索引
    si_body: Vec<u8>,   // si表中查询到的数据, si字段信息在配置里存放
}
#[repr(C)]
pub struct SiItem {
    // 年月 数量
    pub yy: u8,
    pub mm: u8,
    pub count: u16,
}
impl Attachment {
    #[inline]
    pub fn new(count: u16) -> Self {
        Attachment {
            left_count: count,
            header: Vec::with_capacity(8),
            body: Vec::with_capacity(count as usize),
            body_token_count: 0,
            rsp_ok: false,
            si_count: 0,
            current_si: 0,
            si_body: Vec::with_capacity(24),
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
    // si表查询到的数据是否足够
    #[inline]
    pub fn si_enough(&self) -> bool {
        self.left_count <= self.si_count
    }
    // 主动设置si表查询到的数据已足够，如到访问si表达最大次数
    #[inline]
    pub fn set_si_enough(&mut self) {
        self.si_count = self.left_count;
    }
    // 从response中解析si，并按yy mm聚合
    #[inline]
    pub fn decode_si(response: &crate::Command) -> Vec<SiItem> {
        let si = Vec::<SiItem>::with_capacity(response.count() as usize);
        // todo!();
        si
    }
    #[inline]
    pub fn attach_si(&mut self, response: &crate::Command) {
        let sis = Self::decode_si(response);
        sis.iter().for_each(|si| {
            self.si_body.push(si.yy);
            self.si_body.push(si.mm);
            self.si_count += si.count;
            self.si_body.extend_from_slice(&si.count.to_be_bytes())
        });
    }
    #[inline]
    pub fn read_si_item(&mut self) -> (u8, u8, u16) {
        let p = self.si_body.as_ptr();
        let offset = std::mem::size_of::<SiItem>() * self.current_si as usize;
        self.current_si += 1;
        let yy = unsafe { *p.add(offset) };
        let mm = unsafe { *p.add(offset + 1) };
        let count =
            u16::from_be_bytes([unsafe { *p.add(offset + 2) }, unsafe { *p.add(offset + 3) }]);
        (yy, mm, count)
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
