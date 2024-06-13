use bincode::{config, Decode, Encode};

use crate::Command;
use crate::Packet;

#[derive(Debug, Clone, Encode, Decode)]
pub struct Attachment {
    // 查询是否结束
    pub finish: bool,
    // 当前查询的时间游标，一般是年月，like：202405/2405
    //本轮响应是否成功
    pub rsp_ok: bool,
    // header，*2 + column names
    header: Vec<u8>,
    body: Vec<Vec<u8>>,
    // 查询响应的body中token数量
    body_token_count: u16,

    si: Vec<u8>, // si表中查询到的数据, si字段信息在配置里存放
}
#[repr(C)]
pub struct SiItem {
    pub date: VDate,
    pub count: u16,
}
#[repr(C)]
pub struct VDate {
    pub year: u8,  // year
    pub month: u8, // month
}
impl SiItem {
    pub fn new(yy: u8, mm: u8, count: u16) -> Self {
        Self {
            date: VDate {
                year: yy,
                month: mm,
            },
            count,
        }
    }
}

impl Attachment {
    #[inline]
    pub fn new() -> Self {
        Attachment {
            finish: false,
            header: Vec::with_capacity(8),
            body: Vec::with_capacity(1024),
            body_token_count: 0,
            rsp_ok: false,
            si: Vec::with_capacity(6),
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
    // 从response中解析si，并按yy mm聚合
    // 约定：si返回结果的结构： uid、date、count顺序排列
    #[inline]
    pub fn attach_si(&mut self, response: &Command) {
        let rows = response.header.rows;
        let cols = response.header.columns;
        debug_assert_eq!(cols, 3);
        let mut si = Vec::<SiItem>::with_capacity(rows as usize);

        let data = Packet::from(***response);
        let mut oft: usize = 0;
        while oft < data.len() {
            let _uid = data.num(&mut oft).unwrap();
            let date = data.num(&mut oft).unwrap();
            let count = data.num(&mut oft).unwrap();
            let si_item = SiItem::new(date as u8, date as u8, count as u16);
            si.push(si_item);
        }

        // self.si.extend(vec_si_items_to_bytes(si));
    }
    #[inline]
    pub fn has_si(&mut self) -> bool {
        self.si.len() > 0
    }
    #[inline]
    pub fn si(&self) -> &Vec<SiItem> {
        todo!()
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
