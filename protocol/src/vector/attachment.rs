use crate::Command;
use crate::Packet;
use bincode::{config, Decode, Encode};
use ds::RingSlice;
#[derive(Debug, Clone, Encode, Decode, Default)]
pub struct Attachment {
    pub finish: bool,
    // 查询的轮次，0代表si
    pub round: u16,
    // 待查询数量，不能超过u16::MAX
    pub left_count: u16,
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
#[derive(Encode, Decode)]
pub struct SiItem {
    pub date: VDate,
    pub count: u16,
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
#[repr(C)]
#[derive(Encode, Decode)]
pub struct VDate {
    pub year: u8,  // year
    pub month: u8, // month
}
impl VDate {
    // 2411 -> {24, 11}
    pub fn from(d: &RingSlice) -> Self {
        let mut s: Vec<u8> = Vec::with_capacity(d.len());
        d.copy_to_vec(&mut s);
        let s = String::from_utf8(s).expect("Invalid UTF-8 sequence");
        let parts: Vec<&str> = s.split('-').collect();
        assert!(parts.len() >= 2);
        let y: u32 = parts[0].parse().expect("Failed to parse year");
        let m: u8 = parts[1].parse().expect("Failed to parse month");
        Self {
            year: (y / 100) as u8,
            month: m,
        }
    }
    #[inline]
    pub fn year(&self) -> u8 {
        self.year
    }
    #[inline]
    pub fn month(&self) -> u8 {
        self.month
    }
}

impl Attachment {
    #[inline]
    pub fn new(left_count: u16) -> Self {
        Attachment {
            finish: false,
            round: 0,
            left_count,
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
    // 从response中解析si
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
            let d = data.bulk_string(&mut oft).unwrap();
            let date = VDate::from(&d);
            let count = data.num(&mut oft).unwrap();
            let si_item = SiItem::new(date.year(), date.month(), count as u16);
            si.push(si_item);
        }
        // 将si转换为Vec<u8>
        self.si = serialize_sitems(&si);
    }
    #[inline]
    pub fn has_si(&mut self) -> bool {
        self.si.len() > 0
    }
    #[inline]
    pub fn si(&self) -> Vec<SiItem> {
        deserialize_sitems(&self.si)
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

fn serialize_sitems(items: &Vec<SiItem>) -> Vec<u8> {
    let config = config::standard();
    bincode::encode_to_vec(items, config).unwrap()
}

fn deserialize_sitems(bytes: &[u8]) -> Vec<SiItem> {
    let config = config::standard();
    let (data, len) = bincode::decode_from_slice(bytes, config).expect("SiItem");
    assert_eq!(len, bytes.len());
    data
}
