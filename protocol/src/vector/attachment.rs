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
            year: y.checked_rem(100).expect("Year overflow") as u8,
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
    pub fn attach_si(&mut self, response: &Command) -> bool {
        let rows = response.header.rows;
        let cols = response.header.columns;
        debug_assert_eq!(cols, 3);
        let mut si = Vec::<SiItem>::with_capacity(rows as usize);

        let data = Packet::from(***response);
        let mut oft: usize = 0;
        while oft < data.len() {
            if data.num(&mut oft).is_err() {
                return false;
            }
            let d = data.bulk_string(&mut oft);
            if d.is_err() {
                return false;
            }
            if let Ok(count) = data.num(&mut oft) {
                let date = VDate::from(&d.unwrap());
                let si_item = SiItem::new(date.year(), date.month(), count as u16);
                si.push(si_item);
            }
        }
        // 将si转换为Vec<u8>
        self.si = serialize_sitems(&si);
        true
    }
    #[inline]
    pub fn has_si(&mut self) -> bool {
        self.si.len() > 0
    }
    #[inline]
    pub fn si(&self) -> Vec<SiItem> {
        deserialize_sitems(&self.si)
    }
    #[inline]
    pub fn finish(&self) -> bool {
        self.finish
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

#[cfg(test)]
mod tests {
    use ds::MemGuard;

    use crate::ResponseHeader;

    use super::*;
    #[test]
    fn test_attach_si() {
        let header: ResponseHeader = ResponseHeader::new(
            "*3\r\n$3\r\nuid\r\n$10\r\nstart_date\r\n$5\r\ncount\r\n".into(),
            246,
            3,
        );
        let body: MemGuard = MemGuard::from_vec(":6351590999\r\n$10\r\n2024-06-01\r\n:674\r\n:6351590999\r\n$10\r\n2024-05-01\r\n:1113\r\n:6351590999\r\n$10\r\n2024-04-01\r\n:833\r\n:6351590999\r\n$10\r\n2024-03-01\r\n:45\r\n:6351590999\r\n$10\r\n2024-02-01\r\n:61\r\n:6351590999\r\n$10\r\n2024-01-01\r\n:59\r\n:6351590999\r\n$10\r\n2023-12-01\r\n:20\r\n:6351590999\r\n$10\r\n2023-11-01\r\n:9\r\n:6351590999\r\n$10\r\n2023-10-01\r\n:13\r\n:6351590999\r\n$10\r\n2023-09-01\r\n:50\r\n:6351590999\r\n$10\r\n2023-08-01\r\n:16\r\n:6351590999\r\n$10\r\n2023-07-01\r\n:61\r\n:6351590999\r\n$10\r\n2023-06-01\r\n:30\r\n:6351590999\r\n$10\r\n2023-05-01\r\n:41\r\n:6351590999\r\n$10\r\n2023-04-01\r\n:54\r\n:6351590999\r\n$10\r\n2023-03-01\r\n:108\r\n:6351590999\r\n$10\r\n2023-02-01\r\n:213\r\n:6351590999\r\n$10\r\n2023-01-01\r\n:159\r\n:6351590999\r\n$10\r\n2022-12-01\r\n:26\r\n:6351590999\r\n$10\r\n2022-11-01\r\n:16\r\n:6351590999\r\n$10\r\n2022-10-01\r\n:14\r\n:6351590999\r\n$10\r\n2022-09-01\r\n:3\r\n:6351590999\r\n$10\r\n2022-08-01\r\n:10\r\n:6351590999\r\n$10\r\n2022-07-01\r\n:9\r\n:6351590999\r\n$10\r\n2022-06-01\r\n:4\r\n:6351590999\r\n$10\r\n2022-05-01\r\n:23\r\n:6351590999\r\n$10\r\n2022-04-01\r\n:4\r\n:6351590999\r\n$10\r\n2022-03-01\r\n:4\r\n:6351590999\r\n$10\r\n2022-02-01\r\n:4\r\n:6351590999\r\n$10\r\n2022-01-01\r\n:5\r\n:6351590999\r\n$10\r\n2021-12-01\r\n:14\r\n:6351590999\r\n$10\r\n2021-11-01\r\n:4\r\n:6351590999\r\n$10\r\n2021-10-01\r\n:2\r\n:6351590999\r\n$10\r\n2021-09-01\r\n:3\r\n:6351590999\r\n$10\r\n2021-08-01\r\n:25\r\n:6351590999\r\n$10\r\n2021-07-01\r\n:36\r\n:6351590999\r\n$10\r\n2021-06-01\r\n:30\r\n:6351590999\r\n$10\r\n2021-05-01\r\n:18\r\n:6351590999\r\n$10\r\n2021-04-01\r\n:20\r\n:6351590999\r\n$10\r\n2021-03-01\r\n:21\r\n:6351590999\r\n$10\r\n2021-02-01\r\n:35\r\n:6351590999\r\n$10\r\n2021-01-01\r\n:22\r\n:6351590999\r\n$10\r\n2020-12-01\r\n:55\r\n:6351590999\r\n$10\r\n2020-11-01\r\n:22\r\n:6351590999\r\n$10\r\n2020-10-01\r\n:37\r\n:6351590999\r\n$10\r\n2020-09-01\r\n:33\r\n:6351590999\r\n$10\r\n2020-08-01\r\n:15\r\n:6351590999\r\n$10\r\n2020-07-01\r\n:12\r\n:6351590999\r\n$10\r\n2020-06-01\r\n:26\r\n:6351590999\r\n$10\r\n2020-05-01\r\n:54\r\n:6351590999\r\n$10\r\n2020-04-01\r\n:38\r\n:6351590999\r\n$10\r\n2020-03-01\r\n:27\r\n:6351590999\r\n$10\r\n2020-02-01\r\n:80\r\n:6351590999\r\n$10\r\n2020-01-01\r\n:99\r\n:6351590999\r\n$10\r\n2019-12-01\r\n:67\r\n:6351590999\r\n$10\r\n2019-11-01\r\n:120\r\n:6351590999\r\n$10\r\n2019-10-01\r\n:80\r\n:6351590999\r\n$10\r\n2019-09-01\r\n:76\r\n:6351590999\r\n$10\r\n2019-08-01\r\n:120\r\n:6351590999\r\n$10\r\n2019-07-01\r\n:140\r\n:6351590999\r\n$10\r\n2019-06-01\r\n:118\r\n:6351590999\r\n$10\r\n2019-05-01\r\n:146\r\n:6351590999\r\n$10\r\n2019-04-01\r\n:287\r\n:6351590999\r\n$10\r\n2019-03-01\r\n:83\r\n:6351590999\r\n$10\r\n2019-02-01\r\n:88\r\n:6351590999\r\n$10\r\n2019-01-01\r\n:262\r\n:6351590999\r\n$10\r\n2018-12-01\r\n:213\r\n:6351590999\r\n$10\r\n2018-11-01\r\n:251\r\n:6351590999\r\n$10\r\n2018-10-01\r\n:215\r\n:6351590999\r\n$10\r\n2018-09-01\r\n:192\r\n:6351590999\r\n$10\r\n2018-08-01\r\n:208\r\n:6351590999\r\n$10\r\n2018-07-01\r\n:339\r\n:6351590999\r\n$10\r\n2018-06-01\r\n:97\r\n:6351590999\r\n$10\r\n2018-05-01\r\n:162\r\n:6351590999\r\n$10\r\n2018-04-01\r\n:127\r\n:6351590999\r\n$10\r\n2018-03-01\r\n:147\r\n:6351590999\r\n$10\r\n2018-02-01\r\n:529\r\n:6351590999\r\n$10\r\n2018-01-01\r\n:702\r\n:6351590999\r\n$10\r\n2017-12-01\r\n:453\r\n:6351590999\r\n$10\r\n2017-11-01\r\n:70\r\n:6351590999\r\n$10\r\n2017-10-01\r\n:1\r\n:6351590999\r\n$10\r\n2017-08-01\r\n:1\r\n".into());
        let response: Command = Command::with_assemble_pack(true, header, body);
        let mut att = Attachment::new(1);
        let r = att.attach_si(&response);
        assert!(r);
    }
}
