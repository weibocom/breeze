use crate::Attachment;
use crate::Command;
use crate::Packet;
use ds::RingSlice;
#[derive(Debug, Default)]
#[repr(C)]
pub struct VecAttach {
    pub finish: bool,
    pub rsp_ok: bool,
    // 查询的轮次，0代表si
    pub round: u16,
    // 待查询数量，不能超过u16::MAX
    pub left_count: u16,
    body_token_count: u16,
    //本轮响应是否成功
    // header，*2 + column names
    header: Vec<u8>,
    body: Vec<Vec<u8>>,
    // 查询响应的body中token数量
    si: Vec<SiItem>, // si表中查询到的数据, si字段信息在配置里存放
}

#[derive(Debug, Default)]
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
#[derive(Debug, Default)]
pub struct VDate {
    pub year: u8,  // year
    pub month: u8, // month
}
impl VDate {
    // 2411 -> {24, 11}
    pub fn from(d: &RingSlice) -> Self {
        let s = d.as_string_lossy();
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() < 2 {
            return Self { year: 0, month: 0 };
        }
        let y: u32 = parts[0].parse().unwrap_or_default();
        let m: u8 = parts[1].parse().unwrap_or_default();
        Self {
            year: y.checked_rem(100).unwrap_or_default() as u8,
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
    #[inline]
    pub fn is_valid(&self) -> bool {
        self.month > 0 && self.year > 0
    }
}

pub trait VAttach {
    fn attach(&self) -> &VecAttach;
    fn attach_mut(&mut self) -> &mut VecAttach;
}

impl<T: crate::Request> VAttach for T {
    #[inline(always)]
    fn attach(&self) -> &VecAttach {
        unsafe { std::mem::transmute(self.attachment().expect("attach is none")) }
    }
    #[inline(always)]
    fn attach_mut(&mut self) -> &mut VecAttach {
        unsafe { std::mem::transmute(self.attachment_mut().as_mut().expect("attach is none")) }
    }
}

impl VecAttach {
    #[inline(always)]
    pub fn from(att: Attachment) -> VecAttach {
        unsafe { std::mem::transmute(att) }
    }
    #[inline(always)]
    pub fn attach(att: &Attachment) -> &VecAttach {
        unsafe { std::mem::transmute(att) }
    }
    #[inline(always)]
    pub fn attach_mut(att: &mut Attachment) -> &mut VecAttach {
        unsafe { std::mem::transmute(att) }
    }
    #[inline(always)]
    pub fn to_attach(self) -> Attachment {
        unsafe { std::mem::transmute(self) }
    }
    #[inline]
    pub fn init(&mut self, left_count: u16) {
        *self = VecAttach {
            finish: false,
            round: 0,
            left_count,
            header: Vec::with_capacity(8),
            body: Vec::with_capacity(left_count.into()),
            body_token_count: 0,
            rsp_ok: false,
            si: Vec::with_capacity(6),
        };
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
        self.si.reserve(rows as usize);
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
                if count > 0 {
                    let date = VDate::from(&d.unwrap());
                    if date.is_valid() {
                        let si_item = SiItem::new(date.year(), date.month(), count as u16);
                        self.si.push(si_item);
                    }
                }
            }
        }
        true
    }
    #[inline]
    pub fn has_si(&mut self) -> bool {
        self.si.len() > 0
    }
    #[inline]
    pub fn si(&self) -> &Vec<SiItem> {
        &self.si
    }
    #[inline]
    pub fn finish(&self) -> bool {
        self.finish
    }
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
        let mut att = VecAttach::default();
        att.init(1);
        let r = att.attach_si(&response);
        assert!(r);
    }
}
