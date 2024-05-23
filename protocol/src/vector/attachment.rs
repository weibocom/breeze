use bincode::{config, Decode, Encode};
use chrono::{Date, Datelike, Local};

#[derive(Debug, Clone, Encode, Decode)]
pub struct Attachment {
    // 查询的偏移位置
    offset: u16,
    // 待查询数量，不能超过u16::MAX
    pub(crate) left_count: u16,
    // 当前查询的时间游标，一般是年月，like：202405/2405
    cursor: YearMonth,
    // header，*2 + column names
    header: Vec<u8>,
    body: Vec<Vec<u8>>,
    // 查询响应的body中token数量
    body_token_count: u16,
}

/// 年月
#[derive(Debug, Clone, Encode, Decode)]
struct YearMonth {
    year: u16,
    month: u8,
}

impl From<Date<Local>> for YearMonth {
    fn from(date: Date<Local>) -> Self {
        let year = date.year();
        let month = date.month();
        assert!(year >= 2009 && year < 2100, "kvector date:{}", date);

        YearMonth {
            year: year as u16,
            month: month as u8,
        }
    }
}

impl Attachment {
    #[inline]
    pub fn new(offset: u16, count: u16) -> Self {
        Attachment {
            offset,
            left_count: count,
            cursor: Local::today().into(),
            header: Vec::with_capacity(8),
            body: Vec::with_capacity(count as usize),
            body_token_count: 0,
        }
    }

    /// 回滚一个月，回滚方式：减掉当前月的天数，得到上个月的最后一天。
    #[inline]
    pub(crate) fn roll_back_one_month(&mut self) {
        if self.cursor.month > 1 {
            self.cursor.month -= 1;
        } else {
            self.cursor.month = 12;
            self.cursor.year -= 1;
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
        self.left_count.wrapping_sub(rows);
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
