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
    // 查询的子响应，第一个vec是header，后面是tokens
    response: Vec<Vec<u8>>,
    // 查询的子响应数量
    response_tokens: u16,
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
            response: Vec::with_capacity(count as usize),
            response_tokens: 0,
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
        self.response.is_empty()
    }

    pub fn attach_header(&mut self, header: Vec<u8>) {
        assert!(header.len() > 0, "header is emtpy: {:?}", header);
        assert_eq!(self.response.len(), 0, "attachment header is not empty");
        assert_eq!(self.response_tokens, 0, "attachment header is not empty");

        self.response.push(header);
    }
    #[inline]
    pub fn attach_resp_data(&mut self, resp: Vec<u8>, rows: u16, columns: u16) {
        if resp.len() > 0 {
            self.response.push(resp);
        }
        self.response_tokens += rows * columns;
        self.left_count.wrapping_sub(rows);
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
