use super::FromValueError;
use crate::kv::common::value::Value;
use std::time::Duration;
pub(super) struct MyDuration {
    i: usize,
    bytes: Vec<u8>,
}

impl MyDuration {
    fn next(&mut self, b: u8, max: usize) -> Result<usize, FromValueError> {
        if self.i >= self.bytes.len() {
            return Err(FromValueError(Value::Bytes(self.bytes.clone())));
        }
        let mut v = 0usize;
        while self.i < self.bytes.len() {
            let c = self.bytes[self.i];
            if c < b'0' || c > b'9' {
                return Err(FromValueError(Value::Bytes(self.bytes.clone())));
            }
            v = v * 10 + (c - b'0') as usize;
            self.i += 1;
            if self.bytes[self.i] == b {
                break;
            }
        }
        if v >= max {
            return Err(FromValueError(Value::Bytes(self.bytes.clone())));
        }
        Ok(v)
    }
    // value的格式为：HHH:MM:SS[.fraction]
    // r"^\d{2}:[0-5]\d:[0-5]\d$"
    //r"^[0-8]\d\d:[0-5]\d:[0-5]\d$"
    //r"^\d{2}:[0-5]\d:[0-5]\d\.\d{1,6}$"
    //r"^[0-8]\d\d:[0-5]\d:[0-5]\d\.\d{1,6}$"
    pub fn parse(&mut self) -> Result<Duration, FromValueError> {
        let hour = self.next(b':', 900)?;
        let minute = self.next(b':', 60)?;
        let second = self.next(b'.', 60)?;
        let micro = if self.i < self.bytes.len() {
            let pad = 6 - (self.bytes.len() - self.i);
            self.next(b'\0', 1000000)? * 10usize.pow(pad as u32)
        } else {
            0
        };
        Ok(Duration::new(
            (hour * 3600 + minute * 60 + second) as u64,
            micro as u32 * 1000,
        ))
    }
}
