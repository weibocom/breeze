use std::time::Duration;

pub fn parse_duration(s: &[u8]) -> Option<Duration> {
    let mut inner = InnerDuration::new(s);
    inner.parse().ok()
}

#[derive(Debug)]
pub struct MyDuration(pub Duration);

impl TryFrom<&[u8]> for MyDuration {
    type Error = String;
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let mut inner = InnerDuration::new(bytes);
        inner.parse().map(|d| Self(d))
    }
}

impl PartialEq<(u32, u32, u32, u32)> for MyDuration {
    fn eq(&self, other: &(u32, u32, u32, u32)) -> bool {
        let hour = (self.0.as_secs() / 3600) as u32;
        let minute = (self.0.as_secs() % 3600 / 60) as u32;
        let second = (self.0.as_secs() % 60) as u32;
        let micro = (self.0.subsec_micros()) as u32;
        (hour, minute, second, micro) == *other
    }
}

struct InnerDuration<'a> {
    i: usize,
    bytes: &'a [u8],
}
impl<'a> InnerDuration<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { i: 0, bytes }
    }
    fn next(&mut self, b: u8, max: usize) -> Result<usize, String> {
        if self.i >= self.bytes.len() {
            return Err("EOF".to_string());
        }
        let mut v = 0usize;
        while self.i < self.bytes.len() {
            if self.bytes[self.i] == b {
                self.i += 1;
                break;
            }
            let c = self.bytes[self.i];
            if c < b'0' || c > b'9' {
                return Err(format!("invalid byte: {}", c));
            }
            v = v * 10 + (c - b'0') as usize;
            self.i += 1;
        }
        if v >= max {
            Err(format!("invalid value: {v} >= {max}"))
        } else {
            Ok(v)
        }
    }
    // value的格式为：HHH:MM:SS[.fraction]
    // r"^\d{2}:[0-5]\d:[0-5]\d$"
    //r"^[0-8]\d\d:[0-5]\d:[0-5]\d$"
    //r"^\d{2}:[0-5]\d:[0-5]\d\.\d{1,6}$"
    //r"^[0-8]\d\d:[0-5]\d:[0-5]\d\.\d{1,6}$"
    pub fn parse(&mut self) -> Result<Duration, String> {
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
