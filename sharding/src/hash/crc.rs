use super::{Hash, HashKey, CRC32TAB, CRC_SEED};
#[derive(Default)]
pub struct Crc32Hasher<A, B, C, D> {
    range: A,  // 用于截取key的范围
    stop: B,   // 用于判断是否停止
    accept: C, // 用于判断是否接受当前的字符
    post: D,   // 用于处理最终的crc结果
}

impl<A, B, C, D> Crc32Hasher<A, B, C, D> {
    #[inline]
    pub fn new(range: A, stop: B, accept: C, post: D) -> Self {
        Self {
            range,
            stop,
            accept,
            post,
        }
    }
}

#[derive(Default)]
pub struct Noop;
pub trait Range {
    fn get<S: HashKey>(&self, key: &S) -> (usize, usize);
}

pub trait Stop {
    fn is(&self, c: u8) -> bool;
}
#[derive(Default)]
pub struct Digit;
impl Stop for Digit {
    #[inline(always)]
    fn is(&self, c: u8) -> bool {
        c < b'0' || c > b'9'
    }
}
impl Stop for u8 {
    #[inline(always)]
    fn is(&self, c: u8) -> bool {
        *self == c
    }
}
pub trait Accept {
    fn is(&self, c: u8) -> bool;
}
impl Accept for Digit {
    #[inline(always)]
    fn is(&self, c: u8) -> bool {
        c >= b'0' && c <= b'9'
    }
}

pub trait Post {
    fn process(&self, crc: i64) -> i64;
}
#[derive(Default)]
pub struct ToLocal;
#[derive(Default)]
pub struct ToShort;
impl Post for ToLocal {
    #[inline(always)]
    fn process(&self, crc: i64) -> i64 {
        (crc as i32).abs() as i64
    }
}
impl Post for ToShort {
    #[inline(always)]
    fn process(&self, crc: i64) -> i64 {
        ((crc >> 16) & 0x7fff).abs()
    }
}

impl<A: Range, B: Stop, C: Accept, D: Post> Hash for Crc32Hasher<A, B, C, D> {
    #[inline]
    fn hash<S: HashKey>(&self, key: &S) -> i64 {
        let mut crc: i64 = CRC_SEED;
        let (start, end) = self.range.get(key);
        for i in start..end {
            let c = key.at(i);
            if self.stop.is(c) {
                break;
            }
            if self.accept.is(c) {
                //crc = ((crc >> 8) & 0x00FFFFFF) ^ CRC32TAB[((crc ^ (c as i64)) & 0xff) as usize] as i64;
                crc = (crc >> 8) ^ CRC32TAB[((crc ^ (c as i64)) & 0xff) as usize] as i64;
            }
        }

        crc ^= CRC_SEED;
        //crc &= CRC_SEED;
        self.post.process(crc)
    }
}
impl Range for Noop {
    #[inline(always)]
    fn get<S: HashKey>(&self, key: &S) -> (usize, usize) {
        (0, key.len())
    }
}

impl Range for usize {
    #[inline(always)]
    fn get<S: HashKey>(&self, key: &S) -> (usize, usize) {
        (*self, key.len())
    }
}

impl Stop for Noop {
    #[inline(always)]
    fn is(&self, _c: u8) -> bool {
        false
    }
}
impl Accept for Noop {
    #[inline(always)]
    fn is(&self, _c: u8) -> bool {
        true
    }
}

impl Post for Noop {
    #[inline(always)]
    fn process(&self, crc: i64) -> i64 {
        crc
    }
}
#[derive(Default)]
pub struct FindContunueDigits;
// 找到连续大于等于5个的数字，如果没有，则返回整个key
impl Range for FindContunueDigits {
    #[inline(always)]
    fn get<S: HashKey>(&self, key: &S) -> (usize, usize) {
        let mut oft = 0;
        while oft < key.len() {
            // 找到第一个数字
            let start = key.find(oft, |c| c.is_ascii_digit()).unwrap_or(key.len());

            // 第一个非数字
            let end = key
                .find(start + 1, |c| !c.is_ascii_digit())
                .unwrap_or(key.len());

            if end - start >= super::SMARTNUM_MIN_LEN {
                return (start, end);
            }

            oft = end + 1;
        }
        (0, key.len())
    }
}

#[derive(Default)]
pub struct LBCrc32localDelimiter {
    hasher: Crc32local,
}
impl Hash for LBCrc32localDelimiter {
    #[inline]
    fn hash<S: HashKey>(&self, key: &S) -> i64 {
        let new_key = &key.num(0).0.to_be_bytes()[..];
        self.hasher.hash(&new_key)
    }
}
#[derive(Default)]
pub struct Rawcrc32local {
    hasher: Crc32local,
}
impl Hash for Rawcrc32local {
    #[inline]
    fn hash<S: HashKey>(&self, key: &S) -> i64 {
        let (num, b) = key.num(0);
        // key中如果非数字字符不存在，或者是'_'，则直接返回num，否则计算crc
        match b {
            Some(b'_') | None => num,
            _ => self.hasher.hash(key),
        }
    }
}

pub struct RawSuffix {
    delemiter: u8,
}
impl From<u8> for RawSuffix {
    fn from(delemiter: u8) -> Self {
        Self { delemiter }
    }
}
impl Hash for RawSuffix {
    #[inline]
    fn hash<S: HashKey>(&self, key: &S) -> i64 {
        let idx = key.find(0, |c| c == self.delemiter).unwrap_or(key.len());
        let (num, b) = key.num(idx + 1);
        // b如果不为None，说明分隔符后还有非数字，直接返回0
        match b {
            Some(_) => 0,
            None => num,
        }
    }
}

pub type Crc32 = Crc32Hasher<Noop, Noop, Noop, Noop>;
pub type Crc32Short = Crc32Hasher<Noop, Noop, Noop, ToShort>;
pub type Crc32Num = Crc32Hasher<usize, Digit, Noop, Noop>;
pub type Crc32Delimiter = Crc32Hasher<usize, u8, Noop, Noop>;
pub type Crc32SmartNum = Crc32Hasher<FindContunueDigits, Noop, Noop, Noop>;
pub type Crc32MixNum = Crc32Hasher<Noop, Noop, Digit, Noop>;
pub type Crc32Abs = Crc32Hasher<Noop, Noop, Noop, ToLocal>;
pub type Crc32local = Crc32Hasher<Noop, Noop, Noop, ToLocal>;
pub type Crc32localDelimiter = Crc32Hasher<usize, u8, Noop, ToLocal>;
pub type Crc32localSmartNum = Crc32Hasher<FindContunueDigits, Noop, Noop, ToLocal>;

impl From<usize> for Crc32Num {
    fn from(range: usize) -> Self {
        Self::new(range, Digit, Noop, Noop)
    }
}
impl Crc32Delimiter {
    pub fn from(start: usize, delimiter: u8) -> Self {
        Self::new(start, delimiter, Noop, Noop)
    }
}
