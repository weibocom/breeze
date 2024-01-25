use byteorder::{LittleEndian as LE, ReadBytesExt, WriteBytesExt};
use bytes::BufMut;
use ds::{ByteOrder, RingSlice};
// use sha1::digest::typenum::Minimum;
use std::{fmt::Display, io};

use super::proto::MyDeserialize;
use paste::paste;

pub trait BufMutExt: BufMut {
    /// Writes an unsigned integer to self as MySql length-encoded integer.
    fn put_lenenc_int(&mut self, n: u64) {
        if n < 251 {
            self.put_u8(n as u8);
        } else if n < 65_536 {
            self.put_u8(0xFC);
            self.put_uint_le(n, 2);
        } else if n < 16_777_216 {
            self.put_u8(0xFD);
            self.put_uint_le(n, 3);
        } else {
            self.put_u8(0xFE);
            self.put_uint_le(n, 8);
        }
    }

    /// Writes a slice to self as MySql length-encoded string.
    fn put_lenenc_str(&mut self, s: &[u8]) {
        self.put_lenenc_int(s.len() as u64);
        self.put_slice(s);
    }

    /// Writes a 3-bytes unsigned integer.
    fn put_u24_le(&mut self, x: u32) {
        self.put_uint_le(x as u64, 3);
    }

    /// Writes a 3-bytes signed integer.
    fn put_i24_le(&mut self, x: i32) {
        self.put_int_le(x as i64, 3);
    }

    /// Writes a 6-bytes unsigned integer.
    fn put_u48_le(&mut self, x: u64) {
        self.put_uint_le(x, 6);
    }

    /// Writes a 7-bytes unsigned integer.
    fn put_u56_le(&mut self, x: u64) {
        self.put_uint_le(x, 7);
    }

    /// Writes a 7-bytes signed integer.
    fn put_i56_le(&mut self, x: i64) {
        self.put_int_le(x, 7);
    }

    /// Writes a string with u8 length prefix. Truncates, if the length is greater that `u8::MAX`.
    // fn put_u8_str(&mut self, s: &[u8]) {
    fn put_u8_str(&mut self, s: &RingSlice) {
        const U8_MAX: usize = u8::MAX as usize;
        let min = s.len().min(U8_MAX);
        self.put_u8(min as u8);
        // s.copy_to_bufmut(self, U8_MAX);
        self.copy_from_slice(s, min);
    }

    /// Writes a string with u32 length prefix. Truncates, if the length is greater that `u32::MAX`.
    // fn put_u32_str(&mut self, s: &[u8]) {
    fn put_u32_str(&mut self, s: &RingSlice) {
        // let len = std::cmp::min(s.len(), u32::MAX as usize);
        // self.put_u32_le(len as u32);
        // self.put_slice(&s[..len]);
        // 参考上面的代码，注意check一致性 fishermen
        const U32_MAX: usize = u32::MAX as usize;
        let min = s.len().min(U32_MAX);
        self.put_u32_le(min as u32);
        self.copy_from_slice(s, min);
    }

    /// copy 前len个bytes 到 BufMut，注意check len的长度
    #[inline]
    fn copy_from_slice(&mut self, data: &RingSlice, len: usize) {
        data.visit_data(..len, |b| {
            self.put_slice(b);
        });
    }
}

impl<T: BufMut> BufMutExt for T {}

/// 对ringslice进行动态解析，当前的解析位置是oft，所有数据来自ringslice类型的data；
/// oft在此处统一管理，保持ringslice的不变性；
/// 所有对data的访问，需要进行统一封装，避免oft的误操作 fishermen
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct ParseBuf {
    oft: usize,
    data: RingSlice,
}
// pub struct ParseBuf<'a>(pub &'a [u8]);

// impl io::Read for ParseBuf<'_> {
// TODO read操作太重，注意check读取的量，过大需要优化 fishermen
impl io::Read for ParseBuf {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // let count = min(self.0.len(), buf.len());
        // (buf[..count]).copy_from_slice(&self.0[..count]);
        // self.0 = &self.0[count..];
        // Ok(count)

        let count = self.len().min(buf.len());
        let data = self.eat(count);
        data.copy_to_slice(&mut buf[..count]);
        Ok(count)
    }
}

#[rustfmt::skip]
macro_rules! next {
    ($sign:ident, 16) => {paste!{[<$sign 16>]}};
    ($sign:ident, 24) => {paste!{[<$sign 32>]}};
    ($sign:ident, 40) => {paste!{[<$sign 64>]}};
    ($sign:ident, 48) => {paste!{[<$sign 64>]}};
    ($sign:ident, 56) => {paste!{[<$sign 64>]}};
    ($sign:ident, $bits:literal) => {paste!{[<$sign $bits>]}};
}

macro_rules! eat_num {
    ($($bits:literal) +) => {
        $(
        eat_num!(u, $bits, _le);
        eat_num!(i, $bits, _le);
        )+
    };

    ($sign:ident, $bits:literal, $endian:ident) => {
        paste!{
            eat_num!([<eat_ $sign $bits $endian>], [<checked_eat_ $sign  $bits $endian>], $bits, [<$sign $bits $endian>], next!($sign, $bits));
        }
    };
    ($fn:ident, $checked_fn:ident, $bits:literal, $num:ident, $t:ty) => {
        #[doc = "Consumes a number from the head of the buffer."]
        pub fn $fn(&mut self) -> $t {
            let slice = self.eat($bits / 8);
            slice.$num(0)
        }

        #[doc = "Consumes a number from the head of the buffer. Returns `None` if buffer is too small."]
        pub fn $checked_fn(&mut self) -> Option<$t> {
            if self.len() >= std::mem::size_of::<$t>() {
                Some(self.$fn())
            } else {
                None
            }
        }
    };
}

impl ParseBuf {
    #[inline(always)]
    pub fn new(oft: usize, data: RingSlice) -> Self {
        Self { oft, data }
    }

    /// Returns `T: MyDeserialize` deserialized from `self`.
    ///
    /// Note, that this may panic if `T::SIZE.is_some()` and less than `self.0.len()`.
    #[inline(always)]
    pub fn parse_unchecked<T>(&mut self, ctx: T::Ctx) -> io::Result<T>
    where
        T: MyDeserialize,
    {
        T::deserialize(ctx, self)
    }

    /// Checked `parse`.
    #[inline(always)]
    pub fn parse<T>(&mut self, ctx: T::Ctx) -> io::Result<T>
    where
        T: MyDeserialize,
    {
        match T::SIZE {
            Some(size) => {
                let mut buf: ParseBuf = self.parse_unchecked(size)?;
                buf.parse_unchecked(ctx)
            }
            None => self.parse_unchecked(ctx),
        }
    }

    /// Returns true if buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// parseBuf改为一个动态buf，len是自oft到结束位置的长度，即返回尚剩余未解析的字节长度
    pub fn len(&self) -> usize {
        // self.0.len()

        // 语义变了，此处应该是返回剩余长度，即oft到data.len()的长度，而非data.len()本身 fishermen
        assert!(self.oft <= self.data.len(), "Parsebuf: {:?}", self);
        self.data.len() - self.oft
    }

    /// 代理data的at访问，转换原有语义
    #[inline(always)]
    pub(super) fn at(&self, idx: usize) -> u8 {
        self.data.at(self.oft + idx)
    }

    /// 代理ringslice中的find，避免oft的困扰；
    /// 返回值：基于当前oft的位置（保持parbuf的语义）
    #[inline]
    pub(super) fn find(&self, offset: usize, b: u8) -> Option<usize> {
        if let Some(pos) = self.data.find(self.oft + offset, b) {
            Some(pos - self.oft)
        } else {
            None
        }
    }

    /// 代理ringslice中的sub_slice，避免oft的困扰
    #[inline(always)]
    pub(super) fn sub_slice(&self, offset: usize, len: usize) -> RingSlice {
        self.data.sub_slice(self.oft + offset, len)
    }

    /// Skips the given number of bytes.
    ///
    /// Afterwards self contains elements `[cnt, len)`.
    pub fn skip(&mut self, cnt: usize) {
        // self.0 = &self.0[cnt..];

        if cnt <= self.len() {
            // 直接偏移，跟原逻辑差异比较大，会埋坑里，所以还是保持原逻辑吧 fishermen
            // self.oft += cnt;
            self.data = self.sub_slice(cnt, self.len() - cnt);
            self.oft = 0;
        } else {
            log::error!("+++ skip overflow:{}/{:?}", cnt, self);
            assert!(false, "{}/{:?}", cnt, self);
        }
    }

    // /// Same as `skip` but returns `false` if buffer is too small.
    // pub fn checked_skip(&mut self, cnt: usize) -> bool {
    //     if self.len() >= cnt {
    //         self.skip(cnt);
    //         true
    //     } else {
    //         false
    //     }
    // }

    /// Splits the buffer into two at the given index. Returns elements `[0, n)`.
    ///
    /// Afterwards self contains elements `[n, len)`.
    ///
    /// # Panic
    ///
    /// Will panic if `n > self.len()`.
    // pub fn eat(&mut self, n: usize) -> &'a [u8] {
    pub fn eat(&mut self, n: usize) -> RingSlice {
        // let (left, right) = self.0.split_at(n);
        // self.0 = right;
        // left

        let data = self.sub_slice(0, n);
        self.skip(n);
        data
    }

    // pub fn eat_buf(&mut self, n: usize) -> Self {
    //     Self(self.eat(n))
    // }

    /// Same as `eat`. Returns `None` if buffer is too small.
    // pub fn checked_eat(&mut self, n: usize) -> Option<&'a [u8]> {
    pub fn checked_eat(&mut self, n: usize) -> Option<RingSlice> {
        if self.len() >= n {
            Some(self.eat(n))
        } else {
            None
        }
    }

    pub fn checked_eat_buf(&mut self, n: usize) -> Option<Self> {
        // Some(Self(self.checked_eat(n)?))

        if self.len() >= n {
            // self.0 = self.0.sub_slice(n, self.len() - n);
            let data = self.eat(n);
            Some(ParseBuf::new(0, data))
        } else {
            log::error!("buf overflow: {}/{:?}", n, self);
            None
        }
    }

    // pub fn eat_all(&mut self) -> &'a [u8] {
    pub fn eat_all(&mut self) -> RingSlice {
        self.eat(self.len())
    }
    eat_num!(16 24 32 40 48 56 64);
    eat_num!(eat_u8, checked_eat_u8, 8, u8, u8);
    eat_num!(eat_i8, checked_eat_i8, 8, i8, i8);
    eat_num!(f, 32, _le);
    eat_num!(f, 64, _le);

    /// Same as `eat_lenenc_int`. Returns `None` if buffer is too small.
    pub fn checked_eat_lenenc_int(&mut self) -> Option<u64> {
        match self.checked_eat_u8()? {
            x @ 0..=0xfa => Some(x as u64),
            0xfc => self.checked_eat_u16_le().map(|x| x as u64),
            0xfd => self.checked_eat_u24_le().map(|x| x as u64),
            0xfe => self.checked_eat_u64_le(),
            0xfb | 0xff => Some(0),
        }
    }

    // /// Consumes MySql length-encoded string from the head of the buffer.
    // ///
    // /// Returns an empty slice if length is maliformed (starts with 0xff). First byte will be eaten.
    // pub fn eat_lenenc_str(&mut self) -> &'a [u8] {
    //     let len = self.eat_lenenc_int();
    //     self.eat(len as usize)
    // }

    /// Same as `eat_lenenc_str`. Returns `None` if buffer is too small.
    // pub fn checked_eat_lenenc_str(&mut self) -> Option<&'a [u8]> {
    pub fn checked_eat_lenenc_str(&mut self) -> Option<RingSlice> {
        let len = self.checked_eat_lenenc_int()?;
        self.checked_eat(len as usize)
    }

    // /// Consumes MySql string with u8 length prefix from the head of the buffer.
    // pub fn eat_u8_str(&mut self) -> &'a [u8] {
    //     let len = self.eat_u8();
    //     self.eat(len as usize)
    // }

    // /// Same as `eat_u8_str`. Returns `None` if buffer is too small.
    // pub fn checked_eat_u8_str(&mut self) -> Option<&'a [u8]> {
    //     let len = self.checked_eat_u8()?;
    //     self.checked_eat(len as usize)
    // }

    // /// Consumes MySql string with u32 length prefix from the head of the buffer.
    // pub fn eat_u32_str(&mut self) -> &'a [u8] {
    //     let len = self.eat_u32_le();
    //     self.eat(len as usize)
    // }

    /// Same as `eat_u32_str`. Returns `None` if buffer is too small.
    // pub fn checked_eat_u32_str(&mut self) -> Option<&'a [u8]> {
    pub fn checked_eat_u32_str(&mut self) -> Option<RingSlice> {
        let len = self.checked_eat_u32_le()?;
        self.checked_eat(len as usize)
    }

    /// Consumes null-terminated string from the head of the buffer.
    ///
    /// Consumes whole buffer if there is no `0`-byte.
    // pub fn eat_null_str(&mut self) -> &'a [u8] {
    pub fn eat_null_str(&mut self) -> RingSlice {
        // let pos = self
        //     .0
        //     .iter()
        //     .position(|x| *x == 0)
        //     .map(|x| x + 1)
        //     .unwrap_or_else(|| self.len());
        // match self.eat(pos) {
        //     [head @ .., 0_u8] => head,
        //     x => x,
        // }

        // 基于封装的find，返回的pos是基于当前oft的位置
        let pos = match self.find(0, 0) {
            Some(p) => p + 1,
            None => self.len(),
        };
        // 如果结尾是0_u8，去掉，否则直接返回
        // 注意：语义都是基于oft来操作的，如果基于源data操作，需要加上oft
        // 当前data直接使用还
        if self.at(pos - 1) == 0 {
            let data = self.eat(pos - 1);
            // skip 掉一个字节：0
            self.skip(1);
            data
        } else {
            self.eat(pos)
        }
    }
}

impl From<RingSlice> for ParseBuf {
    #[inline(always)]
    fn from(data: RingSlice) -> Self {
        Self::new(0, data)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("Invalid length-encoded integer value (starts with 0xfb|0xff)")]
pub struct InvalidLenghEncodedInteger;

pub trait ReadMysqlExt: ReadBytesExt {
    /// Reads MySql's length-encoded integer.
    fn read_lenenc_int(&mut self) -> io::Result<u64> {
        match self.read_u8()? {
            x if x <= 0xfa => Ok(x.into()),
            0xfc => self.read_uint::<LE>(2),
            0xfd => self.read_uint::<LE>(3),
            0xfe => self.read_uint::<LE>(8),
            0xfb | 0xff => Err(io::Error::new(
                io::ErrorKind::Other,
                InvalidLenghEncodedInteger,
            )),
            _ => unreachable!(),
        }
    }

    /// Reads MySql's length-encoded string.
    fn read_lenenc_str(&mut self) -> io::Result<Vec<u8>> {
        let len = self.read_lenenc_int()?;
        let mut output = vec![0_u8; len as usize];
        self.read_exact(&mut output)?;
        Ok(output)
    }
}

pub trait WriteMysqlExt: WriteBytesExt {
    /// Writes MySql's length-encoded integer.
    fn write_lenenc_int(&mut self, x: u64) -> io::Result<u64> {
        if x < 251 {
            self.write_u8(x as u8)?;
            Ok(1)
        } else if x < 65_536 {
            self.write_u8(0xFC)?;
            self.write_uint::<LE>(x, 2)?;
            Ok(3)
        } else if x < 16_777_216 {
            self.write_u8(0xFD)?;
            self.write_uint::<LE>(x, 3)?;
            Ok(4)
        } else {
            self.write_u8(0xFE)?;
            self.write_uint::<LE>(x, 8)?;
            Ok(9)
        }
    }

    /// Writes MySql's length-encoded string.
    fn write_lenenc_str(&mut self, bytes: &[u8]) -> io::Result<u64> {
        let written = self.write_lenenc_int(bytes.len() as u64)?;
        self.write_all(bytes)?;
        Ok(written + bytes.len() as u64)
    }
}

impl Display for ParseBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ParseBuf [oft: {}, len: {}, data: {}]",
            self.oft,
            self.len(),
            self.data
        )
    }
}

impl<T> ReadMysqlExt for T where T: ReadBytesExt {}
impl<T> WriteMysqlExt for T where T: WriteBytesExt {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn be_le() {
        // let buf = ParseBuf(&[0, 1, 2]);
        let data = vec![0, 1, 2];
        let slice = RingSlice::from_vec(&data);
        let buf = ParseBuf::new(0, slice);
        assert_eq!(buf.clone().eat_u24_le(), 0x00020100);
        // assert_eq!(buf.clone().eat_u24_be(), 0x00000102);
        // let buf = ParseBuf(&[0, 1, 2, 3, 4]);
        // assert_eq!(buf.clone().eat_u40_le(), 0x0000000403020100);
        // assert_eq!(buf.clone().eat_u40_be(), 0x0000000001020304);
        // let buf = ParseBuf(&[0, 1, 2, 3, 4, 5]);
        // assert_eq!(buf.clone().eat_u48_le(), 0x0000050403020100);
        // assert_eq!(buf.clone().eat_u48_be(), 0x0000000102030405);
        // let buf = ParseBuf(&[0, 1, 2, 3, 4, 5, 6]);
        // assert_eq!(buf.clone().eat_u56_le(), 0x0006050403020100);
        // assert_eq!(buf.clone().eat_u56_be(), 0x0000010203040506);
    }
}
