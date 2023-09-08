// TODO 后面统一整合

use byteorder::{LittleEndian as LE, ReadBytesExt, WriteBytesExt};
use bytes::BufMut;
use ds::RingSlice;
// use sha1::digest::typenum::Minimum;
use std::{fmt::Display, io};

use super::proto::MyDeserialize;

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
        // let len = std::cmp::min(s.len(), u8::MAX as usize);
        // self.put_u8(len as u8);
        // self.put_slice(&s[..len]);
        // TODO 参考上面的代码，注意check一致性 fishermen
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
        // TODO 参考上面的代码，注意check一致性 fishermen
        const U32_MAX: usize = u32::MAX as usize;
        let min = s.len().min(U32_MAX);
        self.put_u32_le(min as u32);
        self.copy_from_slice(s, min);
    }

    /// copy 前len个bytes 到 BufMut，注意check len的长度
    #[inline]
    fn copy_from_slice(&mut self, data: &RingSlice, len: usize) {
        let (l, r) = data.data();
        if len <= l.len() {
            self.put_slice(&l[..len]);
            return;
        }

        // len大于l.len
        self.put_slice(l);
        let rmin = r.len().min(len - l.len());
        if rmin > 0 {
            self.put_slice(&r[..rmin]);
        }
    }
}

impl<T: BufMut> BufMutExt for T {}

// oft在此处统一管理，保持ringslice的不变性；
// 所有对data的访问，需要进行统一封装，避免oft的误操作 fishermen
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

        // TODO 参考上面的代码，彻底稳定后，再考虑清理 fishermen
        // let count = self.0.len().min(buf.len());
        // self.0.copy_to_slice(&mut buf[..count]);
        // self.0.eat(count);
        // Ok(count)
        let count = self.len().min(buf.len());
        let data = self.eat(count);
        data.copy_to_slice(&mut buf[..count]);
        Ok(count)
    }
}

macro_rules! eat_num {
    // eat_num!(eat_u8, checked_eat_u8, u8, read_u8_le);
    ($name:ident, $checked:ident, $t:ident, $fn:ident) => {
        #[doc = "Consumes a number from the head of the buffer."]
        pub fn $name(&mut self) -> $t {
            const SIZE: usize = std::mem::size_of::<$t>();
            // let bytes = self.eat(SIZE);
            // unsafe { $t::$fn(*(bytes as *const _ as *const [_; SIZE])) }

            // TODO 统一改造为RingSlice来parse，注意对比原有逻辑 fishermen
            let slice = self.eat(SIZE);
            slice.$fn(0)
            // match slice.try_oneway_slice(0, SIZE) {
            //     Some(bytes) => unsafe { $t::$fn(*(bytes as *const _ as *const [_; SIZE])) },
            //     None => {
            //         let data = slice.dump_ring_part(0, SIZE);
            //         unsafe { $t::$fn(*(data.as_ptr() as *const [_; SIZE])) }
            //     }
            // }
        }

        #[doc = "Consumes a number from the head of the buffer. Returns `None` if buffer is too small."]
        pub fn $checked(&mut self) -> Option<$t> {
            if self.len() >= std::mem::size_of::<$t>() {
                Some(self.$name())
            } else {
                None
            }
        }
    };
    // TODO 这里的size、offset是kv目标类型相关的，在ringSlice对应类型中实现，此处暂时保留，仅做对比用，后续稳定后清理 fishermen
    // eat_num!(eat_u24_le, checked_eat_u24_le, 3, 0, u32, read_u24_le);
    ($name:ident, $checked:ident, $size:literal, $offset:literal, $t:ident, $fn:ident) => {
        #[doc = "Consumes a number from the head of the buffer."]
        pub fn $name(&mut self) -> $t {
            const SIZE: usize = $size;
            // let mut x: $t = 0;
            // let bytes = self.eat(SIZE);
            // for (i, b) in bytes.iter().enumerate() {
            //     x |= (*b as $t) << ((8 * i) + (8 * $offset));
            // }
            // $t::$fn(x)

            // 封装到RingSlice中处理，注意check一致性
            let slice = self.eat(SIZE);
            slice.$fn(0)

            // TODO 注意对比上面原始代码，check一致性 fishermen
            // for i in 0..SIZE {
            //     let b = slice.at(i);
            //     x |= (b as $t) << ((8 * i) + (8 * $offset));
            // }
            // $t::$fn(x)
        }

        #[doc = "Consumes a number from the head of the buffer. Returns `None` if buffer is too small."]
        pub fn $checked(&mut self) -> Option<$t> {
            if self.len() >= $size {
                Some(self.$name())
            } else {
                None
            }
        }
    };
}

// impl<'a> ParseBuf<'a> {
impl<'a> ParseBuf {
    #[inline(always)]
    pub fn new(oft: usize, data: RingSlice) -> Self {
        Self { oft, data }
    }

    // TODO 逻辑统一到len()中，但需要注意len的调用姿势是否需要同步修改  fishermen
    // #[inline(always)]
    // pub fn left_len(&self) -> usize {
    //     // TODO 测试稳定后，此assert可以去掉？ fishermen
    //     assert!(
    //         self.oft <= self.data.len(),
    //         "oft/len: {}/{}",
    //         self.oft,
    //         self.data.len()
    //     );
    //     self.data.len() - self.oft
    // }
    // pub fn from(data: &'a [u8]) -> Self {
    //     let slice = data.into();
    //     Self(slice)
    // }

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

    /// 返回尚未解析的字节长度，注意会去掉oft的长度.
    pub fn len(&self) -> usize {
        // self.0.len()

        // TODO 语义变了，此处应该是返回剩余长度，即oft到data.len()的长度，而非data.len()本身 fishermen
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

        // TODO 参考逻辑2，测试稳定后清理 fishermen
        // if cnt <= self.0.len() {
        //     self.0 = self.0.sub_slice(cnt, self.0.len() - cnt);
        // } else {
        //     self.0 = self.0.sub_slice(self.0.len(), 0);
        // }
        if cnt <= self.len() {
            // TODO 直接偏移，跟原逻辑差异比较大，会埋坑里，所以还是保持原逻辑吧 fishermen
            // self.oft += cnt;
            self.data = self.sub_slice(cnt, self.len() - cnt);
            self.oft = 0;
        } else {
            log::error!("+++ skip overflow:{}/{:?}", cnt, self);
            assert!(false, "{}/{}", cnt, self.len());
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

        // TODO 加上长度判断，避免panic，类似场景需要梳理 fishermen
        // assert!(n < self.len(), "malformed len: {}/{:?}", n, self);
        // let data = unsafe { self.0.limited_slice(0, n) };
        // self.0 = self.0.sub_slice(n, self.len() - n);
        // data
        // TODO 参考代码2 稳定后清理
        // self.0.eat(n)

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

        // TODO 暂时保留做参考2，稳定后清理 fishermen
        // if self.len() >= n {
        //     let data = self.0.sub_slice(0, n);
        //     self.0 = self.0.sub_slice(n, self.len() - n);
        //     Some(ParseBuf(data))
        // } else {
        //     None
        // }

        if self.len() >= n {
            // self.0 = self.0.sub_slice(n, self.len() - n);
            let data = self.eat(n);
            Some(ParseBuf::new(0, data))
        } else {
            log::error!("buf overflow: {:?}", self);
            None
        }
    }

    // pub fn eat_all(&mut self) -> &'a [u8] {
    pub fn eat_all(&mut self) -> RingSlice {
        self.eat(self.len())
    }

    // eat_num!(eat_u8, checked_eat_u8, u8::from_le_bytes);
    // eat_num!(eat_i8, checked_eat_i8, i8::from_le_bytes);
    // eat_num!(eat_u16_le, checked_eat_u16_le, u16::from_le_bytes);
    // eat_num!(eat_i16_le, checked_eat_i16_le, i16::from_le_bytes);

    eat_num!(eat_u8, checked_eat_u8, u8, read_u8);
    eat_num!(eat_i8, checked_eat_i8, i8, read_i8);
    eat_num!(eat_u16_le, checked_eat_u16_le, u16, read_u16_le);
    eat_num!(eat_i16_le, checked_eat_i16_le, i16, read_i16_le);
    eat_num!(eat_u24_le, checked_eat_u24_le, 3, 0, u32, read_u24_le);
    eat_num!(eat_i24_le, _checked_eat_i24_le, 3, 0, i32, read_i24_le);
    eat_num!(eat_u32_le, checked_eat_u32_le, u32, read_u32_le);
    eat_num!(eat_i32_le, checked_eat_i32_le, i32, read_i32_le);
    eat_num!(eat_u48_le, _checked_eat_u48_le, 6, 0, u64, read_u48_le);
    eat_num!(eat_u56_le, _checked_eat_u56_le, 7, 0, u64, read_u56_le);
    eat_num!(eat_i56_le, _checked_eat_i56_le, 7, 0, i64, read_i56_le);
    eat_num!(eat_u64_le, checked_eat_u64_le, u64, read_u64_le);
    eat_num!(eat_i64_le, checked_eat_i64_le, i64, read_i64_le);

    eat_num!(eat_f32_le, checked_eat_f32_le, f32, read_f32_le);
    eat_num!(eat_f64_le, checked_eat_f64_le, f64, read_f64_le);

    // eat_num!(eat_u16_be, checked_eat_u16_be, u16::from_be_bytes);
    // eat_num!(eat_i16_be, checked_eat_i16_be, i16::from_be_bytes);
    // eat_num!(eat_u24_le, checked_eat_u24_le, 3, 0, u32::from_le);
    // eat_num!(eat_i24_le, _checked_eat_i24_le, 3, 0, i32::from_le);
    // eat_num!(eat_u24_be, checked_eat_u24_be, 3, 1, u32::from_be);
    // eat_num!(eat_i24_be, checked_eat_i24_be, 3, 1, i32::from_be);
    // eat_num!(eat_u32_le, checked_eat_u32_le, u32::from_le_bytes);
    // eat_num!(eat_i32_le, checked_eat_i32_le, i32::from_le_bytes);
    // eat_num!(eat_u32_be, checked_eat_u32_be, u32::from_be_bytes);
    // eat_num!(eat_i32_be, checked_eat_i32_be, i32::from_be_bytes);
    // eat_num!(eat_u40_le, checked_eat_u40_le, 5, 0, u64::from_le);
    // eat_num!(eat_i40_le, checked_eat_i40_le, 5, 0, i64::from_le);
    // eat_num!(eat_u40_be, checked_eat_u40_be, 5, 3, u64::from_be);
    // eat_num!(eat_i40_be, checked_eat_i40_be, 5, 3, i64::from_be);
    // eat_num!(eat_u48_le, _checked_eat_u48_le, 6, 0, u64::from_le);
    // eat_num!(eat_i48_le, checked_eat_i48_le, 6, 0, i64::from_le);
    // eat_num!(eat_u48_be, checked_eat_u48_be, 6, 2, u64::from_be);
    // eat_num!(eat_i48_be, checked_eat_i48_be, 6, 2, i64::from_be);
    // eat_num!(eat_u56_le, _checked_eat_u56_le, 7, 0, u64::from_le);
    // eat_num!(eat_i56_le, _checked_eat_i56_le, 7, 0, i64::from_le);
    // eat_num!(eat_u56_be, checked_eat_u56_be, 7, 1, u64::from_be);
    // eat_num!(eat_i56_be, checked_eat_i56_be, 7, 1, i64::from_be);
    // eat_num!(eat_u64_le, checked_eat_u64_le, u64::from_le_bytes);
    // eat_num!(eat_i64_le, checked_eat_i64_le, i64::from_le_bytes);
    // eat_num!(eat_u64_be, checked_eat_u64_be, u64::from_be_bytes);
    // eat_num!(eat_i64_be, checked_eat_i64_be, i64::from_be_bytes);
    // eat_num!(eat_u128_le, checked_eat_u128_le, u128::from_le_bytes);
    // eat_num!(eat_i128_le, checked_eat_i128_le, i128::from_le_bytes);
    // eat_num!(eat_u128_be, checked_eat_u128_be, u128::from_be_bytes);
    // eat_num!(eat_i128_be, checked_eat_i128_be, i128::from_be_bytes);

    // eat_num!(eat_f32_le, checked_eat_f32_le, f32::from_le_bytes);
    // eat_num!(eat_f32_be, checked_eat_f32_be, f32::from_be_bytes);

    // eat_num!(eat_f64_le, checked_eat_f64_le, f64::from_le_bytes);
    // eat_num!(eat_f64_be, checked_eat_f64_be, f64::from_be_bytes);

    // /// Consumes MySql length-encoded integer from the head of the buffer.
    // ///
    // /// Returns `0` if integer is maliformed (starts with 0xff or 0xfb). First byte will be eaten.
    // pub fn eat_lenenc_int(&mut self) -> u64 {
    //     match self.eat_u8() {
    //         x @ 0..=0xfa => x as u64,
    //         0xfc => self.eat_u16_le() as u64,
    //         0xfd => self.eat_u24_le() as u64,
    //         0xfe => self.eat_u64_le(),
    //         0xfb | 0xff => 0,
    //     }
    // }

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

        // 找到第一个非0字节，pos为（index + 1）
        // let pos = match self.0.find(0, 0) {
        //     Some(p) => p + 1,
        //     None => self.len(),
        // };
        // let data = self.0.sub_slice(0, pos);
        // self.0 = self.0.sub_slice(pos, self.len() - pos);

        // // 如果结尾是0_u8，去掉，否则直接返回
        // if data.at(data.len() - 1) == 0 {
        //     unsafe { data.limited_slice(0, data.len() - 1) }
        // } else {
        //     unsafe { data.limited_slice(0, data.len()) }
        // }

        // TODO 第三版，继续延迟copy，注意对比逻辑的一致性 fishermen
        let pos = match self.find(0, 0) {
            Some(p) => p + 1,
            None => self.len(),
        };
        // 如果结尾是0_u8，去掉，否则直接返回
        // TODO 对于基于data的操作特别要注意，当前parsebuf的语义都是基于oft来操作的，如果基于源data操作，需要加上oft
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
