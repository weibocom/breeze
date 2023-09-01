use std::fmt::{Debug, Display, Formatter};
use std::ptr::copy_nonoverlapping;
use std::slice::from_raw_parts;

//从不拥有数据，是对ptr+start的引用
// #[derive(Default, Copy, Clone, Eq, Ord, Hash)]
#[derive(Default, Clone, Copy, Eq, Hash)]
pub struct RingSlice {
    ptr: usize,
    cap: u32,
    start: u32,
    len: u32,
    mask: u32,
}

// 将ring_slice拆分成2个seg。分别调用
macro_rules! with_segment_oft {
    ($self:expr, $oft:expr, $noseg:expr, $seg:expr) => {{
        let len = $self.len() - $oft;
        with_segment_oft_len!($self, $oft, len, $noseg, $seg)
    }};
}

// 基于oft、len对slice的2个seg进行调用
macro_rules! with_segment_oft_len {
    ($self:expr, $oft:expr, $len:expr, $noseg:expr, $seg:expr) => {{
        debug_assert!($oft + $len <= $self.len());
        let oft_start = $self.mask($self.start() + $oft);
        let len = ($self.len() - $oft).min($len);

        if oft_start + len <= $self.cap() {
            unsafe { $noseg($self.ptr().add(oft_start), len) }
        } else {
            let seg1 = $self.cap() - oft_start;
            let seg2 = len - seg1;
            unsafe { $seg($self.ptr().add(oft_start), seg1, $self.ptr(), seg2) }
        }
    }};
}

impl RingSlice {
    const EMPTY: Self = Self {
        ptr: 0,
        cap: 0,
        start: 0,
        len: 0,
        mask: 0,
    };
    #[inline]
    pub fn empty() -> Self {
        Self::EMPTY
    }
    //从不拥有数据
    #[inline]
    pub fn from(ptr: *const u8, cap: usize, start: usize, end: usize) -> Self {
        debug_assert!(cap < u32::MAX as usize);
        debug_assert!(cap.is_power_of_two() || cap == 0, "not valid cap:{}", cap);
        debug_assert!(end >= start && end - start <= cap);
        // cap为0是mask为:u32::MAX，也是合法的
        let mask = cap.wrapping_sub(1) as u32;
        Self {
            ptr: ptr as usize,
            cap: cap as u32,
            start: (start & mask as usize) as u32,
            len: (end - start) as u32,
            mask,
        }
    }
    #[inline(always)]
    pub fn from_vec(data: &Vec<u8>) -> Self {
        let mut mem: RingSlice = data.as_slice().into();
        // 这里面的cap是真实的cap
        mem.cap = data.capacity() as u32;
        mem
    }
    #[inline(always)]
    pub fn slice(&self, offset: usize, len: usize) -> RingSlice {
        self.sub_slice(offset, len)
    }

    #[inline]
    pub fn sub_slice(&self, offset: usize, len: usize) -> RingSlice {
        assert!(offset + len <= self.len());
        Self {
            ptr: self.ptr,
            cap: self.cap,
            start: self.mask(self.start() + offset) as u32,
            len: len as u32,
            mask: self.mask,
        }
    }
    #[inline(always)]
    pub(super) fn visit_segment_oft(&self, oft: usize, v: impl FnMut(*mut u8, usize)) {
        self.visit_segment_oft_len(oft, self.len(), v);
    }
    #[inline(always)]
    pub(super) fn visit_segment_oft_len(
        &self,
        oft: usize,
        len: usize,
        mut v: impl FnMut(*mut u8, usize),
    ) {
        with_segment_oft_len!(self, oft, len, |p, l| v(p, l), |p0, l0, p1, l1| {
            v(p0, l0);
            v(p1, l1);
        });
    }
    #[inline(always)]
    pub fn data_oft_len(&self, oft: usize, len: usize) -> (&[u8], &[u8]) {
        assert!(oft + len <= self.len(), "{}/{} =>{:?}", oft, len, self);

        static EMPTY: &[u8] = &[];
        with_segment_oft_len!(
            self,
            oft,
            len,
            |ptr, len| (from_raw_parts(ptr, len), EMPTY),
            |p0, l0, p1, l1| (from_raw_parts(p0, l0), from_raw_parts(p1, l1))
        )
    }
    #[inline(always)]
    pub fn data_oft(&self, oft: usize) -> (&[u8], &[u8]) {
        self.data_oft_len(oft, self.len() - oft)
    }
    #[inline(always)]
    pub fn data(&self) -> (&[u8], &[u8]) {
        self.data_oft(0)
    }

    // 特殊情况下，打印合法字节，以及buff中全部的字节
    pub unsafe fn data_dump(&self) -> &[u8] {
        from_raw_parts(self.ptr(), self.cap())
    }
    #[inline(always)]
    pub fn fold<I>(&self, mut init: I, mut v: impl FnMut(&mut I, u8)) -> I {
        self.visit_segment_oft(0, |p, l| {
            for i in 0..l {
                unsafe { v(&mut init, *p.add(i)) };
            }
        });
        init
    }
    // 从oft开始访问，走到until返回false
    // 只有until返回true的数据都会被访问
    #[inline]
    pub fn fold_until<I>(
        &self,
        oft: usize,
        mut init: I,
        mut v: impl FnMut(&mut I, u8),
        until: impl Fn(u8) -> bool,
    ) -> I {
        let mut visit = |p: *mut u8, l| -> bool {
            for i in 0..l {
                let c = unsafe { *p.add(i) };
                if !until(c) {
                    return false;
                }
                v(&mut init, c);
            }
            true
        };
        with_segment_oft!(
            self,
            oft,
            |p, l| {
                visit(p, l);
            },
            |p0, l0, p1, l1| {
                if visit(p0, l0) {
                    visit(p1, l1);
                }
            }
        );
        init
    }
    #[inline(always)]
    pub fn visit(&self, mut f: impl FnMut(u8)) {
        self.visit_segment_oft(0, |p, l| {
            for i in 0..l {
                unsafe { f(*p.add(i)) };
            }
        });
    }
    #[inline]
    pub fn copy_to<W: crate::BufWriter>(&self, oft: usize, w: &mut W) -> std::io::Result<()> {
        with_segment_oft!(
            self,
            oft,
            |p, l| w.write_all(from_raw_parts(p, l)),
            |p0, l0, p1, l1| { w.write_seg_all(from_raw_parts(p0, l0), from_raw_parts(p1, l1)) }
        )
    }
    #[inline]
    pub fn copy_to_vec(&self, v: &mut Vec<u8>) {
        // v.reserve(self.len());
        // self.visit_segment_oft(0, |p, l| unsafe {
        //     copy_nonoverlapping(p, v.as_mut_ptr().add(v.len()), l);
        //     v.set_len(v.len() + l);
        // });

        // TODO 参考上面的逻辑，测试稳定前暂不清理 fishermen
        v.reserve(self.len());
        let len = v.len();
        let len_final = len + self.len();
        // 先设置长度，再用切片方式调用，避免越界
        unsafe {
            v.set_len(len_final);
        }
        self.copy_to_slice(&mut v[len..len_final]);
    }
    #[inline]
    pub fn copy_to_vec_with_len(&self, v: &mut Vec<u8>, len: usize) {
        self.copy_to_vec_with_oft_len(0, len, v)
    }
    // TODO 会多生成一个RingSlice，优化的空间有多大？ fishermen
    #[inline]
    pub fn copy_to_vec_with_oft_len(&self, oft: usize, len: usize, v: &mut Vec<u8>) {
        self.sub_slice(oft, len).copy_to_vec(v)
    }
    /// copy 数据到切片/数组中，目前暂时不需要oft，有需求后再加
    #[inline]
    pub fn copy_to_slice(&self, s: &mut [u8]) {
        with_segment_oft_len!(
            self,
            0,
            s.len(),
            |p, l| {
                copy_nonoverlapping(p, s.as_mut_ptr(), l);
            },
            |p0, l0, p1, l1| {
                copy_nonoverlapping(p0, s.as_mut_ptr(), l0);
                copy_nonoverlapping(p1, s.as_mut_ptr().add(l0), l1);
            }
        )
    }
    #[inline(always)]
    pub(super) fn cap(&self) -> usize {
        self.cap as usize
    }
    #[inline(always)]
    fn start(&self) -> usize {
        self.start as usize
    }

    #[inline(always)]
    fn mask(&self, oft: usize) -> usize {
        (self.mask & oft as u32) as usize
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len as usize
    }
    #[inline(always)]
    pub fn at(&self, idx: usize) -> u8 {
        self[idx]
    }
    #[inline(always)]
    pub fn update(&mut self, idx: usize, b: u8) {
        self[idx] = b;
    }
    #[inline(always)]
    pub(super) fn ptr(&self) -> *mut u8 {
        self.ptr as *mut u8
    }

    #[inline]
    pub fn find(&self, offset: usize, b: u8) -> Option<usize> {
        // TODO 先加上assert，安全第一 fishermen
        assert!(offset <= self.len(), "offset:{}, self:{:?}", offset, self);

        for i in offset..self.len() {
            if self[i] == b {
                return Some(i);
            }
        }
        None
    }
    // 查找是否存在 '\r\n' ，返回匹配的第一个字节地址
    #[inline]
    pub fn find_lf_cr(&self, offset: usize) -> Option<usize> {
        for i in offset..self.len() - 1 {
            // 先找'\r'
            if self[i] == b'\r' && self[i + 1] == b'\n' {
                return Some(i);
            }
        }
        None
    }
    #[inline]
    pub fn start_with(&self, oft: usize, s: &[u8]) -> bool {
        if oft + s.len() <= self.len() {
            with_segment_oft!(
                self,
                oft,
                |p, _l| { from_raw_parts(p, s.len()) == s },
                |p0, l0, p1, _l1| {
                    if l0 < s.len() {
                        from_raw_parts(p0, l0) == &s[..l0]
                            && from_raw_parts(p1, s.len() - l0) == &s[l0..]
                    } else {
                        from_raw_parts(p0, s.len()) == s
                    }
                }
            )
        } else {
            false
        }
    }

    // 读取一个u16的数字，大端
    #[inline(always)]
    pub fn read_u16(&self, oft: usize) -> u16 {
        debug_assert!(self.len() >= oft + 2);
        (self[oft] as u16) << 8 | self[oft + 1] as u16
    }

    /// 展示所有内容，仅用于长度比较小的场景 fishermen
    #[inline]
    pub fn as_string_lossy(&self) -> String {
        // 可能存在大内存copy，调用处增加assert，避免长度过大场景
        // debug_assert!(self.len() < 512);
        if self.len() >= 512 {
            log::warn!("as_string_lossy: data too long: {:?}", self);
        }

        let (l, r) = self.data();
        match r.len() {
            0 => String::from_utf8_lossy(l).into(),
            _ => {
                let mut data = Vec::with_capacity(self.len());
                self.copy_to_vec(&mut data);
                String::from_utf8_lossy(data.as_slice()).into()
            }
        }
    }
}

use std::convert::TryInto;
macro_rules! define_read_number {
    (le $($ty:ty)+) => {
        $(paste::paste! {
            define_read_number!([<read_ $ty _le_cmp>], std::mem::size_of::<$ty>(), 0, $ty, $ty, from_le_bytes);
        })+
    };
    (be $($ty:ty)+) => {
        $(paste::paste! {
            define_read_number!([<read_ $ty _be_cmp>], std::mem::size_of::<$ty>(), 0, $ty, $ty, from_be_bytes);
        })+
    };
    ($($ty:ty)+) => {
        define_read_number!(le $($ty)+);
        define_read_number!(be $($ty)+);
    };
    (ole $($actual_ty:ty, $ty:ty, $name:ident, $bits:literal);+) => {
        $(paste::paste! {
            define_read_number!([<read_ $name _le_cmp>], $bits / 8, 0, $actual_ty, $ty, from_le_bytes);
        })+
    };
    (obe $($actual_ty:ty, $ty:ty, $name:ident, $bits:literal);+) => {
        $(paste::paste! {
            define_read_number!([<read_ $name _be_cmp>], $bits / 8, std::mem::size_of::<$ty>() * 8 - $bits, $actual_ty, $ty, from_be_bytes);
        })+
    };
    ($fn:ident, $bytes:expr, $rshift:expr, $actual_ty:tt, $ty:tt, $which:ident) => {
        #[inline]
        pub fn $fn(&self, oft: usize) -> $actual_ty {
            debug_assert!(oft + $bytes <= self.len());
            let start = oft + self.start();
            const SIZE: usize = std::mem::size_of::<$ty>();
            let v = if start + SIZE <= self.cap() {
                let b = unsafe { from_raw_parts(self.ptr().add(start), SIZE) };
                $ty::$which(b.try_into().unwrap()) >> $rshift
            } else {
                use copy_nonoverlapping as copy;
                // 分段读取
                let mut b = [0u8; SIZE];
                let start = self.mask(start);
                const OFT:usize = $rshift / 8;
                let len = (self.cap() - start).min($bytes);
                unsafe { copy(self.ptr().add(start), b.as_mut_ptr().add(OFT), len) };
                unsafe { copy(self.ptr(), b.as_mut_ptr().add(len + OFT), $bytes - len) };
                $ty::$which(b)
            };
            const SHIFT: usize = std::mem::size_of::<$actual_ty>() * 8 - $bytes * 8;
            // 保留符号位
            (v << SHIFT) as $actual_ty >> SHIFT
        }
    };
    ($fn_name:ident, $type_name:tt::$type_fn:ident) => {
        #[inline]
        pub fn $fn_name(&self, oft: usize) -> $type_name {
            const SIZE: usize = std::mem::size_of::<$type_name>();
            debug_assert!(self.len() >= oft + SIZE);
            let oft_start = self.mask(oft + self.start());
            let len = self.cap() - oft_start; // 从oft_start到cap的长度
            if len >= SIZE {
                let b = unsafe { from_raw_parts(self.ptr().add(oft_start), SIZE) };

                // $type_name::from_be_bytes(b[..SIZE].try_into().unwrap())
                $type_name::$type_fn(b[..SIZE].try_into().unwrap())
            } else {
                // 分段读取
                let mut b = [0u8; SIZE];
                use copy_nonoverlapping as copy;
                unsafe { copy(self.ptr().add(oft_start), b.as_mut_ptr(), len) };
                unsafe { copy(self.ptr(), b.as_mut_ptr().add(len), SIZE - len) };

                // $type_name::from_be_bytes(b)

                $type_name::$type_fn(b)
            }
        }
    };

    // 注意这里的offset，是转成目标类型时，在目标类型字节中的偏移
    ($name:ident, $size:literal, $offset:literal, $t:tt::$fn:ident) => {
        #[inline]
        #[doc = "读取指定偏移、字节的数字，仅仅支持小端"]
        pub fn $name(&self, oft: usize) -> $t {
            // pub fn $name(&mut self, oft: usize) -> $t {
            const SIZE: usize = $size;
            debug_assert!(self.len() >= (oft + SIZE));
            let mut x: $t = 0;
            // let bytes = self.eat(SIZE);
            // for (i, b) in bytes.iter().enumerate() {
            //     x |= (*b as $t) << ((8 * i) + (8 * $offset));
            // }
            // $t::$fn(x)

            // eat 放到外层去处理，ringslice后续改为静态类型？ fishermen
            // let slice = self.eat(SIZE);
            for i in 0..SIZE {
                let b = self.at(oft + i);
                x |= (b as $t) << ((8 * i) + (8 * $offset));
            }
            $t::$fn(x)
        }
    };
}

impl RingSlice {
    define_read_number!(u16 u32 u64);
    define_read_number!(le i16 i32 i64);
    define_read_number!(
        ole  u32, u32, u24, 24;
             i32, u32, i24, 24;
             u64, u64, u48, 48;
             u64, u64, u56, 56;
             i64, u64, i56, 56
    );
    define_read_number!(
        obe  u32, u32, u24, 24;
             i32, u32, i24, 24;
             u64, u64, u48, 48;
             u64, u64, u56, 56;
             i64, u64, i56, 56
    );

    // little endian
    define_read_number!(read_u8, u8::from_le_bytes);
    define_read_number!(read_i8, i8::from_le_bytes);
    define_read_number!(read_u16_le, u16::from_le_bytes);
    define_read_number!(read_i16_le, i16::from_le_bytes);
    define_read_number!(read_u24_le, 3, 0, u32::from_le);
    define_read_number!(read_i24_le, 3, 0, i32::from_le);
    define_read_number!(read_u32_le, u32::from_le_bytes);
    define_read_number!(read_i32_le, i32::from_le_bytes);
    define_read_number!(read_u48_le, 6, 0, u64::from_le);
    define_read_number!(read_u56_le, 7, 0, u64::from_le);
    define_read_number!(read_i56_le, 7, 0, i64::from_le);
    define_read_number!(read_u64_le, u64::from_le_bytes);
    define_read_number!(read_i64_le, i64::from_le_bytes);
    define_read_number!(read_f32_le, f32::from_le_bytes);
    define_read_number!(read_f64_le, f64::from_le_bytes);

    // big endian
    define_read_number!(read_u16_be, u16::from_be_bytes);
    define_read_number!(read_u32_be, u32::from_be_bytes);
    define_read_number!(read_u64_be, u64::from_be_bytes);
}

impl From<&[u8]> for RingSlice {
    #[inline]
    fn from(s: &[u8]) -> Self {
        // TODO 诸如quite/quit指令的响应无需内容，可能会存在0长度的data，关注是否有副作用 fishermen
        // assert_ne!(s.len(), 0);
        let cap = s.len().next_power_of_two();
        Self::from(s.as_ptr() as *mut u8, cap, 0, s.len())
    }
}

impl Display for RingSlice {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ptr:{} start:{} len:{} cap:{}",
            self.ptr, self.start, self.len, self.cap
        )
    }
}
impl Debug for RingSlice {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use crate::Utf8;
        let slice = self.sub_slice(0, 512.min(self.len()));
        let mut data = Vec::with_capacity(slice.len());
        slice.copy_to_vec(&mut data);
        write!(
            f,
            "ptr:{} start:{} len:{} cap:{} => {:?}",
            self.ptr,
            self.start,
            self.len,
            self.cap,
            data.utf8()
        )
    }
}

impl std::ops::Index<usize> for RingSlice {
    type Output = u8;
    // idx < len. 否则UB
    #[inline(always)]
    fn index(&self, idx: usize) -> &Self::Output {
        debug_assert!(idx < self.len());
        unsafe { &*self.ptr().add(self.mask(self.start() + idx)) }
    }
}
impl std::ops::IndexMut<usize> for RingSlice {
    // idx < len. 否则UB
    #[inline(always)]
    fn index_mut(&mut self, idx: usize) -> &mut Self::Output {
        debug_assert!(idx < self.len());
        unsafe { &mut *self.ptr().add(self.mask(self.start() + idx)) }
    }
}

impl PartialEq<[u8]> for super::RingSlice {
    #[inline]
    fn eq(&self, other: &[u8]) -> bool {
        self.len() == other.len() && self.start_with(0, other)
    }
}
// 内容相等
impl PartialEq<Self> for super::RingSlice {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        let (f, s) = other.data_oft(0);
        self.len() == other.len() && self.start_with(0, f) && self.start_with(f.len(), s)
    }
}
impl PartialEq<(&[u8], &[u8])> for super::RingSlice {
    #[inline]
    fn eq(&self, other: &(&[u8], &[u8])) -> bool {
        let (f, s) = self.data_oft(0);
        f == other.0 && s == other.1
    }
}
// TODO Ord 对RingSlice怪怪的，目前只是为了满足kv需要，需要考虑统一去掉？fishermen
impl Ord for RingSlice {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.len().cmp(&other.len())
    }
}
impl PartialOrd for RingSlice {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
