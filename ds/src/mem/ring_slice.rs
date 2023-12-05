use std::{
    fmt::{Debug, Display, Formatter},
    slice::from_raw_parts,
};

use crate::{BufWriter, Range, Visit};

//从不拥有数据，是对ptr+start的引用
#[derive(Default, Clone, Copy, Eq, Hash)]
pub struct RingSlice {
    ptr: usize,
    cap: u32,
    start: u32,
    len: u32,
    mask: u32,
}

macro_rules! with_segment {
    ($self:ident, $range:expr, $noseg:expr, $seg:expr) => {{
        let (oft, end) = $range.range($self);
        debug_assert!(oft <= end && end <= $self.len());
        let len = end - oft;
        let oft_start = $self.mask($self.start() + oft);
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
    #[inline]
    pub fn empty() -> Self {
        Self::default()
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
    /// 注意：本方法用vec的capacity作为cap，方便ManuallyDrop结构的恢复及正确drop
    #[inline(always)]
    pub fn from_vec(data: &Vec<u8>) -> Self {
        let mut mem: RingSlice = data.as_slice().into();
        // 这里面的cap是真实的cap
        mem.cap = data.capacity() as u32;
        mem
    }

    /// 注意：对于Vec请使用from_vec，本方法直接用slice的长度作为cap，对ManuallyDrop结构无法友好支持
    #[inline(always)]
    pub fn from_slice(data: &[u8]) -> Self {
        let mut mem: RingSlice = data.into();
        // 这里面的cap是真实的cap
        mem.cap = data.len() as u32;
        mem
    }
    #[inline(always)]
    pub fn slice(&self, offset: usize, len: usize) -> RingSlice {
        self.sub_slice(offset, len)
    }
    #[inline]
    pub fn str_num(&self, r: impl Range) -> usize {
        let (start, end) = r.range(self);
        let mut num = 0usize;
        for i in start..end {
            num = num.wrapping_mul(10) + (self[i] - b'0') as usize;
        }
        num
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
    pub fn visit(&self, mut f: impl FnMut(u8)) {
        self.visit_seg(0, |p, l| {
            for i in 0..l {
                unsafe { f(*p.add(i)) };
            }
        });
    }
    #[inline(always)]
    pub fn visit_seg<R: Range>(&self, r: R, mut f: impl FnMut(*const u8, usize)) {
        with_segment!(self, r, |p, l| f(p, l), |p0, l0, p1, l1| {
            f(p0, l0);
            f(p1, l1)
        })
    }
    #[inline(always)]
    pub fn visit_data(&self, r: impl Range, mut f: impl FnMut(&[u8])) {
        with_segment!(self, r, |p, l| f(from_raw_parts(p, l)), |p0, l0, p1, l1| {
            {
                f(from_raw_parts(p0, l0));
                f(from_raw_parts(p1, l1));
            }
        })
    }
    #[inline(always)]
    pub fn data_r(&self, r: impl Range) -> (&[u8], &[u8]) {
        static EMPTY: &[u8] = &[];
        with_segment!(
            self,
            r,
            |ptr, len| (from_raw_parts(ptr, len), EMPTY),
            |p0, l0, p1, l1| (from_raw_parts(p0, l0), from_raw_parts(p1, l1))
        )
    }
    #[inline(always)]
    pub fn data(&self) -> (&[u8], &[u8]) {
        self.data_r(0)
    }

    // 特殊情况下，打印合法字节，以及buff中全部的字节
    pub unsafe fn data_dump(&self) -> &[u8] {
        from_raw_parts(self.ptr(), self.cap())
    }
    #[inline(always)]
    pub fn fold_r<I, R: Range, V: FnMut(&mut I, u8) -> bool>(
        &self,
        r: R,
        mut init: I,
        mut v: V,
    ) -> I {
        macro_rules! visit {
            ($p:expr, $l:expr) => {{
                for i in 0..$l {
                    if !v(&mut init, *$p.add(i)) {
                        return false;
                    }
                }
                true
            }};
        }
        with_segment!(
            self,
            r,
            |p: *mut u8, l| visit!(p, l),
            |p0: *mut u8, l0, p1: *mut u8, l1| { visit!(p0, l0) && visit!(p1, l1) }
        );
        init
    }
    #[inline(always)]
    pub fn fold<I, R: Range>(&self, r: R, init: I, mut v: impl FnMut(&mut I, u8)) -> I {
        self.fold_r(r, init, |i, b| {
            v(i, b);
            true
        })
    }
    #[inline]
    pub fn copy_to<W: BufWriter + ?Sized, R: Range>(&self, r: R, w: &mut W) -> std::io::Result<()> {
        with_segment!(
            self,
            r,
            |p, l| w.write_all(from_raw_parts(p, l)),
            |p0, l0, p1, l1| { w.write_seg_all(from_raw_parts(p0, l0), from_raw_parts(p1, l1)) }
        )
    }
    #[inline]
    pub fn copy_to_w<W: BufWriter + ?Sized, R: Range>(&self, r: R, w: &mut W) {
        let _r = self.copy_to(r, w);
        debug_assert!(_r.is_ok());
    }
    #[inline]
    pub fn copy_to_vec(&self, v: &mut Vec<u8>) {
        self.copy_to_w(.., v);
    }
    #[inline]
    pub fn copy_to_vec_with_len(&self, v: &mut Vec<u8>, len: usize) {
        self.copy_to_w(..len, v);
    }
    #[inline]
    pub fn copy_to_vec_r<R: Range>(&self, v: &mut Vec<u8>, r: R) {
        self.copy_to_w(r, v);
    }
    /// copy 数据到切片/数组中，目前暂时不需要oft，有需求后再加
    #[inline]
    pub fn copy_to_slice(&self, s: &mut [u8]) {
        self.copy_to_w(.., s);
    }
    #[inline]
    pub fn copy_to_r<R: Range>(&self, s: &mut [u8], r: R) {
        self.copy_to_w(r, s);
    }
    #[inline(always)]
    pub(super) fn cap(&self) -> usize {
        self.cap as usize
    }
    #[inline(always)]
    pub(super) fn start(&self) -> usize {
        self.start as usize
    }

    #[inline(always)]
    pub(super) fn mask(&self, oft: usize) -> usize {
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
        self.find_r(offset, b)
    }
    #[inline]
    pub fn find_r(&self, r: impl Range, mut f: impl Visit) -> Option<usize> {
        let (start, end) = r.range(self);
        for i in start..end {
            if f.check(self[i], i) {
                return Some(i);
            }
        }
        None
    }
    // 跳过num个'\r\n'，返回下一个字节地址
    #[inline]
    pub fn skip_lf_cr(&self, mut oft: usize, num: usize) -> Option<usize> {
        for _ in 0..num {
            oft = self.find_lf_cr(oft)? + 2;
        }
        Some(oft)
    }
    // 查找是否存在 '\r\n' ，返回匹配的第一个字节地址
    #[inline]
    pub fn find_lf_cr(&self, offset: usize) -> Option<usize> {
        self.find_r(offset..self.len() - 1, |b, idx| {
            b == b'\r' && self[idx + 1] == b'\n'
        })
    }
    #[inline]
    pub fn start_with(&self, oft: usize, s: &[u8]) -> bool {
        if oft + s.len() <= self.len() {
            with_segment!(
                self,
                oft..oft + s.len(),
                |p, _l| { from_raw_parts(p, s.len()) == s },
                |p0, l0, p1, _l1| from_raw_parts(p0, l0) == &s[..l0]
                    && from_raw_parts(p1, s.len() - l0) == &s[l0..]
            )
        } else {
            false
        }
    }

    // 读取一个u16的数字，大端
    #[inline(always)]
    pub fn u16_be(&self, oft: usize) -> u16 {
        debug_assert!(self.len() >= oft + 2);
        (self[oft] as u16) << 8 | self[oft + 1] as u16
    }

    /// 展示所有内容，仅用于长度比较小的场景 fishermen
    #[inline]
    pub fn as_string_lossy(&self) -> String {
        if self.len() >= 512 {
            log::warn!("as_string_lossy: data too long: {:?}", self);
        }
        let mut vec = Vec::with_capacity(self.len());
        self.copy_to_w(0, &mut vec);
        String::from_utf8(vec).unwrap_or_default()
    }
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
        let mut data = Vec::with_capacity(self.len().min(512));
        self.copy_to_vec(&mut data);
        write!(f, "{} => {:?}", self, data.utf8())
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
        let (f, s) = other.data_r(0);
        self.len() == other.len() && self.start_with(0, f) && self.start_with(f.len(), s)
    }
}
