use std::{
    fmt::{Debug, Display, Formatter},
    slice::from_raw_parts,
};

use crate::{BufWriter, Range};

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
    ($self:ident, $oft:expr, $len:expr, $noseg:expr, $seg:expr) => {{
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
    ($self:ident, $oft:expr, $noseg:expr, $seg:expr) => {
        with_segment!($self, $oft, $self.len() - $oft, $noseg, $seg)
    };
    ($self:ident, $noseg:expr, $seg:expr) => {
        with_segment!($self, 0, $self.len(), $noseg, $seg)
    };
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
        with_segment!(self, oft, len, |p, l| v(p, l), |p0, l0, p1, l1| {
            v(p0, l0);
            v(p1, l1);
        });
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
    pub fn data_oft_len(&self, oft: usize, len: usize) -> (&[u8], &[u8]) {
        self.data_r(oft..oft + len)
        //assert!(oft + len <= self.len(), "{}/{} =>{:?}", oft, len, self);

        //static EMPTY: &[u8] = &[];
        //with_segment!(
        //    self,
        //    oft,
        //    len,
        //    |ptr, len| (from_raw_parts(ptr, len), EMPTY),
        //    |p0, l0, p1, l1| (from_raw_parts(p0, l0), from_raw_parts(p1, l1))
        //)
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
        with_segment!(
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
        //v.reserve(self.len());
        //let start = v.len();
        //let end = start + self.len();
        //// 先设置长度，再用切片方式调用，避免越界
        //unsafe { v.set_len(end) };
        //self.copy_to_slice(&mut v[start..end]);
    }
    #[inline]
    pub fn copy_to_vec_with_len(&self, v: &mut Vec<u8>, len: usize) {
        self.copy_to_w(..len, v);
        //self.copy_to_vec_with_oft_len(0, len, v)
    }
    // TODO 会多生成一个RingSlice，优化的空间有多大？ fishermen
    #[inline]
    pub fn copy_to_vec_with_oft_len(&self, oft: usize, len: usize, v: &mut Vec<u8>) {
        self.copy_to_w(oft..oft + len, v);
        //self.sub_slice(oft, len).copy_to_vec(v)
    }
    #[inline]
    pub fn copy_to_vec_r<R: Range>(&self, v: &mut Vec<u8>, r: R) {
        self.copy_to_w(r, v);
        //let (start, end) = self.range(r);
        //let len = end - start;
        //let org_len = v.len();
        //v.reserve(len);
        //let total_len = org_len + len;
        //unsafe { v.set_len(total_len) };
        //self.copy_to_r(&mut v[org_len..total_len], start..end);
    }
    /// copy 数据到切片/数组中，目前暂时不需要oft，有需求后再加
    #[inline]
    pub fn copy_to_slice(&self, s: &mut [u8]) {
        self.copy_to_w(.., s);
        //with_segment!(
        //    self,
        //    |p, l| {
        //        copy_nonoverlapping(p, s.as_mut_ptr(), l);
        //    },
        //    |p0, l0, p1, l1| {
        //        copy_nonoverlapping(p0, s.as_mut_ptr(), l0);
        //        copy_nonoverlapping(p1, s.as_mut_ptr().add(l0), l1);
        //    }
        //)
    }
    #[inline]
    pub fn copy_to_cmp(&self, s: &mut [u8], oft: usize, len: usize) {
        //self.copy_to_w(oft..oft + len, s);
        use std::ptr::copy_nonoverlapping;
        with_segment!(
            self,
            oft,
            len,
            |p, l| {
                copy_nonoverlapping(p, s.as_mut_ptr(), l);
            },
            |p0, l0, p1, l1| {
                copy_nonoverlapping(p0, s.as_mut_ptr(), l0);
                copy_nonoverlapping(p1, s.as_mut_ptr().add(l0), l1);
            }
        )
    }
    //#[inline(always)]
    //fn range<R: RangeBounds<usize>>(&self, r: R) -> (usize, usize) {
    //    let start = match r.start_bound() {
    //        Included(&s) => s,
    //        Excluded(&s) => s + 1,
    //        Unbounded => 0,
    //    };
    //    let end = match r.end_bound() {
    //        Included(&e) => e + 1,
    //        Excluded(&e) => e,
    //        Unbounded => self.len(),
    //    };
    //    debug_assert!(end <= self.len());
    //    debug_assert!(start <= end);
    //    (start, end)
    //}
    #[inline]
    pub fn copy_to_r<R: Range>(&self, s: &mut [u8], r: R) {
        self.copy_to_w(r, s);
        //let mut oft = 0;
        //self.visit_seg(r, |p, l| {
        //    debug_assert!(oft + l <= s.len());
        //    unsafe { copy_nonoverlapping(p, s.as_mut_ptr().add(oft), l) };
        //    oft += l;
        //});
    }
    #[inline(always)]
    pub fn visit_seg<R: Range>(&self, r: R, mut f: impl FnMut(*const u8, usize)) {
        let (start, end) = r.range(self);
        debug_assert!(end <= self.len());
        debug_assert!(start <= end);

        if start == end {
            return;
        }
        let oft_start = self.mask(self.start() + start);
        let oft_end = self.mask(self.start() + end);
        if oft_start < oft_end {
            f(unsafe { self.ptr().add(oft_start) }, end - start);
        } else {
            f(unsafe { self.ptr().add(oft_start) }, self.cap() - oft_start);
            f(self.ptr(), oft_end);
        }
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
            with_segment!(
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
    pub fn u16_be(&self, oft: usize) -> u16 {
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

    #[inline(always)]
    pub fn reader(&self) -> RingSliceRead<'_> {
        RingSliceRead { oft: 0, rs: self }
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
        let (f, s) = other.data_oft(0);
        self.len() == other.len() && self.start_with(0, f) && self.start_with(f.len(), s)
    }
}
//impl PartialEq<(&[u8], &[u8])> for super::RingSlice {
//    #[inline]
//    fn eq(&self, other: &(&[u8], &[u8])) -> bool {
//        let (f, s) = self.data_oft(0);
//        f == other.0 && s == other.1
//    }
//}

pub struct RingSliceRead<'a> {
    oft: usize,
    rs: &'a RingSlice,
}

impl<'a> crate::BuffRead for RingSliceRead<'a> {
    type Out = usize;
    #[inline(always)]
    fn read(&mut self, b: &mut [u8]) -> (usize, Self::Out) {
        let len = b.len().min(self.rs.len() - self.oft);
        self.rs.copy_to_r(b, self.oft..len);
        (len, len)
    }
}

impl<'a> std::io::Read for RingSliceRead<'a> {
    #[inline(always)]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let (size, _) = crate::BuffRead::read(self, buf);
        Ok(size)
    }
}
