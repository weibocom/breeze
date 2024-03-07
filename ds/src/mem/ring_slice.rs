use std::{
    fmt::{Debug, Display, Formatter},
    ptr::copy_nonoverlapping,
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
        self.fold(r, 0, |num, b| {
            *num = num.wrapping_mul(10) + (b - b'0') as usize
        })
    }
    // 如果有一个非数字的字符，则返回None
    #[inline]
    pub fn try_str_num(&self, r: impl Range) -> Option<usize> {
        let mut digit = true;
        let num = self.fold_r(r, 0usize, |num, b| {
            digit &= b.is_ascii_digit();
            if digit {
                *num = num.wrapping_mul(10) + (b - b'0') as usize;
            }
            !digit
        });
        digit.then_some(num)
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
    // 这个方法是最核心的方法，所有的方法都是基于这个方法实现的
    // F:ptr, len, oft, segment
    #[inline]
    fn _visit<R, O, F>(&self, r: R, mut f: F) -> O
    where
        O: Merge,
        R: Range,
        F: FnMut(*const u8, usize, usize, bool) -> O,
    {
        let (oft, end) = r.range(self);
        debug_assert!(oft <= end && end <= self.len());
        let len = end - oft;
        let oft_start = self.mask as usize & (self.start() + oft);
        let seg1 = self.cap() - oft_start;
        if len <= seg1 {
            unsafe { f(self.ptr().add(oft_start), len, oft, false) }
        } else {
            let o0 = unsafe { f(self.ptr().add(oft_start), seg1, oft, true) };
            let seg2 = len - seg1;
            o0.merge(|| f(self.ptr(), seg2, self.cap() - self.start(), true))
        }
    }
    #[inline(always)]
    fn _visit_data<R, O, F>(&self, r: R, mut f: F) -> O
    where
        O: Merge,
        R: Range,
        F: FnMut(&[u8], usize, bool) -> O,
    {
        self._visit(r, |p, len, oft, seg| unsafe {
            f(from_raw_parts(p, len), oft, seg)
        })
    }
    #[inline(always)]
    pub fn visit(&self, mut f: impl FnMut(u8)) {
        self._visit(0usize, |p, len, _oft, _seg| {
            for i in 0..len {
                unsafe { f(*p.add(i)) };
            }
        })
    }
    #[inline(always)]
    pub fn visit_seg<R: Range>(&self, r: R, mut f: impl FnMut(*const u8, usize)) {
        self._visit(r, |p, len, _, _| f(p, len))
    }
    #[inline(always)]
    pub fn visit_data(&self, r: impl Range, mut f: impl FnMut(&[u8])) {
        self._visit(r, |p, len, _, _| f(unsafe { from_raw_parts(p, len) }))
    }
    #[inline(always)]
    pub fn data_r(&self, r: impl Range) -> (&[u8], &[u8]) {
        static EMPTY: &[u8] = &[];
        let mut seg = [EMPTY, EMPTY];
        let mut idx = 0;
        self._visit(r, |p, l, _idx, _seg| {
            seg[idx] = unsafe { from_raw_parts(p, l) };
            idx += 1;
        });
        (&seg[0], &seg[1])
    }
    #[inline(always)]
    pub fn data(&self) -> (&[u8], &[u8]) {
        self.data_r(0)
    }

    // 特殊情况下，打印合法字节，以及buff中全部的字节
    pub unsafe fn data_dump(&self) -> &[u8] {
        from_raw_parts(self.ptr(), self.cap())
    }
    // 遍历，直到v返回true时.
    #[inline(always)]
    pub fn fold_r<I, R: Range, V: FnMut(&mut I, u8) -> bool>(&self, r: R, init: I, mut v: V) -> I {
        let mut init = init;
        self._visit(r, |p, l, _oft, _seg| {
            for i in 0..l {
                if !v(&mut init, unsafe { *p.add(i) }) {
                    continue;
                }
                // 说明v返回true，则终止遍历
                return true;
            }
            // 尝试下一个seg
            false
        });
        init
    }
    // 遍历所有的数据。
    #[inline(always)]
    pub fn fold<I, R: Range>(&self, r: R, init: I, mut v: impl FnMut(&mut I, u8)) -> I {
        self.fold_r(r, init, |i, b| {
            v(i, b);
            false
        })
    }
    #[inline]
    pub fn copy_to<W: BufWriter + ?Sized, R: Range>(&self, r: R, w: &mut W) -> std::io::Result<()> {
        self._visit(r, |p, len, _, seg| {
            w.write_all_hint(unsafe { from_raw_parts(p, len) }, seg)
        })
    }
    #[inline]
    fn _copy_to<R: Range, T: AsMut<[u8]>>(&self, r: R, mut o: T) {
        let out = o.as_mut();
        let mut idx = 0;
        self._visit(r, |p, len, _, _| {
            unsafe { copy_nonoverlapping(p, out.as_mut_ptr().add(idx), len) };
            idx += len;
        });
    }
    #[inline]
    pub fn copy_to_w<W: BufWriter + ?Sized, R: Range>(&self, r: R, w: &mut W) {
        let _r = self.copy_to(r, w);
        debug_assert!(_r.is_ok());
    }
    #[inline]
    pub fn copy_to_vec(&self, v: &mut Vec<u8>) {
        self.copy_to_v(.., v);
    }
    #[inline]
    pub fn copy_to_vec_with_len(&self, v: &mut Vec<u8>, len: usize) {
        self.copy_to_v(..len, v);
    }
    #[inline]
    pub fn copy_to_v<R: Range>(&self, r: R, v: &mut Vec<u8>) {
        let rlen = r.r_len(self);
        v.reserve(rlen);
        let len = v.len();
        unsafe { v.set_len(len + rlen) };
        let out = &mut v[len..];
        self._copy_to(r, out);
    }
    /// copy 数据到切片/数组中，目前暂时不需要oft，有需求后再加
    #[inline]
    pub fn copy_to_slice(&self, s: &mut [u8]) {
        self._copy_to(0, s)
    }
    #[inline]
    pub fn copy_to_r<R: Range>(&self, r: R, s: &mut [u8]) {
        self._copy_to(r, s);
    }
    #[inline(always)]
    pub(super) fn cap(&self) -> usize {
        self.cap as usize
    }
    #[inline(always)]
    pub(super) fn start(&self) -> usize {
        debug_assert!(self.start < self.cap || self.start == 0, "{self}");
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
    pub fn first(&self) -> u8 {
        debug_assert!(self.len > 0);
        debug_assert!(self.start < self.cap);
        unsafe { *self.ptr() }
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
    // 找到满足f.check(b, idx)的第一个字节位置，返回idx
    #[inline]
    pub fn find_r(&self, r: impl Range, mut f: impl Visit) -> Option<usize> {
        self._visit(r, |p, len, oft, _seg| {
            for i in 0..len {
                let idx = oft + i;
                if f.check(unsafe { *p.add(i) }, idx) {
                    return Some(idx);
                }
            }
            None
        })
    }
    // 跳过num个'\r\n'，返回下一个字节地址
    #[inline]
    pub fn skip_lf_cr(&self, mut oft: usize, num: usize) -> Option<usize> {
        for _ in 0..num {
            oft = self.find_lf_cr(oft)? + 2;
        }
        Some(oft)
    }
    // 查找是否存在 '\r\n' ，返回'\r' 的位置
    #[inline]
    pub fn find_lf_cr(&self, offset: usize) -> Option<usize> {
        self.find_r(offset..self.len() - 1, |b, idx| {
            debug_assert!(idx < self.len() - 1, "{offset} => {idx}, {self} ");
            b == b'\r' && self[idx + 1] == b'\n'
        })
    }
    #[inline]
    pub fn equal(&self, other: &[u8]) -> bool {
        self.eq(other)
    }
    /// 判断是否相同，忽略大小写
    #[inline]
    pub fn equal_ignore_case(&self, other: &[u8]) -> bool {
        self.len() == other.len() && self.compare(0, other, |a, b| a.eq_ignore_ascii_case(b))
    }
    #[inline]
    fn compare(&self, r: impl Range, s: &[u8], eq: impl Fn(&[u8], &[u8]) -> bool) -> bool {
        debug_assert!(r.r_len(self) == s.len());
        let mut cmp = s;
        let mut matched = true;
        self._visit_data(r, |data, _, _| {
            let l = data.len().min(cmp.len());
            let d = &data[..l];
            matched &= eq(d, &cmp[..l]);
            cmp = &cmp[l..];
            // 还有未比对完成的数据，且当前数据不是最后一个数据，则继续比对
            cmp.len() == 0 || !matched
        });
        matched
    }
    #[inline]
    pub fn start_with(&self, oft: usize, s: &[u8]) -> bool {
        oft + s.len() <= self.len() && self.compare(oft..oft + s.len(), s, |a, b| a == b)
    }

    #[inline]
    pub fn start_ignore_case(&self, oft: usize, s: &[u8]) -> bool {
        oft + s.len() <= self.len()
            && self.compare(oft..oft + s.len(), s, |a, b| a.eq_ignore_ascii_case(b))
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
        let mut vec = Vec::with_capacity(self.len());
        self.copy_to_v(0, &mut vec);
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
        self.len() == other.len() && self.compare(0, other, |a, b| a == b)
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

use crate::{Merge, Slicer};
impl Slicer for RingSlice {
    #[inline(always)]
    fn len(&self) -> usize {
        self.len as usize
    }
    #[inline]
    fn with_seg<R: Range, O: Merge>(&self, r: R, v: impl FnMut(&[u8], usize, bool) -> O) -> O {
        self._visit_data(r, v)
    }
}
