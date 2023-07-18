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
        // debug_assert!($oft <= $self.len());
        // let oft_start = $self.mask($self.start() + $oft);
        // let len = $self.len() - $oft;

        // if oft_start + len <= $self.cap() {
        //     unsafe { $noseg($self.ptr().add(oft_start), len) }
        // } else {
        //     let seg1 = $self.cap() - oft_start;
        //     let seg2 = len - seg1;
        //     unsafe { $seg($self.ptr().add(oft_start), seg1, $self.ptr(), seg2) }
        // }
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
        // with_segment_oft!(self, oft, |p, l| v(p, l), |p0, l0, p1, l1| {
        //     v(p0, l0);
        //     v(p1, l1);
        // });
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
        // static EMPTY: &[u8] = &[];
        // with_segment_oft!(
        //     self,
        //     oft,
        //     |ptr, len| (from_raw_parts(ptr, len), EMPTY),
        //     |p0, l0, p1, l1| (from_raw_parts(p0, l0), from_raw_parts(p1, l1))
        // )
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
        if len == self.len() {
            // 一般场景，都是copy完整的slice到vector
            self.copy_to_vec(v);
        } else if len < self.len() {
            // 小概率场景，len较小，只copy部分slice到vector
            self.sub_slice(0, len).copy_to_vec(v);
        } else {
            // 基本不会走到这里，除非调用出现bug
            assert!(false, "too big len:{} => {:?}", len, self);
        }
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
    // /// copy 前len个bytes 到 BufMut，注意check len的长度
    // #[inline]
    // pub fn copy_to_bufmut(&self, buf: &mut dyn BufMut, len: usize) {
    //     let (l, r) = self.data();
    //     if len <= l.len() {
    //         buf.put_slice(&l[..len]);
    //         return;
    //     }

    //     // len大于l.len
    //     buf.put_slice(l);
    //     let rmin = r.len().min(len - l.len());
    //     buf.put_slice(&r[..rmin]);
    // }
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

    #[inline(always)]
    pub fn read_num_be(&self, oft: usize) -> u64 {
        const SIZE: usize = std::mem::size_of::<u64>();
        assert!(self.len() >= oft + SIZE);
        with_segment_oft!(
            self,
            oft,
            |ptr, _len| u64::from_be_bytes(from_raw_parts(ptr, SIZE)[..SIZE].try_into().unwrap()),
            |ptr0, len0, ptr1, _len1| {
                if len0 >= SIZE {
                    u64::from_be_bytes(from_raw_parts(ptr0, SIZE)[..SIZE].try_into().unwrap())
                } else {
                    let mut b = [0u8; SIZE];
                    copy_nonoverlapping(ptr0, b.as_mut_ptr(), len0);
                    copy_nonoverlapping(ptr1, b.as_mut_ptr().add(len0), SIZE - len0);
                    u64::from_be_bytes(b)
                }
            }
        )
    }
    // 读取一个u16的数字，大端
    #[inline(always)]
    pub fn read_u16(&self, oft: usize) -> u16 {
        debug_assert!(self.len() >= oft + 2);
        (self[oft] as u16) << 8 | self[oft + 1] as u16
    }

    /// 尝试低成本获取一个单向的切片，如果有折返，则返回None
    #[inline]
    pub fn try_oneway_slice(&self, oft: usize, len: usize) -> Option<&[u8]> {
        // 根据oft、len拿到两段数据，第二段可能为空
        let (l, r) = self.data_oft_len(oft, len);

        // 如果第二段长度为0，说明是单向slice
        if r.len() == 0 {
            Some(l)
        } else {
            None
        }
    }

    /// dump折返字节【必须包含折返】到目标dest vec中
    /// 注意：目标数据必须有折返，否则assert失败；
    /// 使用姿势：先使用try_oneway_slice尝试获取单向切片数据，失败后，再使用本方法；
    #[inline]
    pub fn dump_ring_part(&self, oft: usize, len: usize) -> Vec<u8> {
        let start_mask = self.mask(self.start() + oft);
        assert!(start_mask + len > self.cap(), "{}/{}=>{:?}", oft, len, self);

        // copy 2 段到一个vec中
        let mut dest = Vec::with_capacity(len);
        let len_first = self.cap() - start_mask;
        unsafe { copy_nonoverlapping(self.ptr().add(oft), dest.as_mut_ptr(), len_first) };
        unsafe {
            copy_nonoverlapping(
                self.ptr(),
                dest.as_mut_ptr().add(len_first),
                len - len_first,
            );
        }
        // dest原始长度为0，所以此处直接设为len即可
        unsafe { dest.set_len(len) };
        log::debug!("+++ copy ring part data to vec: {:?}", dest);

        dest
    }

    // 先注释掉，换条路，进一步延迟copy的时机 fishermen
    // /// <pre>获取有限制长度的slice，最大长度暂定为32字节，超过就panic;
    // /// 前置条件：
    // ///     1 len不可以超过32；
    // ///     2 oft+len不可超过本slice的最大长度[slice的基本要求] </pre>
    // #[inline(always)]
    // pub unsafe fn limited_slice(&self, oft: usize, len: usize) -> &[u8] {
    //     const MAX_LEN: usize = 32;
    //     if oft + len > self.len() || len > MAX_LEN {
    //         panic!("may out boundary: {}/{} =>{:?}", oft, len, self.data_dump());
    //     }

    //     // oft、end是相对位置，需要进行mask修正
    //     let oft_start = self.mask(self.start() + oft);
    //     if oft_start + len <= self.cap() {
    //         return from_raw_parts(self.ptr().add(oft_start), len);
    //     } else {
    //         // 有折返，需要组装
    //         static mut TMP_BUFF: [u8; 16] = [0_u8; 16];
    //         let len_first = self.cap() - oft_start;
    //         copy_nonoverlapping(self.ptr().add(oft_start), TMP_BUFF.as_mut_ptr(), len_first);
    //         copy_nonoverlapping(
    //             self.ptr(),
    //             TMP_BUFF[len_first..].as_mut_ptr(),
    //             len - len_first,
    //         );
    //         return &TMP_BUFF;
    //     }
    // }

    /// 以指定的位置将slice分拆为2部分，返回[0, n）
    #[inline]
    pub fn eat(&mut self, n: usize) -> Self {
        let eaten = self.sub_slice(0, n);
        self.skip(n);

        eaten
    }

    #[inline]
    pub fn skip(&mut self, n: usize) {
        assert!(n <= self.len(), "too big to skip: {}/{:?}", n, self);

        // 如果RingSlice的结构变化，注意审视这里是否需要调整 fishermen
        self.start = self.mask(self.start() + n) as u32;
        self.len = self.len - n as u32;
    }
}

//unsafe impl Send for RingSlice {}
//unsafe impl Sync for RingSlice {}

use std::convert::TryInto;
macro_rules! define_read_number {
    ($fn_name:ident, $type_name:tt) => {
        #[inline]
        pub fn $fn_name(&self, oft: usize) -> $type_name {
            const SIZE: usize = std::mem::size_of::<$type_name>();
            debug_assert!(self.len() >= oft + SIZE);
            let oft_start = self.mask(oft + self.start());
            let len = self.cap() - oft_start; // 从oft_start到cap的长度
            if len >= SIZE {
                let b = unsafe { from_raw_parts(self.ptr().add(oft_start), SIZE) };
                $type_name::from_be_bytes(b[..SIZE].try_into().unwrap())
            } else {
                // 分段读取
                let mut b = [0u8; SIZE];
                use copy_nonoverlapping as copy;
                unsafe { copy(self.ptr().add(oft_start), b.as_mut_ptr(), len) };
                unsafe { copy(self.ptr(), b.as_mut_ptr().add(len), SIZE - len) };
                $type_name::from_be_bytes(b)
            }
        }
    };
}

impl RingSlice {
    define_read_number!(read_u32, u32);
    define_read_number!(read_u64, u64);
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

// / RingSlice增加对Iterator的支持
// / 注意：当前只需更新start、len，如果后续RingSlice结构变化，注意审视该方法是否需要变化 fishermen
// impl Iterator for RingSlice {
//     type Item = u8;
//     fn next(&mut self) -> Option<Self::Item> {
//         if self.len() > 0 {
//             let val = self.at(0);
//             self.start = self.mask(self.start() + 1) as u32;
//             self.len = self.len - 1;

//             Some(val)
//         } else {
//             None
//         }
//     }
// }
