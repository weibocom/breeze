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

    // TODO 测试完毕后，清理dead code
    // /// 尝试低成本获取一个单向的切片，如果有折返，则返回None
    // #[inline]
    // pub fn try_oneway_slice(&self, oft: usize, len: usize) -> Option<&[u8]> {
    //     // 根据oft、len拿到两段数据，如果第二段长度为0，说明是单向slice
    //     let (l, r) = self.data_oft_len(oft, len);
    //     match r.len() {
    //         0 => Some(l),
    //         _ => None,
    //     }
    // }

    // /// dump折返字节【必须包含折返】到目标dest vec中
    // /// 注意：目标数据必须有折返，否则assert失败；
    // /// 使用姿势：先使用try_oneway_slice尝试获取单向切片数据，失败后，再使用本方法；
    // #[inline]
    // pub fn dump_ring_part(&self, oft: usize, len: usize) -> Vec<u8> {
    //     let mut dest = Vec::with_capacity(len);
    //     self.copy_to_vec_with_oft_len(oft, len, &mut dest);
    //     log::debug!("+++ copy ring part data to vec: {:?}", dest);

    //     dest
    // }

    // TODO 保持ringslice的不可变特性，把位置变化，移到外部解析的parsebuf中
    // /// 以指定的位置将slice分拆为2部分，返回[0, n）
    // #[inline]
    // pub fn eat(&mut self, n: usize) -> Self {
    //     let eaten = self.sub_slice(0, n);
    //     self.skip(n);

    //     eaten
    // }

    // #[inline]
    // pub fn skip(&mut self, n: usize) {
    //     assert!(n <= self.len(), "too big to skip: {}/{:?}", n, self);

    //     // 如果RingSlice的结构变化，注意审视这里是否需要调整 fishermen
    //     self.start = self.mask(self.start() + n) as u32;
    //     self.len = self.len - n as u32;
    // }

    /// 展示所有内容，仅用于长度比较小的场景 fishermen
    #[inline]
    pub fn as_string_lossy(&self) -> String {
        debug_assert!(self.len() < 512);

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

    // Value 改造成RingSlice，影响范围太大，且受益看似不大，先撤回 fishermen
    // /// 检测档期slice自oft开始，是否全部是合法的utf字节序
    // #[inline]
    // pub fn utf8_check(&self, oft: usize) -> bool {
    //     let (valid, pos) = self.utf8_validation(oft);
    //     if !valid {
    //         log::info!("found invalid utf8 bytes: {}/{:?}", pos, self);
    //     }
    //     valid
    // }
}

//unsafe impl Send for RingSlice {}
//unsafe impl Sync for RingSlice {}

use std::convert::TryInto;
macro_rules! define_read_number {
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
    // little endian
    define_read_number!(read_u8_le, u8::from_le_bytes);
    define_read_number!(read_i8_le, i8::from_le_bytes);
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

// impl RingSlice {
//     // utf8 校验，不想做内存copy，所以搬出来实现 fishermen
//     // #[rustc_const_unstable(feature = "str_internals", issue = "none")]
//     #[inline(always)]
//     // pub(super) const fn run_utf8_validation(v: &[u8]) -> Result<(), Utf8Error> {
//     fn utf8_validation(&self, oft: usize) -> (bool, usize) {
//         let mut index = oft;
//         let len = self.len();

//         let usize_bytes = std::mem::size_of::<usize>();
//         let ascii_block_size = 2 * usize_bytes;
//         let blocks_end = if len >= ascii_block_size {
//             len - ascii_block_size + 1
//         } else {
//             0
//         };
//         let start_mask = self.mask(self.start() + oft);
//         // let align = v.as_ptr().align_offset(usize_bytes);
//         let align = unsafe { self.ptr().add(start_mask).align_offset(usize_bytes) };

//         while index < len {
//             let old_offset = index;
//             macro_rules! err {
//                 ($error_len: expr) => {
//                     // return Err(Utf8Error {
//                     //     old_offset,
//                     //     $error_len,
//                     // })
//                     return (false, old_offset);
//                 };
//             }

//             macro_rules! next {
//                 () => {{
//                     index += 1;
//                     // we needed data, but there was none: error!
//                     if index >= len {
//                         err!(None)
//                     }
//                     // self[index]
//                     self.at(index)
//                 }};
//             }

//             // 不用[]来取，方便代码跟踪
//             // let first = v[index];
//             let first = self.at(index);
//             if first >= 128 {
//                 let w = utf8_char_width(first);
//                 // 2-byte encoding is for codepoints  \u{0080} to  \u{07ff}
//                 //        first  C2 80        last DF BF
//                 // 3-byte encoding is for codepoints  \u{0800} to  \u{ffff}
//                 //        first  E0 A0 80     last EF BF BF
//                 //   excluding surrogates codepoints  \u{d800} to  \u{dfff}
//                 //               ED A0 80 to       ED BF BF
//                 // 4-byte encoding is for codepoints \u{1000}0 to \u{10ff}ff
//                 //        first  F0 90 80 80  last F4 8F BF BF
//                 //
//                 // Use the UTF-8 syntax from the RFC
//                 //
//                 // https://tools.ietf.org/html/rfc3629
//                 // UTF8-1      = %x00-7F
//                 // UTF8-2      = %xC2-DF UTF8-tail
//                 // UTF8-3      = %xE0 %xA0-BF UTF8-tail / %xE1-EC 2( UTF8-tail ) /
//                 //               %xED %x80-9F UTF8-tail / %xEE-EF 2( UTF8-tail )
//                 // UTF8-4      = %xF0 %x90-BF 2( UTF8-tail ) / %xF1-F3 3( UTF8-tail ) /
//                 //               %xF4 %x80-8F 2( UTF8-tail )
//                 match w {
//                     2 => {
//                         if next!() as i8 >= -64 {
//                             err!(Some(1))
//                         }
//                     }
//                     3 => {
//                         match (first, next!()) {
//                             (0xE0, 0xA0..=0xBF)
//                             | (0xE1..=0xEC, 0x80..=0xBF)
//                             | (0xED, 0x80..=0x9F)
//                             | (0xEE..=0xEF, 0x80..=0xBF) => {}
//                             _ => err!(Some(1)),
//                         }
//                         if next!() as i8 >= -64 {
//                             err!(Some(2))
//                         }
//                     }
//                     4 => {
//                         match (first, next!()) {
//                             (0xF0, 0x90..=0xBF)
//                             | (0xF1..=0xF3, 0x80..=0xBF)
//                             | (0xF4, 0x80..=0x8F) => {}
//                             _ => err!(Some(1)),
//                         }
//                         if next!() as i8 >= -64 {
//                             err!(Some(2))
//                         }
//                         if next!() as i8 >= -64 {
//                             err!(Some(3))
//                         }
//                     }
//                     _ => err!(Some(1)),
//                 }
//                 index += 1;
//             } else {
//                 // Ascii case, try to skip forward quickly.
//                 // When the pointer is aligned, read 2 words of data per iteration
//                 // until we find a word containing a non-ascii byte.
//                 if align != usize::MAX && align.wrapping_sub(index) % usize_bytes == 0 {
//                     // let ptr = v.as_ptr();
//                     let ptr = self.ptr();
//                     while index < blocks_end {
//                         // SAFETY: since `align - index` and `ascii_block_size` are
//                         // multiples of `usize_bytes`, `block = ptr.add(index)` is
//                         // always aligned with a `usize` so it's safe to dereference
//                         // both `block` and `block.add(1)`.
//                         unsafe {
//                             let block = ptr.add(index) as *const usize;
//                             // break if there is a nonascii byte
//                             let zu = contains_nonascii(*block);
//                             let zv = contains_nonascii(*block.add(1));
//                             if zu || zv {
//                                 break;
//                             }
//                         }
//                         index += ascii_block_size;
//                     }
//                     // step from the point where the wordwise loop stopped
//                     // while index < len && v[index] < 128 {
//                     while index < len && self.at(index) < 128 {
//                         index += 1;
//                     }
//                 } else {
//                     index += 1;
//                 }
//             }
//         }

//         (true, 0)
//         // Ok(())
//     }
// }

// // https://tools.ietf.org/html/rfc3629
// const UTF8_CHAR_WIDTH: &[u8; 256] = &[
//     // 1  2  3  4  5  6  7  8  9  A  B  C  D  E  F
//     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0
//     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 1
//     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 2
//     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 3
//     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 4
//     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 5
//     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 6
//     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 7
//     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 8
//     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 9
//     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // A
//     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // B
//     0, 0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, // C
//     2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, // D
//     3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, // E
//     4, 4, 4, 4, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // F
// ];

// /// Given a first byte, determines how many bytes are in this UTF-8 character.
// //#[unstable(feature = "str_internals", issue = "none")]
// #[must_use]
// #[inline]
// const fn utf8_char_width(b: u8) -> usize {
//     UTF8_CHAR_WIDTH[b as usize] as usize
// }

// const NONASCII_MASK: usize = repeat_u8(0x80);

// /// Returns `true` if any byte in the word `x` is nonascii (>= 128).
// #[inline]
// const fn contains_nonascii(x: usize) -> bool {
//     (x & NONASCII_MASK) != 0
// }

// /// Returns an `usize` where every byte is equal to `x`.
// #[inline]
// const fn repeat_u8(x: u8) -> usize {
//     usize::from_ne_bytes([x; std::mem::size_of::<usize>()])
// }
