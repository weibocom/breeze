use std::fmt::{Debug, Display, Formatter};
use std::io::{Error, ErrorKind, Result};
use std::ptr::copy_nonoverlapping;
use std::slice::from_raw_parts;

#[derive(Default)]
pub struct RingSlice {
    ptr: usize,
    cap: usize,
    start: usize,
    len: usize,
}

// 将ring_slice拆分成2个seg。分别调用
macro_rules! with_segment_oft {
    ($self:expr, $oft:expr, $noseg:expr, $seg:expr) => {{
        debug_assert!($oft < $self.len);
        let oft_start = $self.mask($self.start + $oft);
        let len = $self.len - $oft;
        if oft_start + len <= $self.cap {
            unsafe { $noseg($self.ptr().add(oft_start), len) }
        } else {
            let seg1 = $self.cap - oft_start;
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
    };
    #[inline]
    pub fn empty() -> Self {
        Self::EMPTY
    }
    #[inline]
    pub fn from(ptr: *const u8, cap: usize, start: usize, end: usize) -> Self {
        assert!(cap == 0 || cap.is_power_of_two(), "not valid cap:{}", cap);
        assert!(end >= start);
        Self {
            ptr: ptr as usize,
            cap,
            start,
            len: end - start,
        }
    }

    #[inline]
    pub fn sub_slice(&self, offset: usize, len: usize) -> RingSlice {
        assert!(offset + len <= self.len());
        Self {
            ptr: self.ptr,
            cap: self.cap,
            start: self.start + offset,
            len,
        }
    }
    // 读取数据. 可能只读取可读数据的一部分。
    #[inline]
    pub fn read(&self, offset: usize) -> &[u8] {
        assert!(offset < self.len());
        let oft = self.mask(self.start + offset);
        let l = (self.cap - oft).min(self.len() - offset);
        unsafe { from_raw_parts(self.ptr().offset(oft as isize), l) }
    }
    #[inline(always)]
    fn visit_segment_oft(&self, oft: usize, mut v: impl FnMut(*mut u8, usize)) {
        debug_assert!(oft < self.len());
        with_segment_oft!(self, oft, |p, l| v(p, l), |p0, l0, p1, l1| {
            v(p0, l0);
            v(p1, l1);
        });
    }
    #[inline(always)]
    pub fn data_oft(&self, oft: usize) -> (&[u8], &[u8]) {
        static EMPTY: &[u8] = &[];
        with_segment_oft!(
            self,
            oft,
            |ptr, len| (from_raw_parts(ptr, len), EMPTY),
            |p0, l0, p1, l1| (from_raw_parts(p0, l0), from_raw_parts(p1, l1))
        )
    }
    #[inline(always)]
    pub fn data(&self) -> (&[u8], &[u8]) {
        self.data_oft(0)
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
    #[inline(always)]
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
    pub fn copy_to_vec(&self, v: &mut Vec<u8>) {
        let len = self.len();
        v.reserve(len);
        let mut oft = 0;
        use crate::Buffer;
        while oft < len {
            let data = self.read(oft);
            oft += data.len();
            v.write(data);
        }
    }

    #[inline(always)]
    fn mask(&self, oft: usize) -> usize {
        // 兼容cap是0的场景
        self.cap.wrapping_sub(1) & oft
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }
    #[inline(always)]
    pub fn at(&self, idx: usize) -> u8 {
        assert!(idx < self.len());
        unsafe { *self.oft_ptr(idx) }
    }
    #[inline]
    pub fn update(&mut self, idx: usize, b: u8) {
        assert!(idx < self.len());
        unsafe { *self.oft_ptr(idx) = b }
    }
    #[inline]
    pub fn ptr(&self) -> *mut u8 {
        self.ptr as *mut u8
    }
    #[inline(always)]
    unsafe fn oft_ptr(&self, idx: usize) -> *mut u8 {
        debug_assert!(idx < self.len());
        let oft = self.mask(self.start + idx);
        self.ptr().offset(oft as isize)
    }

    #[inline]
    pub fn find(&self, offset: usize, b: u8) -> Option<usize> {
        for i in offset..self.len() {
            if self.at(i) == b {
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
            if self.at(i) == b'\r' {
                // 再验证'\n'
                if self.at(i + 1) == b'\n' {
                    return Some(i);
                }
            }
        }
        None
    }

    // 查找是否以dest字符串作为最前面的字符串
    #[inline]
    pub fn start_with(&self, offset: usize, dest: &[u8]) -> Result<bool> {
        let mut len = dest.len();
        if (self.len() - offset) < dest.len() {
            len = self.len() - offset;
        }

        for i in 0..len {
            if self.at(offset + i) != dest[i] {
                return Ok(false);
            }
        }

        if len == dest.len() {
            return Ok(true);
        }
        Err(Error::new(ErrorKind::Other, "no enough bytes"))
    }

    #[inline]
    fn end(&self) -> usize {
        self.start + self.len
    }

    #[inline]
    pub fn start_with_ignore_case(&self, offset: usize, dest: &[u8]) -> std::io::Result<bool> {
        let mut len = dest.len();
        if self.len() - offset < dest.len() {
            len = self.len() - offset;
        }

        for i in 0..len {
            let c = dest[i] as char;
            // 对于非ascii字母，直接比较，否则忽略大小写比较
            if !c.is_ascii_alphabetic() {
                if self.at(offset + i) != dest[i] {
                    return Ok(false);
                }
            } else {
                let c_lower = c.to_ascii_lowercase() as u8;
                let c_uper = c.to_ascii_uppercase() as u8;
                let src = self.at(offset + i);
                if src != c_lower && src != c_uper {
                    return Ok(false);
                }
            }
        }

        if len == dest.len() {
            return Ok(true);
        }

        Err(Error::new(ErrorKind::Other, "no enough bytes"))
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
}

unsafe impl Send for RingSlice {}
unsafe impl Sync for RingSlice {}

use std::convert::TryInto;
macro_rules! define_read_number {
    ($fn_name:ident, $type_name:tt) => {
        #[inline]
        pub fn $fn_name(&self, offset: usize) -> $type_name {
            const SIZE: usize = std::mem::size_of::<$type_name>();
            assert!(self.len() >= offset + SIZE);
            unsafe {
                let oft_start = (self.start + offset) & (self.cap - 1);
                let oft_end = self.end() & (self.cap - 1);
                if oft_end > oft_start || self.cap >= oft_start + SIZE {
                    let b = from_raw_parts(self.ptr().offset(oft_start as isize), SIZE);
                    $type_name::from_be_bytes(b[..SIZE].try_into().unwrap())
                } else {
                    // start索引更高
                    // 拐弯了
                    let mut b = [0u8; SIZE];
                    let n = self.cap - oft_start;
                    copy_nonoverlapping(self.ptr().offset(oft_start as isize), b.as_mut_ptr(), n);
                    copy_nonoverlapping(self.ptr(), b.as_mut_ptr().offset(n as isize), SIZE - n);
                    $type_name::from_be_bytes(b)
                }
            }
        }
    };
}

impl RingSlice {
    // big endian
    //define_read_number!(read_u8, u8);
    define_read_number!(read_u16, u16);
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

pub mod tests {
    impl PartialEq<[u8]> for super::RingSlice {
        #[inline]
        fn eq(&self, other: &[u8]) -> bool {
            if self.len() == other.len() {
                for i in 0..other.len() {
                    if self.at(i) != other[i] {
                        return false;
                    }
                }
                true
            } else {
                false
            }
        }
    }
    // 内容相等
    impl PartialEq<Self> for super::RingSlice {
        #[inline]
        fn eq(&self, other: &Self) -> bool {
            if self.len() == other.len() {
                for i in 0..other.len() {
                    if self.at(i) != other.at(i) {
                        return false;
                    }
                }
                true
            } else {
                false
            }
        }
    }
}
