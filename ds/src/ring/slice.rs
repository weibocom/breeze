use std::ptr::copy_nonoverlapping;
use std::slice::from_raw_parts;

use crate::Slice;

#[derive(Clone, Debug)]
pub struct RingSlice {
    ptr: *const u8,
    cap: usize,
    start: usize,
    end: usize,
}

impl Default for RingSlice {
    fn default() -> Self {
        RingSlice {
            ptr: 0 as *mut u8,
            start: 0,
            end: 0,
            cap: 0,
        }
    }
}

impl RingSlice {
    #[inline(always)]
    pub fn from(ptr: *const u8, cap: usize, start: usize, end: usize) -> Self {
        debug_assert!(cap > 0);
        debug_assert_eq!(cap, cap.next_power_of_two());
        debug_assert!(end >= start);
        let me = Self {
            ptr: ptr,
            cap: cap,
            start: start,
            end: end,
        };
        me
    }
    #[inline(always)]
    pub fn sub_slice(&self, offset: usize, len: usize) -> RingSlice {
        debug_assert!(offset + len <= self.len());
        Self::from(
            self.ptr,
            self.cap,
            self.start + offset,
            self.start + offset + len,
        )
    }
    #[inline]
    pub fn as_slices(&self) -> Vec<Slice> {
        let mut slices = Vec::with_capacity(2);

        let mut read = self.start;
        while read < self.end {
            let oft = read & (self.cap - 1);
            let l = (self.cap - oft).min(self.end - read);
            slices.push(unsafe { Slice::new(self.ptr.offset(oft as isize) as usize, l) });
            read += l;
        }

        slices
    }
    #[inline]
    pub fn data(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(self.len());
        self.copy_to_vec(&mut v);
        v
    }
    #[inline]
    pub fn copy_to_vec(&self, v: &mut Vec<u8>) {
        v.reserve(self.len());
        for slice in self.as_slices() {
            slice.copy_to_vec(v);
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        debug_assert!(self.end >= self.start);
        self.end - self.start
    }
    #[inline(always)]
    pub fn at(&self, idx: usize) -> u8 {
        debug_assert!(idx < self.len());
        unsafe {
            *self
                .ptr
                .offset(((self.start + idx) & (self.cap - 1)) as isize)
        }
    }

    #[inline(always)]
    pub fn location(&self) -> (usize, usize) {
        (self.start, self.end)
    }

    pub fn split(&self, splitter: &[u8]) -> Vec<Self> {
        let mut pos = 0 as usize;
        let mut result: Vec<RingSlice> = vec![];
        loop {
            let new_pos = self.find_sub(pos, splitter);
            if new_pos.is_none() {
                if pos < self.len() {
                    result.push(self.sub_slice(pos, self.len() - pos));
                }
                return result;
            }
            else {
                let new_pos = new_pos.unwrap();
                result.push(self.sub_slice(pos, new_pos));
                if new_pos + splitter.len() == self.end - self.start {
                    return result;
                }
                pos = pos + new_pos + splitter.len();
            }
        }
    }
    // 从offset开始，查找s是否存在
    // 最坏时间复杂度 O(self.len() * s.len())
    // 但通常在协议处理过程中，被查的s都是特殊字符，而且s的长度通常比较小，因为时间复杂度会接近于O(self.len())
    pub fn find_sub(&self, offset: usize, s: &[u8]) -> Option<usize> {
        if self.start + offset + s.len() > self.end {
            return None;
        }
        let mut i = 0 as usize;
        let i_cap = self.len() - s.len() - offset;
        while i <= i_cap {
            let mut found_len = 0 as usize;
            for j in 0..s.len() {
                if self.read_u8(offset + i + j) != s[j] {
                    i += 1;
                    break;
                }
                else {
                    found_len = found_len + 1;
                }
            }
            if found_len == s.len() {
                return Some(i);
            }
        }
        None
    }
    // 查找是否存在 '\r\n' ，返回匹配的第一个字节地址
    pub fn index_lf_cr(&self, offset: usize) -> Option<usize> {
        self.find_sub(offset, &[b'\r', b'\n'])
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
            debug_assert!(self.len() >= offset + SIZE);
            unsafe {
                let oft_start = (self.start + offset) & (self.cap - 1);
                let oft_end = self.end & (self.cap - 1);
                if oft_end > oft_start || self.cap >= oft_start + SIZE {
                    let b = from_raw_parts(self.ptr.offset(oft_start as isize), SIZE);
                    $type_name::from_be_bytes(b[..SIZE].try_into().unwrap())
                } else {
                    // start索引更高
                    // 拐弯了
                    let mut b = [0u8; SIZE];
                    let n = self.cap - oft_start;
                    copy_nonoverlapping(self.ptr.offset(oft_start as isize), b.as_mut_ptr(), n);
                    copy_nonoverlapping(self.ptr, b.as_mut_ptr().offset(n as isize), SIZE - n);
                    $type_name::from_be_bytes(b)
                }
            }
        }
    };
}

impl RingSlice {
    // big endian
    define_read_number!(read_u8, u8);
    define_read_number!(read_u16, u16);
    define_read_number!(read_u32, u32);
    define_read_number!(read_u64, u64);
}

impl PartialEq<[u8]> for RingSlice {
    fn eq(&self, other: &[u8]) -> bool {
        println!("eq ref slice");
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

impl PartialEq<Slice> for RingSlice {
    fn eq(&self, other: &Slice) -> bool {
        self == other.data()
    }
}
impl Eq for RingSlice {}
impl PartialEq for RingSlice {
    fn eq(&self, other: &Self) -> bool {
        if self.len() == other.len() {
            for i in 0..self.len() {
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

use std::hash::{Hash, Hasher};
impl Hash for RingSlice {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        let slices = self.as_slices();
        unsafe {
            if slices.len() == 1 {
                slices.get_unchecked(0).data().hash(state);
            } else {
                let mut v = Vec::with_capacity(self.len());
                self.copy_to_vec(&mut v);
                v.hash(state);
            }
        }
    }
}

impl From<Slice> for RingSlice {
    #[inline]
    fn from(s: Slice) -> Self {
        let len = s.len();
        let cap = len.next_power_of_two();
        Self::from(s.as_ptr(), cap, 0, s.len())
    }
}
impl From<&[u8]> for RingSlice {
    #[inline]
    fn from(s: &[u8]) -> Self {
        let len = s.len();
        let cap = len.next_power_of_two();
        Self::from(s.as_ptr(), cap, 0, s.len())
    }
}
use std::fmt;
use std::fmt::{Display, Formatter};
impl Display for RingSlice {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RingSlice: ptr:{} start:{} end:{} cap:{}",
            self.ptr as usize, self.start, self.end, self.cap
        )
    }
}
