use crate::RingSlice;
use procs::impl_number_ringslice;
// 如果方法名中没有包含be或者le，则默认为be
#[impl_number_ringslice(default = "be")]
pub trait ByteOrder {
    fn i8(&self, oft: usize) -> i8;
    fn u8(&self, oft: usize) -> u8;
    fn u16_le(&self, oft: usize) -> u16;
    fn i16_le(&self, oft: usize) -> i16;
    fn u24_le(&self, oft: usize) -> u32;
    fn i24_le(&self, oft: usize) -> i32;
    fn u32_le(&self, oft: usize) -> u32;
    fn i32_le(&self, oft: usize) -> i32;
    fn u40_le(&self, oft: usize) -> u64;
    fn i40_le(&self, oft: usize) -> i64;
    fn u48_le(&self, oft: usize) -> u64;
    fn i48_le(&self, oft: usize) -> i64;
    fn u56_le(&self, oft: usize) -> u64;
    fn i56_le(&self, oft: usize) -> i64;
    fn u64_le(&self, oft: usize) -> u64;
    fn i64_le(&self, oft: usize) -> i64;

    fn u16_be(&self, oft: usize) -> u16;
    fn i24_be(&self, oft: usize) -> i32;
    fn u32_be(&self, oft: usize) -> u32;
    fn u64_be(&self, oft: usize) -> u64;
    fn f32_le(&self, oft: usize) -> f32;
    fn f64_le(&self, oft: usize) -> f64;
}

pub trait Range {
    fn range(&self, slice: &RingSlice) -> (usize, usize);
    #[inline]
    fn start(&self, slice: &RingSlice) -> usize {
        self.range(slice).0
    }
}

pub trait Visit {
    fn check(&mut self, b: u8, idx: usize) -> bool;
}
impl Visit for u8 {
    #[inline(always)]
    fn check(&mut self, b: u8, _idx: usize) -> bool {
        *self == b
    }
}
impl<T: FnMut(u8, usize) -> bool> Visit for T {
    #[inline(always)]
    fn check(&mut self, b: u8, idx: usize) -> bool {
        self(b, idx)
    }
}

type Offset = usize;
impl Range for Offset {
    #[inline(always)]
    fn range(&self, slice: &RingSlice) -> (usize, usize) {
        debug_assert!(*self <= slice.len());
        (*self, slice.len())
    }
}

impl Range for std::ops::Range<usize> {
    #[inline(always)]
    fn range(&self, slice: &RingSlice) -> (usize, usize) {
        debug_assert!(self.start <= slice.len());
        debug_assert!(self.end <= slice.len());
        (self.start, self.end)
    }
}
impl Range for std::ops::RangeFrom<usize> {
    #[inline(always)]
    fn range(&self, slice: &RingSlice) -> (usize, usize) {
        debug_assert!(self.start <= slice.len());
        (self.start, slice.len())
    }
}
impl Range for std::ops::RangeTo<usize> {
    #[inline(always)]
    fn range(&self, slice: &RingSlice) -> (usize, usize) {
        debug_assert!(self.end <= slice.len());
        (0, self.end)
    }
}
impl Range for std::ops::RangeFull {
    #[inline(always)]
    fn range(&self, slice: &RingSlice) -> (usize, usize) {
        (0, slice.len())
    }
}
