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
