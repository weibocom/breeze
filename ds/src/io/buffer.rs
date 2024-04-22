use crate::{RingSlice, Slicer};
pub trait Buffer: Sized {
    fn write<S: Slicer>(&mut self, data: &S);
    #[inline(always)]
    fn write_u16(&mut self, num: u16) {
        self.write(&num.to_be_bytes());
    }
    #[inline(always)]
    fn write_u32(&mut self, num: u32) {
        self.write(&num.to_be_bytes());
    }
    #[inline(always)]
    fn write_u64(&mut self, num: u64) {
        self.write(&num.to_be_bytes());
    }
    #[inline(always)]
    fn write_slice(&mut self, slice: &RingSlice) {
        self.write(slice)
    }
}
use crate::Writer;
impl<T: Writer> Buffer for T {
    #[inline(always)]
    fn write<S: Slicer>(&mut self, data: &S) {
        self.write_r(0, data).expect("no err")
    }
}
