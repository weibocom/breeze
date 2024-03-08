mod ephemera;
pub use ephemera::*;

pub trait Buffer: Sized {
    fn write<D: AsRef<[u8]>>(&mut self, data: D);
    #[inline(always)]
    fn write_u16(&mut self, num: u16) {
        self.write(num.to_be_bytes());
    }
    #[inline(always)]
    fn write_u32(&mut self, num: u32) {
        self.write(num.to_be_bytes());
    }
    #[inline(always)]
    fn write_u64(&mut self, num: u64) {
        self.write(num.to_be_bytes());
    }
    #[inline(always)]
    fn write_slice(&mut self, slice: &crate::RingSlice) {
        slice.copy_to_vec(self);
    }
}
impl Buffer for Vec<u8> {
    #[inline]
    fn write<D: AsRef<[u8]>>(&mut self, data: D) {
        let b = data.as_ref();
        use std::ptr::copy_nonoverlapping as copy;
        self.reserve(b.len());
        unsafe {
            copy(
                b.as_ptr() as *const u8,
                self.as_mut_ptr().add(self.len()),
                b.len(),
            );
            self.set_len(self.len() + b.len());
        }
    }
}

pub trait Add<T> {
    fn add(&mut self, t: T);
}

impl<T> Add<T> for Vec<T>
where
    T: std::cmp::PartialEq,
{
    #[inline]
    fn add(&mut self, e: T) {
        if !self.contains(&e) {
            self.push(e);
        }
    }
}
