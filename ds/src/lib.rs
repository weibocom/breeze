mod bit_map;
mod cid;
mod offset;
mod ring;
mod slice;

pub use bit_map::BitMap;
pub use cid::*;
pub use offset::*;
pub use ring::{ResizedRingBuffer, RingBuffer, RingSlice};
pub use slice::*;

pub trait Buffer {
    fn write<D: AsRef<[u8]>>(&mut self, data: D);
}

impl Buffer for Vec<u8> {
    fn write<D: AsRef<[u8]>>(&mut self, data: D) {
        let b = data.as_ref();
        use std::ptr::copy_nonoverlapping as copy;
        self.reserve(b.len());
        unsafe {
            copy(
                b.as_ptr() as *const u8,
                self.as_mut_ptr().offset(self.len() as isize),
                b.len(),
            );
            self.set_len(self.len() + b.len());
        }
    }
}
