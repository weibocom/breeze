#![cfg(not(feature = "buffer-pool"))]

use std::ops::Deref;

use ds::RingSlice;

#[derive(Debug)]
#[repr(transparent)]
pub struct Buffer(RingSlice);
// pub struct Buffer(Vec<u8>);

// impl AsMut<Vec<u8>> for Buffer {
//     fn as_mut(&mut self) -> &mut Vec<u8> {
//         &mut self.0
//     }
// }
impl AsMut<RingSlice> for Buffer {
    fn as_mut(&mut self) -> &mut RingSlice {
        &mut self.0
    }
}

// impl Deref for Buffer {
//     type Target = [u8];

//     fn deref(&self) -> &Self::Target {
//         self.0.deref()
//     }
// }

impl Deref for Buffer {
    type Target = RingSlice;

    fn deref(&self) -> &Self::Target {
        // self.0.deref()
        &self.0
    }
}

// pub const fn get_buffer() -> Buffer {
//     Buffer(Vec::new())
// }

impl Buffer {
    // pub fn new(payload: Vec<u8>) -> Self {
    //     Buffer(payload)
    // }
    pub fn new(payload: RingSlice) -> Self {
        Buffer(payload)
    }
}
