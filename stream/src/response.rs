use super::RingBufferStream;
use ds::RingSlice;

use std::sync::Arc;

pub(crate) struct Item {
    slice: RingSlice,
    done: Option<(usize, Arc<RingBufferStream>)>,
}

pub struct Response {
    pub(crate) items: Vec<Item>,
}

impl Response {
    fn _from(slice: RingSlice, done: Option<(usize, Arc<RingBufferStream>)>) -> Self {
        Self {
            items: vec![Item {
                slice: slice,
                done: done,
            }],
        }
    }
    pub fn from(slice: RingSlice, cid: usize, release: Arc<RingBufferStream>) -> Self {
        Self::_from(slice, Some((cid, release)))
    }
    pub fn from_slice(slice: &'static [u8]) -> Self {
        let slice = RingSlice::from(
            slice.as_ptr(),
            slice.len().next_power_of_two(),
            0,
            slice.len(),
        );
        Self::_from(slice, None)
    }
    pub fn append(&mut self, other: Response) {
        self.items.extend(other.items);
    }
    pub(crate) fn into_items(self) -> Vec<Item> {
        self.items
    }
}

unsafe impl Send for Response {}
unsafe impl Sync for Response {}

impl AsRef<RingSlice> for Response {
    fn as_ref(&self) -> &RingSlice {
        debug_assert!(self.items.len() > 0);
        unsafe { &self.items.get_unchecked(self.items.len() - 1) }
    }
}

impl Drop for Item {
    fn drop(&mut self) {
        if let Some((cid, done)) = self.done.take() {
            done.response_done(cid, &self.slice);
        }
    }
}

impl AsRef<RingSlice> for Item {
    fn as_ref(&self) -> &RingSlice {
        &self.slice
    }
}

use std::ops::{Deref, DerefMut};
impl Deref for Item {
    type Target = RingSlice;
    fn deref(&self) -> &Self::Target {
        &self.slice
    }
}
impl DerefMut for Item {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.slice
    }
}
