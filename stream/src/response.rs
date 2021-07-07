use super::RingBufferStream;
use ds::RingSlice;
use protocol::RequestId;

use std::sync::Arc;

pub(crate) struct Item {
    data: ResponseData,
    done: Option<(usize, Arc<RingBufferStream>)>,
}

pub struct ResponseData {
    data: RingSlice,
    req_id: RequestId,
}
impl ResponseData {
    pub fn from(data: RingSlice, rid: RequestId) -> Self {
        Self {
            data: data,
            req_id: rid,
        }
    }
    pub fn data(&self) -> &RingSlice {
        &self.data
    }
    pub fn rid(&self) -> &RequestId {
        &self.req_id
    }
}

pub struct Response {
    pub(crate) items: Vec<Item>,
}

impl Response {
    fn _from(slice: ResponseData, done: Option<(usize, Arc<RingBufferStream>)>) -> Self {
        Self {
            items: vec![Item {
                data: slice,
                done: done,
            }],
        }
    }
    pub fn from(slice: ResponseData, cid: usize, release: Arc<RingBufferStream>) -> Self {
        Self::_from(slice, Some((cid, release)))
    }
    pub fn from_slice(slice: &'static [u8]) -> Self {
        let slice = RingSlice::from(
            slice.as_ptr(),
            slice.len().next_power_of_two(),
            0,
            slice.len(),
        );
        let data = ResponseData {
            data: slice,
            req_id: Default::default(),
        };
        Self::_from(data, None)
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
            done.response_done(cid, &self.data);
        }
    }
}

impl AsRef<RingSlice> for Item {
    fn as_ref(&self) -> &RingSlice {
        &self.data.data
    }
}

use std::ops::{Deref, DerefMut};
impl Deref for Item {
    type Target = RingSlice;
    fn deref(&self) -> &Self::Target {
        &self.data.data
    }
}
impl DerefMut for Item {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data.data
    }
}
