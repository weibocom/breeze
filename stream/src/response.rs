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
    pub fn append(&mut self, other: Response) {
        self.items.extend(other.items);
    }
    // 去掉消息的结尾部分
    pub fn cut_tail(&mut self, tail_size: usize) -> bool {
        if self.items.len() == 0 {
            println!(" no tail to cut");
            return false;
        }
        // 之前已经都出了response
        let idx = self.items.len() - 1;
        let last_resp = self.items.get_mut(idx).unwrap();
        let last_len = last_resp.len();
        if last_len > tail_size {
            last_resp.resize(last_len - tail_size);
            println!(" cut tail/{} from len/{}", tail_size, last_len);
        } else if last_len < tail_size {
            println!(
                "found malformed response when cut tail with size/{}",
                tail_size
            );
            return false;
        } else if last_len == tail_size {
            // 上一个响应是一个empty response，扔掉该响应
            self.items.pop();
            println!("cut an empty resp");
        }
        return true;
    }
    pub(crate) fn into_items(self) -> Vec<Item> {
        self.items
    }
    pub fn len(&self) -> usize {
        let mut l = 0;
        for item in self.items.iter() {
            l += item.available();
        }
        l
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

impl Item {
    pub fn rid(&self) -> &RequestId {
        &self.data.req_id
    }
}
