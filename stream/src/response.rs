use super::MpmcStream;
use ds::RingSlice;
use protocol::RequestId;

use std::sync::Arc;

pub(crate) struct Item {
    data: ResponseData,
    cid: usize,
    stream: Arc<MpmcStream>,
}

pub struct ResponseData {
    data: protocol::Response,
    req_id: RequestId,
    seq: usize, // response的seq
}
impl ResponseData {
    pub fn from(data: protocol::Response, rid: RequestId, resp_seq: usize) -> Self {
        Self {
            data: data,
            req_id: rid,
            seq: resp_seq,
        }
    }
    #[inline(always)]
    pub fn rid(&self) -> &RequestId {
        &self.req_id
    }
    #[inline(always)]
    pub fn seq(&self) -> usize {
        self.seq
    }
}

impl std::ops::Deref for ResponseData {
    type Target = protocol::Response;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

use std::fmt::{self, Display, Formatter};
impl Display for ResponseData {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "rid:{} data:{}", self.req_id, self.data)
    }
}

pub struct Response {
    rid: RequestId,
    pub(crate) items: Vec<Item>,
}

impl Response {
    #[inline]
    pub fn from(data: ResponseData, cid: usize, stream: Arc<MpmcStream>) -> Self {
        let rid = data.req_id;
        let mut items = Vec::with_capacity(4);
        items.push(Item { data, cid, stream });
        Self { rid, items }
    }
    #[inline]
    pub fn append(&mut self, other: Response) {
        if self.items.capacity() - self.items.len() < other.items.len() {
            self.items.reserve(other.items.len().max(16));
        }
        self.items.extend(other.items);
    }
    #[inline]
    pub fn keys_num(&self) -> usize {
        let mut num = 0;
        for item in &self.items {
            num += item.data.data.keys().len();
        }
        num
    }
    #[inline]
    pub fn rid(&self) -> RequestId {
        self.rid
    }

    #[inline]
    pub fn iter(&self) -> ResponseIter {
        ResponseIter {
            response: self,
            idx: 0,
        }
    }
}

pub struct ResponseIter<'a> {
    idx: usize,
    response: &'a Response,
}

impl<'a> Iterator for ResponseIter<'a> {
    // 0: 当前response是否为最后一个
    // 1: response
    type Item = &'a protocol::Response;
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.response.items.len() {
            None
        } else {
            let idx = self.idx;
            self.idx += 1;
            unsafe { Some(&self.response.items.get_unchecked(idx).data.data) }
        }
    }
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let left = self.response.items.len() - self.idx;
        (left, Some(left))
    }
}

unsafe impl Send for Response {}
unsafe impl Sync for Response {}

impl AsRef<RingSlice> for Response {
    // 如果有多个item,应该使迭代方式
    #[inline(always)]
    fn as_ref(&self) -> &RingSlice {
        debug_assert!(self.items.len() == 1);
        unsafe { &self.items.get_unchecked(self.items.len() - 1) }
    }
}

impl Drop for Item {
    #[inline]
    fn drop(&mut self) {
        self.stream.response_done(self.cid, &self.data);
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
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.data.data
    }
}
impl DerefMut for Item {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data.data
    }
}
