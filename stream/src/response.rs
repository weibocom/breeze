use super::MpmcStream;
use ds::RingSlice;
use protocol::RequestId;

use std::sync::Arc;

pub(crate) struct Item {
    data: protocol::Response,
    cid: usize,
    stream: Arc<MpmcStream>,
}

pub struct Response {
    rid: RequestId,
    pub(crate) items: Vec<Item>,
    key_indexes: Vec<Vec<usize>>,
}

impl Response {
    #[inline]
    pub fn from(
        rid: RequestId,
        data: protocol::Response,
        cid: usize,
        stream: Arc<MpmcStream>,
    ) -> Self {
        let mut items = Vec::with_capacity(4);
        items.push(Item { data, cid, stream });
        Self {
            rid,
            items,
            key_indexes: vec![],
        }
    }
    // TODO: just for quit test fishermen
    pub fn with_quit(rid: RequestId, cid: usize) -> Self {
        Response::from(
            rid,
            protocol::Response::with_quit(),
            cid,
            Arc::new(MpmcStream::with_capacity(
                1,
                "test",
                "test",
                protocol::Resource::Redis,
            )),
        )
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
            num += item.data.keys().len();
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

    #[inline]
    pub fn append_index(&mut self, index: Vec<usize>) {
        self.key_indexes.push(index);
    }

    #[inline]
    pub fn indexes(&self) -> Vec<Vec<usize>> {
        self.key_indexes.clone()
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
            unsafe { Some(&self.response.items.get_unchecked(idx).data) }
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
    #[inline]
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
    #[inline]
    fn as_ref(&self) -> &RingSlice {
        &self.data
    }
}

use std::ops::{Deref, DerefMut};
impl Deref for Item {
    type Target = RingSlice;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}
impl DerefMut for Item {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}
