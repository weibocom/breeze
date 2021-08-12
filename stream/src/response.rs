use super::RingBufferStream;
use ds::RingSlice;
use ds::Slice;
use protocol::{Protocol, RequestId};

use std::sync::Arc;

pub(crate) struct Item {
    data: ResponseData,
    done: Option<(usize, Arc<RingBufferStream>)>,
}

pub struct ResponseData {
    data: RingSlice,
    req_id: RequestId,
    seq: usize, // response的seq
}
impl ResponseData {
    pub fn from(data: RingSlice, rid: RequestId, resp_seq: usize) -> Self {
        Self {
            data: data,
            req_id: rid,
            seq: resp_seq,
        }
    }
    #[inline(always)]
    pub fn data(&self) -> &RingSlice {
        &self.data
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

    pub(crate) fn into_reader<P: Protocol>(mut self, parser: &P) -> ResponseReader<'_, P> {
        // 设置每个item的待trim tail的size
        let tail_trim_lens = self.trim_unnessary_tail(parser);
        ResponseReader {
            idx: 0,
            items: self.items,
            tail_trim_lens: tail_trim_lens,
            parser: parser,
        }
    }
    pub fn len(&self) -> usize {
        let mut l = 0;
        for item in self.items.iter() {
            l += item.available();
        }
        l
    }
    pub fn iter(&self) -> ResponseRingSliceIter {
        ResponseRingSliceIter {
            response: self,
            idx: 0,
        }
    }
    // 除了最后一个item，前面所有的item需要进行tail trim，当前仅适用于getMulti
    fn trim_unnessary_tail<P: Protocol>(&mut self, parser: &P) -> Vec<usize> {
        // 最后一个item需要保留tail
        let mut tail_trim_lens = Vec::with_capacity(self.items.len());
        for i in 0..(self.items.len() - 1) {
            let item = self.items.get_mut(i).unwrap();
            // 其他item需要trim掉tail
            let avail = item.available();
            if avail > 0 {
                let tail_size = parser.trim_tail(&item);
                if tail_size > 0 {
                    debug_assert!(avail >= tail_size);
                }
                tail_trim_lens.push(tail_size);
            } else {
                tail_trim_lens.push(0);
                log::warn!("warn - found empty item in response!");
            }
        }

        // 最后一个item的tail不处理
        tail_trim_lens.push(0);
        log::debug!("trim_len: {:?}", tail_trim_lens);
        tail_trim_lens
    }
}

pub struct ResponseRingSliceIter<'a> {
    idx: usize,
    response: &'a Response,
}

impl<'a> Iterator for ResponseRingSliceIter<'a> {
    type Item = &'a RingSlice;
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.response.items.len() {
            None
        } else {
            let idx = self.idx;
            self.idx += 1;
            unsafe { Some(&self.response.items.get_unchecked(idx)) }
        }
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

pub(crate) struct ResponseReader<'a, P> {
    idx: usize,
    items: Vec<Item>,
    tail_trim_lens: Vec<usize>,
    // TODO 之前用于parse，暂时保留，后续确定不用时，清理
    #[allow(dead_code)]
    parser: &'a P,
}

impl<'a, P> Iterator for ResponseReader<'a, P>
where
    P: Protocol,
{
    type Item = Slice;
    fn next(&mut self) -> Option<Self::Item> {
        debug_assert_eq!(self.items.len(), self.tail_trim_lens.len());

        let len = self.items.len();
        while self.idx < len {
            let item = unsafe { self.items.get_unchecked_mut(self.idx) };
            let avail = item.available();
            let trim_len = *self.tail_trim_lens.get(self.idx).unwrap();
            if avail > trim_len {
                if trim_len > 0 {
                    let mut data = item.take_slice();
                    // 剩下的不够trim，trim本次的data
                    if item.available() < trim_len {
                        data.backwards(trim_len - item.available());
                        return Some(data);
                    }
                    return Some(data);
                }
                return Some(item.take_slice());
            }
            self.idx += 1;
        }
        None
    }
}
