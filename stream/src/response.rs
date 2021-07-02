use tokio::io::ReadBuf;

use super::RingBufferStream;
use ds::RingSlice;
use protocol::Protocol;

use std::sync::Arc;

struct Item {
    slice: RingSlice,
    done: Option<(usize, Arc<RingBufferStream>)>,
}

pub struct Response {
    // 当前写入数据的位置
    idx: usize,
    items: Vec<Item>,
}

impl Response {
    fn _from(slice: RingSlice, done: Option<(usize, Arc<RingBufferStream>)>) -> Self {
        Self {
            items: vec![Item {
                slice: slice,
                done: done,
            }],
            idx: 0,
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
    // 返回true，说明所有Response的数据都写入完成
    // 如果有多个item，则前n-1个item需要去除EOF，最后一个保留EOF
    pub fn write_to<P>(&mut self, buff: &mut ReadBuf, parser: &P) -> bool
    where
        P: Protocol,
    {
        debug_assert!(self.items.len() > 0);
        unsafe {
            let last_idx = self.items.len() - 1;
            // 先处理前n-1个，每一个截断EOF个字节
            for i in self.idx..last_idx {
                let b = std::mem::transmute(buff.unfilled_mut());
                let slice = self.items.get_unchecked_mut(i);
                let n = slice.read(b);
                buff.advance(n);
                if slice.available() == 0 {
                    // 读完了，读取下一个
                    self.idx += 1;
                    let eof_len = parser.trim_eof(&slice);
                    let filled = buff.filled().len();
                    debug_assert!(filled >= eof_len);
                    buff.set_filled(filled - eof_len);
                } else {
                    // 一次没读完，说明buff不够
                    return false;
                }
            }
            let last = self.items.get_unchecked_mut(last_idx);
            let b = std::mem::transmute(buff.unfilled_mut());
            let n = last.read(b);
            buff.advance(n);
            last.available() == 0
        }
    }
    pub fn append(&mut self, other: Response) {
        debug_assert_eq!(self.idx, 0);
        debug_assert_eq!(other.idx, 0);
        self.items.extend(other.items);
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
