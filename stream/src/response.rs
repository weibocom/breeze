use tokio::io::ReadBuf;

use super::RingBufferStream;
use ds::RingSlice;
use std::sync::Arc;

pub struct Response {
    slice: RingSlice,
    // 在drop response的时候，需要将执行RingBufferStream::response_done。释放资源
    done: Option<(usize, Arc<RingBufferStream>)>,
}

impl Response {
    pub fn from(slice: RingSlice, cid: usize, release: Arc<RingBufferStream>) -> Self {
        Self {
            slice: slice,
            done: Some((cid, release)),
        }
    }
    pub fn from_slice(slice: &'static [u8]) -> Self {
        let slice = RingSlice::from(
            slice.as_ptr(),
            slice.len().next_power_of_two(),
            0,
            slice.len(),
        );
        Self {
            slice: slice,
            done: None,
        }
    }
    // 返回true，说明所有Response的数据都写入完成
    pub fn write_to(&mut self, buff: &mut ReadBuf) -> bool {
        let b = unsafe { std::mem::transmute(buff.unfilled_mut()) };
        let n = self.slice.read(b);
        buff.advance(n);
        self.slice.available() == 0
    }
    pub fn len(&self) -> usize {
        todo!("not supported");
    }
    pub fn append(&mut self, other: Response) {
        todo!("not supported");
    }
    pub fn advance(&mut self, n: usize) {
        todo!("not supported");
    }
    pub fn backwards(&mut self, n: usize) {
        todo!("not supported");
    }
}

impl AsRef<RingSlice> for Response {
    fn as_ref(&self) -> &RingSlice {
        &self.slice
    }
}

unsafe impl Send for Response {}
unsafe impl Sync for Response {}

impl Drop for Response {
    fn drop(&mut self) {
        if let Some((cid, done)) = self.done.take() {
            done.response_done(cid, &self.slice);
        }
    }
}
