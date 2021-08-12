use crate::{AsyncReadAll, Response};

use protocol::{Protocol, RequestId};

use futures::ready;

use tokio::io::AsyncWrite;

use super::IoMetric;
use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};
pub(super) struct Sender {
    // 这个没有使用buffer writer，主要是考虑到要快速的释放response。
    // 避免往buffer writer写的时候，阻塞导致Response Buffer的资源无法释放(buffer full)，影响其他请求。
    idx: usize,
    buff: Vec<u8>,
}

impl Sender {
    pub fn new() -> Self {
        Self {
            buff: Vec::with_capacity(2048),
            idx: 0,
        }
    }
    pub fn poll_copy_one<R, W, P>(
        &mut self,
        cx: &mut Context,
        mut r: Pin<&mut R>,
        w: Pin<&mut W>,
        parser: &P,
        rid: &RequestId,
        metric: &mut IoMetric,
    ) -> Poll<Result<()>>
    where
        R: AsyncReadAll + ?Sized,
        W: AsyncWrite + ?Sized,
        P: Protocol,
    {
        // cache里面有数据，当前请求是上一次pending触发
        if self.buff.len() == 0 {
            log::debug!("io-sender-poll: poll response from agent. {:?}", rid);
            let response = ready!(r.as_mut().poll_next(cx))?;
            metric.response_ready();
            log::debug!("io-sender-poll: response polled. {:?}", rid);
            // cache 之后，response会立即释放。避免因数据写入到client耗时过长，导致资源难以释放
            self.write_to_buffer(response, parser);
        }
        self.flush(cx, w, rid, metric)
    }
    #[inline(always)]
    fn flush<W>(
        &mut self,
        cx: &mut Context,
        mut w: Pin<&mut W>,
        _rid: &RequestId,
        metric: &mut IoMetric,
    ) -> Poll<Result<()>>
    where
        W: AsyncWrite + ?Sized,
    {
        log::debug!("flush idx:{} len:{} {:?}", self.idx, self.buff.len(), _rid);
        while self.idx < self.buff.len() {
            let n = ready!(w.as_mut().poll_write(cx, &self.buff[self.idx..]))?;
            if n == 0 {
                return Poll::Ready(Err(Error::new(ErrorKind::WriteZero, "write zero response")));
            }
            metric.response_sent(n);
            self.idx += n;
        }
        ready!(w.as_mut().poll_flush(cx))?;
        self.idx = 0;
        unsafe {
            self.buff.set_len(0);
        }
        return Poll::Ready(Ok(()));
    }
    // 选择一次copy，而不是尝试写一次io或者使用buffer，主要考虑：
    // 1. response要以最快的速度释放，因为会阻塞ResizedRingBuffer::reset_read。
    // 2. 虽然平时写io的速度很快，但长尾可能导致一个连接慢了之后影响其他连接。
    #[inline(always)]
    fn write_to_buffer<P>(&mut self, response: Response, parser: &P)
    where
        P: Protocol,
    {
        for slice in response.into_reader(parser) {
            self.put_slice(slice.data());
        }
    }
    #[inline]
    fn put_slice(&mut self, b: &[u8]) {
        debug_assert!(b.len() > 0);
        self.buff.reserve(b.len());
        let l = self.buff.len();
        use std::ptr::copy_nonoverlapping as copy;
        unsafe {
            copy(
                b.as_ptr(),
                self.buff.as_mut_ptr().offset(l as isize),
                b.len(),
            );
            self.buff.set_len(l + b.len());
        }
    }
}
