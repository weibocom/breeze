use crate::{AsyncReadAll, Response};

use ds::RingSlice;
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
    done: bool, // 上一次请求是否结束
}

impl Sender {
    pub fn new() -> Self {
        Self {
            buff: Vec::with_capacity(2048),
            idx: 0,
            done: true,
        }
    }
    #[inline]
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
        if self.done {
            log::debug!("io-sender-poll: poll response from agent. {:?}", rid);
            let response = ready!(r.as_mut().poll_next(cx))?;
            metric.response_ready();
            log::debug!("io-sender-poll: response polled. {:?}", rid);
            // cache 之后，response会立即释放。避免因数据写入到client耗时过长，导致资源难以释放
            self.write_to_buffer(response, parser);
            self.done = false;
        }
        ready!(self.flush(cx, w, rid, metric))?;
        self.done = true;
        Poll::Ready(Ok(()))
    }
    #[inline(always)]
    fn flush<W>(
        &mut self,
        cx: &mut Context,
        mut w: Pin<&mut W>,
        rid: &RequestId,
        metric: &mut IoMetric,
    ) -> Poll<Result<()>>
    where
        W: AsyncWrite + ?Sized,
    {
        log::debug!("flush idx:{} len:{} {}", self.idx, self.buff.len(), rid);
        while self.idx < self.buff.len() {
            let n = ready!(w.as_mut().poll_write(cx, &self.buff[self.idx..]))?;
            if n == 0 {
                return Poll::Ready(Err(Error::new(ErrorKind::WriteZero, "write zero response")));
            }
            metric.response_sent(n);
            self.idx += n;
        }
        self.idx = 0;
        unsafe { self.buff.set_len(0) };
        w.as_mut().poll_flush(cx)
    }
    // 选择一次copy，而不是尝试写一次io或者使用buffer，主要考虑：
    // 1. response要以最快的速度释放，因为会阻塞ResizedRingBuffer::reset_read。
    // 2. 虽然平时写io的速度很快，但长尾可能导致一个连接慢了之后影响其他连接。
    #[inline(always)]
    fn write_to_buffer<P>(&mut self, response: Response, parser: &P)
    where
        P: Protocol,
    {
        parser.write_response(response.iter(), self);
    }
    #[inline(always)]
    fn buff_backwark(&mut self, back: usize) {
        unsafe {
            if back > 0 {
                debug_assert!(self.buff.len() >= back);
                self.buff.set_len(self.buff.len() - back);
            }
        }
    }
}

impl protocol::BackwardWrite for Sender {
    #[inline(always)]
    fn write(&mut self, data: &RingSlice, backward: usize) {
        data.copy_to_vec(&mut self.buff);
        debug_assert!(data.len() >= backward);
        self.buff_backwark(backward);
    }
    #[inline(always)]
    fn write_on<F: Fn(&mut [u8])>(&mut self, data: &RingSlice, update: F) {
        let old = self.buff.len();
        self.write(data, 0);
        update(&mut self.buff[old..]);
    }
    #[inline(always)]
    fn forward(&mut self, l: usize) {
        unsafe {
            debug_assert!(self.buff.len() + l <= self.buff.capacity());
            self.buff.set_len(self.buff.len() + l);
        }
    }
}
