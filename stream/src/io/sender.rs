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
            //ready!(self.write_to(response, cx, w.as_mut(), parser, rid, metric))?;
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
        log::debug!(
            "io-sender: flushing idx:{} len:{} {:?}",
            self.idx,
            self.buff.len(),
            _rid
        );
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
    //fn _write_to<W, P>(
    //    &mut self,
    //    response: Response,
    //    cx: &mut Context,
    //    mut w: Pin<&mut W>,
    //    parser: &P,
    //    _rid: &RequestId,
    //    metric: &mut IoMetric,
    //) -> Poll<Result<()>>
    //where
    //    W: AsyncWrite + ?Sized,
    //    P: Protocol,
    //{
    //    let reader = response.into_reader(parser);

    //    // true: 直接往client写
    //    // false: 往cache写
    //    let mut direct = true;
    //    let mut left = reader.available();
    //    for slice in reader {
    //        if direct {
    //            match w.as_mut().poll_write(cx, slice.data())? {
    //                Poll::Ready(n) => {
    //                    left -= n;
    //                    if n == 0 {
    //                        return Poll::Ready(Err(Error::new(
    //                            ErrorKind::WriteZero,
    //                            "write zero bytes to client",
    //                        )));
    //                    }
    //                    metric.response_sent(n);
    //                    // 一次未写完。不再尝试, 需要快速释放response中的item
    //                    if n != slice.len() {
    //                        self.put_slice(&slice.data()[n..]);
    //                        log::info!("io-sender: sent direct partially {}/{}", n, slice.len());
    //                        direct = false;
    //                        break;
    //                    }
    //                }
    //                Poll::Pending => {
    //                    self.put_slice(&slice.data());
    //                    direct = false;
    //                    break;
    //                }
    //            }
    //        } else {
    //            self.put_slice(slice.data());
    //        }
    //    }
    //    ready!(w.as_mut().poll_flush(cx))?;

    //    if !direct {
    //        log::debug!("io-sender-poll: response buffered. {:?}", _rid);
    //        Poll::Pending
    //    } else {
    //        log::debug!("io-sender-poll: response direct. {:?}", _rid);
    //        Poll::Ready(Ok(()))
    //    }
    //}
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
