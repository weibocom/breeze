use crate::{AsyncReadAll, Response};

use protocol::{Protocol, RequestId};

use futures::ready;

use tokio::io::AsyncWrite;

use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};
pub(super) struct Sender {
    // 这个没有使用buffer writer，主要是考虑到要快速的释放response。
    // 避免往buffer writer写的时候，阻塞导致Response Buffer的资源无法释放(buffer full)，影响其他请求。
    idx: usize,
    buff: Vec<u8>,
    bytes: usize,
}

impl Sender {
    pub fn new() -> Self {
        Self {
            buff: Vec::with_capacity(2048),
            idx: 0,
            bytes: 0,
        }
    }
    pub fn poll_copy_one<R, W, P>(
        &mut self,
        cx: &mut Context,
        mut r: Pin<&mut R>,
        mut w: Pin<&mut W>,
        parser: &P,
        rid: &RequestId,
    ) -> Poll<Result<usize>>
    where
        R: AsyncReadAll + ?Sized,
        W: AsyncWrite + ?Sized,
        P: Protocol,
    {
        // cache里面有数据，当前请求是上一次pending触发
        if self.buff.len() > 0 {
            self.flush(cx, w, rid)
        } else {
            log::debug!("io-sender-poll: poll response from agent. {:?}", rid);
            let response = ready!(r.as_mut().poll_next(cx))?;
            log::debug!("io-sender-poll: response polled. {:?}", rid);
            // cache 之后，response会立即释放。避免因数据写入到client耗时过长，导致资源难以释放
            ready!(self.write_to(response, cx, w.as_mut(), parser, rid))?;
            let bytes = self.bytes;
            self.bytes = 0;
            Poll::Ready(Ok(bytes))
        }
    }
    #[inline(always)]
    fn flush<W>(
        &mut self,
        cx: &mut Context,
        mut w: Pin<&mut W>,
        _rid: &RequestId,
    ) -> Poll<Result<usize>>
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
            self.bytes += n;
            if n == 0 {
                return Poll::Ready(Err(Error::new(ErrorKind::WriteZero, "write zero response")));
            }
            self.idx += n;
        }
        ready!(w.as_mut().poll_flush(cx))?;
        self.idx = 0;
        unsafe {
            self.buff.set_len(0);
        }
        let bytes = self.bytes;
        self.bytes = 0;
        return Poll::Ready(Ok(bytes));
    }
    // 先直接写w，直到pending，将剩下的数据写入到cache
    // 返回直接写入到client的字节数
    fn write_to<W, P>(
        &mut self,
        response: Response,
        cx: &mut Context,
        mut w: Pin<&mut W>,
        parser: &P,
        _rid: &RequestId,
    ) -> Poll<Result<()>>
    where
        W: AsyncWrite + ?Sized,
        P: Protocol,
    {
        let reader = response.into_reader(parser);

        // true: 直接往client写
        // false: 往cache写
        let mut direct = true;
        let mut left = reader.available();
        for slice in reader {
            if direct {
                match w.as_mut().poll_write(cx, slice.data())? {
                    Poll::Ready(n) => {
                        left -= n;
                        self.bytes += n;
                        if n == 0 {
                            return Poll::Ready(Err(Error::new(
                                ErrorKind::WriteZero,
                                "write zero bytes to client",
                            )));
                        }
                        // 一次未写完。不再尝试, 需要快速释放response中的item
                        if n != slice.len() {
                            self.reserve(left);
                            self.put_slice(&slice.data()[n..]);
                            direct = false;
                            break;
                        }
                    }
                    Poll::Pending => {
                        self.reserve(left);
                        self.put_slice(&slice.data());
                        direct = false;
                        break;
                    }
                }
            } else {
                self.put_slice(slice.data());
            }
        }
        ready!(w.as_mut().poll_flush(cx))?;

        if !direct {
            log::debug!("io-sender-poll: response buffered. {:?}", _rid);
            Poll::Pending
        } else {
            log::debug!("io-sender-poll: response direct. {:?}", _rid);
            Poll::Ready(Ok(()))
        }
    }
    #[inline]
    fn reserve(&mut self, l: usize) {
        if self.buff.capacity() - self.buff.len() < l {
            self.buff.reserve(l);
        }
    }
    #[inline]
    fn put_slice(&mut self, b: &[u8]) {
        debug_assert!(b.len() > 0);
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
