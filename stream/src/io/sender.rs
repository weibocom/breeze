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
        mut w: Pin<&mut W>,
        parser: &P,
        rid: &RequestId,
    ) -> Poll<Result<usize>>
    where
        R: AsyncReadAll + ?Sized,
        W: AsyncWrite + ?Sized,
        P: Protocol,
    {
        if self.buff.len() == 0 {
            log::debug!("io-sender-poll: poll response from agent. {:?}", rid);
            let response = ready!(r.as_mut().poll_next(cx))?;
            log::debug!("io-sender-poll: response polled. {:?}", rid);
            // cache 之后，response会立即释放。避免因数据写入到client耗时过长，导致资源难以释放
            self.cache(response, parser, rid)?;
            log::debug!("io-sender-poll: response buffered. {:?}", rid);
        }
        log::debug!(
            "io-sender: flushing idx:{} len:{} {:?}",
            self.idx,
            self.buff.len(),
            rid
        );
        while self.idx < self.buff.len() {
            let n = ready!(w.as_mut().poll_write(cx, &self.buff[self.idx..]))?;
            if n == 0 {
                return Poll::Ready(Err(Error::new(ErrorKind::WriteZero, "write zero response")));
            }
            self.idx += n;
        }
        ready!(w.as_mut().poll_flush(cx))?;
        log::debug!(
            "io-sender-poll response flushed. len:{} {:?} ",
            self.idx,
            rid
        );
        let bytes = self.idx;
        self.idx = 0;
        unsafe {
            self.buff.set_len(0);
        }
        Poll::Ready(Ok(bytes))
    }
    fn cache<P>(&mut self, response: Response, parser: &P, rid: &RequestId) -> Result<()>
    where
        P: Protocol,
    {
        let l = response.len();
        self.reserve(l);
        let mut items = response.into_items();
        let i_l = items.len();
        log::debug!("io-sender: cache bytes:{} items:{} {:?}", l, i_l, rid);
        for (i, item) in items.iter_mut().enumerate() {
            debug_assert_eq!(item.rid(), rid);
            let eof = parser.trim_eof(&item);
            debug_assert!(item.available() >= eof);
            if self.buff.len() + item.available() > protocol::MAX_SENT_BUFFER_SIZE {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "response size over limited.",
                ));
            }
            while item.available() > 0 {
                let data = item.next_slice();
                self.put_slice(data.data());
                item.advance(data.len());
            }
            // 前n-1个item需要trim掉eof
            if i < i_l - 1 {
                let l = self.buff.len() - eof;
                unsafe {
                    self.buff.set_len(l);
                }
            }
        }

        drop(items);
        Ok(())
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
