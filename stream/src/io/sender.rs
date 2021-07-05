use crate::{AsyncReadAll, Item, Response};

use protocol::Protocol;

use futures::ready;

use tokio::io::AsyncWrite;

use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};
pub(super) struct Sender {
    response: Option<ResponseReader>,
}

impl Sender {
    pub fn new() -> Self {
        Self { response: None }
    }
    pub fn poll_copy_one<R, W, P>(
        &mut self,
        cx: &mut Context,
        mut reader: Pin<&mut R>,
        mut writer: Pin<&mut W>,
        parser: &P,
    ) -> Poll<Result<usize>>
    where
        R: AsyncReadAll + ?Sized,
        W: AsyncWrite + ?Sized,
        P: Protocol,
    {
        log::debug!("io-sender-poll");
        if self.response.is_none() {
            let response = ready!(reader.as_mut().poll_next(cx))?;
            log::debug!("io-sender-poll: response found");
            self.response = Some(ResponseReader::from(response, parser));
        }
        log::debug!("io-sender-poll: try to write to client");
        if let Some(ref mut rr) = self.response {
            ready!(rr.poll_write_to(cx, writer.as_mut()))?;
        }
        log::debug!("io-sender-poll: try to write to client complete");
        let old = self.response.take();
        drop(old);
        Poll::Ready(Ok(1))
    }
}

pub struct ResponseReader {
    idx: usize,
    items: Vec<Item>,
    bytes: usize,
}

impl ResponseReader {
    fn from<P>(response: Response, parser: &P) -> Self
    where
        P: Protocol,
    {
        let mut items = response.into_items();
        // 如果有多个response合并，则需要trim掉前n-1个item的eof
        for i in 0..items.len() - 1 {
            let item = unsafe { items.get_unchecked_mut(i) };
            let available = item.available();
            let eof = parser.trim_eof(&item);
            debug_assert!(available >= eof);
            item.resize(available - eof);
        }
        Self {
            idx: 0,
            items: items,
            bytes: 0,
        }
    }
    pub fn poll_write_to<W>(&mut self, cx: &mut Context, mut w: Pin<&mut W>) -> Poll<Result<usize>>
    where
        W: AsyncWrite + ?Sized,
    {
        for item in self.items[self.idx..].iter_mut() {
            while item.available() > 0 {
                let b = item.next_slice();
                debug_assert!(b.len() > 0);
                let n = ready!(w.as_mut().poll_write(cx, b.data()))?;
                if n == 0 {
                    return Poll::Ready(Err(Error::new(
                        ErrorKind::WriteZero,
                        "write zero bytes to client",
                    )));
                }
                item.advance(n);
                self.bytes += n;
            }
            self.idx += 1;
        }
        Poll::Ready(Ok(self.bytes))
    }
}
