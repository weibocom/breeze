use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use futures::ready;

use super::AsyncWriteAll;
use crate::Protocol;
use hash::Hash;

pub struct AsyncSharding<B, H, P> {
    idx: usize,
    shards: Vec<B>,
    hasher: H,
    parser: P,
}

impl<B, H, P> AsyncSharding<B, H, P> {
    pub fn from(shards: Vec<B>, hasher: H, parser: P) -> Self {
        let idx = 0;
        Self {
            shards,
            hasher,
            parser,
            idx,
        }
    }
}

impl<B, H, P> AsyncWriteAll for AsyncSharding<B, H, P> {}

impl<B, H, P> AsyncWrite for AsyncSharding<B, H, P>
where
    B: AsyncWriteAll + AsyncWrite + Unpin,
    H: Unpin + Hash,
    P: Unpin + Protocol,
{
    #[inline]
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>> {
        let me = &mut *self;
        debug_assert!(me.idx < me.shards.len());
        let key = me.parser.parse_key(buf);
        let h = me.hasher.hash(key) as usize;
        me.idx = h % me.shards.len();
        unsafe {
            let w = ready!(Pin::new(me.shards.get_unchecked_mut(me.idx)).poll_write(cx, buf))?;
            debug_assert_eq!(w, buf.len());
        }
        Poll::Ready(Ok(buf.len()))
    }
    #[inline]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        let me = &mut *self;
        for b in me.shards.iter_mut() {
            ready!(Pin::new(b).poll_flush(cx))?;
        }
        Poll::Ready(Ok(()))
    }
    #[inline]
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        let me = &mut *self;
        for b in me.shards.iter_mut() {
            ready!(Pin::new(b).poll_shutdown(cx))?;
        }
        Poll::Ready(Ok(()))
    }
}

impl<B, H, P> AsyncRead for AsyncSharding<B, H, P>
where
    B: AsyncRead + Unpin,
    H: Unpin,
    P: Unpin,
{
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<Result<()>> {
        let me = &mut *self;
        if me.shards.len() == 0 {
            return Poll::Ready(Err(Error::new(
                ErrorKind::NotConnected,
                "not connected, maybe topology not inited",
            )));
        }
        unsafe { Pin::new(me.shards.get_unchecked_mut(me.idx)).poll_read(cx, buf) }
    }
}
