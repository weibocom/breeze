use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use futures::ready;

use super::AsyncWriteAll;
use crate::Router;

/// 这个只支持ping-pong请求。将请求按照固定的路由策略分发到不同的dest
/// 并且AsyncRoute的buf必须包含一个完整的请求。
pub struct AsyncRoute<B, R> {
    backends: Vec<B>,
    router: R,
    idx: usize,
}

impl<B, R> AsyncRoute<B, R> {
    pub fn from(backends: Vec<B>, router: R) -> Self
    where
        B: AsyncWriteAll + AsyncWrite + Unpin,
        R: Router + Unpin,
    {
        let idx = 0;
        Self {
            backends,
            router,
            idx,
        }
    }
}

impl<B, R> AsyncWriteAll for AsyncRoute<B, R> {}

impl<B, R> AsyncWrite for AsyncRoute<B, R>
where
    B: AsyncWriteAll + AsyncWrite + Unpin,
    R: Router + Unpin,
{
    #[inline]
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>> {
        let me = &mut *self;
        // ping-pong请求，有写时，read一定是读完成了
        me.idx = me.router.route(buf);
        debug_assert!(me.idx < me.backends.len());
        unsafe {
            let w = ready!(Pin::new(me.backends.get_unchecked_mut(me.idx)).poll_write(cx, buf))?;
            debug_assert_eq!(w, buf.len());
        }
        Poll::Ready(Ok(buf.len()))
    }
    #[inline]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        let me = &mut *self;
        for b in me.backends.iter_mut() {
            ready!(Pin::new(b).poll_flush(cx))?;
        }
        Poll::Ready(Ok(()))
    }
    #[inline]
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        let me = &mut *self;
        for b in me.backends.iter_mut() {
            ready!(Pin::new(b).poll_shutdown(cx))?;
        }
        Poll::Ready(Ok(()))
    }
}

impl<B, R> AsyncRead for AsyncRoute<B, R>
where
    B: AsyncRead + Unpin,
    R: Unpin,
{
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<Result<()>> {
        let me = &mut *self;
        unsafe { Pin::new(me.backends.get_unchecked_mut(me.idx)).poll_read(cx, buf) }
    }
}
