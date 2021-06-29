use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use futures::ready;

use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};

use protocol::{AsyncReadAll, AsyncWriteAll, MetaType, Protocol, ResponseItem};

pub struct MetaStream<P, B> {
    instances: Vec<B>,
    idx: usize,
    parser: P,
}

impl<P, B> MetaStream<P, B> {
    pub fn from(parser: P, instances: Vec<B>) -> Self {
        Self {
            idx: 0,
            instances: instances,
            parser: parser,
        }
    }
}

impl<P, B> AsyncWriteAll for MetaStream<P, B> {}

impl<P, B> AsyncReadAll for MetaStream<P, B>
where
    P: Unpin,
    B: AsyncReadAll + Unpin,
{
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<ResponseItem>> {
        let me = &mut *self;
        unsafe { Pin::new(me.instances.get_unchecked_mut(me.idx)).poll_next(cx) }
    }
    fn poll_done(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        let me = &mut *self;
        unsafe { Pin::new(me.instances.get_unchecked_mut(me.idx)).poll_done(cx) }
    }
}

impl<P, B> AsyncWrite for MetaStream<P, B>
where
    P: Protocol,
    B: AsyncWrite + Unpin,
{
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>> {
        let me = &mut *self;
        match me.parser.meta_type(buf) {
            MetaType::Version => {
                // 只需要发送请求到一个backend即可
                for (i, b) in me.instances.iter_mut().enumerate() {
                    let n = ready!(Pin::new(b).poll_write(cx, buf))?;
                    me.idx = i;
                    return Poll::Ready(Ok(n));
                }
            }
        }
        return Poll::Ready(Err(Error::new(
            ErrorKind::Other,
            "all meta instance failed",
        )));
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>>
    where
        B: AsyncWrite + Unpin,
    {
        let me = &mut *self;
        unsafe { Pin::new(me.instances.get_unchecked_mut(me.idx)).poll_flush(cx) }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>>
    where
        B: AsyncWrite + Unpin,
    {
        let me = &mut *self;
        unsafe { Pin::new(me.instances.get_unchecked_mut(me.idx)).poll_shutdown(cx) }
    }
}
