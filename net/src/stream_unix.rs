use tokio::net::{TcpStream, UnixStream};

pub struct Stream {
    inner: UnixStream,
}

impl From<UnixStream> for Stream {
    #[inline]
    fn from(stream: UnixStream) -> Self {
        Self { inner: stream }
    }
}
impl From<TcpStream> for Stream {
    #[inline]
    fn from(_stream: TcpStream) -> Self {
        panic!("tcp stream not supported");
    }
}

use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
impl AsyncRead for Stream {
    #[inline(always)]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        let me = &mut *self;
        Pin::new(&mut me.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for Stream {
    #[inline(always)]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        let me = &mut *self;
        Pin::new(&mut me.inner).poll_write(cx, buf)
    }
    #[inline(always)]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let me = &mut *self;
        Pin::new(&mut me.inner).poll_flush(cx)
    }
    #[inline(always)]
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let me = &mut *self;
        Pin::new(&mut me.inner).poll_shutdown(cx)
    }
}
