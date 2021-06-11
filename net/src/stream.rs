//use tokio::io::BufStream;
use tokio::net::{TcpStream, UnixStream};
pub enum Stream {
    Unix(UnixStream),
    Tcp(TcpStream),
}

impl From<TcpStream> for Stream {
    #[inline]
    fn from(stream: TcpStream) -> Self {
        //Self::Tcp(BufStream::with_capacity(2048, 2048, stream))
        Self::Tcp(stream)
    }
}
impl From<UnixStream> for Stream {
    #[inline]
    fn from(stream: UnixStream) -> Self {
        //Self::Unix(BufStream::with_capacity(2048, 2048, stream))
        Self::Unix(stream)
    }
}

use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
impl AsyncRead for Stream {
    #[inline(always)]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        match self.get_mut() {
            Stream::Unix(ref mut stream) => AsyncRead::poll_read(Pin::new(stream), cx, buf),
            Stream::Tcp(ref mut stream) => AsyncRead::poll_read(Pin::new(stream), cx, buf),
        }
    }
}

macro_rules! impl_async_write {
    ($($method:tt),+) => {
        $(
            #[inline(always)]
            fn $method(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Result<()>> {
                match self.get_mut() {
                    Stream::Unix(ref mut stream) => AsyncWrite::$method(Pin::new(stream), cx),
                    Stream::Tcp(ref mut stream) => AsyncWrite::$method(Pin::new(stream), cx),
                }
            }
        )*
    };
}

impl AsyncWrite for Stream {
    #[inline(always)]
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        match self.get_mut() {
            Stream::Unix(ref mut stream) => AsyncWrite::poll_write(Pin::new(stream), cx, buf),
            Stream::Tcp(ref mut stream) => AsyncWrite::poll_write(Pin::new(stream), cx, buf),
        }
    }
    impl_async_write!(poll_flush, poll_shutdown);
}
