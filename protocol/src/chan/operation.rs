use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::AsyncWriteAll;

pub enum AsyncOperation<Get, Gets, Store, Meta>
where
    Get: AsyncRead + AsyncWrite + AsyncWriteAll + Unpin,
    Gets: AsyncRead + AsyncWrite + AsyncWriteAll + Unpin,
    Store: AsyncRead + AsyncWrite + AsyncWriteAll + Unpin,
    Meta: AsyncRead + AsyncWrite + AsyncWriteAll + Unpin,
{
    Get(Get),
    Gets(Gets),
    Store(Store),
    Meta(Meta),
}

impl<Get, Gets, Store, Meta> AsyncWriteAll for AsyncOperation<Get, Gets, Store, Meta>
where
    Get: AsyncRead + AsyncWrite + AsyncWriteAll + Unpin,
    Gets: AsyncRead + AsyncWrite + AsyncWriteAll + Unpin,
    Store: AsyncRead + AsyncWrite + AsyncWriteAll + Unpin,
    Meta: AsyncRead + AsyncWrite + AsyncWriteAll + Unpin,
{
}

impl<Get, Gets, Store, Meta> AsyncRead for AsyncOperation<Get, Gets, Store, Meta>
where
    Get: AsyncRead + AsyncWrite + AsyncWriteAll + Unpin,
    Gets: AsyncRead + AsyncWrite + AsyncWriteAll + Unpin,
    Store: AsyncRead + AsyncWrite + AsyncWriteAll + Unpin,
    Meta: AsyncRead + AsyncWrite + AsyncWriteAll + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<Result<()>> {
        let me = &mut *self;
        match me {
            Self::Get(ref mut s) => Pin::new(s).poll_read(cx, buf),
            Self::Gets(ref mut s) => Pin::new(s).poll_read(cx, buf),
            Self::Store(ref mut s) => Pin::new(s).poll_read(cx, buf),
            Self::Meta(ref mut s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}
impl<Get, Gets, Store, Meta> AsyncWrite for AsyncOperation<Get, Gets, Store, Meta>
where
    Get: AsyncRead + AsyncWrite + AsyncWriteAll + Unpin,
    Gets: AsyncRead + AsyncWrite + AsyncWriteAll + Unpin,
    Store: AsyncRead + AsyncWrite + AsyncWriteAll + Unpin,
    Meta: AsyncRead + AsyncWrite + AsyncWriteAll + Unpin,
{
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>> {
        let me = &mut *self;
        match me {
            Self::Get(ref mut s) => Pin::new(s).poll_write(cx, buf),
            Self::Gets(ref mut s) => Pin::new(s).poll_write(cx, buf),
            Self::Store(ref mut s) => Pin::new(s).poll_write(cx, buf),
            Self::Meta(ref mut s) => Pin::new(s).poll_write(cx, buf),
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        let me = &mut *self;
        match me {
            Self::Get(ref mut s) => Pin::new(s).poll_flush(cx),
            Self::Gets(ref mut s) => Pin::new(s).poll_flush(cx),
            Self::Store(ref mut s) => Pin::new(s).poll_flush(cx),
            Self::Meta(ref mut s) => Pin::new(s).poll_flush(cx),
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        let me = &mut *self;
        match me {
            Self::Get(ref mut s) => Pin::new(s).poll_shutdown(cx),
            Self::Gets(ref mut s) => Pin::new(s).poll_shutdown(cx),
            Self::Store(ref mut s) => Pin::new(s).poll_shutdown(cx),
            Self::Meta(ref mut s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}
