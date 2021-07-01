use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::AsyncWrite;

use crate::{AsyncReadAll, AsyncWriteAll, Response};

pub enum AsyncOperation<Get, Gets, Store, Meta> {
    Get(Get),
    Gets(Gets),
    Store(Store),
    Meta(Meta),
}

impl<Get, Gets, Store, Meta> AsyncWriteAll for AsyncOperation<Get, Gets, Store, Meta>
where
    Get: AsyncReadAll + AsyncWrite + AsyncWriteAll + Unpin,
    Gets: AsyncReadAll + AsyncWrite + AsyncWriteAll + Unpin,
    Store: AsyncReadAll + AsyncWrite + AsyncWriteAll + Unpin,
    Meta: AsyncReadAll + AsyncWrite + AsyncWriteAll + Unpin,
{
}

impl<Get, Gets, Store, Meta> AsyncReadAll for AsyncOperation<Get, Gets, Store, Meta>
where
    Get: AsyncReadAll + AsyncWrite + AsyncWriteAll + Unpin,
    Gets: AsyncReadAll + AsyncWrite + AsyncWriteAll + Unpin,
    Store: AsyncReadAll + AsyncWrite + AsyncWriteAll + Unpin,
    Meta: AsyncReadAll + AsyncWrite + AsyncWriteAll + Unpin,
{
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        let me = &mut *self;
        match me {
            Self::Get(ref mut s) => Pin::new(s).poll_next(cx),
            Self::Gets(ref mut s) => Pin::new(s).poll_next(cx),
            Self::Store(ref mut s) => Pin::new(s).poll_next(cx),
            Self::Meta(ref mut s) => Pin::new(s).poll_next(cx),
        }
    }
    //fn poll_done(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
    //    let me = &mut *self;
    //    match me {
    //        Self::Get(ref mut s) => Pin::new(s).poll_done(cx),
    //        Self::Gets(ref mut s) => Pin::new(s).poll_done(cx),
    //        Self::Store(ref mut s) => Pin::new(s).poll_done(cx),
    //        Self::Meta(ref mut s) => Pin::new(s).poll_done(cx),
    //    }
    //}
}
impl<Get, Gets, Store, Meta> AsyncWrite for AsyncOperation<Get, Gets, Store, Meta>
where
    Get: AsyncReadAll + AsyncWrite + AsyncWriteAll + Unpin,
    Gets: AsyncReadAll + AsyncWrite + AsyncWriteAll + Unpin,
    Store: AsyncReadAll + AsyncWrite + AsyncWriteAll + Unpin,
    Meta: AsyncReadAll + AsyncWrite + AsyncWriteAll + Unpin,
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
