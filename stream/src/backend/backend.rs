//use super::super::{IdAsyncRead, IdAsyncWrite};

use crate::{AsyncReadAll, AsyncWriteAll, Response, RingBufferStream};

use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::super::Cid;
use futures::ready;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use enum_dispatch::enum_dispatch;

#[enum_dispatch(AsyncReadAll)]
pub enum BackendStream {
    NotConnected(NotConnected),
    Backend(Backend),
}

impl AsyncWriteAll for BackendStream {}

impl BackendStream {
    pub fn not_connected() -> Self {
        BackendStream::NotConnected(NotConnected)
    }
    pub fn from(id: Cid, inner: Arc<RingBufferStream>) -> Self {
        BackendStream::Backend(Backend::from(id, inner))
    }
}

pub struct Backend {
    id: Cid,
    inner: Arc<RingBufferStream>,
}

impl Backend {
    pub fn from(id: Cid, inner: Arc<RingBufferStream>) -> Self {
        Self {
            id: id,
            inner: inner,
        }
    }
}

impl AsyncReadAll for Backend {
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        let me = &*self;
        let slice = ready!(me.inner.poll_next(me.id.id(), cx))?;
        Poll::Ready(Ok(Response::from(slice, me.id.id(), me.inner.clone())))
    }
    //fn poll_done(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<()>> {
    //    //todo!("not suppotred");
    //    Poll::Ready(Ok(()))
    //}
}

impl AsyncWrite for Backend {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>> {
        let me = &*self;
        ready!(me.inner.poll_write(me.id.id(), cx, buf))?;
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        let me = &*self;
        me.inner.poll_shutdown(me.id.id(), cx)
    }
}

impl AsyncReadAll for BackendStream {
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        let me = &mut *self;
        match me {
            BackendStream::Backend(ref mut stream) => Pin::new(stream).poll_next(cx),
            BackendStream::NotConnected(ref mut stream) => Pin::new(stream).poll_next(cx),
        }
    }
    //fn poll_done(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
    //    let me = &mut *self;
    //    match me {
    //        BackendStream::Backend(ref mut stream) => Pin::new(stream).poll_done(cx),
    //        BackendStream::NotConnected(ref mut stream) => Pin::new(stream).poll_done(cx),
    //    }
    //}
}

impl AsyncWrite for BackendStream {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>> {
        let me = &mut *self;
        match me {
            BackendStream::Backend(ref mut stream) => Pin::new(stream).poll_write(cx, buf),
            BackendStream::NotConnected(ref mut stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        let me = &mut *self;
        match me {
            BackendStream::Backend(ref mut stream) => Pin::new(stream).poll_flush(cx),
            BackendStream::NotConnected(ref mut stream) => Pin::new(stream).poll_flush(cx),
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        let me = &mut *self;
        match me {
            BackendStream::Backend(ref mut stream) => Pin::new(stream).poll_shutdown(cx),
            BackendStream::NotConnected(ref mut stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

pub struct NotConnected;
impl AsyncReadAll for NotConnected {
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<Response>> {
        Poll::Ready(Err(Error::new(
            ErrorKind::NotConnected,
            "read from an unconnected stream",
        )))
    }
    //fn poll_done(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<()>> {
    //    Poll::Ready(Err(Error::new(
    //        ErrorKind::NotConnected,
    //        "poll done from an unconnected stream",
    //    )))
    //}
}

impl AsyncWrite for NotConnected {
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context, _buf: &[u8]) -> Poll<Result<usize>> {
        Poll::Ready(Err(Error::new(
            ErrorKind::NotConnected,
            "write to an unconnected stream",
        )))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
}
