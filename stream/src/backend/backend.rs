use crate::{AsyncReadAll, AsyncWriteAll, Request, Response, RingBufferStream};

use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use ds::Cid;
use futures::ready;

use enum_dispatch::enum_dispatch;

#[enum_dispatch(AsyncReadAll)]
pub enum BackendStream {
    NotConnected(NotConnected),
    Backend(Backend),
}

impl BackendStream {
    pub fn not_connected() -> Self {
        BackendStream::NotConnected(NotConnected)
    }
    pub fn from(id: Cid, addr: String, inner: Arc<RingBufferStream>) -> Self {
        BackendStream::Backend(Backend::from(id, addr, inner))
    }
}

pub struct Backend {
    id: Cid,
    addr: String,
    inner: Arc<RingBufferStream>,
}

impl Backend {
    pub fn from(id: Cid, addr: String, inner: Arc<RingBufferStream>) -> Self {
        Self {
            id: id,
            addr: addr,
            inner: inner,
        }
    }

    pub fn addr(&self) -> String {
        self.addr
    }
}

impl AsyncReadAll for Backend {
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        let me = &*self;
        let slice = ready!(me.inner.poll_next(me.id.id(), cx))?;
        Poll::Ready(Ok(Response::from(slice, me.id.id(), me.inner.clone())))
    }
}

impl AsyncWriteAll for Backend {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &Request) -> Poll<Result<()>> {
        let me = &*self;
        me.inner.poll_write(me.id.id(), cx, buf)
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
}

impl AsyncWriteAll for BackendStream {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &Request) -> Poll<Result<()>> {
        let me = &mut *self;
        match me {
            BackendStream::Backend(ref mut stream) => Pin::new(stream).poll_write(cx, buf),
            BackendStream::NotConnected(ref mut stream) => Pin::new(stream).poll_write(cx, buf),
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
}

impl AsyncWriteAll for NotConnected {
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context, _buf: &Request) -> Poll<Result<()>> {
        Poll::Ready(Err(Error::new(
            ErrorKind::NotConnected,
            "write to an unconnected stream",
        )))
    }
}

pub trait AddressEnable {
    fn get_address(&self) -> String;
}

impl AddressEnable for BackendStream {
    fn get_address(&self) -> String {
        match self {
            BackendStream::Backend(backend) => backend.addr(),
            BackendStream::NotConnected(_) => "not connected".to_string(),
        }
    }
}
