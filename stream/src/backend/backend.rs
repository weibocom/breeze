use crate::{AsyncReadAll, AsyncWriteAll, Request, Response, RingBufferStream};

use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use ds::Cid;
use futures::ready;
use protocol::Operation;

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
    pub fn from(id: Cid, inner: Arc<RingBufferStream>) -> Self {
        BackendStream::Backend(Backend::from(id, inner))
    }
}

pub struct Backend {
    id: Cid,
    inner: Arc<RingBufferStream>,

    // 用来记录metrics
    instant: Instant,
    tx: usize,
    op: Operation,
}

impl Backend {
    pub fn from(id: Cid, stream: Arc<RingBufferStream>) -> Self {
        Self {
            id: id,
            inner: stream,
            instant: Instant::now(),
            tx: 0,
            op: protocol::Operation::Other,
        }
    }

    pub fn addr(&self) -> &str {
        &self.inner.addr()
    }
}

impl AsyncReadAll for Backend {
    #[inline(always)]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        let me = &mut *self;
        let slice = ready!(me.inner.poll_next(me.id.id(), cx))?;

        let elapst = me.instant.elapsed();
        let rx = slice.len();
        let metric_id = me.inner.metric_id();
        metrics::qps("bytes.rx", rx, metric_id);
        metrics::qps("bytes.tx", me.tx, metric_id);
        metrics::duration(me.op.name(), elapst, metric_id);

        Poll::Ready(Ok(Response::from(slice, me.id.id(), me.inner.clone())))
    }
}

impl AsyncWriteAll for Backend {
    #[inline(always)]
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &Request) -> Poll<Result<()>> {
        let me = &mut *self;
        me.instant = Instant::now();
        me.tx = buf.len();
        me.op = buf.operation();
        me.inner.poll_write(me.id.id(), cx, buf)
    }
}

impl Drop for Backend {
    #[inline(always)]
    fn drop(&mut self) {
        self.inner.shutdown(self.id.id());
    }
}

impl AsyncReadAll for BackendStream {
    #[inline(always)]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        let me = &mut *self;
        match me {
            BackendStream::Backend(ref mut stream) => Pin::new(stream).poll_next(cx),
            BackendStream::NotConnected(ref mut stream) => Pin::new(stream).poll_next(cx),
        }
    }
}

impl AsyncWriteAll for BackendStream {
    #[inline(always)]
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
    #[inline(always)]
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<Response>> {
        Poll::Ready(Err(Error::new(
            ErrorKind::NotConnected,
            "read from an unconnected stream",
        )))
    }
}

impl AsyncWriteAll for NotConnected {
    #[inline(always)]
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
    #[inline(always)]
    fn get_address(&self) -> String {
        match self {
            BackendStream::Backend(backend) => backend.addr().to_string(),
            BackendStream::NotConnected(_) => "not connected".to_string(),
        }
    }
}
