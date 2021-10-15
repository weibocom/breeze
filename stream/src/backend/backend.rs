use crate::{Address, Addressed, AsyncReadAll, AsyncWriteAll, MpmcStream, Request, Response};

use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use ds::Cid;
use futures::ready;
use protocol::Operation;

pub struct BackendStream {
    id: Cid,
    inner: Arc<MpmcStream>,

    // 用来记录metrics
    instant: Instant,
    tx: usize,
    op: Operation,
}

impl BackendStream {
    pub fn from(id: Cid, stream: Arc<MpmcStream>) -> Self {
        Self {
            id: id,
            inner: stream,
            instant: Instant::now(),
            tx: 0,
            op: protocol::Operation::Other,
        }
    }

    // 不使用cid. 当前stream，用于noreply请求
    pub fn faked_clone(&self) -> Self {
        Self {
            id: Cid::fake(),
            inner: self.inner.clone(),
            instant: Instant::now(),
            tx: 0,
            op: Operation::Other,
        }
    }

    pub fn addr(&self) -> &str {
        &self.inner.addr()
    }
}

impl AsyncReadAll for BackendStream {
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

impl AsyncWriteAll for BackendStream {
    #[inline(always)]
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &Request) -> Poll<Result<()>> {
        let me = &mut *self;
        if !buf.noreply() && me.id.faked() {
            return Poll::Ready(Err(Error::new(ErrorKind::NotConnected, "faked cid")));
        }
        me.instant = Instant::now();
        me.tx = buf.len();
        me.op = buf.operation();
        me.inner.poll_write(me.id.id(), cx, buf)
    }
}

impl Drop for BackendStream {
    #[inline(always)]
    fn drop(&mut self) {
        if !self.id.faked() {
            self.inner.shutdown(self.id.id());
        }
    }
}

impl Addressed for BackendStream {
    #[inline(always)]
    fn addr(&self) -> Address {
        self.inner.addr().to_string().into()
    }
}
