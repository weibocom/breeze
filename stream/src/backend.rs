use super::{IdAsyncRead, IdAsyncWrite};

use protocol::chan::{AsyncReadAll, AsyncWriteAll, ResponseItem};
use protocol::Protocol;

use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;
use tokio::io::{AsyncWrite, ReadBuf};

use enum_dispatch::enum_dispatch;

#[enum_dispatch(AsyncReadAll)]
pub enum BackendStream<I, Id> {
    NotConnected(NotConnected),
    Backend(Backend<I, Id>),
}

impl<I, Id> AsyncWriteAll for BackendStream<I, Id> {}

impl<I, Id> BackendStream<I, Id> {
    pub fn not_connected() -> Self {
        BackendStream::NotConnected(NotConnected)
    }
    pub fn from(id: Id, inner: I) -> Self {
        BackendStream::Backend(Backend::from(id, inner))
    }
}

pub struct Backend<I, Id> {
    id: Id,
    inner: I,
}

impl<I, Id> Backend<I, Id> {
    pub fn from(id: Id, stream: I) -> Self {
        Self {
            id: id,
            inner: stream,
        }
    }
}

impl<I, Id> AsyncReadAll for Backend<I, Id>
where
    I: IdAsyncRead,
    Id: super::Id,
{
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<ResponseItem>> {
        let me = &*self;
        me.inner.poll_next(me.id.id(), cx)
    }
    fn poll_done(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        let me = &*self;
        me.inner.poll_done(me.id.id(), cx)
    }
}

impl<I, Id> AsyncWrite for Backend<I, Id>
where
    I: IdAsyncWrite,
    Id: super::Id,
{
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

impl<I, Id> AsyncReadAll for BackendStream<I, Id>
where
    Id: Unpin + super::Id,
    I: IdAsyncRead + Unpin,
{
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<ResponseItem>> {
        let me = &mut *self;
        match me {
            BackendStream::Backend(ref mut stream) => Pin::new(stream).poll_next(cx),
            BackendStream::NotConnected(ref mut stream) => Pin::new(stream).poll_next(cx),
        }
    }
    fn poll_done(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        let me = &mut *self;
        match me {
            BackendStream::Backend(ref mut stream) => Pin::new(stream).poll_done(cx),
            BackendStream::NotConnected(ref mut stream) => Pin::new(stream).poll_done(cx),
        }
    }
}

impl<I, Id> AsyncWrite for BackendStream<I, Id>
where
    Id: Unpin + super::Id,
    I: IdAsyncWrite + Unpin,
{
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>> {
        let me = &mut *self;
        match me {
            BackendStream::Backend(ref mut stream) => Pin::new(stream).poll_write(cx, buf),
            BackendStream::NotConnected(ref mut stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>>
    where
        Id: Unpin + super::Id,
        I: IdAsyncWrite + Unpin,
    {
        let me = &mut *self;
        match me {
            BackendStream::Backend(ref mut stream) => Pin::new(stream).poll_flush(cx),
            BackendStream::NotConnected(ref mut stream) => Pin::new(stream).poll_flush(cx),
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>>
    where
        Id: Unpin + super::Id,
        I: IdAsyncWrite + Unpin,
    {
        let me = &mut *self;
        match me {
            BackendStream::Backend(ref mut stream) => Pin::new(stream).poll_shutdown(cx),
            BackendStream::NotConnected(ref mut stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

pub struct NotConnected;
impl AsyncReadAll for NotConnected {
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<ResponseItem>> {
        Poll::Ready(Err(Error::new(
            ErrorKind::NotConnected,
            "read from an unconnected stream",
        )))
    }
    fn poll_done(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<()>> {
        Poll::Ready(Err(Error::new(
            ErrorKind::NotConnected,
            "poll done from an unconnected stream",
        )))
    }
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

use super::{Cid, Ids, RingBufferStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
pub struct BackendBuilder {
    closed: AtomicBool,
    done: Arc<AtomicBool>,
    addr: String,
    stream: Arc<RingBufferStream>,
    ids: Arc<Ids>,
}

impl BackendBuilder {
    pub fn from<P>(
        parser: P,
        addr: String,
        req_buf: usize,
        resp_buf: usize,
        parallel: usize,
    ) -> Arc<Self>
    where
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        Self::from_with_response(parser, addr, req_buf, resp_buf, parallel, false)
    }
    pub fn ignore_response<P>(parser: P, addr: String, req_buf: usize, parallel: usize) -> Arc<Self>
    where
        P: Unpin + Send + Sync + Protocol + Clone + 'static,
    {
        Self::from_with_response(parser, addr, req_buf, 2048, parallel, true)
    }
    pub fn from_with_response<P>(
        parser: P,
        addr: String,
        req_buf: usize,
        resp_buf: usize,
        parallel: usize,
        ignore_response: bool,
    ) -> Arc<Self>
    where
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        let done = Arc::new(AtomicBool::new(true));
        let min = parser.min_last_response_size();
        let stream = RingBufferStream::with_capacity(min, parallel, done.clone());
        let me = Self {
            closed: AtomicBool::new(false),
            done: done.clone(),
            addr: addr,
            stream: Arc::new(stream),
            ids: Arc::new(Ids::with_capacity(parallel)),
        };
        let me = Arc::new(me);
        println!("request buffer:{} response buffer:{}", req_buf, resp_buf);
        let checker = BackendChecker::from(me.clone(), ignore_response, req_buf, resp_buf);
        checker.start_check(parser);
        me
    }
    pub fn build(&self) -> BackendStream<Arc<RingBufferStream>, Cid> {
        self.ids
            .next()
            .map(|cid| BackendStream::from(Cid::new(cid, self.ids.clone()), self.stream.clone()))
            .unwrap_or_else(|| {
                println!("connection id overflow, connection established failed");
                BackendStream::not_connected()
            })
    }
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.done.store(true, Ordering::Release);
    }
}

use tokio::time::{interval, Interval};
pub struct BackendChecker {
    req_buf: usize,
    resp_buf: usize,
    inner: Arc<BackendBuilder>,

    // 记录两个任务是否完成。
    ignore_response: Arc<AtomicBool>,
    tick: Interval,
}

impl BackendChecker {
    fn from(
        builder: Arc<BackendBuilder>,
        ignore_response: bool,
        req_buf: usize,
        resp_buf: usize,
    ) -> Self {
        Self {
            inner: builder,
            ignore_response: Arc::new(AtomicBool::new(ignore_response)),
            tick: interval(std::time::Duration::from_secs(3)),
            req_buf: req_buf,
            resp_buf: resp_buf,
        }
    }
    fn start_check<P>(mut self, parser: P)
    where
        P: Clone + Send + Sync + Protocol + 'static,
    {
        tokio::spawn(async move {
            self.check(parser).await;
        });
    }
    async fn check<P>(&mut self, parser: P)
    where
        P: Clone + Send + Sync + Protocol + 'static,
    {
        while !self.inner.closed.load(Ordering::Acquire) {
            self.check_reconnected_once(&parser).await;
            self.tick.tick().await;
        }
    }

    async fn check_reconnected_once<P>(&self, parser: &P)
    where
        P: Clone + Send + Sync + Protocol + 'static,
    {
        // 说明连接未主动关闭，但任务已经结束，需要再次启动
        if self.inner.done.load(Ordering::Acquire) {
            if !self.inner.stream.reset().await {
                // stream已经没有在运行，但没有结束。说明可能有一些waker等数据没有处理完。通知处理
                return;
            }
            let parser = parser.clone();
            // 开始建立连接
            match tokio::net::TcpStream::connect(&self.inner.addr).await {
                Ok(stream) => {
                    let (r, w) = stream.into_split();
                    let req_stream = self.inner.stream.clone();
                    if !self.ignore_response.load(Ordering::Acquire) {
                        req_stream.bridge(parser, self.req_buf, self.resp_buf, r, w);
                    } else {
                        req_stream.bridge_no_reply(self.resp_buf, r, w);
                    }
                }
                // TODO
                Err(e) => {
                    println!(
                        "failed to establish a connection to {} err:{:?}",
                        self.inner.addr, e
                    );
                }
            }
        }
    }
}
