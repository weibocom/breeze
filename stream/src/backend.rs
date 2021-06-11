use super::{IdAsyncRead, IdAsyncWrite};

use protocol::chan::AsyncWriteAll;
use protocol::ResponseParser;

use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

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

impl<I, Id> AsyncRead for Backend<I, Id>
where
    I: IdAsyncRead,
    Id: super::Id,
{
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context, buf: &mut ReadBuf) -> Poll<Result<()>> {
        let me = &*self;
        me.inner.poll_read(me.id.id(), cx, buf)
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
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl<I, Id> AsyncRead for BackendStream<I, Id>
where
    Id: Unpin + super::Id,
    I: IdAsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<Result<()>> {
        let me = &mut *self;
        match me {
            BackendStream::Backend(ref mut stream) => Pin::new(stream).poll_read(cx, buf),
            BackendStream::NotConnected(ref mut stream) => Pin::new(stream).poll_read(cx, buf),
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
impl AsyncRead for NotConnected {
    fn poll_read(self: Pin<&mut Self>, _cx: &mut Context, _buf: &mut ReadBuf) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for NotConnected {
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context, _buf: &[u8]) -> Poll<Result<usize>> {
        Poll::Ready(Err(Error::from(ErrorKind::NotConnected)))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
}

use super::{Cid, Ids, MpscRingBufferStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
pub struct BackendBuilder {
    closed: AtomicBool,
    addr: String,
    stream: Arc<MpscRingBufferStream>,
    ids: Arc<Ids>,
}

impl BackendBuilder {
    pub fn from<P>(addr: String, req_buf: usize, resp_buf: usize, parallel: usize) -> Arc<Self>
    where
        P: Unpin + Send + Sync + ResponseParser + Default + 'static,
    {
        Self::from_with_response::<P>(addr, req_buf, resp_buf, parallel, false)
    }
    pub fn ignore_response<P>(addr: String, req_buf: usize, parallel: usize) -> Arc<Self>
    where
        P: Unpin + Send + Sync + ResponseParser + Default + 'static,
    {
        Self::from_with_response::<P>(addr, req_buf, 2048, parallel, true)
    }
    pub fn from_with_response<P>(
        addr: String,
        req_buf: usize,
        resp_buf: usize,
        parallel: usize,
        ignore_response: bool,
    ) -> Arc<Self>
    where
        P: Unpin + Send + Sync + ResponseParser + Default + 'static,
    {
        let stream = MpscRingBufferStream::with_capacity(req_buf, resp_buf, parallel);
        let me = Self {
            closed: AtomicBool::new(false),
            addr: addr,
            stream: Arc::new(stream),
            ids: Arc::new(Ids::with_capacity(parallel)),
        };
        let me = Arc::new(me);
        let checker = BackendChecker::<P>::from(me.clone(), ignore_response);
        checker.start_check();
        me
    }
    pub fn build(&self) -> BackendStream<Arc<MpscRingBufferStream>, Cid> {
        self.ids
            .next()
            .map(|cid| BackendStream::from(Cid::new(cid, self.ids.clone()), self.stream.clone()))
            .unwrap_or_else(|| BackendStream::not_connected())
    }
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }
}

use tokio::time::{interval, Interval};
pub struct BackendChecker<P> {
    inner: Arc<BackendBuilder>,

    // 记录两个任务是否完成。
    ignore_response: Arc<AtomicBool>,
    tick: Interval,
    _mark: std::marker::PhantomData<P>,
}

impl<P> BackendChecker<P>
where
    P: Unpin + Send + Sync + ResponseParser + Default + 'static,
{
    fn from(builder: Arc<BackendBuilder>, ignore_response: bool) -> Self {
        Self {
            inner: builder,
            ignore_response: Arc::new(AtomicBool::new(ignore_response)),
            tick: interval(std::time::Duration::from_secs(3)),
            _mark: Default::default(),
        }
    }
    fn start_check(mut self) {
        tokio::spawn(async move {
            self.check().await;
        });
    }
    async fn check(&mut self) {
        while !self.inner.closed.load(Ordering::Acquire) {
            self.check_reconnected_once().await;
            self.tick.tick().await;
        }
    }

    async fn check_reconnected_once(&self) {
        if !self.inner.stream.running() {
            if !self.inner.stream.is_complete() {
                // stream已经没有在运行，但没有结束。说明可能有一些waker等数据没有处理完。通知处理
                self.inner.stream.try_complete();
                return;
            }
            // 开始建立连接
            match tokio::net::TcpStream::connect(&self.inner.addr).await {
                Ok(stream) => {
                    let (r, w) = stream.into_split();
                    let req_stream = self.inner.stream.clone();
                    if !self.ignore_response.load(Ordering::Acquire) {
                        req_stream.bridge(r, w, P::default());
                    } else {
                        req_stream.bridge_no_reply(r, w, P::default());
                    }
                }
                // TODO
                Err(_e) => {}
            }
        }
    }
}
