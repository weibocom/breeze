use super::{IdAsyncRead, IdAsyncWrite};

use protocol::chan::{AsyncReadAll, AsyncWriteAll, ResponseItem};
use protocol::Protocol;

use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll, Waker, RawWaker};

use futures::ready;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use std::thread;

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
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};
pub struct BackendBuilder {
    connected: Arc<AtomicBool>,
    finished: Arc<AtomicBool>,
    closed: AtomicBool,
    done: Arc<AtomicBool>,
    addr: String,
    stream: Arc<RingBufferStream>,
    ids: Arc<Ids>,
    check_waker: Arc<RwLock<BackendWaker>>,
}

impl BackendBuilder {
    pub fn from<P>(
        parser: P,
        addr: String,
        req_buf: usize,
        resp_buf: usize,
        parallel: usize,
    ) -> Arc<BackendBuilder>
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
        println!("come into from_with_response");
        let done = Arc::new(AtomicBool::new(true));
        let min = parser.min_last_response_size();
        let stream = RingBufferStream::with_capacity(min, parallel, done.clone());
        let me = Self {
            connected: Arc::new(AtomicBool::new(false)),
            finished: Arc::new(AtomicBool::new(false)),
            closed: AtomicBool::new(false),
            done: done.clone(),
            addr: addr,
            stream: Arc::new(stream),
            ids: Arc::new(Ids::with_capacity(parallel)),
            check_waker: Arc::new(RwLock::new(BackendWaker::new())),
        };
        let me = Arc::new(me);
        println!("request buffer:{} response buffer:{}", req_buf, resp_buf);
        let checker = Arc::new(BackendChecker::from(me.clone(), ignore_response, req_buf, resp_buf));
        let t = me.start_check(checker, parser.clone());

        me.check_waker.clone().write().unwrap().check_task = Some(t);
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
    pub fn finish(&self) {
        self.finished.store(true, Ordering::Release);
    }
    pub fn reconnect(&self) {
        if !self.finished.load(Ordering::Acquire) {
            self.stream.clone().try_complete();
            self.connected.store(false, Ordering::Release);
            self.check_waker.read().unwrap().wake();
        }
    }

    pub fn do_reconnect(&self) {
        self.done.store(true, Ordering::Release);
    }

    pub fn start_check<P>(&self, checker: Arc<BackendChecker>, parser: P) -> JoinHandle<()>
        where
            P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        let runtime = Runtime::new().unwrap();
        let cloned_parser = parser.clone();
        let res = std::thread::spawn(move || {
            let check_future = checker.check(cloned_parser);
            runtime.block_on(check_future);
        });
        res
    }
}

use tokio::time::{interval, Interval};
use std::time::Duration;
use std::sync::RwLock;
use tokio::runtime::Runtime;
use std::thread::JoinHandle;

pub struct BackendWaker {
    check_task: Option<std::thread::JoinHandle<()>>,
}

impl BackendWaker {
    fn new() -> Self {
        BackendWaker {
            check_task: None,
        }
    }

    fn wake(&self) {
        if self.check_task.is_some() {
            self.check_task.as_ref().unwrap().thread().unpark();
        }
    }
}

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
        let me = Self {
            inner: builder,
            ignore_response: Arc::new(AtomicBool::new(ignore_response)),
            tick: interval(std::time::Duration::from_secs(3)),
            req_buf: req_buf,
            resp_buf: resp_buf,
        };
        me

    }

    async fn check<P>(&self, parser: P)
        where
            P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        println!("come into check");
        while !self.inner.finished.load(Ordering::Acquire) {
            self.check_reconnected_once(parser.clone()).await;
            thread::park_timeout(Duration::from_millis(1000 as u64));
        }
        if !self.inner.stream.clone().is_complete() {
            // stream已经没有在运行，但没有结束。说明可能有一些waker等数据没有处理完。通知处理
            self.inner.stream.clone().try_complete();
        }
    }

    async fn check_reconnected_once<P>(&self, parser: P)
        where
            P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        if self.inner.clone().done.load(Ordering::Acquire) {
            println!("need to reconnect");
            self.inner.clone().reconnect();
            //self.inner.clone().read().unwrap().done.store(false, Ordering::Release);
        }
        // 说明连接未主动关闭，但任务已经结束，需要再次启动
        let connected = self.inner.connected.load(Ordering::Acquire);
        if !connected {
            let addr = &self.inner.addr;
            println!("connection is closed");
            // 开始建立连接
            match tokio::net::TcpStream::connect(addr).await {
                Ok(stream) => {
                    println!("connected to {}", addr);
                    let (r, w) = stream.into_split();
                    let req_stream = self.inner.stream.clone();

                    if !self.ignore_response.load(Ordering::Acquire) {
                        println!("go to bridge");
                        req_stream.bridge(parser.clone(), self.req_buf, self.resp_buf, r, w, self.inner.clone());
                    } else {
                        println!("go to bridge_no_reply");
                        req_stream.bridge_no_reply(self.resp_buf, r, w, self.inner.clone());
                    }
                    println!("set false to closed");
                    self.inner.connected.store(true, Ordering::Release);
                }
                // TODO
                Err(_e) => {
                    println!("connect to {} failed, error = {}", addr, _e);
                }
            }
        }
    }
}
