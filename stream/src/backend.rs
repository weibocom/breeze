use super::{IdAsyncRead, IdAsyncWrite};

use protocol::chan::AsyncWriteAll;
use protocol::ResponseParser;

use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use futures::ready;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use std::thread;

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
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        let me = &*self;
        me.inner.poll_shutdown(me.id.id(), cx)
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
    addr: String,
    stream: Arc<RingBufferStream>,
    ids: Arc<Ids>,
    //checker: Option<Arc<Mutex<BackendChecker<()>>>>,
    check_task: Option<std::thread::JoinHandle<()>>,
}

impl BackendBuilder {
    pub fn from<P>(addr: String, req_buf: usize, resp_buf: usize, parallel: usize) -> Arc<RwLock<Self>>
    where
        P: Unpin + Send + Sync + ResponseParser + Default + 'static,
    {
        Self::from_with_response::<P>(addr, req_buf, resp_buf, parallel, false)
    }
    pub fn ignore_response<P>(addr: String, req_buf: usize, parallel: usize) -> Arc<RwLock<Self>>
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
    ) -> Arc<RwLock<Self>>
    where
        P: Unpin + Send + Sync + ResponseParser + Default + 'static,
    {
        println!("come into from_with_response");
        let done = Arc::new(AtomicBool::new(true));
        let stream = RingBufferStream::with_capacity(parallel, done.clone());
        let me_builder = Self {
            connected: Arc::new(AtomicBool::new(false)),
            finished: Arc::new(AtomicBool::new(false)),
            addr: addr,
            stream: Arc::new(stream),
            ids: Arc::new(Ids::with_capacity(parallel)),
            //checker: None,
            check_task: None
        };
        let me = Arc::new(RwLock::new(me_builder));
        let checker = BackendChecker::<P>::from(me.clone(), ignore_response, req_buf, resp_buf);
        let t = me.read().unwrap().start_check(checker);
        me.write().unwrap().check_task = Some(t);
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
    pub fn finish(&self) {
        self.finished.store(true, Ordering::Release);
    }
    pub fn reconnect(&self) {
        if !self.finished.load(Ordering::Acquire) {
            self.stream.try_complete();
            self.connected.store(false, Ordering::Release);
            if self.check_task.is_some() {
                self.check_task.as_ref().unwrap().thread().unpark();
            }
        }
    }

    pub fn start_check<P>(&self, checker: BackendChecker<(P)>) -> JoinHandle<()>
        where
            P: Unpin + Send + Sync + ResponseParser + Default + 'static,
    {
        //let arc_mutex_checker = Arc::new(RwLock::new(checker));
        let runtime = Runtime::new().unwrap();
        //self.checker = Some(arc_mutex_checker.clone());
        let res = std::thread::spawn(move || {
            //let arc_mutex_checker = Arc::new(checker);
            //let check_future = arc_mutex_checker.check();
            //runtime.block_on(check_future);
            let check_future = checker.check();
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

pub struct BackendChecker<P> {
    req_buf: usize,
    resp_buf: usize,
    inner: Arc<RwLock<BackendBuilder>>,

    // 记录两个任务是否完成。
    ignore_response: Arc<AtomicBool>,
    tick: Interval,
    _mark: std::marker::PhantomData<P>,
}

impl<P> BackendChecker<P>
where
    P: Unpin + Send + Sync + ResponseParser + Default + 'static,
{
    fn from(
        builder: Arc<RwLock<BackendBuilder>>,
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
            _mark: Default::default(),
        };
        me

    }
    async fn check(self) {
        println!("come into check");
        while !self.inner.read().unwrap().finished.load(Ordering::Acquire) {
            self.check_reconnected_once().await;
            thread::park_timeout(Duration::from_millis(1000 as u64));
        }
        if !self.inner.read().unwrap().stream.is_complete() {
            // stream已经没有在运行，但没有结束。说明可能有一些waker等数据没有处理完。通知处理
            self.inner.read().unwrap().stream.try_complete();
        }
    }

    async fn check_reconnected_once(&self) {
        // 说明连接未主动关闭，但任务已经结束，需要再次启动
        let connected = self.inner.read().unwrap().connected.load(Ordering::Acquire);
        if !connected {
            let addr = &self.inner.read().unwrap().addr;
            println!("connection is closed");
            // 开始建立连接
            match tokio::net::TcpStream::connect(addr).await {
                Ok(stream) => {
                    println!("connected to {}", addr);
                    let (r, w) = stream.into_split();
                    let req_stream = self.inner.read().unwrap().stream.clone();

                    if !self.ignore_response.load(Ordering::Acquire) {
                        println!("go to bridge");
                        req_stream.bridge(self.req_buf, self.resp_buf, r, w, P::default(), self.inner.clone());
                    } else {
                        println!("go to bridge_no_reply");
                        req_stream.bridge_no_reply(self.resp_buf, r, w, P::default(), self.inner.clone());
                    }
                    println!("set false to closed");
                    self.inner.read().unwrap().connected.store(true, Ordering::Release);
                }
                // TODO
                Err(_e) => {
                    println!("connect to {} failed, error = {}", addr, _e);
                }
            }
        }
    }
}
