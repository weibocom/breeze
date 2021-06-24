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
use std::sync::{Arc, Mutex};
pub struct BackendBuilder {
    closed: Arc<AtomicBool>,
    finished: Arc<AtomicBool>,
    addr: String,
    stream: Arc<RingBufferStream>,
    ids: Arc<Ids>,
    //checker: Option<Arc<Mutex<BackendChecker<()>>>>,
    check_task: Option<std::thread::JoinHandle<()>>,
}

impl BackendBuilder {
    pub fn from<P>(addr: String, req_buf: usize, resp_buf: usize, parallel: usize) -> Arc<Mutex<Self>>
    where
        P: Unpin + Send + Sync + ResponseParser + Default + 'static,
    {
        Self::from_with_response::<P>(addr, req_buf, resp_buf, parallel, false)
    }
    pub fn ignore_response<P>(addr: String, req_buf: usize, parallel: usize) -> Arc<Mutex<Self>>
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
    ) -> Arc<Mutex<Self>>
    where
        P: Unpin + Send + Sync + ResponseParser + Default + 'static,
    {
        let done = Arc::new(AtomicBool::new(false));
        let stream = RingBufferStream::with_capacity(parallel, done.clone());
        let me_builder = Self {
            closed: Arc::new(AtomicBool::new(true)),
            finished: done.clone(),
            addr: addr,
            stream: Arc::new(stream),
            ids: Arc::new(Ids::with_capacity(parallel)),
            //checker: None,
            check_task: None
        };
        let mut me = Arc::new(Mutex::new(me_builder));
        let checker = BackendChecker::<P>::from(me.clone(), ignore_response, req_buf, resp_buf);
        me.lock().unwrap().start_check::<P>(checker);
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
        self.finished.store(true, Ordering::Release);
    }
    pub fn reconnect(&self) {
        self.closed.store(true, Ordering::Release);
        if self.check_task.is_some() {
            self.check_task.as_ref().unwrap().thread().unpark();
        }
    }

    fn start_check<P>(&mut self, checker: BackendChecker<(P)>)
        where
            P: Unpin + Send + Sync + ResponseParser + Default + 'static,
    {
        let arc_mutex_checker = Arc::new(Mutex::new(checker));
        //self.checker = Some(arc_mutex_checker.clone());
        let res = std::thread::spawn(move || {
            arc_mutex_checker.clone().as_ref().lock().unwrap().check();
        });
        self.check_task = Some(res);
    }
}

use tokio::time::{interval, Interval};
use std::time::Duration;

pub struct BackendChecker<P> {
    req_buf: usize,
    resp_buf: usize,
    inner: Arc<Mutex<BackendBuilder>>,

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
        builder: Arc<Mutex<BackendBuilder>>,
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
    async fn check(&mut self) {
        while !self.inner.lock().unwrap().finished.load(Ordering::Acquire) {
            self.check_reconnected_once().await;
            thread::park_timeout(Duration::from_micros(1000 as u64));
        }
        if !self.inner.lock().unwrap().stream.is_complete() {
            // stream已经没有在运行，但没有结束。说明可能有一些waker等数据没有处理完。通知处理
            self.inner.lock().unwrap().stream.try_complete();
        }
    }

    async fn check_reconnected_once(&self) {
        // 说明连接未主动关闭，但任务已经结束，需要再次启动
        if self.inner.lock().unwrap().closed.load(Ordering::Acquire) {
            // 开始建立连接
            match tokio::net::TcpStream::connect(&self.inner.lock().unwrap().addr).await {
                Ok(stream) => {
                    let (r, w) = stream.into_split();
                    let req_stream = self.inner.lock().unwrap().stream.clone();
                    if !self.ignore_response.load(Ordering::Acquire) {
                        req_stream.bridge(self.req_buf, self.resp_buf, r, w, P::default());
                    } else {
                        req_stream.bridge_no_reply(self.resp_buf, r, w, P::default());
                    }
                    self.inner.lock().unwrap().closed.store(false, Ordering::Release);
                }
                // TODO
                Err(_e) => {
                    println!("connect to {} failed, error = {}", self.inner.lock().unwrap().addr, _e);
                }
            }
        }
    }
}
