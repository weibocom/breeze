use crate::{BackendStream, RingBufferStream};
use ds::{Cid, Ids};
use protocol::Protocol;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

static RECONNECT_ERROR_CAP: usize = 5 as usize;

enum BackendErrorType {
    ConnError = 0 as isize,
    RequestError,
}

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
    ) -> Arc<Self>
    where
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        log::info!("come into from_with_response");
        let done = Arc::new(AtomicBool::new(true));
        let stream = RingBufferStream::with_capacity(parallel, done.clone());
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
        log::info!("request buffer:{} response buffer:{}", req_buf, resp_buf);
        let checker = Arc::new(BackendChecker::from(me.clone(), req_buf, resp_buf));
        let t = me.start_check(checker, parser.clone());

        me.check_waker.clone().write().unwrap().check_task = Some(t);
        me
    }
    pub fn build(&self) -> BackendStream {
        self.ids
            .next()
            .map(|cid| BackendStream::from(Cid::new(cid, self.ids.clone()), self.stream.clone()))
            .unwrap_or_else(|| {
                log::info!("connection id overflow, connection established failed");
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

use std::sync::RwLock;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, UNIX_EPOCH, SystemTime};
use tokio::runtime::Runtime;
use tokio::time::{interval, Interval};
use std::collections::LinkedList;
use std::net::SocketAddr;

pub struct BackendWaker {
    check_task: Option<std::thread::JoinHandle<()>>,
}

impl BackendWaker {
    fn new() -> Self {
        BackendWaker { check_task: None }
    }

    fn wake(&self) {
        if self.check_task.is_some() {
            self.check_task.as_ref().unwrap().thread().unpark();
        }
    }
}

pub struct BackendErrorCounter {
    error_window_size: u64,
    error_count: usize,
    error_type: BackendErrorType,
    error_time_list: LinkedList<(u64, usize)>,
    error_total_value: usize,
}

impl BackendErrorCounter {
    fn new(error_window_size: u64, error_type: BackendErrorType) -> BackendErrorCounter {
        BackendErrorCounter {
            error_window_size,
            error_count: 0 as usize,
            error_type,
            error_time_list: LinkedList::new(),
            error_total_value: 0 as usize,
        }
    }

    fn add_error(&mut self, error_value: usize) {
        let current = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        self.judge_window();
        self.error_total_value += error_value;
        self.error_time_list.push_back((current, error_value));
        self.error_count += 1;
    }

    fn judge_error(&mut self, error_cap: usize) -> bool {
        self.judge_window();
        self.error_total_value >= error_cap
    }

    fn judge_window(&mut self) {
        let current = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        while !self.error_time_list.is_empty() {
            if self.error_time_list.front().unwrap().0.lt(&(current - self.error_window_size)) {
                self.error_total_value -= self.error_time_list.front().unwrap().1;
                self.error_time_list.pop_front();
                self.error_count -= 1;
            }
            else {
                break;
            }
        }
    }
}

pub struct BackendChecker {
    req_buf: usize,
    resp_buf: usize,
    inner: Arc<BackendBuilder>,

    tick: Interval,
}

impl BackendChecker {
    fn from(builder: Arc<BackendBuilder>, req_buf: usize, resp_buf: usize) -> Self {
        let me = Self {
            inner: builder,
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
        let mut reconnect_error = BackendErrorCounter::new(60 as u64, BackendErrorType::ConnError);
        log::info!("come into check");
        while !self.inner.finished.load(Ordering::Acquire) {
            self.check_reconnected_once(parser.clone(), &mut reconnect_error).await;
            std::thread::park_timeout(Duration::from_millis(1000 as u64));
        }
        if !self.inner.stream.clone().is_complete() {
            // stream已经没有在运行，但没有结束。说明可能有一些waker等数据没有处理完。通知处理
            self.inner.stream.clone().try_complete();
        }
    }

    async fn check_reconnected_once<P>(&self, parser: P, reconnect_error: &mut BackendErrorCounter)
    where
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        if self.inner.clone().done.load(Ordering::Acquire) {
            log::info!("need to reconnect");
            self.inner.clone().reconnect();
            //self.inner.clone().read().unwrap().done.store(false, Ordering::Release);
        }
        // 说明连接未主动关闭，但任务已经结束，需要再次启动
        let connected = self.inner.connected.load(Ordering::Acquire);
        if !connected {
            let addr = &self.inner.addr;
            log::info!("connection is closed");
            if reconnect_error.judge_error(RECONNECT_ERROR_CAP) {
                log::warn!("connect to {} error over {} times, will not connect recently", self.inner.addr, RECONNECT_ERROR_CAP);
                return;
            }
            match tokio::time::timeout(std::time::Duration::from_secs(2), tokio::net::TcpStream::connect(addr)).await {
                Ok(connect_result) => {
                    if connect_result.is_ok() {
                        let stream = connect_result.unwrap();
                        log::info!("connected to {}", addr);
                        let _ = stream.set_nodelay(true);
                        let (r, w) = stream.into_split();
                        let req_stream = self.inner.stream.clone();

                        log::info!("go to bridge");
                        req_stream.bridge(
                            parser.clone(),
                            self.req_buf,
                            self.resp_buf,
                            r,
                            w,
                            self.inner.clone(),
                        );
                        log::info!("set false to closed");
                        self.inner.connected.store(true, Ordering::Release);
                    }
                    else {
                        reconnect_error.add_error(1);
                        log::info!("connect to {} failed, error = {}", addr, connect_result.unwrap_err());
                    }

                }
                // TODO
                Err(_e) => {
                    reconnect_error.add_error(1);
                    log::info!("connect to {} timeout, error = {}", addr, _e);
                }
            }
        }
    }
}
