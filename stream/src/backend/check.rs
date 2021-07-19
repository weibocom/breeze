use crate::{BackendStream, RingBufferStream};
use ds::{Cid, Ids};
use protocol::Protocol;

use std::io::Result;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

static RECONNECT_ERROR_CAP: usize = 5 as usize;
static RECONNECT_ERROR_WINDOW: u64 = 30 as u64;

use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::sleep;

enum BackendErrorType {
    ConnError = 0 as isize,
    //RequestError,
}

pub struct BackendBuilder {
    finished: Arc<AtomicBool>,
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
    ) -> Self
    where
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        let stream = Arc::new(RingBufferStream::with_capacity(parallel));
        let me = Self {
            finished: Arc::new(AtomicBool::new(false)),
            stream: stream.clone(),
            ids: Arc::new(Ids::with_capacity(parallel)),
        };
        let (tx, rx) = channel(8);
        let checker = Arc::new(BackendChecker::from(
            stream.clone(),
            req_buf,
            resp_buf,
            addr,
            me.finished.clone(),
            tx,
        ));
        checker.clone().start_check_done(parser.clone(), rx);
        checker.start_check_timeout();
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
    fn finish(&self) {
        self.finished.store(true, Ordering::Release);
    }
}

impl Drop for BackendBuilder {
    fn drop(&mut self) {
        self.finish();
    }
}

use std::collections::LinkedList;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct BackendErrorCounter {
    error_window_size: u64,
    error_count: usize,
    _error_type: BackendErrorType,
    error_time_list: LinkedList<(u64, usize)>,
    error_total_value: usize,
}

impl BackendErrorCounter {
    fn new(error_window_size: u64, error_type: BackendErrorType) -> BackendErrorCounter {
        BackendErrorCounter {
            error_window_size,
            error_count: 0 as usize,
            _error_type: error_type,
            error_time_list: LinkedList::new(),
            error_total_value: 0 as usize,
        }
    }

    fn add_error(&mut self, error_value: usize) {
        let current = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
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
        let current = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        while !self.error_time_list.is_empty() {
            if self
                .error_time_list
                .front()
                .unwrap()
                .0
                .lt(&(current - self.error_window_size))
            {
                self.error_total_value -= self.error_time_list.front().unwrap().1;
                self.error_time_list.pop_front();
                self.error_count -= 1;
            } else {
                break;
            }
        }
    }
}

pub struct BackendChecker {
    inner: Arc<RingBufferStream>,
    tx: Arc<Sender<u8>>,
    req_buf: usize,
    resp_buf: usize,
    addr: String,
    finished: Arc<AtomicBool>,
}

impl BackendChecker {
    fn from(
        stream: Arc<RingBufferStream>,
        req_buf: usize,
        resp_buf: usize,
        addr: String,
        finished: Arc<AtomicBool>,
        tx: Sender<u8>,
    ) -> Self {
        if let Err(e) = tx.try_send(0) {
            log::error!("check: failed to send connect signal to {}:{:?}", addr, e);
        }
        let me = Self {
            tx: Arc::new(tx),
            inner: stream,
            req_buf: req_buf,
            resp_buf: resp_buf,
            addr: addr,
            finished: finished,
        };
        me
    }

    fn start_check_done<P>(self: Arc<Self>, parser: P, mut rx: Receiver<u8>)
    where
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        tokio::spawn(async move {
            while let Some(_) = rx.recv().await {
                log::info!(
                    "check: connect signal recived, try to connect:{}",
                    self.addr
                );
                self.connect(parser.clone()).await;
            }
            log::info!("check: stream complete:{}", self.addr);
        });
    }

    async fn connect<P>(&self, parser: P)
    where
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        let mut tries = 0;
        //let mut reconnect_error =
        //    BackendErrorCounter::new(RECONNECT_ERROR_WINDOW, BackendErrorType::ConnError);
        //log::info!("come into check");
        while !self.finished.load(Ordering::Acquire) {
            // if reconnect_error.judge_error(RECONNECT_ERROR_CAP) {
            //     log::warn!(
            //         "check: connect to {} error over {} times, will not connect recently",
            //         self.addr,
            //         RECONNECT_ERROR_CAP
            //     );
            //     sleep(Duration::from_secs(1)).await;
            // } else {
            match self.check_reconnected_once(parser.clone()).await {
                Ok(_) => {
                    log::debug!("check: {} connected tries:{}", self.addr, tries);
                    break;
                }
                Err(e) => {
                    log::info!(
                        "check: {} connected failed:{:?} tries:{}",
                        self.addr,
                        e,
                        tries
                    );
                    let secs = (1 << tries).min(31);
                    tries += 1;
                    sleep(Duration::from_secs(secs)).await;
                } //   reconnect_error.add_error(1);
            }
            //}
        }
    }

    async fn check_reconnected_once<P>(&self, parser: P) -> Result<()>
    where
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        let addr = &self.addr;
        let stream =
            tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(addr)).await??;
        log::info!("check: connected to {}", addr);
        let _ = stream.set_nodelay(true);
        let (r, w) = stream.into_split();
        let req_stream = self.inner.clone();

        req_stream.bridge(
            parser.clone(),
            self.req_buf,
            self.resp_buf,
            r,
            w,
            Notifier {
                tx: self.tx.clone(),
            },
        );
        Ok(())
    }
    fn start_check_timeout(self: Arc<Self>) {
        log::info!("check: {} timeout task started", self.addr);
        tokio::spawn(async move {
            self._start_check_timeout().await;
        });
    }

    // 满足以下所有条件，则把done调整为true，当前实例快速失败。
    // 1. req_num停止更新；
    // 2. resp_num > req_num;
    // 3. 超过7秒钟。why 7 secs?
    async fn _start_check_timeout(self: Arc<Self>) {
        use std::time::Instant;
        const TIME_OUT: Duration = Duration::from_secs(7);

        let (mut last_req, _) = self.inner.load_ping_ping();
        let mut duration = Instant::now();
        while !self.finished.load(Ordering::Acquire) {
            sleep(Duration::from_secs(1)).await;
            // 已经done了，忽略
            if self.inner.done() {
                continue;
            }
            let (req_num, resp_num) = self.inner.load_ping_ping();
            // req有更新
            if req_num != last_req {
                last_req = req_num;
                duration = Instant::now();
                continue;
            }
            // 当前没有请求堆积
            if resp_num == req_num {
                continue;
            }
            // 有请求正在处理
            // 判断是否超时
            let elap = duration.elapsed();
            if elap <= TIME_OUT {
                // 还未超时
                continue;
            }
            log::error!(
                "mpmc-timeout: no response return in {:?}. stream marked done",
                elap
            );
            self.inner.mark_done();
        }
    }
}

impl crate::Notify for Notifier {
    fn notify(&self) {
        if let Err(e) = self.tx.try_send(0) {
            log::error!("notify: failed to send notify signal:{:?}", e);
        }
    }
}

#[derive(Clone)]
pub struct Notifier {
    tx: Arc<Sender<u8>>,
}
