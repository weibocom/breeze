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
        let checker =
            BackendChecker::from(stream.clone(), req_buf, resp_buf, addr, me.finished.clone());
        checker.start_check(parser.clone());
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
    //pub fn reconnect(&self) {
    //    if let Err(e) = self.notify.try_send(0) {
    //        log::error!("check: reconnect failed. notify error:{:?}", e);
    //    }
    //    //if !self.finished.load(Ordering::Acquire) {
    //    //    self.connected.store(false, Ordering::Release);
    //    //    self.check_waker.read().unwrap().wake();
    //    //}
    //}

    //pub fn start_check<P>(&self, checker: Arc<BackendChecker>, parser: P) -> JoinHandle<()>
    //where
    //    P: Unpin + Send + Sync + Protocol + 'static + Clone,
    //{
    //    let runtime = Runtime::new().unwrap();
    //    let cloned_parser = parser.clone();
    //    let res = std::thread::spawn(move || {
    //        let check_future = checker.check(cloned_parser);
    //        runtime.block_on(check_future);
    //    });
    //    res
    //}
}

impl Drop for BackendBuilder {
    fn drop(&mut self) {
        self.finish();
    }
}

use std::collections::LinkedList;
use std::time::{SystemTime, UNIX_EPOCH};

//pub struct BackendWaker {
//    check_task: Option<std::thread::JoinHandle<()>>,
//}
//
//impl BackendWaker {
//    fn new() -> Self {
//        BackendWaker { check_task: None }
//    }
//
//    fn wake(&self) {
//        if self.check_task.is_some() {
//            self.check_task.as_ref().unwrap().thread().unpark();
//        }
//    }
//}

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
    tx: Arc<Sender<u8>>,
    rx: Receiver<u8>,
    req_buf: usize,
    resp_buf: usize,
    inner: Arc<RingBufferStream>,
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
    ) -> Self {
        let (tx, rx) = channel(8);
        let me = Self {
            tx: Arc::new(tx),
            rx: rx,
            inner: stream,
            req_buf: req_buf,
            resp_buf: resp_buf,
            addr: addr,
            finished: finished,
        };
        me
    }

    fn start_check<P>(mut self, parser: P)
    where
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        tokio::spawn(async move {
            while let Some(_) = self.rx.recv().await {
                log::info!(
                    "check: connect signal recived, try to connect:{}",
                    self.addr
                );
                self.check(parser.clone()).await;
            }
            log::info!("check: stream complete");
        });
    }

    async fn check<P>(&self, parser: P)
    where
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        let mut reconnect_error =
            BackendErrorCounter::new(RECONNECT_ERROR_WINDOW, BackendErrorType::ConnError);
        log::info!("come into check");
        while !self.finished.load(Ordering::Acquire) {
            if reconnect_error.judge_error(RECONNECT_ERROR_CAP) {
                log::warn!(
                    "check: connect to {} error over {} times, will not connect recently",
                    self.addr,
                    RECONNECT_ERROR_CAP
                );
                sleep(Duration::from_secs(1)).await;
            } else {
                if let Err(e) = self.check_reconnected_once(parser.clone()).await {
                    log::warn!("check: connect {} error:{:?}", self.addr, e);
                    reconnect_error.add_error(1);
                }
            }
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
