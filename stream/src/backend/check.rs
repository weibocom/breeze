use crate::{BackendStream, RingBufferStream};
use ds::{Cid, Ids};
use protocol::{Protocol, Resource};

use std::io::Result;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{interval_at, Interval};

use futures::executor::block_on;

pub struct BackendBuilder {
    finished: Arc<AtomicBool>,
    inited: Arc<AtomicBool>,
    stream: Arc<RingBufferStream>,
    ids: Arc<Ids>,
}

impl BackendBuilder {
    pub fn from<P>(parser: P, addr: &str, parallel: usize, rsrc: Resource, biz: &str) -> Self
    where
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        let finished = Arc::new(AtomicBool::new(false));
        let init = Arc::new(AtomicBool::new(false));
        let stream = Arc::new(RingBufferStream::with_capacity(parallel, biz, addr, rsrc));
        let checker = BackendChecker::from(stream.clone(), finished.clone(), init.clone(), parser);
        tokio::spawn(async { checker.await });

        Self {
            stream: stream,
            ids: Arc::new(Ids::with_capacity(parallel)),
            finished: finished,
            inited: init,
        }
    }
    pub fn build(&self, fake_cid: bool) -> BackendStream {
        let cid = if fake_cid {
            Cid::fake()
        } else {
            self.ids
                .next()
                .map(|cid| Cid::new(cid, self.ids.clone()))
                .unwrap_or_else(|| {
                    log::info!("connection id overflow. {}", self.stream.addr());
                    Cid::fake()
                })
        };
        BackendStream::from(cid, self.stream.clone())
    }
    // 已经连接上或者至少连接了一次
    #[inline]
    pub fn inited(&self) -> bool {
        self.inited.load(Ordering::Acquire)
    }
}

impl Drop for BackendBuilder {
    fn drop(&mut self) {
        self.finished.store(true, Ordering::Release);
    }
}

pub struct BackendChecker<P> {
    inner: Arc<RingBufferStream>,
    rx: Receiver<u8>,
    tx: Arc<Sender<u8>>,
    connecting: bool, // 当前是否需要重新建立连接
    finished: Arc<AtomicBool>,
    inited: Arc<AtomicBool>,
    instant_conn: Instant,    // 最近一次连接开始时间
    instant_timeout: Instant, // 最近一次开始计算timeout的时间
    tries: usize,             // 最近一次连接失败的次数
    tick: Interval,
    req_num: usize, // 检查timeout时，上一次的请求号。
    parser: P,
    close: Instant, // 关闭流的时间
    complete: bool,
}

impl<P> BackendChecker<P> {
    fn from(
        stream: Arc<RingBufferStream>,
        finished: Arc<AtomicBool>,
        inited: Arc<AtomicBool>,
        parser: P,
    ) -> Self {
        let (tx, rx) = channel(8);
        // 用一个random的耗时，将不同的backend的tick错开。
        const SECS: u64 = 3;
        let latency_ms: u64 = rand::random::<u64>() % (SECS * 1000);
        let start = Instant::now() + Duration::from_millis(latency_ms);
        let tick = interval_at(start.into(), Duration::from_secs(SECS));
        let (req_num, _) = stream.load_ping_ping();
        Self {
            tx: Arc::new(tx),
            rx: rx,
            inner: stream,
            req_num: req_num,
            tries: 0,
            connecting: true, // 初始化时需要建立连接
            instant_conn: Instant::now(),
            instant_timeout: Instant::now(),
            tick: tick,
            finished: finished,
            inited: inited,
            parser: parser,
            complete: false,
            close: Instant::now(),
        }
    }
    fn connect(&mut self) -> bool
    where
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        if self.tries == 0 {
            self.instant_conn = Instant::now();
        }
        let expected = Duration::from_secs(self.tries.min(31) as u64);
        self.tries += 1;
        let mut succ = false;
        if self.instant_conn.elapsed() >= expected {
            log::debug!("try to connect {} tries:{}", self.addr(), self.tries);
            match block_on(async { self.reconnected_once().await }) {
                Ok(_) => {
                    succ = true;
                    self.tries = 0;
                }
                Err(e) => {
                    log::warn!("failed to connect {} err:{}", self.addr(), e);
                }
            };
        }
        self.inited.store(true, Ordering::Release);
        succ
    }
    async fn reconnected_once(&self) -> Result<()>
    where
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        let addr = self.addr();
        let stream =
            tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(addr)).await??;
        log::debug!("connected to {}", addr);
        let _ = stream.set_nodelay(true);
        let (r, w) = stream.into_split();
        let req_stream = self.inner.clone();

        req_stream.bridge(
            self.parser.clone(),
            r,
            w,
            Notifier {
                tx: self.tx.clone(),
            },
        );
        Ok(())
    }

    // 满足以下所有条件，则把done调整为true，当前实例快速失败。
    // 1. req_num停止更新；
    // 2. resp_num > req_num;
    // 3. 超过7秒钟。why 7 secs?
    fn check_timeout(&mut self) {
        const TIME_OUT: Duration = Duration::from_secs(4);
        // 已经done了，忽略
        let done = self.inner.done();
        let (req_num, resp_num) = self.inner.load_ping_ping();
        if done || req_num != self.req_num || resp_num == req_num {
            self.req_num = req_num;
            self.instant_timeout = Instant::now();
            return;
        }
        // 判断是否超时
        let elap = self.instant_timeout.elapsed();
        if elap <= TIME_OUT {
            // 还未超时
            return;
        }
        log::error!(
            "timeout: no response return in {:?}. stream marked done. req:{} resp:{}, addr:{}",
            elap,
            req_num,
            resp_num,
            self.addr()
        );
        self.inner.mark_done();
        metrics::status("status", metrics::Status::Timeout, self.metric_id());
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

impl<P> std::ops::Deref for BackendChecker<P> {
    type Target = RingBufferStream;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;

impl<P> Future for BackendChecker<P>
where
    P: Unpin + Send + Sync + Protocol + 'static + Clone,
{
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut self;
        // 没有结束，就一直check
        while !me.finished.load(Ordering::Acquire) {
            if me.connecting {
                if me.connect() {
                    me.connecting = false;
                }
            }
            me.check_timeout();
            match me.rx.poll_recv(cx) {
                Poll::Ready(Some(_)) => {
                    me.connecting = true;
                    me.instant_timeout = Instant::now();
                }
                _ => {}
            }
            ready!(me.tick.poll_tick(cx));
        }

        if !me.complete {
            log::info!(
                "task finished {}. stream closed immediately, and shutdown in 15seconds.",
                me.addr()
            );
            me.complete = true;
            me.try_close();
            me.close = Instant::now();
        }
        while me.close.elapsed() <= Duration::from_secs(15) {
            ready!(me.tick.poll_tick(cx));
        }
        log::info!("stream shutting down. {}", me.addr());
        me.shutdown_all();
        Poll::Ready(())
    }
}
