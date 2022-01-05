use std::collections::VecDeque;
use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use tokio::sync::mpsc::Receiver;
use tokio::time::{sleep, timeout};

use protocol::{Error, Protocol, Request};

use crate::handler::Handler;
use ds::{GuardedBuffer, Switcher};
use metrics::{Metric, Path};

pub struct BackendChecker<P, Req> {
    rx: Receiver<Req>,
    run: Switcher,
    finish: Switcher,
    init: Switcher,
    parser: P,
    s_metric: Metric,
    //bytes_tx: Metric,
    //bytes_rx: Metric,
    // ratio: Metric,
    rtt: Metric,
    addr: String,
    last_conn: Instant,
    timeout: Duration,
}

impl<P, Req> BackendChecker<P, Req> {
    pub(crate) fn from(
        addr: &str,
        rx: Receiver<Req>,
        run: Switcher,
        finish: Switcher,
        init: Switcher,
        parser: P,
        path: &Path,
        timeout: Duration,
    ) -> Self {
        Self {
            addr: addr.to_string(),
            rx,
            run,
            finish,
            init,
            parser,
            s_metric: path.status("reconn"),
            //bytes_tx: path.qps("bytes.tx"),
            //bytes_rx: path.qps("bytes.rx"),
            rtt: path.rtt("req"),
            last_conn: Instant::now(),
            timeout,
        }
    }
    pub(crate) async fn start_check(&mut self)
    where
        P: Protocol,
        Req: Request,
    {
        while !self.finish.get() {
            let stream = self.try_connect().await;
            let (r, w) = stream.into_split();
            let rx = &mut self.rx;
            self.run.on();
            log::debug!("handler started:{}", self.s_metric);
            use crate::buffer::StreamGuard;
            use crate::gc::DelayedDrop;
            let mut buf: DelayedDrop<_> =
                StreamGuard::from(GuardedBuffer::new(2048, 1 << 20, 16 * 1024, |_, _| {})).into();
            let mut pending = VecDeque::with_capacity(31);
            let p = self.parser.clone();
            if let Err(e) = Handler::from(
                rx,
                &mut pending,
                &mut buf,
                w,
                r,
                //&mut self.bytes_tx,
                //&mut self.bytes_rx,
                &mut self.rtt,
                p,
                self.timeout,
            )
            .await
            {
                log::info!("{} handler error:{:?}", self.s_metric, e);
            }
            // 先关闭，关闭之后不会有新的请求发送
            self.run.off();
            // 1. 把未处理的请求提取出来，通知。
            // 在队列中未发送的。
            let noop = noop_waker::noop_waker();
            let mut ctx = std::task::Context::from_waker(&noop);
            use std::task::Poll;
            // 有请求在队列中未发送。
            while let Poll::Ready(Some(req)) = rx.poll_recv(&mut ctx) {
                req.on_err(Error::Pending);
            }
            // 2. 有请求已经发送，但response未获取到
            while let Some(req) = pending.pop_front() {
                req.on_err(Error::Waiting);
            }
            // buf需要延迟释放
            crate::gc::delayed_drop(buf);
        }
        debug_assert!(!self.run.get());
        log::info!("{} finished {}", self.s_metric, self.addr);
    }
    async fn try_connect(&mut self) -> TcpStream
    where
        P: Protocol,
        Req: Request,
    {
        if self.init.get() {
            // 之前已经初始化过，离上一次成功连接需要超过10s。保护后端资源
            const DELAY: Duration = Duration::from_secs(15);
            if self.last_conn.elapsed() < DELAY {
                sleep(DELAY - self.last_conn.elapsed()).await;
            }
        }
        let mut tries = 0;
        loop {
            if self.init.get() {
                // 第一次初始化不计算。
                self.s_metric += 1;
            }
            log::debug!("try to connect {} tries:{}", self.addr, tries);
            match self.reconnected_once().await {
                Ok(stream) => {
                    self.init.on();
                    self.last_conn = Instant::now();
                    return stream;
                }
                Err(e) => {
                    self.init.on();
                    log::debug!("{}-th conn to {} err:{}", tries, self.addr, e);
                }
            }
            tries += 1;

            sleep(Duration::from_secs(7)).await;
        }
    }
    #[inline]
    async fn reconnected_once(&self) -> std::result::Result<TcpStream, Box<dyn std::error::Error>>
    where
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
        Req: Request + Send + Sync + Unpin + 'static,
    {
        let stream = timeout(Duration::from_secs(2), TcpStream::connect(&self.addr)).await??;
        let _ = stream.set_nodelay(true);
        Ok(stream)
    }
}
