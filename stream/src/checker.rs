use std::collections::VecDeque;
use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use tokio::sync::mpsc::Receiver;
use tokio::time::{sleep, timeout};

use protocol::{Error, Protocol, Request};

use crate::handler::Handler;
use ds::Switcher;
use metrics::{BASE_PATH, Metric, Path};

pub struct BackendChecker<P, Req> {
    rx: Receiver<Req>,
    run: Switcher,
    finish: Switcher,
    init: Switcher,
    parser: P,
    addr: String,
    last_conn: Instant,
    timeout: Duration,
    path: Path,
}

impl<P, Req> BackendChecker<P, Req> {
    pub(crate) fn from(
        addr: &str,
        rx: Receiver<Req>,
        run: Switcher,
        finish: Switcher,
        init: Switcher,
        parser: P,
        path: Path,
        timeout: Duration,
    ) -> Self {
        Self {
            addr: addr.to_string(),
            rx,
            run,
            finish,
            init,
            parser,
            last_conn: Instant::now(),
            timeout,
            path,
        }
    }
    pub(crate) async fn start_check(&mut self)
    where
        P: Protocol,
        Req: Request,
    {
        let mut s_metric = self.path.status("reconn");
        let mut m_timeout_biz = self.path.qps("timeout");
        let mut m_timeout = Path::new(vec![BASE_PATH]).rtt("timeout");
        let mut tries = 0;
        while !self.finish.get() {
            let stream = self.try_connect(&mut s_metric, &mut tries).await;
            if stream.is_none() {
                continue;
            }
            let (r, w) = stream.expect("not expected").into_split();
            let rx = &mut self.rx;
            self.run.on();
            log::debug!("handler started:{}", s_metric);
            use crate::buffer::StreamGuard;
            use crate::gc::DelayedDrop;
            let mut buf: DelayedDrop<_> = StreamGuard::new().into();
            let pending = &mut VecDeque::with_capacity(31);
            let p = self.parser.clone();
            let handler = Handler::from(rx, pending, &mut buf, w, r, p, &self.path);
            let handler = rt::Timeout::from(handler, self.timeout);
            if let Err(e) = handler.await {
                if let protocol::Error::Timeout(_) = e {
                    m_timeout += 1;
                    m_timeout_biz += 1;
                }
                log::info!("{} error:{:?} pending:{}", s_metric, e, pending.len());
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
        log::info!("{} finished {}", s_metric, self.addr);
    }
    async fn try_connect(&mut self, reconn: &mut Metric, tries: &mut usize) -> Option<TcpStream>
    where
        P: Protocol,
        Req: Request,
    {
        if self.init.get() {
            // 第一次初始化不计算。
            *reconn += 1;
            // 等于0说明一次成功都没有
            if *tries > 0 {
                // 距离上一次成功连接需要超过60秒
                let elapsed = self.last_conn.elapsed();
                if elapsed < Duration::from_secs(60) {
                    sleep(Duration::from_secs(60) - elapsed).await;
                }
            }
        }
        log::debug!("try to connect {} tries:{}", self.addr, tries);
        match self.reconnected_once().await {
            Ok(stream) => {
                self.init.on();
                self.last_conn = Instant::now();
                *tries += 1;
                return Some(stream);
            }
            Err(e) => {
                self.init.on();
                log::debug!("{}-th conn to {} err:{}", tries, self.addr, e);
            }
        }
        // 失败了，sleep一段时间再重试
        let secs = 1 << (6.min(*tries + 1));
        sleep(Duration::from_secs(secs)).await;
        None
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
