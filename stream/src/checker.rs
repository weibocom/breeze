use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

use protocol::{Error, Protocol, Request};

use crate::handler::Handler;
use ds::chan::mpsc::Receiver;
use ds::Switcher;
use metrics::{Metric, Path};

pub struct BackendChecker<P, Req> {
    rx: Receiver<Req>,
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
        finish: Switcher,
        init: Switcher,
        parser: P,
        path: Path,
        timeout: Duration,
    ) -> Self {
        Self {
            addr: addr.to_string(),
            rx,
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
        let mut m_timeout = Path::base().qps("timeout");
        let mut tries = 0;
        while !self.finish.get() {
            let stream = self.try_connect(&mut s_metric, &mut tries).await;
            if stream.is_none() {
                continue;
            }
            let stream = rt::Stream::from(stream.expect("not expected"));
            let rx = &mut self.rx;
            log::debug!("handler started:{}", s_metric);
            let p = self.parser.clone();
            let handler = Handler::from(rx, stream, p, &self.path);
            let handler = rt::Timeout::from(handler, self.timeout);
            if let Err(e) = handler.await {
                match e {
                    Error::Timeout(_) => {
                        m_timeout += 1;
                        m_timeout_biz += 1;
                        log::debug!("{} error: {:?}", s_metric, e);
                    }
                    _ => log::info!("{} error: {:?}", s_metric, e),
                }
            }
        }
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
