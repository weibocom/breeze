use std::sync::{atomic::AtomicBool, Arc};
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::time::timeout;

use protocol::{Error, Protocol, Request};

use crate::handler::Handler;
use ds::chan::mpsc::Receiver;
use ds::Switcher;
use metrics::Path;

pub struct BackendChecker<P, Req> {
    rx: Receiver<Req>,
    finish: Switcher,
    init: Switcher,
    parser: P,
    addr: String,
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
            timeout,
            path,
        }
    }
    pub(crate) async fn start_check(&mut self, single: Arc<AtomicBool>)
    where
        P: Protocol,
        Req: Request,
    {
        let path_addr = self.path.clone().push(&self.addr);
        let mut m_timeout = path_addr.qps("timeout");
        let mut reconn = crate::reconn::ReconnPolicy::new(&self.path, single);
        metrics::incr_task();
        while !self.finish.get() {
            // reconn.check().await;
            let stream = self.try_connect().await;
            if stream.is_none() {
                // 连接失败，按策略sleep
                reconn.conn_failed().await;
                self.init.on();
                continue;
            }
            // 连接成功
            // reconn.success();
            reconn.connected();

            let rtt = path_addr.rtt("req");
            let stream = rt::Stream::from(stream.expect("not expected"));
            let rx = &mut self.rx;
            rx.enable();
            self.init.on();
            log::debug!("handler started:{:?}", self.path);
            let p = self.parser.clone();
            let handler = Handler::from(rx, stream, p, rtt);
            let handler = rt::Entry::from(handler, self.timeout);
            if let Err(e) = handler.await {
                match e {
                    Error::Timeout(_t) => {
                        m_timeout += 1;
                        log::info!("backend/{:?} timeout: {:?}", path_addr, _t);
                    }
                    _ => log::info!("backend/{:?} error: {:?}", path_addr, e),
                }
            }
        }
        metrics::decr_task();
        log::info!("{:?} finished {}", path_addr, self.addr);
    }
    async fn try_connect(&mut self) -> Option<TcpStream>
    where
        P: Protocol,
        Req: Request,
    {
        match self.reconnected_once().await {
            Ok(stream) => {
                return Some(stream);
            }
            Err(_e) => {
                log::debug!("conn to {} err:{}", self.addr, _e);
            }
        }
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
