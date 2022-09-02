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
        let mut m_timeout_biz = self.path.qps("timeout");
        let mut m_timeout = Path::base().qps("timeout");
        let mut reconn = crate::reconn::ReconnPolicy::new(&self.path, single);
        metrics::incr_task();
        while !self.finish.get() {
            reconn.check().await;
            let stream = self.try_connect().await;
            if stream.is_none() {
                self.init.on();
                continue;
            }
            reconn.success();
            let stream = rt::Stream::from(stream.expect("not expected"));
            let rx = &mut self.rx;
            rx.enable();
            self.init.on();
            log::debug!("handler started:{:?}", self.path);
            let p = self.parser.clone();
            let handler = Handler::from(rx, stream, p, &self.path);
            let handler = rt::Entry::from(handler, self.timeout);
            if let Err(e) = handler.await {
                match e {
                    Error::Timeout(t) => {
                        m_timeout += 1;
                        m_timeout_biz += 1;
                        log::info!("{:?} timeout: {:?}", self.path, t);
                    }
                    _ => log::info!("{:?} error: {:?}", self.path, e),
                }
            }
        }
        metrics::decr_task();
        log::info!("{:?} finished {}", self.path, self.addr);
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
            Err(e) => {
                log::debug!("conn to {} err:{}", self.addr, e);
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
