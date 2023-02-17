use discovery::Inited;
use ds::time::Duration;
use std::sync::{atomic::AtomicBool, Arc};

use tokio::net::TcpStream;
use tokio::time::timeout;

use protocol::{Error, Protocol, Request, ResOption};

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
    option: ResOption,
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
        option: ResOption,
    ) -> Self {
        Self {
            addr: addr.to_string(),
            rx,
            finish,
            init,
            parser,
            timeout,
            path,
            option,
        }
    }
    pub(crate) async fn start_check(&mut self, single: Arc<AtomicBool>)
    where
        P: Protocol,
        Req: Request,
    {
        let path_addr = self.path.clone().push(&self.addr);
        let mut m_timeout = path_addr.qps("timeout");
        let mut timeout = Path::base().qps("timeout");
        let mut reconn = crate::reconn::ReconnPolicy::new(&self.path, single);
        metrics::incr_task();
        while !self.finish.get() {
            // reconn.check().await;
            let stream = self.reconnect().await;
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
            if !self.parser.need_auth() {
                self.init.on();
            }
            log::debug!("handler started:{:?}", self.path);
            let p = self.parser.clone();
            let handler = Handler::from(rx, stream, p, rtt, &mut self.option, &mut self.init);
            let handler = rt::Entry::timeout(handler, rt::Timeout::from(self.timeout));
            if let Err(e) = handler.await {
                log::info!("backend error {:?} => {:?}", path_addr, e);
                match e {
                    Error::Timeout(_t) => {
                        m_timeout += 1;
                        timeout += 1;
                    }
                    _ => {}
                }
            }
        }
        metrics::decr_task();
        log::info!("{:?} finished {}", path_addr, self.addr);
    }
    async fn reconnect(&self) -> Option<TcpStream> {
        timeout(Duration::from_secs(2), TcpStream::connect(&self.addr))
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::TimedOut, e))
            .and_then(|x| x)
            .map_err(|_e| log::debug!("conn to {} err:{}", self.addr, _e))
            .ok()
            .map(|s| {
                let _ = s.set_nodelay(true);
                s
            })
    }
}
