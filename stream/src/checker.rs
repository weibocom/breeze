use ds::time::{timeout, Duration};
use rt::Cancel;
use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Poll};

use tokio::io::AsyncWrite;
use tokio::net::TcpStream;

use protocol::{Error, HandShake, Protocol, Request, ResOption, Result, Stream};

use crate::handler::Handler;
use ds::chan::mpsc::Receiver;
use ds::Switcher;
use metrics::Path;

use rt::{Entry, Timeout};

pub struct BackendChecker<P, Req> {
    rx: Receiver<Req>,
    finish: Switcher,
    init: Switcher,
    parser: P,
    addr: String,
    timeout: endpoint::Timeout,
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
        timeout: endpoint::Timeout,
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
    pub(crate) async fn start_check(mut self)
    where
        P: Protocol,
        Req: Request,
    {
        let path_addr = self.path.clone().push(&self.addr);
        let mut be_conns = path_addr.qps("be_conn");
        let mut timeout = Path::base().qps("timeout");
        let mut reconn = crate::reconn::ReconnPolicy::new();
        metrics::incr_task();
        while !self.finish.get() {
            be_conns += 1;
            let stream = self.reconnect().await;
            if stream.is_none() {
                // 连接失败，按策略sleep
                log::debug!("+++ connected failed to:{}", self.addr);
                reconn.conn_failed().await;
                self.init.on();
                continue;
            }

            let rtt = path_addr.rtt("req");
            let mut stream = rt::Stream::from(stream.expect("not expected"));
            let rx = &mut self.rx;

            if self.parser.config().need_auth {
                let auth = Auth {
                    option: &mut self.option,
                    s: &mut stream,
                    parser: self.parser.clone(),
                };
                if let Err(_e) = auth.await {
                    log::warn!("+++ auth err {} to: {}", _e, self.addr);
                    let mut auth_failed = path_addr.status("auth_failed");
                    auth_failed += metrics::Status::ERROR;
                    stream.cancel();
                    //当作连接失败处理，不立马重试
                    //todo：可以尝试将等待操作统一提取到循环开头
                    reconn.conn_failed().await;
                    continue;
                }
            }

            // auth成功才算连接成功
            reconn.connected();

            rx.enable();
            self.init.on();
            log::debug!("handler started:{:?} with: {}", self.path, self.addr);
            let p = self.parser.clone();
            let handler = Handler::from(rx, stream, p, rtt);
            let handler = Entry::timeout(handler, Timeout::from(self.timeout.ms()));
            let ret = handler.await;
            log::error!("backend error {:?} => {:?}", path_addr, ret);
            // handler 一定返回err，不会返回ok
            match ret.err().expect("handler return ok") {
                Error::Timeout(_t) => {
                    let mut m_timeout = path_addr.qps("timeout");
                    m_timeout += 1;
                    timeout += 1;
                }
                Error::UnexpectedData => {
                    let mut unexpected_resp = path_addr.num("unexpected_resp");
                    unexpected_resp += 1;
                }
                _ => {}
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

struct Auth<'a, P, S> {
    pub option: &'a mut ResOption,
    pub s: &'a mut S,
    pub parser: P,
}

impl<'a, P, S> Future for Auth<'a, P, S>
where
    S: Stream + Unpin + AsyncWrite,
    P: Protocol + Unpin,
{
    type Output = Result<()>;
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let me = &mut *self;
        let recv_result = me.s.poll_recv(cx)?;

        let auth_result = match me.parser.handshake(me.s, me.option) {
            Err(e) => Poll::Ready(Err(e)),
            Ok(HandShake::Failed) => Poll::Ready(Err(Error::AuthFailed)),
            Ok(HandShake::Continue) => Poll::Pending,
            Ok(HandShake::Success) => Poll::Ready(Ok(())),
        };

        let flush_result = Pin::new(&mut *me.s).as_mut().poll_flush(cx);

        let _ = ready!(flush_result);
        if auth_result.is_ready() {
            me.s.try_gc();
            auth_result
        } else {
            ready!(recv_result);
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}
