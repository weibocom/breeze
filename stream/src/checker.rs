use ds::time::Duration;
use rt::Cancel;
use std::future::Future;
use std::pin::Pin;
use std::sync::{atomic::AtomicBool, Arc};
use std::task::Poll;

use tokio::io::AsyncWrite;
use tokio::net::TcpStream;
use tokio::time::timeout;

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
    pub(crate) async fn start_check(&mut self, _single: Arc<AtomicBool>)
    where
        P: Protocol,
        Req: Request,
    {
        let path_addr = self.path.clone().push(&self.addr);
        let mut m_timeout = path_addr.qps("timeout");
        let mut auth_failed = path_addr.status("auth_failed");
        let mut timeout = Path::base().qps("timeout");
        let mut reconn = crate::reconn::ReconnPolicy::new(&path_addr);
        metrics::incr_task();
        while !self.finish.get() {
            // reconn.check().await;
            let stream = self.reconnect().await;
            if stream.is_none() {
                // 连接失败，按策略sleep
                log::debug!("+++ connected failed to:{}", self.addr);
                reconn.conn_failed().await;
                self.init.on();
                continue;
            }
            // 连接成功
            // reconn.success();
            reconn.connected();

            let rtt = path_addr.rtt("req");
            let mut stream = rt::Stream::from(stream.expect("not expected"));
            let rx = &mut self.rx;

            if self.parser.need_auth() {
                //todo 处理认证结果
                let auth = Auth {
                    option: &mut self.option,
                    s: &mut stream,
                    parser: self.parser.clone(),
                };
                if let Err(_e) = auth.await {
                    //todo 需要减一吗，listen_failed好像没有减
                    log::warn!("+++ auth err {} to: {}", _e, self.addr);
                    auth_failed += 1;
                    stream.cancel();
                    continue;
                }
            }

            rx.enable();
            self.init.on();
            log::debug!("handler started:{:?} with: {}", self.path, self.addr);
            let p = self.parser.clone();
            let handler = Handler::from(rx, stream, p, rtt);
            let handler = Entry::timeout(handler, Timeout::from(self.timeout.ms()));
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
        //是否auth，parser有状态了，状态机？parser 传进去的是buf，不是client，所以前后还要加异步读写
        //todo:读buf代码重复了与下面
        // let mut cx1 = Context::from_waker(cx.waker());

        // let poll_read = me.buf.write(&mut reader)?;
        // log::debug!("+++ 111 read size:{}", me.buf.slice().len());
        //有可能出错了，会有未使用的读取，放使用后会有两个mut
        // if let Poll::Ready(_) = poll_read {
        //     reader.check()?;
        // }
        // while let Poll::Ready(_) = me.buf.write(&mut reader)? {
        //     log::debug!("+++ in 222 read size:{}", me.buf.slice().len());
        //     reader.check()?;
        //     log::debug!("+++ in 222.111 read size:{}", me.buf.slice().len());
        //     reader = crate::buffer::Reader::from(&mut me.s, cx);
        // }
        //todo为啥需要loop？
        while let Poll::Ready(_) = me.s.poll_recv(cx)? {}

        let result = match me.parser.handshake(me.s, me.option) {
            Err(e) => Poll::Ready(Err(e)),
            Ok(HandShake::Failed) => Poll::Ready(Err(Error::AuthFailed)),
            Ok(HandShake::Continue) => Poll::Pending,
            Ok(HandShake::Success) => {
                // me.init.on();
                // me.authed = true;
                Poll::Ready(Ok(()))
            }
        };

        //todo 成功失败后可能会有数据flush pending，单后续handle会flush，问题应该不大
        let _ = Pin::new(&mut *me.s).as_mut().poll_flush(cx);

        if result.is_ready() {
            me.s.try_gc();
        }

        result
    }
}
