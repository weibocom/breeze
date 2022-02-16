use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;
use protocol::{Error, Protocol, Request, Result};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::Receiver;

use crate::buffer::StreamGuard;

use metrics::{Metric, Path};

pub(crate) struct Handler<'r, Req, P, S> {
    data: &'r mut Receiver<Req>,
    pending: &'r mut VecDeque<Req>,

    s: S,
    // 用来处理发送数据
    cache: bool, // pending的尾部数据是否需要发送。true: 需要发送
    oft_c: usize,
    // 处理接收数据
    buf: &'r mut StreamGuard,
    parser: P,
    flushing: bool,

    // 处理timeout
    num_rx: usize,
    num_tx: usize,

    rtt: Metric,
}
impl<'r, Req, P, S> Future for Handler<'r, Req, P, S>
where
    Req: Request + Unpin,
    S: AsyncRead + AsyncWrite + Unpin,
    P: Protocol + Unpin,
{
    type Output = Result<()>;

    #[inline(always)]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        let request = me.poll_request(cx)?;
        let flush = me.poll_flush(cx)?;
        let response = me.poll_response(cx)?;
        ready!(response);
        ready!(flush);
        ready!(request);
        Poll::Ready(Ok(()))
    }
}
impl<'r, Req, P, S> Handler<'r, Req, P, S> {
    pub(crate) fn from(
        data: &'r mut Receiver<Req>,
        pending: &'r mut VecDeque<Req>,
        buf: &'r mut StreamGuard,
        s: S,
        parser: P,
        path: &Path,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        Self {
            data,
            cache: false,
            pending: pending,
            s,
            parser,
            oft_c: 0,
            buf,
            flushing: false,
            rtt: path.rtt("req"),
            num_rx: 0,
            num_tx: 0,
        }
    }
    // 发送request.
    #[inline(always)]
    fn poll_request(&mut self, cx: &mut Context) -> Poll<Result<()>>
    where
        Req: Request,
        S: AsyncWrite + Unpin,
    {
        loop {
            if self.cache {
                debug_assert!(self.pending.len() > 0);
                if let Some(req) = self.pending.back_mut() {
                    while self.oft_c < req.len() {
                        let data = req.read(self.oft_c);
                        self.flushing = true;
                        let n = ready!(Pin::new(&mut self.s).poll_write(cx, data))?;
                        self.oft_c += n;
                    }
                    req.on_sent();
                    if req.sentonly() {
                        // 只发送的请求，不需要等待response，pop掉。
                        self.pending.pop_back();
                        self.num_rx += 1;
                    }
                }
                self.oft_c = 0;
                self.cache = false;
            }
            match ready!(self.data.poll_recv(cx)) {
                None => return Poll::Ready(Err(Error::QueueClosed)), // 说明当前实例已经被销毁，需要退出。
                Some(req) => {
                    self.pending.push_back(req);
                    self.cache = true;
                    self.num_tx += 1;
                }
            }
        }
    }
    #[inline(always)]
    fn poll_response(&mut self, cx: &mut Context) -> Poll<Result<()>>
    where
        S: AsyncRead + Unpin,
        P: Protocol,
        Req: Request,
    {
        while self.pending.len() > 0 {
            let mut cx = Context::from_waker(cx.waker());
            let mut reader = crate::buffer::Reader::from(&mut self.s, &mut cx);
            ready!(self.buf.buf.write(&mut reader))?;
            // num == 0 说明是buffer满了。等待下一次事件，buffer释放后再读取。
            let num = reader.check_eof_num()?;
            log::debug!("{} bytes received.", num);
            use protocol::Stream;
            while self.buf.len() > 0 {
                match self.parser.parse_response(self.buf)? {
                    None => break,
                    Some(cmd) => {
                        debug_assert_ne!(self.pending.len(), 0);
                        let req = self.pending.pop_front().expect("take response");
                        self.num_rx += 1;
                        // 统计请求耗时。
                        self.rtt += req.start_at().elapsed();
                        req.on_complete(cmd);
                    }
                }
            }
            // TODO: 当前buf不能满。如果满了，说明最大buf已溢出
        }
        Poll::Ready(Ok(()))
    }
    #[inline(always)]
    fn poll_flush(&mut self, cx: &mut Context) -> Poll<Result<()>>
    where
        S: AsyncWrite + Unpin,
    {
        ready!(Pin::new(&mut self.s).poll_flush(cx))?;
        self.flushing = false;
        Poll::Ready(Ok(()))
    }
}
unsafe impl<'r, Req, P, S> Send for Handler<'r, Req, P, S> {}
unsafe impl<'r, Req, P, S> Sync for Handler<'r, Req, P, S> {}
impl<'r, Req, P, S> rt::ReEnter for Handler<'r, Req, P, S> {
    #[inline(always)]
    fn num_rx(&self) -> usize {
        self.num_rx
    }
    #[inline(always)]
    fn num_tx(&self) -> usize {
        self.num_tx
    }
}

use std::fmt::{self, Debug, Formatter};
impl<'r, Req, P, S> Debug for Handler<'r, Req, P, S> {
    #[inline(always)]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "handler pending:{} flushing:{} cached:{} oft:{} {}",
            self.pending.len(),
            self.flushing,
            self.cache,
            self.oft_c,
            self.rtt,
        )
    }
}
