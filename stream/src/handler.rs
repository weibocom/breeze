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
    buf: &'r mut StreamGuard,
    parser: P,

    // 处理timeout
    num_rx: usize,
    num_tx: usize,

    rtt: Metric,
}
impl<'r, Req, P, S> Future for Handler<'r, Req, P, S>
where
    Req: Request + Unpin,
    S: AsyncRead + AsyncWrite + protocol::Writer + Unpin,
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
        S: AsyncRead + AsyncWrite + protocol::Writer + Unpin,
    {
        Self {
            data,
            pending: pending,
            s,
            parser,
            buf,
            rtt: path.rtt("req"),
            num_rx: 0,
            num_tx: 0,
        }
    }
    // 发送request. 读空所有的request，并且发送。直到pending或者error
    #[inline(always)]
    fn poll_request(&mut self, cx: &mut Context) -> Poll<Result<()>>
    where
        Req: Request,
        S: AsyncWrite + protocol::Writer + Unpin,
    {
        let mut c = 0;
        self.s.cache(false);
        while let Some(mut req) = ready!(self.data.poll_recv(cx)) {
            c += 1;
            if c == 2 {
                self.s.cache(true);
            }
            self.s.write_slice(req.data(), 0)?;
            req.on_sent();
            if req.sentonly() {
                self.num_rx += 1;
            } else {
                self.pending.push_back(req);
            }
        }
        Poll::Ready(Err(Error::QueueClosed))
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
            if num == 0 {
                log::info!("buffer full:{:?}", self);
                // TODO: 可能触发多次重试失败。
                return Poll::Ready(Err(Error::ResponseBufferFull));
            }
            log::debug!("{} bytes received. {:?}", num, self);
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
        }
        Poll::Ready(Ok(()))
    }
    #[inline(always)]
    fn poll_flush(&mut self, cx: &mut Context) -> Poll<Result<()>>
    where
        S: AsyncWrite + Unpin,
    {
        ready!(Pin::new(&mut self.s).poll_flush(cx))?;
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
            "handler pending:{} {} buf:{}",
            self.pending.len(),
            self.rtt,
            self.buf
        )
    }
}
