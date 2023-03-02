use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use ds::chan::mpsc::Receiver;
use ds::Switcher;
use protocol::{Error, HandShake, Protocol, Request, ResOption, Result, Stream, Writer};
use std::task::ready;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::buffer::StreamGuard;

use metrics::Metric;

pub struct Handler<'r, Req, P, S> {
    data: &'r mut Receiver<Req>,
    pending: VecDeque<Req>,

    s: S,
    buf: StreamGuard,
    parser: P,

    // 处理timeout
    num_rx: usize,
    num_tx: usize,

    rtt: Metric,
}
impl<'r, Req, P, S> Future for Handler<'r, Req, P, S>
where
    Req: Request + Unpin,
    S: AsyncRead + AsyncWrite + Writer + Unpin,
    P: Protocol + Unpin,
{
    type Output = Result<()>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;

        let request = me.poll_request(cx)?;
        let flush = me.poll_flush(cx)?;
        let response = me.poll_response(cx)?;
        // 必须要先flush，否则可能有请求未发送导致超时。
        ready!(flush);
        ready!(response);
        ready!(request);
        Poll::Ready(Ok(()))
    }
}
impl<'r, Req, P, S> Handler<'r, Req, P, S>
where
    Req: Request + Unpin,
    S: AsyncRead + AsyncWrite + protocol::Writer + Unpin,
    P: Protocol + Unpin,
{
    pub(crate) fn from(data: &'r mut Receiver<Req>, s: S, parser: P, rtt: Metric) -> Self {
        data.enable();
        Self {
            data,
            pending: VecDeque::with_capacity(31),
            s,
            parser,
            buf: StreamGuard::new(),
            rtt,
            num_rx: 0,
            num_tx: 0,
        }
    }

    // 发送request. 读空所有的request，并且发送。直到pending或者error
    #[inline]
    fn poll_request(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        self.s.cache(self.data.size_hint() > 1);
        while let Some(mut req) = ready!(self.data.poll_recv(cx)) {
            //插入mysql seqid
            self.parser.before_send(&mut self.buf, &mut req);
            self.num_tx += 1;
            self.s.write_slice(req.data(), 0)?;
            //此处paser需要插钩子，设置seqid
            match req.on_sent() {
                Some(r) => self.pending.push_back(r),
                None => {
                    self.num_rx += 1;
                }
            }
        }
        Poll::Ready(Err(Error::QueueClosed))
    }
    #[inline]
    fn poll_response(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        while self.pending.len() > 0 {
            // let mut cx = Context::from_waker(cx.waker());
            let mut reader = crate::buffer::Reader::from(&mut self.s, cx);
            let poll_read = self.buf.write(&mut reader)?;

            while self.buf.len() > 0 {
                match self.parser.parse_response(&mut self.buf)? {
                    None => break,
                    Some(cmd) => {
                        let req = self.pending.pop_front().expect("take response");
                        self.num_rx += 1;
                        // 统计请求耗时。
                        self.rtt += req.elapsed_current_req();
                        self.parser.check(req.cmd(), &cmd);
                        req.on_complete(cmd);
                    }
                }
            }
            ready!(poll_read);
            reader.check()?;
        }
        Poll::Ready(Ok(()))
    }
    #[inline(always)]
    fn poll_flush(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        ready!(Pin::new(&mut self.s).poll_flush(cx))?;
        Poll::Ready(Ok(()))
    }
}
unsafe impl<'r, Req, P, S> Send for Handler<'r, Req, P, S> {}
unsafe impl<'r, Req, P, S> Sync for Handler<'r, Req, P, S> {}
impl<'r, Req: Request, P, S: AsyncRead + AsyncWrite + Unpin + Writer> rt::ReEnter
    for Handler<'r, Req, P, S>
{
    #[inline]
    fn last(&self) -> Option<ds::time::Instant> {
        if self.pending.len() > 0 {
            assert_ne!(self.num_rx, self.num_tx, "{:?}", self);
            Some(self.pending.front().expect("empty").last_start_at())
        } else {
            None
        }
    }
    #[inline]
    fn close(&mut self) -> bool {
        self.data.disable();
        let noop = noop_waker::noop_waker();
        let mut ctx = std::task::Context::from_waker(&noop);
        // 有请求在队列中未发送。
        while let Poll::Ready(Some(req)) = self.data.poll_recv(&mut ctx) {
            req.on_err(Error::Pending);
        }
        // 2. 有请求已经发送，但response未获取到
        while let Some(req) = self.pending.pop_front() {
            req.on_err(Error::Waiting);
        }
        // 3. cancel
        use rt::Cancel;
        self.s.cancel();

        self.buf.try_gc()
    }
    #[inline]
    fn refresh(&mut self) -> bool {
        log::debug!("handler:{:?}", self);
        self.buf.try_gc();
        self.buf.shrink();
        self.s.shrink();
        self.buf.cap() + self.s.cap() >= crate::REFRESH_THREASHOLD
    }
}

use std::fmt::{self, Debug, Formatter};
impl<'r, Req, P, S> Debug for Handler<'r, Req, P, S> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "handler tx_seq:{} rx_seq:{} p_req:{} {} buf:{:?}",
            self.num_tx,
            self.num_rx,
            self.pending.len(),
            self.rtt,
            self.buf,
        )
    }
}
