use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use ds::chan::mpsc::Receiver;
use futures::ready;
use protocol::{Error, Protocol, Request, Result, Stream};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::buffer::StreamGuard;

use metrics::{Metric, Path};

pub(crate) struct Handler<'r, Req, P, S> {
    data: &'r mut Receiver<Req>,
    pending: VecDeque<Req>,

    s: S,
    buf: StreamGuard,
    parser: P,

    // 处理timeout
    num_rx: usize,
    num_tx: usize,

    rtt: Metric,

    last_req_buf: Vec<u8>,
}
impl<'r, Req, P, S> Future for Handler<'r, Req, P, S>
where
    Req: Request + Unpin,
    S: AsyncRead + AsyncWrite + protocol::Writer + Unpin,
    P: Protocol + Unpin,
{
    type Output = Result<()>;

    #[inline]
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
    pub(crate) fn from(data: &'r mut Receiver<Req>, s: S, parser: P, path: &Path) -> Self
    where
        S: AsyncRead + AsyncWrite + protocol::Writer + Unpin,
    {
        data.enable();
        Self {
            data,
            pending: VecDeque::with_capacity(31),
            s,
            parser,
            buf: StreamGuard::new(),
            rtt: path.rtt("req"),
            num_rx: 0,
            num_tx: 0,
            last_req_buf: Vec::with_capacity(512),
        }
    }
    // 发送request. 读空所有的request，并且发送。直到pending或者error
    #[inline]
    fn poll_request(&mut self, cx: &mut Context) -> Poll<Result<()>>
    where
        Req: Request,
        S: AsyncWrite + protocol::Writer + Unpin,
    {
        let mut c = 0;
        self.s.cache(false);
        while let Some(mut req) = ready!(self.data.poll_recv(cx)) {
            self.num_tx += 1;
            //use protocol::Utf8;
            //log::info!("request received {:?} {:?}", self, req.data().utf8());
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
    #[inline]
    fn poll_response(&mut self, cx: &mut Context) -> Poll<Result<()>>
    where
        S: AsyncRead + Unpin,
        P: Protocol,
        Req: Request,
    {
        while self.pending.len() > 0 {
            let mut cx = Context::from_waker(cx.waker());
            let mut reader = crate::buffer::Reader::from(&mut self.s, &mut cx);
            let poll_read = self.buf.write(&mut reader)?;
            // num == 0 说明是buffer满了。等待下一次事件，buffer释放后再读取。
            let num = reader.check_eof_num()?;
            if num == 0 {
                log::debug!("buffer full:{:?}", self);
                // TODO: 可能触发多次重试失败。
                return Poll::Ready(Err(Error::ResponseBufferFull));
            }
            log::debug!("{} bytes received. {:?}", num, self);
            let pl = self.pending.len();
            unsafe { self.last_req_buf.set_len(0) };
            while self.buf.len() > 0 {
                match self.parser.parse_response(&mut self.buf)? {
                    None => break,
                    Some(cmd) => {
                        if self.pending.len() == 0 {
                            use protocol::Utf8;
                            log::info!(
                                "no request reserved for response found => {} req:{:?} response:{:?}, buf data:{:?}",
                                pl,
                                self.last_req_buf,
                                cmd.data().utf8(),
                                self.buf
                            );
                        }
                        assert_ne!(self.pending.len(), 0);
                        let req = self.pending.pop_front().expect("take response");
                        use protocol::Writer;
                        self.last_req_buf.write_slice(req.data(), 0)?;
                        self.num_rx += 1;
                        // 统计请求耗时。
                        self.rtt += req.start_at().elapsed();
                        req.on_complete(cmd);
                    }
                }
            }
            ready!(poll_read);
        }
        Poll::Ready(Ok(()))
    }
    #[inline]
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
impl<'r, Req: Request, P, S: AsyncRead + AsyncWrite + Unpin> rt::ReEnter
    for Handler<'r, Req, P, S>
{
    #[inline]
    fn num_rx(&self) -> usize {
        self.num_rx
    }
    #[inline]
    fn num_tx(&self) -> usize {
        self.num_tx
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
}

use std::fmt::{self, Debug, Formatter};
impl<'r, Req, P, S> Debug for Handler<'r, Req, P, S> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "handler tx_seq:{} rx_seq:{} pending:{} {} buf:{}",
            self.num_tx,
            self.num_rx,
            self.pending.len(),
            self.rtt,
            self.buf
        )
    }
}
