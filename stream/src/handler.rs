use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use ds::chan::mpsc::Receiver;
use protocol::{Error, Protocol, Request, Result, Stream, Utf8};
use std::task::ready;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::buffer::StreamGuard;

use metrics::Metric;

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
    slow: Metric,
    //mcq 空读
    miss: Metric,
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
        // 必须要先flush，否则可能有请求未发送导致超时。
        ready!(flush);
        ready!(response);
        ready!(request);
        Poll::Ready(Ok(()))
    }
}
impl<'r, Req, P, S> Handler<'r, Req, P, S> {
    pub(crate) fn from(
        data: &'r mut Receiver<Req>,
        s: S,
        parser: P,
        rtt: Metric,
        slow: Metric,
        miss: Metric,
    ) -> Self
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
            rtt,
            slow,
            miss,
            num_rx: 0,
            num_tx: 0,
        }
    }
    // 发送request. 读空所有的request，并且发送。直到pending或者error
    #[inline]
    fn poll_request(&mut self, cx: &mut Context) -> Poll<Result<()>>
    where
        Req: Request,
        S: AsyncWrite + protocol::Writer + Unpin,
    {
        self.s.cache(self.data.size_hint() > 1);
        while let Some(req) = ready!(self.data.poll_recv(cx)) {
            self.num_tx += 1;
            self.s.write_slice(req.data(), 0)?;
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
            while self.buf.len() > 0 {
                match self.parser.parse_response(&mut self.buf)? {
                    None => break,
                    Some(cmd) => {
                        debug_assert_ne!(self.pending.len(), 0, "{:?}", self);
                        let req = self.pending.pop_front().expect("take response");
                        self.num_rx += 1;
                        // 统计请求耗时。
                        let rtt = req.start_at().elapsed();
                        self.rtt += rtt;
                        if rtt >= metrics::MAX {
                            self.slow += rtt;
                        }
                        if cmd.empty_response() {
                            self.miss += 1;
                        }
                        debug_assert!(
                            self.parser.check(req.cmd(), &cmd),
                            "{:?} {:?} => {:?}",
                            self,
                            req.cmd().data().utf8(),
                            cmd.data().utf8()
                        );
                        req.on_complete(cmd);
                    }
                }
            }
            ready!(poll_read);
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
            "handler tx_seq:{} rx_seq:{} pending:{} {} buf:{:?}",
            self.num_tx,
            self.num_rx,
            self.pending.len(),
            self.rtt,
            self.buf,
        )
    }
}

//const H_SIZE: usize = 16;
//const H_MASK: usize = H_SIZE - 1;
//#[derive(Default, Debug)]
//struct History {
//    //reqs: [Vec<u8>; H_SIZE],
////resp: [Vec<u8>; H_SIZE],
//}
//impl History {
//    #[inline]
//    fn insert_req<Req: Request>(&mut self, seq: usize, req: &Req) {
//        //let idx = seq & H_MASK;
//        //self.reqs[idx] = req.data().to_vec();
//    }
//    #[inline]
//    fn insert_resp(&mut self, seq: usize, resp: &protocol::Command) {
//        //let idx = seq & H_MASK;
//        //self.resp[idx] = resp.data().to_vec();
//    }
//    #[inline]
//    fn insert_resp_empty(&mut self, seq: usize) {
//        //let idx = seq & H_MASK;
//        //self.resp[idx] = Vec::new();
//    }
//}
