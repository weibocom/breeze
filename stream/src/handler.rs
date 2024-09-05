use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use ds::chan::mpsc::Receiver;
use ds::time::Instant;
use protocol::metrics::HostMetric;
use protocol::{Error, Protocol, Request, Result, Stream};
use tokio::io::ReadBuf;
use tokio::io::{AsyncRead, AsyncWrite};

use metrics::{Metric, Path};

pub struct Handler<'r, Req, P, S> {
    data: &'r mut Receiver<Req>,
    pending: VecDeque<(Req, Instant)>,

    s: S,
    parser: P,
    rtt: Metric,
    host_metric: HostMetric,

    num: Number,

    req_buf: Vec<Req>,

    // 连续多少个cycle检查到当前没有请求发送，则发送一个ping
    ping_cycle: u16,
    name: Path,
}
impl<'r, Req, P, S> Future for Handler<'r, Req, P, S>
where
    Req: Request + Unpin,
    S: AsyncRead + AsyncWrite + Stream + Unpin,
    P: Protocol + Unpin,
{
    type Output = Result<()>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        debug_assert!(self.num.check_pending(self.pending.len()), "{:?}", self);
        let me = &mut *self;

        let request = me.poll_request(cx)?;
        // 必须要先flush，否则可能有请求未发送导致超时。
        let flush = me.poll_flush(cx)?;
        let response = me.poll_response(cx)?;

        ready!(flush);
        ready!(response);
        ready!(request);
        Poll::Ready(Ok(()))
    }
}
impl<'r, Req, P, S> Handler<'r, Req, P, S>
where
    Req: Request + Unpin,
    S: AsyncRead + AsyncWrite + Stream + Unpin,
    P: Protocol + Unpin,
{
    pub(crate) fn from(data: &'r mut Receiver<Req>, s: S, parser: P, path: Path) -> Self {
        data.enable();
        let name = path.clone();
        let rtt = path.rtt("req");
        Self {
            data,
            pending: VecDeque::with_capacity(31),
            s,
            parser,
            rtt,
            host_metric: HostMetric::from(path),
            num: Number::default(),
            req_buf: Vec::with_capacity(4),
            ping_cycle: 0,
            name,
        }
    }
    // 检查连接是否存在
    // 1. 连续5分钟没有发送请求，则进行检查
    // 2. 从io进行一次poll_read
    // 3. 如果poll_read返回Pending，则说明连接正常
    // 4. 如果poll_read返回Ready，并且返回的数据为0，则说明连接已经断开
    // 5. 如果poll_read返回Ready，并且返回的数据不为0，则说明收到异常请求
    #[inline]
    fn check_alive(&mut self) -> Result<()> {
        if self.pending.len() != 0 {
            // 有请求发送，不需要ping
            self.ping_cycle = 0;
            return Ok(());
        }
        self.ping_cycle += 1;
        // 目前调用方每隔30秒调用一次，所以这里是5分钟检查一次心跳
        // 如果最近5分钟之内pending为0（pending为0并不意味着没有请求），则发送一个ping作为心跳
        if self.ping_cycle <= 10 {
            return Ok(());
        }
        self.ping_cycle = 0;
        assert_eq!(self.pending.len(), 0, "pending must be empty=>{:?}", self);
        let noop = noop_waker::noop_waker();
        let mut ctx = std::task::Context::from_waker(&noop);
        // cap == 0 说明从来没有发送过request，不需要poll_response。
        if self.s.cap() > 0 {
            self.poll_sentonly_response(&mut ctx)
        } else {
            self.poll_checkalive(&mut ctx)
        }
    }

    // 发送request. 读空所有的request，并且发送。直到pending或者error
    #[inline]
    fn poll_request(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        while ready!(self.data.poll_recv_many(cx, &mut self.req_buf, 4)) > 0 {
            self.s.cache(self.req_buf.len() > 1);
            let ptr = self.req_buf.as_ptr();
            for i in 0..self.req_buf.len() {
                let req = unsafe { ptr.add(i).read() };
                self.s.write_slice(&*req, 0).expect("should not err");
                self.num.tx();
                self.parser.on_sent(req.operation(), &mut self.host_metric);
                match req.on_sent() {
                    Some(r) => self.pending.push_back((r, Instant::now())),
                    None => self.num.rx(),
                }
            }
            // 发送完成后，清空buffer
            // 在这是安全的，因为在L110已经读取所有数据，并且这当中不会中断
            unsafe { self.req_buf.set_len(0) };
        }
        debug_assert_eq!(self.req_buf.len(), 0);
        Poll::Ready(Err(Error::ChanReadClosed))
    }
    #[inline]
    fn poll_response_inner(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        let poll_read = self.s.poll_recv(cx);

        while self.s.len() > 0 {
            let l = self.s.len();
            if let Some(cmd) = self.parser.parse_response(&mut self.s)? {
                if self.pending.len() == 0 {
                    panic!("unexpect response handler:{:?}", &self);
                }
                let (req, start) = self.pending.pop_front().expect("take response");
                self.num.rx();
                // 统计请求耗时。
                self.rtt += start.elapsed();
                self.parser.check(&*req, &cmd);
                req.on_complete(&self.parser, cmd);
                continue;
            }
            if l == self.s.len() {
                // 说明当前的数据不足以解析一个完整的响应。
                break;
            }
        }

        poll_read
    }
    // 有些请求，是sentonly的，通常情况下，不需要等待响应。
    // 但是部分sentonly的请求，也会有响应，这时候直接会被直接丢弃。
    fn poll_sentonly_response(&mut self, cx: &mut Context) -> Result<()> {
        assert!(self.pending.len() == 0);
        loop {
            match self.poll_response_inner(cx)? {
                Poll::Pending => return Ok(()),
                Poll::Ready(()) => continue,
            }
        }
    }
    // 当前handler没有发送过任何请求。
    fn poll_checkalive(&mut self, cx: &mut Context) -> Result<()> {
        // 通过一次poll read来判断是否连接已经断开。
        let mut data = [0u8; 8];
        let mut buf = ReadBuf::new(&mut data);
        match Pin::new(&mut self.s).poll_read(cx, &mut buf) {
            Poll::Ready(Ok(_)) => {
                debug_assert_eq!(buf.filled().len(), 0, "unexpected:{:?} => {:?}", self, data);
                if buf.filled().len() > 0 {
                    log::error!("unexpected data from server:{:?} => {:?}", self, data);
                    Err(Error::UnexpectedData)
                } else {
                    // 读到了EOF，连接已经断开。
                    Err(Error::Eof)
                }
            }
            Poll::Ready(Err(e)) => Err(e.into()),
            Poll::Pending => Ok(()),
        }
    }

    #[inline(always)]
    fn poll_response(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        while self.pending.len() > 0 {
            ready!(self.poll_response_inner(cx))?;
        }
        Poll::Ready(Ok(()))
    }
    #[inline(always)]
    fn poll_flush(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        match Pin::new(&mut self.s).poll_flush(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
            //仅仅有异步请求未flush成功，则检查是否存在大量异步请求内存占用，如果占用大于等于512M，则关闭连接。
            Poll::Pending => {
                if self.pending.len() == 0 && self.s.unread_len() >= 512 * 1024 * 1024 {
                    return Poll::Ready(Err(Error::TxBufFull));
                }
                Poll::Pending
            }
        }
    }
}
impl<'r, Req: Request, P: Protocol, S: AsyncRead + AsyncWrite + Unpin + Stream> rt::ReEnter
    for Handler<'r, Req, P, S>
{
    #[inline]
    fn last(&self) -> Option<ds::time::Instant> {
        self.pending.front().map(|(_, t)| *t)
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
        while let Some((req, _)) = self.pending.pop_front() {
            req.on_err(Error::Waiting);
        }
        // 3. cancel
        use rt::Cancel;
        self.s.cancel();

        self.s.try_gc()
    }
    #[inline]
    fn refresh(&mut self) -> Result<bool> {
        log::debug!("handler:{:?}", self);
        self.s.try_gc();
        self.s.shrink();

        self.check_alive()?;
        Ok(true)
    }
}

use std::fmt::{self, Debug, Formatter};
impl<'r, Req, P, S: Debug> Debug for Handler<'r, Req, P, S> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "handler num:{:?} resource:{:?} p_req:{} {} {} buf:{:?} data:{:?}",
            self.num,
            self.name,
            self.pending.len(),
            self.rtt,
            self.host_metric,
            self.s,
            self.data
        )
    }
}

#[derive(Default, Debug)]
struct Number {
    #[cfg(any(feature = "trace"))]
    rx: usize,
    #[cfg(any(feature = "trace"))]
    tx: usize,
}
#[cfg(any(feature = "trace"))]
impl Number {
    #[inline(always)]
    fn rx(&mut self) {
        self.rx += 1;
    }
    #[inline(always)]
    fn tx(&mut self) {
        self.tx += 1;
    }
    #[inline(always)]
    fn check_pending(&self, len: usize) -> bool {
        len == self.tx - self.rx
    }
}

#[cfg(not(feature = "trace"))]
impl Number {
    #[inline(always)]
    fn rx(&mut self) {}
    #[inline(always)]
    fn tx(&mut self) {}
    #[inline(always)]
    fn check_pending(&self, _len: usize) -> bool {
        true
    }
}
