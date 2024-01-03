use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use ds::chan::mpsc::Receiver;
use ds::time::Instant;
use protocol::{Error, Protocol, Request, Result, Stream};
use tokio::io::ReadBuf;
use tokio::io::{AsyncRead, AsyncWrite};

use metrics::Metric;

pub struct Handler<'r, Req, P, S> {
    data: &'r mut Receiver<Req>,
    pending: VecDeque<(Req, Instant)>,
    last_async_req: Option<Instant>,
    s: S,
    parser: P,
    rtt: Metric,

    // 处理timeout
    num: Number,

    // 连续多少个cycle检查到当前没有请求发送，则发送一个ping
    ping_cycle: u16,
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
    pub(crate) fn from(data: &'r mut Receiver<Req>, s: S, parser: P, rtt: Metric) -> Self {
        data.enable();
        Self {
            data,
            pending: VecDeque::with_capacity(31),
            s,
            parser,
            rtt,
            num: Number::default(),
            ping_cycle: 0,
            last_async_req: None,
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
        // 通过一次poll read来判断是否连接已经断开。
        let noop = noop_waker::noop_waker();
        let mut ctx = std::task::Context::from_waker(&noop);
        let mut data = [0u8; 8];
        let mut buf = ReadBuf::new(&mut data);
        let poll_read = Pin::new(&mut self.s).poll_read(&mut ctx, &mut buf);
        // 只有Pending才说明连接是正常的。
        match poll_read {
            Poll::Ready(Ok(_)) => {
                // 没有请求，但是读到了数据？bug
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

    // 发送request. 读空所有的request，并且发送。直到pending或者error
    #[inline]
    fn poll_request(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        self.s.cache(self.data.has_multi());
        while let Some(req) = ready!(self.data.poll_recv(cx)) {
            self.num.tx();

            self.s.write_slice(&*req, 0)?;

            match req.on_sent() {
                Some(r) => self.pending.push_back((r, Instant::now())),
                None => {
                    self.num.rx();
                }
            }
        }
        Poll::Ready(Err(Error::ChanReadClosed))
    }
    #[inline]
    fn poll_response(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        while self.pending.len() > 0 {
            let poll_read = self.s.poll_recv(cx);

            while self.s.len() > 0 {
                match self.parser.parse_response(&mut self.s) {
                    Ok(None) => break,

                    Ok(Some(cmd)) => {
                        let (req, start) = self.pending.pop_front().expect("take response");
                        self.num.rx();
                        // 统计请求耗时。
                        self.rtt += start.elapsed();
                        self.parser.check(&*req, &cmd);
                        req.on_complete(cmd);
                    }
                    Err(e) => match e {
                        // Error::UnexpectedData => {
                        //     let req = self
                        //         .pending
                        //         .iter()
                        //         .map(|(r, _)| r.data())
                        //         .collect::<Vec<_>>();
                        //     let rsp_data = self.s.slice();
                        //     let rsp_buf = unsafe { rsp_data.data_dump() };
                        //     panic!(
                        //         "unexpected:{:?} rsp:{:?} buff:{:?} pending req:[{:?}] ",
                        //         self, rsp_data, rsp_buf, req
                        //     );
                        // }
                        _ => {
                            return Poll::Ready(Err(e.into()));
                        }
                    },
                }
            }

            ready!(poll_read)?;
        }
        Poll::Ready(Ok(()))
    }
    #[inline(always)]
    fn poll_flush(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        let out = Pin::new(&mut self.s).poll_flush(cx);

        //检查存在全部且大量异步请求时，异步请求有没有超时
        //当本轮没有同步请求且tx buf有未flush出去的数据时，我们认为有异步请求未发送成功，
        //需要在poll_rsp之前check，可能会漏掉有老同步请求未收到响应的情况，这情况我们会认为发送成功了，但出问题的异步场景后续的异步请求还会出问题
        //如果在poll_rsp之后check，如果有新的同步请求并立马收到回复，我们可能会认为还没有flush成功
        //有可能在持续有异步请求进来的情况，tx buf一直有少量数据未flush，我们会认为不成功，但这种状况非稳态，不会持续很长时间
        if out.is_pending() && self.pending.len() == 0 {
            self.last_async_req.get_or_insert_with(|| Instant::now());
        } else {
            //否则，认为全部异步请求发送成功
            if self.last_async_req.is_some() {
                self.last_async_req.take();
            }
        }

        ready!(out)?;
        Poll::Ready(Ok(()))
    }
}
impl<'r, Req: Request, P: Protocol, S: AsyncRead + AsyncWrite + Unpin + Stream> rt::ReEnter
    for Handler<'r, Req, P, S>
{
    #[inline]
    fn last(&self) -> Option<ds::time::Instant> {
        self.pending
            .front()
            .map(|(_, t)| *t)
            .or(self.last_async_req)
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

        self.s.try_gc() && self.data.is_empty_hint()
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
            "handler num:{:?}  p_req:{} {} buf:{:?} data:{:?}",
            self.num,
            self.pending.len(),
            self.rtt,
            self.s,
            self.data
        )
    }
}

#[derive(Default, Debug)]
struct Number {
    #[cfg(debug_assertions)]
    rx: usize,
    #[cfg(debug_assertions)]
    tx: usize,
}
#[cfg(debug_assertions)]
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
#[cfg(not(debug_assertions))]
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
