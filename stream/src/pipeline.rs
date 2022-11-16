use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicUsize, Ordering::*},
    Arc,
};
use std::task::{ready, Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite};

use ds::{
    time::{Duration, Instant},
    AtomicWaker,
};
use protocol::{
    Commander, HashedCommand, Protocol, Result, Stream, Topology, TopologyCheck, Writer,
};

use crate::buffer::{Reader, StreamGuard};
use crate::{Callback, CallbackContext, CallbackContextPtr, Request, StreamMetrics};

pub async fn copy_bidirectional<C, P, T>(
    top: T,
    metrics: Arc<StreamMetrics>,
    client: C,
    parser: P,
) -> Result<()>
where
    C: AsyncRead + AsyncWrite + Writer + Unpin,
    P: Protocol + Unpin,
    T: Topology<Item = Request> + Unpin + TopologyCheck,
{
    *metrics.conn() += 1; // cps
    *metrics.conn_num() += 1;
    let cb = unsafe { callback(&top) };
    let pipeline = CopyBidirectional {
        cb,
        top,
        metrics,
        rx_buf: StreamGuard::new(),
        client,
        parser,
        pending: VecDeque::with_capacity(63),
        waker: AtomicWaker::default(),
        flush: false,
        start: Instant::now(),
        start_init: false,
        first: true, // 默认当前请求是第一个
        async_pending: AtomicUsize::new(0),

        dropping: Vec::new(),
    };
    rt::Entry::from(pipeline, Duration::from_secs(10)).await
}

pub struct CopyBidirectional<C, P, T> {
    top: T,
    rx_buf: StreamGuard,
    client: C,
    parser: P,
    pending: VecDeque<CallbackContextPtr>,
    waker: AtomicWaker,
    flush: bool,
    cb: Callback,

    metrics: Arc<StreamMetrics>,
    // 上一次请求的开始时间。用在multiget时计算整体耗时。
    // 如果一个multiget被拆分成多个请求，则start存储的是第一个请求的时间。
    start: Instant,
    start_init: bool,
    first: bool, // 当前解析的请求是否是第一个。

    async_pending: AtomicUsize, // 异步请求中的数量。

    // 等待删除的top. 第三个元素是dropping时的req_new的值。
    dropping: Vec<(T, Callback)>,
}
impl<C, P, T> Future for CopyBidirectional<C, P, T>
where
    C: AsyncRead + AsyncWrite + Writer + Unpin,
    P: Protocol + Unpin,
    T: Topology<Item = Request> + Unpin + TopologyCheck,
{
    type Output = Result<()>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.waker.register(cx.waker());
        loop {
            // 从client接收数据写入到buffer
            let request = self.poll_recv(cx)?;
            // 解析buffer中的请求，并且发送请求。
            self.parse_request()?;

            // 把已经返回的response，写入到buffer中。
            self.process_pending()?;
            let flush = self.poll_flush(cx)?;

            ready!(flush);
            ready!(request);

            if self.pending.len() > 0 {
                // CallbackContext::on_done负责唤醒
                return Poll::Pending;
            }
        }
    }
}
impl<C, P, T> CopyBidirectional<C, P, T>
where
    C: AsyncRead + AsyncWrite + Writer + Unpin,
    P: Protocol + Unpin,
    T: Topology<Item = Request> + Unpin + TopologyCheck,
{
    // 从client读取request流的数据到buffer。
    #[inline]
    fn poll_recv(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        if self.pending.len() == 0 {
            let Self { client, rx_buf, .. } = self;
            let mut cx = Context::from_waker(cx.waker());
            let mut rx = Reader::from(client, &mut cx);
            ready!(rx_buf.write(&mut rx))?;
            rx.check()?;
        }
        Poll::Ready(Ok(()))
    }
    // 解析buffer，并且发送请求.
    #[inline]
    fn parse_request(&mut self) -> Result<()> {
        if self.rx_buf.len() == 0 {
            return Ok(());
        }
        let Self {
            top,
            parser,
            pending,
            waker,
            rx_buf,
            first,
            cb,
            ..
        } = self;
        // 解析请求，发送请求，并且注册回调
        let mut processor = Visitor {
            pending,
            waker,
            top,
            cb,
            first,
        };

        parser.parse_request(rx_buf, top.hasher(), &mut processor)
    }
    // 处理pending中的请求，并且把数据发送到buffer
    #[inline]
    fn process_pending(&mut self) -> Result<()> {
        let Self {
            top,
            client,
            pending,
            parser,
            start,
            start_init,
            metrics,
            flush,
            ..
        } = self;
        // 处理回调
        client.cache(pending.len() > 1);
        while let Some(ctx) = pending.front_mut() {
            // 当前请求是第一个请求
            if !*start_init {
                *start = ctx.start_at();
            }
            if !ctx.complete() {
                break;
            }
            let mut ctx: CallbackContext = pending.pop_front().expect("front").into();
            let last = ctx.last();
            // 当前不是最后一个值。也优先写入cache
            client.cache(!last);
            let op = ctx.request().operation();
            *metrics.key() += 1;

            if parser.cache() && op.is_query() {
                *metrics.cache() += ctx.response_ok();
            }

            if !ctx.response_ok() && op.is_store() {
                *metrics.storeerr() += 1;
            }

            if ctx.inited() && !ctx.request().ignore_rsp() {
                let nil_convert = parser.write_response(&mut ctx, client)?;
                if nil_convert > 0 {
                    *metrics.nilconvert() += nil_convert;
                }
                if ctx.is_write_back() && ctx.response_ok() {
                    self.async_pending.fetch_add(1, AcqRel);
                    ctx.async_write_back(parser, &self.async_pending);
                }
            } else if ctx.request().ignore_rsp() {
                // do nothing!
                log::debug!("+++ ignore resp:{:?}=>{:?}", ctx.request(), ctx.response())
            } else {
                let req = ctx.request();
                if !req.noforward() {
                    *metrics.noresponse() += 1;
                }

                // 传入top，某些指令需要
                let nil_convert =
                    parser.write_no_response(req, client, |hash| top.shard_idx(hash))?;
                if nil_convert > 0 {
                    *metrics.nilconvert() += nil_convert;
                }
            }

            // 数据写完，统计耗时。当前数据只写入到buffer中，
            // 但mesh通常与client部署在同一台物理机上，buffer flush的耗时通常在微秒级。
            if last {
                *metrics.ops(op) += start.elapsed();
                *flush = true;
                *start_init = false;
            }
        }
        Ok(())
    }
    // 把response数据flush到client
    #[inline]
    fn poll_flush(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        if self.flush {
            ready!(Pin::new(&mut self.client).as_mut().poll_flush(cx)?);
            self.flush = false;
        }
        Poll::Ready(Ok(()))
    }
}

struct Visitor<'a, T> {
    pending: &'a mut VecDeque<CallbackContextPtr>,
    waker: &'a AtomicWaker,
    cb: &'a Callback,
    top: &'a T,
    first: &'a mut bool,
}

impl<'a, T: Topology<Item = Request>> protocol::RequestProcessor for Visitor<'a, T> {
    #[inline]
    fn process(&mut self, cmd: HashedCommand, last: bool) {
        let first = *self.first;
        // 如果当前是最后一个子请求，那下一个请求就是一个全新的请求。
        // 否则下一个请求是子请求。
        *self.first = last;
        let cb = self.cb.into();
        let mut ctx: CallbackContextPtr =
            CallbackContext::new(cmd, &self.waker, cb, first, last).into();
        let mut req: Request = ctx.build_request();
        self.pending.push_back(ctx);
        use protocol::req::Request as RequestTrait;
        if req.cmd().noforward() {
            req.on_noforward();
        } else {
            self.top.send(req);
        }
    }
}
impl<C, P, T> Drop for CopyBidirectional<C, P, T> {
    #[inline]
    fn drop(&mut self) {
        *self.metrics.conn_num() -= 1;
    }
}

use std::fmt::{self, Debug, Formatter};
impl<C, P, T> rt::ReEnter for CopyBidirectional<C, P, T>
where
    C: AsyncRead + AsyncWrite + Writer + Unpin,
    T: TopologyCheck + Topology<Item = Request>,
{
    #[inline]
    fn close(&mut self) -> bool {
        // take走，close后不需要再wake。避免Future drop后再次被wake，导致UB
        self.waker.take();
        use rt::Cancel;
        self.client.cancel();
        // 剔除已完成的请求
        while let Some(ctx) = self.pending.front_mut() {
            if !ctx.complete() {
                break;
            }
            self.pending.pop_front();
        }
        self.rx_buf.try_gc() && self.pending.len() == 0 && self.async_pending.load(Acquire) == 0
    }
    #[inline]
    fn refresh(&mut self) -> bool {
        if let Some(top) = self.top.check() {
            unsafe {
                let old = std::ptr::replace(&mut self.top as *mut T, top);

                let cb = callback(&self.top);
                let old_cb = std::ptr::replace(&mut self.cb as *mut _, cb);

                self.dropping.push((old, old_cb));
            }
        }
        if self.dropping.len() > 0 && self.async_pending.load(Acquire) == 0 {
            self.dropping.clear();
        }
        self.rx_buf.try_gc();
        self.rx_buf.shrink();
        self.client.shrink();
        // 1. buffer 过大；2. 有异步请求未完成; 3. top 未drop
        (self.rx_buf.cap() + self.client.cap() >= crate::REFRESH_THREASHOLD)
            && self.async_pending.load(Acquire) > 0
            && self.dropping.len() > 0
    }
}
impl<C, P, T> Debug for CopyBidirectional<C, P, T> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} => pending:{} flush:{} rx buf: {} dropping:{}  => {}",
            self.metrics.biz(),
            self.pending.len(),
            self.flush,
            self.rx_buf.len(),
            self.dropping.len(),
            self.async_pending.load(Acquire)
        )
    }
}

unsafe fn callback<T: Topology<Item = Request>>(top: &T) -> Callback {
    let receiver = top as *const T as usize;
    let send = Box::new(move |req| {
        let t = &*(receiver as *const T);
        t.send(req)
    });
    let exp_sec = Box::new(move || {
        let t = &*(receiver as *const T);
        t.exp_sec()
    });
    Callback::new(send, exp_sec)
}
