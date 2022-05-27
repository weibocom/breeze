use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use ds::AtomicWaker;
use futures::ready;
use tokio::io::{AsyncRead, AsyncWrite};

use protocol::{HashedCommand, Protocol, Result, Stream, Topology, TopologyCheck, Writer};

use crate::buffer::{Reader, StreamGuard};
use crate::{Callback, CallbackContext, CallbackContextPtr, Request, StreamMetrics};

pub async fn copy_bidirectional<C, P, T>(
    name: String,
    top: T,
    mut metrics: StreamMetrics,
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
        rx_buf: StreamGuard::new(name),
        client,
        parser,
        pending: VecDeque::with_capacity(63),
        waker: AtomicWaker::default(),
        flush: false,
        start: Instant::now(),
        start_init: false,
        first: true, // 默认当前请求是第一个
        req_new: 0,
        req_dropped: AtomicUsize::new(0),

        dropping: Vec::new(),
        req_new_s: 0,
        dropping_at: Instant::now(),
    };
    let timeout = std::time::Duration::from_secs(10);
    rt::Entry::from(pipeline, timeout).await
}

struct CopyBidirectional<C, P, T> {
    top: T,
    rx_buf: StreamGuard,
    client: C,
    parser: P,
    pending: VecDeque<CallbackContextPtr>,
    waker: AtomicWaker,
    flush: bool,
    cb: Callback,

    metrics: StreamMetrics,
    // 上一次请求的开始时间。用在multiget时计算整体耗时。
    // 如果一个multiget被拆分成多个请求，则start存储的是第一个请求的时间。
    start: Instant,
    start_init: bool,
    first: bool, // 当前解析的请求是否是第一个。

    req_new: usize,           // 当前连接创建的req数量
    req_dropped: AtomicUsize, // 销毁的连接的数量

    // 等待删除的top
    dropping: Vec<(T, Callback)>,
    req_new_s: usize,
    dropping_at: Instant,
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
            let request = self.poll_request(cx)?;
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
    fn poll_request(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        if self.pending.len() == 0 {
            let Self { client, rx_buf, .. } = self;
            let mut cx = Context::from_waker(cx.waker());
            let mut rx = Reader::from(client, &mut cx);
            ready!(rx_buf.write(&mut rx))?;
            let num = rx.check_eof_num()?;
            // buffer full
            if num == 0 {}
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
            req_new,
            req_dropped,
            ..
        } = self;
        // 解析请求，发送请求，并且注册回调
        let mut processor = Visitor {
            pending,
            waker,
            top,
            cb,
            first,
            req_new,
            req_dropped,
        };
        parser.parse_request(rx_buf, top.hasher(), &mut processor)
    }
    // 处理pending中的请求，并且把数据发送到buffer
    #[inline]
    fn process_pending(&mut self) -> Result<()> {
        let Self {
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
            let mut ctx = pending.pop_front().expect("front");
            let last = ctx.last();
            if !last {
                // 当前不是最后一个值。也优先写入cache
                client.cache(true);
            }
            let op = ctx.request().operation();
            *metrics.key() += 1;

            if op.is_query() {
                let hit = ctx.response_ok() as usize;
                *metrics.hit() += hit;
                *metrics.cache() += (hit, 1);
            }

            if ctx.inited() {
                parser.write_response(&mut ctx, client)?;
                ctx.async_start_write_back(parser);
            } else {
                let req = ctx.request();
                if !req.noforward() {
                    *metrics.noresponse() += 1;
                }
                parser.write_no_response(req, client)?;
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
    req_new: &'a mut usize,
    req_dropped: &'a AtomicUsize,
}

impl<'a, T: Topology<Item = Request>> protocol::RequestProcessor for Visitor<'a, T> {
    #[inline]
    fn process(&mut self, cmd: HashedCommand, last: bool) {
        *self.req_new += 1;
        let first = *self.first;
        // 如果当前是最后一个子请求，那下一个请求就是一个全新的请求。
        // 否则下一个请求是子请求。
        *self.first = last;
        let cb = self.cb.into();
        let mut ctx: CallbackContextPtr =
            CallbackContext::new(cmd, &self.waker, cb, first, last, self.req_dropped).into();
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
impl<C: AsyncRead + AsyncWrite + Unpin, P, T: TopologyCheck + Topology<Item = Request>> rt::ReEnter
    for CopyBidirectional<C, P, T>
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
        self.rx_buf.try_gc()
            && self.pending.len() == 0
            && self.req_new == self.req_dropped.load(Ordering::Acquire)
    }
    #[inline]
    fn need_refresh(&self) -> bool {
        true
    }
    #[inline]
    fn refresh(&mut self) {
        assert!(
            self.req_new >= self.req_dropped.load(Ordering::Acquire),
            "{:?}",
            self
        );
        if let Some(top) = self.top.check() {
            unsafe {
                let old = std::ptr::replace(&mut self.top as *mut T, top);

                let cb = callback(&self.top);
                let old_cb = std::ptr::replace(&mut self.cb as *mut _, cb);

                self.dropping.push((old, old_cb));
                self.req_new_s = self.req_new;
                self.dropping_at = Instant::now();
            }
        }
        if self.dropping.len() > 0 {
            let req_dropped = self.req_dropped.load(Ordering::Acquire);
            if req_dropped >= self.req_new {
                self.dropping.clear();
                return;
            }
            // 1024是一个经验值
            // 如果访问量比较低，则很满足满足req_dropped > req_new
            if req_dropped > self.req_new_s + 1024
                && self.dropping_at.elapsed() >= Duration::from_secs(15)
            {
                log::warn!("top pending over 15 secs, dropping forcefully. {:?}", self);
                self.dropping.clear();
            }
        }
    }
}
impl<C, P, T> Debug for CopyBidirectional<C, P, T> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} => pending:{} flush:{} rx buf: {} dropping:{} requests: {}({}) => {}",
            self.metrics.biz(),
            self.pending.len(),
            self.flush,
            self.rx_buf.len(),
            self.dropping.len(),
            self.req_new,
            self.req_new_s,
            self.req_dropped.load(Ordering::Acquire)
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
