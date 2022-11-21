use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};

use tokio::io::{AsyncRead, AsyncWrite};

use ds::{
    arena::Arena,
    time::{Duration, Instant},
    AtomicWaker,
};
use endpoint::{Topology, TopologyCheck};
use protocol::{callback::ResponseContext, HashedCommand, Protocol, Result, Stream, Writer};

use crate::{
    buffer::{Reader, StreamGuard},
    Callback, CallbackContext, CallbackContextPtr, Request, StreamMetrics,
};

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
        async_pending: VecDeque::new(),

        dropping: Vec::new(),
        arena: Arena::cache(32),
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

    async_pending: VecDeque<CallbackContextPtr>, // 异步请求中的数量。

    // 等待删除的top. 第三个元素是dropping时的req_new的值。
    dropping: Vec<(T, Callback)>,

    arena: Arena<CallbackContext>,
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
        self.process_async_pending();
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
            arena,
            ..
        } = self;
        // 解析请求，发送请求，并且注册回调
        let mut processor = Visitor {
            pending,
            waker,
            top,
            // parser,
            cb,
            first,
            arena,
        };

        parser.parse_request(rx_buf, top.hasher(), &mut processor)
    }
    // 处理pending中的请求，并且把数据发送到buffer
    #[inline]
    fn process_pending(&mut self) -> Result<()> {
        let Self {
            // 修改处理流程后，不再用pedding
            // top,
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
                *start_init = true;
            }
            if !ctx.complete() {
                break;
            }
            let mut ctx = pending.pop_front().expect("front");
            let last = ctx.last();
            // 当前不是最后一个值。也优先写入cache
            client.cache(!last);
            let op = ctx.request().operation();
            *metrics.key() += 1;

            let mut response = ctx.take_response_or(|ctx| {
                parser.build_local_response(ctx.request(), |hash| self.top.shard_idx(hash))
            });

            if parser.cache() && op.is_query() {
                *metrics.cache() += response.ok();
            }

            // 流程调整后，所有request都必须有response，异常请求、noforward请求，放在这里统一构建rsp fishermen
            //if !ctx.inited() {
            //    let local_resp =
            //        parser.build_local_response(ctx.request(), |hash| self.top.shard_idx(hash));
            //    ctx.adapt_local_response(local_resp);
            //}

            // 至此，所有req都有response，统一处理
            // assert!(ctx.inited(), "ctx req:[{:?}]", ctx.request(),);
            // parser.write_response(&mut ctx, client, metrics)?;
            //// 至此，所有req都有response，统一处理
            //assert!(ctx.inited(), "ctx req:[{:?}]", ctx.request(),);
            parser.write_response(
                &mut ResponseContext(&mut ctx, &mut response, metrics, Default::default()),
                client,
            )?;

            //TODO 部分请求的nil convert发生在write_response，待鑫鑫完成修改，这个统计后续也改到write_response中 fishermen 2022.11.17
            if response.nil_converted() {
                *metrics.nilconvert() += 1;
            }

            // 如果需要回写，且response正确，则进行回写
            if ctx.is_write_back() && response.ok() {
                ctx.async_write_back(parser, response, self.top.exp_sec(), metrics);
                self.async_pending.push_back(ctx);
            } else {
                self.arena.dealloc(ctx.into());
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
    #[inline]
    fn process_async_pending(&mut self) {
        if self.async_pending.len() > 0 {
            while let Some(ctx) = self.async_pending.front_mut() {
                if !ctx.async_done() {
                    break;
                }
                let ctx = self.async_pending.pop_front().expect("pop");
                self.arena.dealloc(ctx.into());
            }
        }
    }
}

// struct Visitor<'a, P, T> {
struct Visitor<'a, T> {
    pending: &'a mut VecDeque<CallbackContextPtr>,
    waker: &'a AtomicWaker,
    cb: &'a Callback,
    top: &'a T,
    // parser: &'a P,
    first: &'a mut bool,
    arena: &'a mut Arena<CallbackContext>,
}

// impl<'a, P, T: Topology<Item = Request>> protocol::RequestProcessor for Visitor<'a, P, T>
// where
//     P: Protocol + Unpin,
impl<'a, T: Topology<Item = Request>> protocol::RequestProcessor for Visitor<'a, T> {
    #[inline]
    fn process(&mut self, cmd: HashedCommand, last: bool) {
        let first = *self.first;
        // 如果当前是最后一个子请求，那下一个请求就是一个全新的请求。
        // 否则下一个请求是子请求。
        *self.first = last;
        let cb = self.cb.into();
        let mut ctx: CallbackContextPtr = self
            .arena
            .alloc(CallbackContext::new(cmd, &self.waker, cb, first, last))
            .into();

        // pendding 会move走ctx，所以提前把req给封装好
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
    P: Protocol + Unpin,
    T: Topology<Item = Request> + Unpin + TopologyCheck,
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
        // 处理异步请求
        self.process_async_pending();
        self.rx_buf.try_gc() && self.pending.len() == 0 && self.async_pending.len() == 0
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
        self.process_async_pending();
        if self.dropping.len() > 0 && self.async_pending.len() == 0 {
            self.dropping.clear();
        }
        self.rx_buf.try_gc();
        self.rx_buf.shrink();
        self.client.shrink();
        // 1. buffer 过大；2. 有异步请求未完成; 3. top 未drop
        (self.rx_buf.cap() + self.client.cap() >= crate::REFRESH_THREASHOLD)
            && self.async_pending.len() > 0
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
            self.async_pending.len()
        )
    }
}

unsafe fn callback<T: Topology<Item = Request>>(top: &T) -> Callback {
    let receiver = top as *const T as usize;
    let send = Box::new(move |req| {
        let t = &*(receiver as *const T);
        t.send(req)
    });
    Callback::new(send)
}
