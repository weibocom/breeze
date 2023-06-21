use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::topology::TopologyCheck;
use ds::{time::Instant, AtomicWaker};
use endpoint::Topology;
use protocol::{HashedCommand, Protocol, Result, Stream};

use crate::{
    arena::CallbackContextArena,
    context::{CallbackContextPtr, ResponseContext},
    CallbackContext, Request, StreamMetrics,
};

pub async fn copy_bidirectional<C, P, T>(
    top: T,
    metrics: Arc<StreamMetrics>,
    client: C,
    parser: P,
    pipeline: bool,
) -> Result<()>
where
    C: AsyncRead + AsyncWrite + Stream + Unpin,
    P: Protocol + Unpin,
    T: Topology<Item = Request> + Unpin + TopologyCheck,
{
    *metrics.conn() += 1; // cps
    *metrics.conn_num() += 1;
    let pipeline = CopyBidirectional {
        pipeline,
        top,
        metrics,
        client,
        parser,
        pending: VecDeque::with_capacity(15),
        waker: AtomicWaker::default(),
        flush: false,
        start: Instant::now(),
        start_init: false,
        first: true, // 默认当前请求是第一个
        async_pending: VecDeque::new(),

        arena: CallbackContextArena::with_cache(32),
    };
    rt::Entry::timeout(pipeline, rt::DisableTimeout).await
}

pub struct CopyBidirectional<C, P, T> {
    top: T,
    client: C,
    parser: P,
    pending: VecDeque<CallbackContextPtr>,
    waker: AtomicWaker,

    metrics: Arc<StreamMetrics>,
    // 上一次请求的开始时间。用在multiget时计算整体耗时。
    // 如果一个multiget被拆分成多个请求，则start存储的是第一个请求的时间。
    start: Instant,
    pipeline: bool, // 请求是否需要以pipeline方式进行
    flush: bool,
    start_init: bool,
    first: bool, // 当前解析的请求是否是第一个。

    async_pending: VecDeque<CallbackContextPtr>, // 异步请求中的数量。

    arena: CallbackContextArena,
}
impl<C, P, T> Future for CopyBidirectional<C, P, T>
where
    C: AsyncRead + AsyncWrite + Stream + Unpin,
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
            let request = self.client.poll_recv(cx)?;
            // 解析buffer中的请求，并且发送请求。
            self.parse_request()?;

            // 把已经返回的response，写入到buffer中。
            self.process_pending()?;
            let flush = self.poll_flush(cx)?;

            if self.pending.len() > 0 && !self.pipeline {
                // CallbackContext::on_done负责唤醒
                // 非pipeline请求（即ping-pong），已经有ping了，因此等待pong即可。
                return Poll::Pending;
            }

            ready!(flush);
            ready!(request);
        }
    }
}
impl<C, P, T> CopyBidirectional<C, P, T>
where
    C: AsyncRead + AsyncWrite + Stream + Unpin,
    P: Protocol + Unpin,
    T: Topology<Item = Request> + Unpin + TopologyCheck,
{
    // 解析buffer，并且发送请求.
    #[inline]
    fn parse_request(&mut self) -> Result<()> {
        if self.client.len() == 0 {
            return Ok(());
        }
        let Self {
            client,
            top,
            parser,
            pending,
            waker,
            first,
            arena,
            ..
        } = self;
        // 解析请求，发送请求，并且注册回调
        let mut processor = Visitor {
            pending,
            waker,
            top,
            // parser,
            first,
            arena,
        };

        parser
            .parse_request(client, top, &mut processor)
            .map_err(|e| {
                log::info!("parse request error: {:?} on client:{:?}", e, client);
                match e {
                    protocol::Error::FlushOnClose(ref emsg) => {
                        // 此处只处理FLushOnClose，用于发送异常给client
                        let _write_rs = client.write_all(emsg);
                        let _flush_rs = client.flush();
                        log::warn!("+++ flush emsg[{:?}], client:[{:?}]", emsg, client);
                        e
                    }
                    _ => e,
                }
                // e
            })
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
            if !last {
                client.cache(true);
            }

            *metrics.key() += 1;
            let mut response = ctx.take_response();

            parser.write_response(
                &mut ResponseContext::new(&mut ctx, metrics, |hash| self.top.shard_idx(hash)),
                response.as_mut(),
                client,
            )?;

            let op = ctx.request().operation();
            if let Some(rsp) = response {
                if ctx.is_write_back() && rsp.ok() {
                    ctx.async_write_back(parser, rsp, self.top.exp_sec(), metrics);
                    self.async_pending.push_back(ctx);
                }
            }

            // 数据写完，统计耗时。当前数据只写入到buffer中，
            // 但mesh通常与client部署在同一台物理机上，buffer flush的耗时通常在微秒级。
            if last {
                let elapsed = start.elapsed();
                *metrics.ops(op) += elapsed;
                // 统计整机耗时
                *metrics.rtt() += elapsed;
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
                let _ctx = self.async_pending.pop_front();
            }
        }
    }
}

// struct Visitor<'a, P, T> {
struct Visitor<'a, T> {
    pending: &'a mut VecDeque<CallbackContextPtr>,
    waker: &'a AtomicWaker,
    top: &'a T,
    // parser: &'a P,
    first: &'a mut bool,
    arena: &'a mut CallbackContextArena,
}

// impl<'a, P, T: Topology<Item = Request>> protocol::RequestProcessor for Visitor<'a, P, T>
// where
//     P: Protocol + Unpin,
impl<'a, T: Topology<Item = Request> + TopologyCheck> protocol::RequestProcessor
    for Visitor<'a, T>
{
    #[inline]
    fn process(&mut self, cmd: HashedCommand, last: bool) {
        let first = *self.first;
        // 如果当前是最后一个子请求，那下一个请求就是一个全新的请求。
        // 否则下一个请求是子请求。
        *self.first = last;
        let cb = self.top.callback();
        let ctx = self
            .arena
            .alloc(CallbackContext::new(cmd, &self.waker, cb, first, last));
        let mut ctx = CallbackContextPtr::from(ctx, self.arena);

        // pendding 会move走ctx，所以提前把req给封装好
        let mut req: Request = ctx.build_request();
        self.pending.push_back(ctx);

        use protocol::req::Request as RequestTrait;
        if req.noforward() {
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
    C: AsyncRead + AsyncWrite + Stream + Unpin,
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
            let mut ctx = self.pending.pop_front().expect("empty");

            // TODO: 临时加日志，check mysql req被清理的key
            log::info!("+++ will clear req:{:?}", ctx.request().data());

            // 如果已经有response记入到ctx，需要take走，保证rsp drop时状态的一致性
            let _dropped = ctx.take_response();
        }
        // 处理异步请求
        self.process_async_pending();
        self.client.try_gc() && self.pending.len() == 0 && self.async_pending.len() == 0
    }
    #[inline]
    fn refresh(&mut self) -> Result<bool> {
        if self.top.refresh() {
            log::info!("topology refreshed: {:?}", self);
        }
        //self.process_async_pending();
        self.client.try_gc();
        self.client.shrink();
        Ok(true)
        // 满足条件之一说明需要刷新
        // 1. buffer 过大；2. 有异步请求未完成; 3. top 未drop
        //Ok(self.client.cap() >= crate::REFRESH_THREASHOLD || self.async_pending.len() > 0)
    }
}
impl<C: Debug, P, T> Debug for CopyBidirectional<C, P, T> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} => pending:({},{}) frist => {:?} flush:{}  => {:?}",
            self.metrics.biz(),
            self.pending.len(),
            self.async_pending.len(),
            self.pending.get(0).map(|r| &**r),
            self.flush,
            self.client,
        )
    }
}

//unsafe fn callback<T: Topology<Item = Request>>(top: &T) -> Callback {
//    let receiver = top as *const T as usize;
//    let send = Box::new(move |req| {
//        let t = &*(receiver as *const T);
//        t.send(req)
//    });
//    Callback::new(send)
//}
