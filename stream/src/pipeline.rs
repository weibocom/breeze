use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
//use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Instant;

use ds::AtomicWaker;
use futures::ready;
use tokio::io::{AsyncRead, AsyncWrite};

use protocol::{HashedCommand, Protocol, Result, Stream, Writer};
use sharding::hash::Hasher;

use crate::buffer::{Reader, StreamGuard};
use crate::{CallbackContext, CallbackContextPtr, CallbackPtr, Request, StreamMetrics};

pub async fn copy_bidirectional<'a, C, P>(
    cb: CallbackPtr,
    mut metrics: StreamMetrics,
    hash: &'a Hasher,
    client: C,
    parser: P,
) -> Result<()>
where
    C: AsyncRead + AsyncWrite + Writer + Unpin,
    P: Protocol + Unpin,
{
    *metrics.conn() += 1; // cps
    *metrics.conn_num() += 1;
    let pipeline = CopyBidirectional {
        metrics,
        hash,
        rx_buf: StreamGuard::new(),
        client,
        parser,
        pending: VecDeque::with_capacity(63),
        waker: AtomicWaker::default(),
        flush: false,
        cb,
        start: Instant::now(),
        start_init: false,
        first: true, // 默认当前请求是第一个
    };
    let timeout = std::time::Duration::from_millis(0);
    rt::Timeout::from(pipeline, timeout).await
}

// TODO TODO CopyBidirectional在退出时，需要确保不存在pending中的请求，否则需要会存在内存访问异常。
struct CopyBidirectional<'a, C, P> {
    rx_buf: StreamGuard,
    hash: &'a Hasher,
    client: C,
    parser: P,
    pending: VecDeque<CallbackContextPtr>,
    waker: AtomicWaker,
    flush: bool,
    cb: CallbackPtr,

    metrics: StreamMetrics,
    // 上一次请求的开始时间。用在multiget时计算整体耗时。
    // 如果一个multiget被拆分成多个请求，则start存储的是第一个请求的时间。
    start: Instant,
    start_init: bool,
    first: bool, // 当前解析的请求是否是第一个。
}
impl<'a, C, P> Future for CopyBidirectional<'a, C, P>
where
    C: AsyncRead + AsyncWrite + Writer + Unpin,
    P: Protocol + Unpin,
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
impl<'a, C, P> CopyBidirectional<'a, C, P>
where
    C: AsyncRead + AsyncWrite + Writer + Unpin,
    P: Protocol + Unpin,
{
    // 从client读取request流的数据到buffer。
    #[inline]
    fn poll_request(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        if self.pending.len() == 0 {
            let Self { client, rx_buf, .. } = self;
            let mut cx = Context::from_waker(cx.waker());
            let mut rx = Reader::from(client, &mut cx);
            ready!(rx_buf.buf.write(&mut rx))?;
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
            hash,
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
            cb,
            first,
        };
        parser.parse_request(rx_buf, *hash, &mut processor)
    }
    // 处理pending中的请求，并且把数据发送到buffer
    #[inline]
    fn process_pending(&mut self) -> Result<()> {
        let Self {
            client,
            cb,
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
                ctx.async_start_write_back(parser, cb.exp_sec());
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

struct Visitor<'a> {
    pending: &'a mut VecDeque<CallbackContextPtr>,
    waker: &'a AtomicWaker,
    cb: &'a CallbackPtr,
    first: &'a mut bool,
}

impl<'a> protocol::RequestProcessor for Visitor<'a> {
    #[inline]
    fn process(&mut self, cmd: HashedCommand, last: bool) {
        let first = *self.first;
        // 如果当前是最后一个子请求，那下一个请求就是一个全新的请求。
        // 否则下一个请求是子请求。
        *self.first = last;
        let cb = self.cb.clone();
        let mut ctx: CallbackContextPtr =
            CallbackContext::new(cmd, &self.waker, cb, first, last).into();
        let req: Request = ctx.build_request();
        self.pending.push_back(ctx);
        req.start();
    }
}
impl<'a, C, P> Drop for CopyBidirectional<'a, C, P> {
    #[inline]
    fn drop(&mut self) {
        *self.metrics.conn_num() -= 1;
    }
}

use std::fmt::{self, Debug, Formatter};
impl<'a, C, P> rt::ReEnter for CopyBidirectional<'a, C, P> {
    #[inline]
    fn close(&mut self) -> bool {
        // 剔除已完成的请求
        while let Some(ctx) = self.pending.front_mut() {
            if !ctx.complete() {
                break;
            }
            self.pending.pop_front();
        }
        // take走，close后不需要再wake。避免Future drop后再次被wake，导致UB
        self.waker.take();
        self.rx_buf.try_gc() && self.pending.len() == 0
    }
}
impl<'a, C, P> Debug for CopyBidirectional<'a, C, P> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "pending:{} flush:{} rx buf: {}  copy_bidirectional => {}",
            self.pending.len(),
            self.flush,
            self.rx_buf.len(),
            self.metrics.biz(),
        )
    }
}
