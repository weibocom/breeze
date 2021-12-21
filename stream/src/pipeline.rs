use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use ds::AtomicWaker;
use futures::ready;
use tokio::io::{AsyncRead, AsyncWrite};

use ds::GuardedBuffer;
use protocol::Stream;
use protocol::{HashedCommand, Protocol, Result};
use sharding::hash::Hasher;

use crate::buffer::{Reader, StreamGuard};
use crate::gc::DelayedDrop;
use crate::{CallbackContext, CallbackContextPtr, CallbackPtr, Request, StreamMetrics};

pub async fn copy_bidirectional<'a, C, P>(
    cb: CallbackPtr,
    mut metrics: StreamMetrics,
    hash: &'a Hasher,
    client: C,
    parser: P,
) -> Result<()>
where
    C: AsyncRead + AsyncWrite + Unpin,
    P: Protocol + Unpin,
{
    *metrics.conn() += 1; // cps
    *metrics.conn_num() += 1;
    let mut rx_buf: DelayedDrop<_> = StreamGuard::from(GuardedBuffer::new(
        1024,
        1 << 20,
        32 * 1024,
        |_old, _delta| {},
    ))
    .into();
    let waker: DelayedDrop<_> = AtomicWaker::default().into();
    let mut pending: DelayedDrop<_> = VecDeque::with_capacity(127).into();
    let ret = CopyBidirectional {
        metrics,
        hash,
        rx_buf: &mut rx_buf,
        client,
        parser,
        pending: &mut pending,
        waker: &*waker,
        tx_idx: 0,
        tx_buf: Vec::with_capacity(1024),
        cb,
        start: Instant::now(),
        first: true, // 默认当前请求是第一个
    }
    .await;
    crate::gc::delayed_drop((rx_buf, pending, waker));

    ret
}

// TODO TODO CopyBidirectional在退出时，需要确保不存在pending中的请求，否则需要会存在内存访问异常。
struct CopyBidirectional<'a, C, P> {
    rx_buf: &'a mut StreamGuard,
    hash: &'a Hasher,
    client: C,
    parser: P,
    pending: &'a mut VecDeque<CallbackContextPtr>,
    waker: &'a AtomicWaker,
    tx_idx: usize,
    tx_buf: Vec<u8>,
    cb: CallbackPtr,

    metrics: StreamMetrics,
    // 上一次请求的开始时间。用在multiget时计算整体耗时。
    // 如果一个multiget被拆分成多个请求，则start存储的是第一个请求的时间。
    start: Instant,
    first: bool, // 当前解析的请求是否是第一个。
}
impl<'a, C, P> Future for CopyBidirectional<'a, C, P>
where
    C: AsyncRead + AsyncWrite + Unpin,
    P: Protocol + Unpin,
{
    type Output = Result<()>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.waker.register(cx.waker());
        loop {
            // 从client接收数据写入到buffer
            let request = self.poll_fill_buff(cx)?;
            // 解析buffer中的请求，并且发送请求。
            self.parse_request()?;

            // 把已经返回的response，写入到buffer中。
            let response = self.process_pending(cx)?;

            ready!(request);
            ready!(response);
        }
    }
}
impl<'a, C, P> CopyBidirectional<'a, C, P>
where
    C: AsyncRead + AsyncWrite + Unpin,
    P: Protocol + Unpin,
{
    // 从client读取request流的数据到buffer。
    #[inline(always)]
    fn poll_fill_buff(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        let Self { client, rx_buf, .. } = self;
        let mut cx = Context::from_waker(cx.waker());
        let mut rx = Reader::from(client, &mut cx);
        loop {
            ready!(rx_buf.buf.write(&mut rx))?;
            let num = rx.check_eof_num()?;
            // buffer full
            if num == 0 {
                break;
            }
        }
        Poll::Ready(Ok(()))
    }
    // 解析buffer，并且发送请求.
    #[inline(always)]
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
        parser.parse_request(*rx_buf, *hash, &mut processor)
    }
    // 处理pending中的请求，并且把数据发送到buffer
    #[inline(always)]
    fn process_pending(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        let Self {
            cb,
            client,
            tx_buf,
            tx_idx,
            pending,
            parser,
            start,
            metrics,
            ..
        } = self;
        let mut w = Pin::new(client);
        // 处理回调
        while let Some(ctx) = pending.front_mut() {
            if !ctx.complete() {
                break;
            }
            let mut ctx = pending.pop_front().expect("front");
            // 当前请求是第一个请求
            if ctx.first() {
                *start = ctx.start_at();
            }
            let last = ctx.last();
            let op = ctx.request().operation();

            if ctx.inited() {
                parser.write_response(&mut ctx, tx_buf)?;
                ctx.async_start_write_back(parser, cb.exp_sec());
            } else {
                let req = ctx.request();
                parser.write_no_response(req, tx_buf)?;
            }

            // 数据写完，统计耗时。当前数据只写入到buffer中，
            // 但mesh通常与client部署在同一台物理机上，buffer flush的耗时通常在微秒级。
            if last {
                *metrics.ops(op) += start.elapsed();
            }

            if tx_buf.len() >= 32 * 1024 {
                ready!(Self::poll_flush(cx, tx_idx, tx_buf, w.as_mut()))?;
            }
        }
        Self::poll_flush(cx, tx_idx, tx_buf, w.as_mut())
    }
    // 把response数据flush到client
    #[inline(always)]
    fn poll_flush(
        cx: &mut Context,
        idx: &mut usize,
        buf: &mut Vec<u8>,
        mut writer: Pin<&mut C>,
    ) -> Poll<Result<()>> {
        if buf.len() > 0 {
            while *idx < buf.len() {
                *idx += ready!(writer.as_mut().poll_write(cx, &buf[*idx..]))?;
            }
            *idx = 0;
            unsafe {
                buf.set_len(0);
            }
            ready!(writer.as_mut().poll_flush(cx)?);
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
    #[inline(always)]
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
    #[inline(always)]
    fn drop(&mut self) {
        *self.metrics.conn_num() -= 1;
    }
}
