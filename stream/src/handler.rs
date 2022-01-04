use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::ready;
use protocol::{Error, Protocol, Request, Result};
use tokio::io::{AsyncRead, AsyncWrite, BufWriter};
use tokio::sync::mpsc::Receiver;
use tokio::time::{interval, Interval, MissedTickBehavior};

use crate::buffer::StreamGuard;
use crate::timeout::TimeoutChecker;

use metrics::Metric;

pub(crate) struct Handler<'r, Req, P, W, R> {
    data: &'r mut Receiver<Req>,
    pending: &'r mut VecDeque<Req>,

    tx: BufWriter<W>,
    rx: R,
    // 用来处理发送数据
    cache: bool, // pending的尾部数据是否需要发送。true: 需要发送
    oft_c: usize,
    // 处理接收数据
    buf: &'r mut StreamGuard,
    parser: P,
    flushing: bool,

    // 做超时控制
    timeout: TimeoutChecker,
    tick: Interval, // 多长时间检查一次

    // metrics
    //bytes_rx: &'r mut Metric,
    //bytes_tx: &'r mut Metric,
    rtt: &'r mut Metric,

    // 验证tokio 调整框架调度延迟
    last_schedule: Instant,
}
impl<'r, Req, P, W, R> Future for Handler<'r, Req, P, W, R>
where
    Req: Request + Unpin,
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    P: Protocol + Unpin,
{
    type Output = Result<()>;

    #[inline(always)]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        if me.pending.len() == 0 {
            me.timeout.reset()
        }
        if me.pending.len() > 0 || me.flushing {
            if me.last_schedule.elapsed() >= Duration::from_millis(50) {
                log::info!("schedule {:?} rtt:{}", me.last_schedule.elapsed(), me.rtt);
            }
        }
        loop {
            let request = me.poll_request(cx)?;
            let _flush = me.poll_flush(cx)?;
            let _response = me.poll_response(cx)?;
            me.last_schedule = Instant::now();
            if me.pending.len() > 0 {
                //ready!(_response);
                ready!(me.tick.poll_tick(cx));
                me.timeout.check()?;
                // 继续下一次循环，不pending 在request上。
                continue;
            }
            // 只有pending.len()为0的时候，才阻塞在request上。
            ready!(request);
        }
    }
}
impl<'r, Req, P, W, R> Handler<'r, Req, P, W, R> {
    pub(crate) fn from(
        data: &'r mut Receiver<Req>,
        pending: &'r mut VecDeque<Req>,
        buf: &'r mut StreamGuard,
        tx: W,
        rx: R,
        //bytes_tx: &'r mut Metric,
        //bytes_rx: &'r mut Metric,
        rtt: &'r mut Metric,
        parser: P,
        cycle: Duration,
    ) -> Self
    where
        W: AsyncWrite + Unpin,
    {
        let tx_buf_size = 8192;
        let least = 2;
        let timeout = TimeoutChecker::new(cycle, least);
        let mut tick = interval(cycle / 2);
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        Self {
            data,
            cache: false,
            pending: pending,
            tx: BufWriter::with_capacity(tx_buf_size, tx),
            rx,
            parser,
            oft_c: 0,
            buf,
            flushing: false,

            timeout,
            tick,
            //bytes_tx,
            //bytes_rx,
            rtt,
            last_schedule: Instant::now(),
        }
    }
    // 发送request.
    #[inline(always)]
    fn poll_request(&mut self, cx: &mut Context) -> Poll<Result<()>>
    where
        Req: Request,
        W: AsyncWrite + Unpin,
    {
        loop {
            if self.cache {
                debug_assert!(self.pending.len() > 0);
                if let Some(req) = self.pending.back_mut() {
                    while self.oft_c < req.len() {
                        let data = req.read(self.oft_c);
                        let n = ready!(Pin::new(&mut self.tx).poll_write(cx, data))?;
                        self.oft_c += n;
                    }
                    req.on_sent();
                    if req.sentonly() {
                        // 只发送的请求，不需要等待response，pop掉。
                        self.pending.pop_back();
                    }
                }
                //*self.bytes_tx += self.oft_c;
                self.oft_c = 0;
                self.flushing = true;
                self.cache = false;
            }
            match ready!(self.data.poll_recv(cx)) {
                None => return Poll::Ready(Err(Error::QueueClosed)), // 说明当前实例已经被销毁，需要退出。
                Some(req) => {
                    self.pending.push_back(req);
                    self.cache = true;
                }
            }
        }
    }
    #[inline(always)]
    fn poll_response(&mut self, cx: &mut Context) -> Poll<Result<()>>
    where
        R: AsyncRead + Unpin,
        P: Protocol,
        Req: Request,
    {
        loop {
            let mut cx = Context::from_waker(cx.waker());
            let mut reader = crate::buffer::Reader::from(&mut self.rx, &mut cx);
            ready!(self.buf.buf.write(&mut reader))?;
            // num == 0 说明是buffer满了。等待下一次事件，buffer释放后再读取。
            let num = reader.check_eof_num()?;
            log::debug!("{} bytes received.", num);
            if num == 0 {
                return Poll::Ready(Ok(()));
            }
            //*self.bytes_rx += num;
            use protocol::Stream;
            while self.buf.len() > 0 {
                match self.parser.parse_response(self.buf)? {
                    None => break,
                    Some(cmd) => {
                        self.timeout.tick();
                        debug_assert_ne!(self.pending.len(), 0);
                        let req = self.pending.pop_front().expect("take response");
                        // 统计请求耗时。
                        *self.rtt += req.start_at().elapsed();
                        req.on_complete(cmd);
                    }
                }
            }
        }
    }
    #[inline(always)]
    fn poll_flush(&mut self, cx: &mut Context) -> Poll<Result<()>>
    where
        W: AsyncWrite + Unpin,
    {
        ready!(Pin::new(&mut self.tx).poll_flush(cx))?;
        self.flushing = false;
        Poll::Ready(Ok(()))
    }
}
unsafe impl<'r, Req, P, W, R> Send for Handler<'r, Req, P, W, R> {}
unsafe impl<'r, Req, P, W, R> Sync for Handler<'r, Req, P, W, R> {}
