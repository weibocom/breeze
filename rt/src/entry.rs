use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::ready;
use metrics::{Metric, Path, BASE_PATH};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    time::{interval, Interval, MissedTickBehavior},
};

pub trait ReEnter {
    // 发送的请求数量
    #[inline]
    fn num_tx(&self) -> usize {
        0
    }
    // 接收到的请求数量
    #[inline]
    fn num_rx(&self) -> usize {
        0
    }
    // 在Future.poll返回前执行。
    // 可能会多次执行，直到close返回true。
    // true: 成功关闭，释放相关资源
    // false: 还有资源未释放
    fn close(&mut self) -> bool;
    #[inline]
    fn need_refresh(&self) -> bool {
        false
    }
    #[inline]
    fn refresh(&mut self) {}
}
pub trait Cancel {
    fn cancel(&mut self);
}
impl<T: AsyncRead + AsyncWrite + Unpin> Cancel for T {
    // cancel掉Stream，避免在Future::ready后，drop之前，后再次wake导致panic
    fn cancel(&mut self) {
        let noop = noop_waker::noop_waker();
        let mut ctx = std::task::Context::from_waker(&noop);
        let mut stream = Pin::new(self);
        let _ = stream.as_mut().poll_shutdown(&mut ctx);
        let mut ignore = [0u8; 8];
        let mut buf = tokio::io::ReadBuf::new(&mut ignore);

        let _ = stream.as_mut().poll_read(&mut ctx, &mut buf);
    }
}
//  统计
//  1. 每次poll的执行耗时
//  2. 重入耗时间隔
pub struct Entry<F> {
    last: Instant,
    last_rx: Instant, // 上一次有接收到请求的时间
    inner: F,
    timeout: Duration,
    tick: Interval,
    m_reenter: Metric,
    ready: bool,
    refresh_tick: Interval,
    out: Option<Result<()>>,
    runs: usize,
}
impl<F: Future<Output = Result<()>> + Unpin + ReEnter + Debug> Entry<F> {
    #[inline]
    pub fn from(f: F, timeout: Duration) -> Self {
        let mut tick = interval(timeout.max(Duration::from_millis(50)));
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut refresh_tick = interval(Duration::from_secs(3));
        refresh_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let m_reenter = Path::new(vec![BASE_PATH]).rtt("reenter10ms");
        metrics::incr_task();
        Self {
            inner: f,
            last: Instant::now(),
            last_rx: Instant::now(),
            timeout,
            tick,
            m_reenter,
            ready: false,
            out: None,
            refresh_tick,
            runs: 0,
        }
    }
    #[inline]
    fn poll_run(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let now = Instant::now();
        if self.inner.need_refresh() {
            self.runs += 1;
            if self.runs & 15 == 0 {
                self.inner.refresh();
            }
        }

        let (tx, rx) = (self.inner.num_tx(), self.inner.num_rx());
        if tx > rx {
            if now - self.last >= Duration::from_millis(10) {
                let elapsed = now - self.last;
                self.m_reenter += elapsed;
            }
            if now - self.last_rx >= self.timeout {
                return Poll::Ready(Err(protocol::Error::Timeout(now - self.last_rx)));
            }
        } else {
            self.last_rx = now;
        }
        let ret = Pin::new(&mut self.inner).poll(cx);
        let (tx_post, rx_post) = (self.inner.num_tx(), self.inner.num_rx());
        if tx_post > rx_post {
            self.last = Instant::now();
            // 有接收到请求，则更新timeout基准
            if rx_post > rx {
                self.last_rx = self.last;
            }
            loop {
                ready!(self.tick.poll_tick(cx));
            }
        } else {
            if self.inner.need_refresh() {
                // 如果需要刷新，则不阻塞在原有的pending上
                if ret.is_pending() {
                    loop {
                        ready!(self.refresh_tick.poll_tick(cx));
                    }
                }
            }
            ret
        }
    }
}

use protocol::Result;
impl<F: Future<Output = Result<()>> + ReEnter + Debug + Unpin> Future for Entry<F> {
    type Output = F::Output;
    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.ready {
            self.out = Some(ready!(self.as_mut().poll_run(cx)));
            self.ready = true;
            // 复用last来统计close的耗时
            self.last = Instant::now();
        }
        // close
        while !self.inner.close() {
            ready!(self.tick.poll_tick(cx));
            log::info!("closing => {:?} {:?}", self.inner, self.out);
        }
        Poll::Ready(self.out.take().unwrap())
    }
}
impl<F> Drop for Entry<F> {
    #[inline]
    fn drop(&mut self) {
        metrics::decr_task();
    }
}
