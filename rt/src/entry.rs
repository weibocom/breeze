use ds::time::{Duration, Instant};
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use metrics::base::*;

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
    // 定期会调用，通常用来清理内存，更新数据等信息。
    // 返回true: 表示期待进行下一次调用
    // 返回false: 表示资源已经释放，不再需要调用。
    fn refresh(&mut self) -> bool;
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
    ready: bool,
    refresh_tick: Interval,
    out: Option<Result<()>>,
    refresh_next: bool,
    last_refresh: Instant,
}
impl<F: Future<Output = Result<()>> + Unpin + ReEnter + Debug> Entry<F> {
    #[inline]
    pub fn from(f: F, timeout: Duration) -> Self {
        let mut tick = interval(timeout.max(Duration::from_millis(50)));
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut refresh_tick = interval(Duration::from_secs(9));
        refresh_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        Self {
            inner: f,
            last: Instant::now(),
            last_rx: Instant::now(),
            timeout,
            tick,
            ready: false,
            out: None,
            refresh_tick,
            refresh_next: false,
            last_refresh: Instant::now(),
        }
    }
    #[inline]
    fn poll_run(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let now = Instant::now();
        if (now - self.last_refresh).as_secs() > 3 {
            self.refresh_next = self.inner.refresh();
            self.last_refresh = now;
        }

        let (tx, rx) = (self.inner.num_tx(), self.inner.num_rx());
        if tx > rx {
            if now - self.last >= Duration::from_millis(10) {
                REENTER_10MS.incr();
            }
            if now - self.last_rx >= self.timeout {
                return Poll::Ready(Err(protocol::Error::Timeout(now - self.last_rx)));
            }
        } else {
            self.last_rx = now;
        }

        let ret = Pin::new(&mut self.inner).poll(cx)?;
        let (tx_post, rx_post) = (self.inner.num_tx(), self.inner.num_rx());
        if tx_post > rx_post {
            self.last = Instant::now();
            // 有接收到请求，则更新timeout基准
            if rx_post > rx {
                self.last_rx = self.last;
            }
            ready!(self.tick.poll_tick(cx));
            self.tick.reset();
        } else {
            if ret.is_pending() {
                if self.refresh_next {
                    ready!(self.refresh_tick.poll_tick(cx));
                    self.refresh_tick.reset();
                }
            }
        }
        ret.map(|r| Ok(r))
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
            if self.last.elapsed().as_secs() % 15 == 0 {
                LEAKED_CONN.incr();
            }
        }
        Poll::Ready(self.out.take().unwrap())
    }
}
