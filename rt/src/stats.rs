use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::ready;
use metrics::{BASE_PATH, Metric, Path};
use tokio::time::{interval, Interval};

pub trait ReEnter {
    // 发送的请求数量
    #[inline(always)]
    fn num_tx(&self) -> usize {
        0
    }
    // 接收到的请求数量
    #[inline(always)]
    fn num_rx(&self) -> usize {
        0
    }
}
//  统计
//  1. 每次poll的执行耗时
//  2. 重入耗时间隔
pub struct Timeout<F> {
    last: Instant,
    last_rx: Instant, // 上一次有接收到请求的时间
    inner: F,
    timeout: Duration,
    tick: Interval,
    m_reenter: Metric,
}
impl<F: Future + Unpin + ReEnter + Debug> Timeout<F> {
    #[inline]
    pub fn from(f: F, timeout: Duration) -> Self {
        let tick = interval(timeout);
        let m_reenter = Path::new(vec![BASE_PATH]).rtt("reenter10ms");
        metrics::incr_task();
        Self {
            inner: f,
            last: Instant::now(),
            last_rx: Instant::now(),
            timeout,
            tick,
            m_reenter,
        }
    }
}

use protocol::Result;
impl<F: Future<Output = Result<()>> + ReEnter + Debug + Unpin> Future for Timeout<F> {
    type Output = F::Output;
    #[inline(always)]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let now = Instant::now();
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
            ret
        }
    }
}
impl<F> Drop for Timeout<F> {
    #[inline]
    fn drop(&mut self) {
        metrics::decr_task();
    }
}
