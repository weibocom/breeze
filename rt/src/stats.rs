use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::ready;
use metrics::TASK_NUM;
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
    id: usize,
    last: Instant,
    last_rx: Instant, // 上一次有接收到请求的时间
    inner: F,
    timeout: Duration,
    tick: Interval,
}
impl<F: Future + Unpin + ReEnter + Debug> Timeout<F> {
    #[inline]
    pub fn from(f: F, timeout: Duration) -> Self {
        TASK_NUM.fetch_add(1, Ordering::Relaxed);
        let tick = interval(timeout / 2);
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        Self {
            inner: f,
            last: Instant::now(),
            last_rx: Instant::now(),
            timeout,
            tick,
            id: SEQ.fetch_add(1, Ordering::Relaxed),
        }
    }
}

use protocol::Result;
impl<F: Future<Output = Result<()>> + ReEnter + Debug + Unpin> Future for Timeout<F> {
    type Output = F::Output;
    #[inline(always)]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (tx, rx) = (self.inner.num_tx(), self.inner.num_rx());
        if tx > rx {
            if self.last.elapsed() >= Duration::from_millis(20) {
                log::info!(
                    "reenter-{} {:?} {} > {} => {:?}",
                    self.id,
                    self.last.elapsed(),
                    tx,
                    rx,
                    self.inner
                );
            }
            if self.last_rx.elapsed() >= self.timeout {
                let elapsed = self.last_rx.elapsed();
                log::info!(
                    "reenter-{} timeout {:?}. ({} {}) => {:?}",
                    self.id,
                    elapsed,
                    tx,
                    rx,
                    self.inner
                );
                TASK_NUM.fetch_sub(1, Ordering::Relaxed);
                return Poll::Ready(Err(protocol::Error::Timeout(elapsed)));
            }
        } else {
            self.last_rx = Instant::now();
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
            let ret = ready!(ret);
            TASK_NUM.fetch_sub(1, Ordering::Relaxed);
            Poll::Ready(ret)
        }
    }
}
