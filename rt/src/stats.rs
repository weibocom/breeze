use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::ready;
use metrics::{Metric, Path};
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
    m_task: Metric,
    m_reenter: Metric,
    m_timeout: Metric,
}
impl<F: Future + Unpin + ReEnter + Debug> Timeout<F> {
    #[inline]
    pub fn from(f: F, timeout: Duration) -> Self {
        let tick = interval(timeout / 2);
        let m_reenter = Path::new(vec!["mesh"]).rtt("reenter10ms+");
        let m_timeout = Path::new(vec!["mesh"]).qps("timeout");
        let mut m_task = Path::new(vec!["mesh"]).count("task");
        m_task += 1;
        log::info!("timeout task crated:{:?}", f);

        Self {
            inner: f,
            last: Instant::now(),
            last_rx: Instant::now(),
            timeout,
            tick,
            m_reenter,
            m_timeout,
            m_task,
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
            let reenter = self.last.elapsed();
            if reenter >= Duration::from_millis(10) {
                self.m_reenter += reenter;
            }
            if self.last_rx.elapsed() >= self.timeout {
                let elapsed = self.last_rx.elapsed();
                self.m_timeout += 1;
                return Poll::Ready(Err(protocol::Error::Timeout(elapsed)));
            }
        } else {
            self.m_task.try_flush();
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
            Poll::Ready(ret)
        }
    }
}
impl<F> Drop for Timeout<F> {
    #[inline]
    fn drop(&mut self) {
        self.m_task -= 1;
    }
}
