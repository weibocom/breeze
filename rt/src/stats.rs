use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

pub trait ReEnter {
    // 重入之后是否有数据需要处理。
    fn need_process(&self) -> bool;
}
//  统计
//  1. 每次poll的执行耗时
//  2. 重入耗时间隔
pub struct StatsFuture<F> {
    last: Instant,
    inner: F,
}
impl<F: Future + Unpin + ReEnter + Debug> From<F> for StatsFuture<F> {
    #[inline]
    fn from(f: F) -> Self {
        Self {
            inner: f,
            last: Instant::now(),
        }
    }
}

impl<F: Future + ReEnter + Debug + Unpin> Future for StatsFuture<F> {
    type Output = F::Output;
    #[inline(always)]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.inner.need_process() {
            let elapsed = self.last.elapsed();
            if elapsed >= Duration::from_millis(20) {
                log::info!("reenter schedule elapsed:{:?} => {:?}", elapsed, self.inner);
            }
        }
        // 统计一次poll需要的时间
        let start = Instant::now();
        let ret = Pin::new(&mut self.inner).poll(cx);
        if start.elapsed() >= Duration::from_millis(10) {
            log::info!("poll elapsed:{:?} => {:?}", start.elapsed(), self.inner);
        }

        self.last = Instant::now();

        ret
    }
}
