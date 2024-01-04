use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use super::timeout::*;

use ds::time::{interval, Duration, Instant, Interval};

use tokio::io::{AsyncRead, AsyncWrite};

pub trait ReEnter {
    #[inline]
    fn last(&self) -> Option<Instant> {
        None
    }
    // 定期会调用，通常用来清理内存，更新数据等信息。
    // 返回true: 表示期待进行下一次调用
    // 返回false: 表示资源已经释放，不再需要调用。
    fn refresh(&mut self) -> Result<bool>;
    // 在Future.poll返回前执行。
    // 可能会多次执行，直到close返回true。
    // true: 成功关闭，释放相关资源
    // false: 还有资源未释放
    fn close(&mut self) -> bool;
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
pub struct Entry<F, T> {
    inner: F,
    refresh_tick: Interval,
    status: Status,
    timeout: T,
}
impl<T: TimeoutCheck + Sized + Unpin, F: Future<Output = Result<()>> + Unpin + ReEnter + Debug>
    Entry<F, T>
{
    #[inline]
    pub fn timeout(f: F, timeout: T) -> Self {
        let refresh_tick = interval(Duration::from_secs(30));

        Self {
            inner: f,
            timeout,
            status: Status { closing: 0 },
            refresh_tick,
        }
    }
    #[inline]
    fn poll_run(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let Self { timeout, inner, .. } = &mut *self;
        let ret = Pin::new(&mut *inner).poll(cx)?;
        ready!(timeout.poll_check(cx, inner)?);
        // 运行到这里说明：没有需要check timeout的请求

        // 只要当前poll进入pending，就会触发持续refresh
        while ret.is_pending() {
            ready!(self.refresh_tick.poll_tick(cx));
            let _ = self.inner.refresh()?;
        }
        ret.map(|r| Ok(r))
    }
}

use protocol::Result;
impl<T: TimeoutCheck + Unpin, F: Future<Output = Result<()>> + ReEnter + Debug + Unpin> Future
    for Entry<F, T>
{
    type Output = F::Output;
    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.status.running() {
            let out = ready!(self.as_mut().poll_run(cx));
            self.status.close(out);
        }
        // close
        while !self.inner.close() {
            self.refresh_tick.reset_after(Duration::from_millis(50));
            ready!(self.refresh_tick.poll_tick(cx));
        }
        Poll::Ready(self.status.take_out())
    }
}

// 运行过程中使用closing判断running
// 运行结束后使用out存储结果。
union Status {
    closing: usize,
    out: *mut Result<()>,
}
impl Status {
    #[inline(always)]
    fn running(&self) -> bool {
        unsafe { self.closing == 0 }
    }
    #[inline(always)]
    fn close(&mut self, out: Result<()>) {
        let out = Box::leak(Box::new(out));
        self.out = out;
        assert!(!self.running());
    }
    #[inline(always)]
    fn take_out(&mut self) -> Result<()> {
        unsafe { *Box::from_raw(self.out) }
    }
}
unsafe impl Send for Status {}
unsafe impl Sync for Status {}
