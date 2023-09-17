use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use super::timeout::*;

use ds::time::{interval, Duration, Instant, Interval};
use metrics::base::*;

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
    out: Option<Result<()>>,
    closing: u32,

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
            out: None,
            refresh_tick,
            closing: 0,
        }
    }
    #[inline]
    fn poll_run(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let Self { timeout, inner, .. } = &mut *self;
        let ret = Pin::new(&mut *inner).poll(cx)?;
        ready!(timeout.poll_check(cx, inner)?);
        // 运行到这里说明：没有需要check timeout的请求

        if ret.is_pending() {
            // 只有pengding时，才尝试刷新
            loop {
                ready!(self.refresh_tick.poll_tick(cx));
                // 总是定期刷新
                let _ = self.inner.refresh()?;
            }
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
        if self.closing == 0 {
            self.out = Some(ready!(self.as_mut().poll_run(cx)));
            self.closing = 1;
        }
        // close
        while !self.inner.close() {
            if self.closing == 1 {
                // 复用原来的tick
                self.refresh_tick = interval(Duration::from_millis(50));
            }
            ready!(self.refresh_tick.poll_tick(cx));
            self.closing = self.closing.wrapping_add(1).max(2);
            // 一次tick是50ms，约1秒钟统计一次
            if self.closing % 256 == 0 {
                println!("closing=>{} {:?} {:?}", self.closing, self.inner, self.out);
                log::error!("closing=>{} {:?} {:?}", self.closing, self.inner, self.out);
                LEAKED_CONN.incr();
            }
        }
        Poll::Ready(self.out.take().unwrap())
    }
}
