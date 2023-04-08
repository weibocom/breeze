use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use super::timeout::*;

use ds::time::{Duration, Instant};
use metrics::base::*;

use tokio::{
    io::{AsyncRead, AsyncWrite},
    time::{interval, MissedTickBehavior},
};

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
#[derive(Debug)]
pub struct Entry<F, T> {
    inner: F,
    out: Option<Result<()>>,
    closing: u16,
    refresh_id: u32,
    refresh_cycle: u8,

    timeout: T,
}
impl<T: TimeoutCheck + Sized + Unpin, F: Future<Output = Result<()>> + Unpin + ReEnter + Debug>
    Entry<F, T>
{
    #[inline]
    pub fn timeout(f: F, timeout: T) -> Self {
        let mut refresh_tick = interval(Duration::from_secs(30));
        refresh_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        Self {
            inner: f,
            timeout,
            out: None,
            closing: 0,
            refresh_id: u32::MAX,
            refresh_cycle: 0,
        }
    }
    #[inline(always)]
    fn poll_run(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.refresh_id == u32::MAX {
            self.refresh_id = crate::interval::register(cx.waker().clone());
        }
        let Self {
            timeout,
            inner,
            refresh_cycle,
            ..
        } = &mut *self;
        let ret = Pin::new(&mut *inner).poll(cx)?;
        ready!(timeout.poll_check(cx, inner)?);
        // 运行到这里说明：没有需要check timeout的请求

        let cycle = super::interval::refresh_cycle();
        if *refresh_cycle != cycle as u8 {
            *refresh_cycle = cycle as u8;
            let _goon = inner.refresh()?;
            log::debug!("refreshed:{:?}", self);
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
        if !self.inner.close() {
            if self.closing == 1 {
                crate::interval::unregistering(self.refresh_id);
            }
            // max(1) 避免closing为0，又被重入到poll_run
            self.closing = self.closing.wrapping_add(1).max(1);
            // 一次tick是200ms，10秒钟统计一次
            if self.closing % crate::interval::INTERVALS_PER_SEC as u16 == 0 {
                println!("closing=>{} {:?} {:?}", self.closing, self.inner, self.out);
                log::error!("closing=>{} {:?} {:?}", self.closing, self.inner, self.out);
                LEAKED_CONN.incr();
            }
            return Poll::Pending;
        }
        crate::interval::unregister(self.refresh_id);
        Poll::Ready(self.out.take().unwrap())
    }
}
