use std::task::{ready, Context, Poll};

use ds::time::{interval, Duration, Interval};

use super::entry::ReEnter;

pub trait TimeoutCheck {
    fn poll_check<T: ReEnter>(&mut self, ctx: &mut Context<'_>, to: &T) -> Poll<Result<(), u64>>;
}

pub struct Timeout {
    timeout_ms: u16,
    tick: Interval,
}

impl TimeoutCheck for Timeout {
    // now作为参数传入，是为了降低一次Instant::now的请求
    #[inline]
    fn poll_check<T: ReEnter>(&mut self, cx: &mut Context<'_>, to: &T) -> Poll<Result<(), u64>> {
        if let Some(last) = to.last() {
            let elapsed = last.elapsed().as_millis() as u64;
            if elapsed >= self.timeout_ms as u64 {
                return Poll::Ready(Err(elapsed));
            }
            loop {
                ready!(self.tick.poll_tick(cx));
            }
        }
        Poll::Ready(Ok(()))
    }
}

impl From<u16> for Timeout {
    #[inline]
    fn from(timeout_ms: u16) -> Self {
        let tick = interval(Duration::from_millis(timeout_ms.max(50) as u64));

        Self { timeout_ms, tick }
    }
}

pub struct DisableTimeout;

impl TimeoutCheck for DisableTimeout {
    #[inline(always)]
    fn poll_check<T: ReEnter>(&mut self, _ctx: &mut Context<'_>, _to: &T) -> Poll<Result<(), u64>> {
        Poll::Ready(Ok(()))
    }
}
