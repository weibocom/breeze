use std::task::{ready, Context, Poll};

use tokio::time::{interval, Interval, MissedTickBehavior};

use ds::time::Duration;

use super::entry::ReEnter;

pub trait TimeoutCheck {
    fn poll_check<T: ReEnter>(
        &mut self,
        ctx: &mut Context<'_>,
        to: &T,
    ) -> Poll<Result<(), Duration>>;
}

pub struct Timeout {
    timeout: Duration,
    tick: Interval,
}

impl TimeoutCheck for Timeout {
    // now作为参数传入，是为了降低一次Instant::now的请求
    #[inline]
    fn poll_check<T: ReEnter>(
        &mut self,
        cx: &mut Context<'_>,
        to: &T,
    ) -> Poll<Result<(), Duration>> {
        if let Some(last) = to.last() {
            let elapsed = last.elapsed();
            if elapsed >= self.timeout {
                return Poll::Ready(Err(elapsed));
            }
            ready!(self.tick.poll_tick(cx));
            self.tick.reset();
            ready!(self.tick.poll_tick(cx));
            panic!("never should run here");
        }
        Poll::Ready(Ok(()))
    }
}

impl From<Duration> for Timeout {
    #[inline]
    fn from(timeout: Duration) -> Self {
        let mut tick = interval(timeout.max(Duration::from_millis(50)));
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        Self { timeout, tick }
    }
}

pub struct DisableTimeout;

impl TimeoutCheck for DisableTimeout {
    #[inline(always)]
    fn poll_check<T: ReEnter>(
        &mut self,
        _ctx: &mut Context<'_>,
        _to: &T,
    ) -> Poll<Result<(), Duration>> {
        Poll::Ready(Ok(()))
    }
}
