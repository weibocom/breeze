use std::task::Waker;
use std::time::Duration;

//use tokio::time::{interval, Interval};

pub(super) struct TimeoutEvent {
    to: Duration,
}

//struct TimeoutEventSubscriber {
//    events: (),
//}

pub(super) fn subscribe_timeout_event(to: Duration) -> TimeoutEvent {
    TimeoutEvent { to }
}

impl TimeoutEvent {
    #[inline]
    pub fn pause(&mut self) {}
    #[inline]
    pub fn refresh(&mut self) {}
    #[inline]
    pub fn stop(&mut self) {}
    #[inline]
    pub fn start(&mut self, _wakeup: Waker) {}
    #[inline]
    pub fn is_timeout(&self) -> bool {
        false
    }
    #[inline]
    pub fn elapsed(&self) -> Duration {
        self.to
    }
}
