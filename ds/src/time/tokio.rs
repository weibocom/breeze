use crate::time::Duration;
use std::future::Future;
pub use tokio::time::Interval;

pub fn interval(duration: Duration) -> Interval {
    let mut interval = tokio::time::interval(duration.into());
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval
}

pub fn timeout<F>(duration: Duration, future: F) -> tokio::time::Timeout<F>
where
    F: Future,
{
    tokio::time::timeout(duration.into(), future)
}

pub fn sleep(duration: Duration) -> tokio::time::Sleep {
    tokio::time::sleep(duration.into())
}
