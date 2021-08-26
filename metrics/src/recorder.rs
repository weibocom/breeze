use std::cell::RefCell;
use std::time::Duration;
use tokio::sync::mpsc::Sender;

const COMMIT_TICK: Duration = Duration::from_secs(1);

use super::Snapshot;

pub struct Recorder {
    sender: Sender<Snapshot>,
}

thread_local! {
    static SNAPSHOT: RefCell<Snapshot> = RefCell::new(Snapshot::new());
}

impl Recorder {
    pub(crate) fn new(sender: Sender<Snapshot>) -> Self {
        Self { sender: sender }
    }
    pub fn counter(&self, key: &'static str, c: usize) {
        self.counter_with_service(key, c, 0)
    }
    pub(crate) fn counter_with_service(&self, key: &'static str, c: usize, service: usize) {
        SNAPSHOT.with(|ss| {
            let mut snapshot = ss.borrow_mut();
            snapshot.count(key, c, service);
            self.try_flush(&mut snapshot);
        });
    }
    pub(crate) fn duration(&self, key: &'static str, d: Duration) {
        self.duration_with_service(key, d, 0);
    }
    pub(crate) fn duration_with_service(&self, key: &'static str, d: Duration, service: usize) {
        SNAPSHOT.with(|ss| {
            let mut snapshot = ss.borrow_mut();
            snapshot.duration(key, d, service);
            self.try_flush(&mut snapshot);
        });
    }
    // 每10秒钟，flush一次
    fn try_flush(&self, ss: &mut Snapshot) {
        if ss.elapsed() >= COMMIT_TICK {
            let one = ss.take();
            if let Err(e) = self.sender.try_send(one) {
                log::warn!("metrics-flush: failed to send. {}", e);
            }
        }
    }
}
