#[macro_export]
macro_rules! define_metrics {
    ($($name:ident, $in:ty, $item:tt);+) => {

pub(crate) struct Snapshot {
    pub(crate) last_commit: Instant,
    $(pub(crate) $name: SnapshotItem<$item>,)+
}

impl Snapshot {
    pub fn new() -> Self {
        Self {
            last_commit: Instant::now(),
            $($name: SnapshotItem::<$item>::new(),)+
        }
    }
    #[inline]
    pub fn elapsed(&self) -> Duration {
        self.last_commit.elapsed()
    }
    $(
    #[inline(always)]
    pub fn $name(&mut self, key:&'static str, v: $in, service: usize) {
        self.$name.apply(service, key, v);
    }
    )+
    #[inline]
    pub(crate) fn take(&mut self) -> Self {
        let last = self.last_commit;
        self.last_commit = Instant::now();
        Self {
            last_commit: last,
            $($name: self.$name.take(),)+
        }
    }
    #[inline]
    pub(crate) fn reset(&mut self) {
        self.last_commit = Instant::now();
        $(self.$name.reset();)+
    }
    #[inline]
    pub(crate) fn visit_item<P:KV>(&self, secs:f64, packet:&P) {
        $(
            for (service, group) in self.$name.iter().enumerate(){
                for (key, item) in group.iter() {
                    item.with_item(
                        secs,
                        |sub_key, v| packet.kv(service, key, sub_key, v)
                        );
                }
            }
        )+
    }
}
impl std::ops::AddAssign for Snapshot{
    #[inline]
    fn add_assign(&mut self, other: Self) {
        $(
        self.$name += other.$name;
        )+
    }
}


// g下面定义Recorder
thread_local! {
    static SNAPSHOT: std::cell::RefCell<Snapshot> = std::cell::RefCell::new(Snapshot::new());
}

const COMMIT_TICK: Duration = Duration::from_secs(2);

pub struct Recorder {
    sender: tokio::sync::mpsc::Sender<Snapshot>,
}

impl Recorder {
    pub(crate) fn new(sender: tokio::sync::mpsc::Sender<Snapshot>) -> Self {
        Self { sender: sender }
    }
    $(
    #[inline]
    pub(crate) fn $name(&self, key: &'static str, v: $in, service: usize) {
        SNAPSHOT.with(|ss| {
            let mut snapshot = ss.borrow_mut();
            snapshot.$name(key, v, service);
            self.try_flush(&mut snapshot);
        });
    }
    )+
    // 每10秒钟，flush一次
    #[inline]
    fn try_flush(&self, ss: &mut Snapshot) {
        if ss.elapsed() >= COMMIT_TICK {
            let one = ss.take();
            if let Err(e) = self.sender.try_send(one) {
                log::warn!("metrics-flush: failed to send. {}", e);
            }
        }
    }
}

$(
#[inline(always)]
pub fn $name(key: &'static str, v: $in, metric_id: usize) {
    if let Some(recorder) = RECORDER.get() {
        recorder.$name(key, v, metric_id);
    }
}
)+

};} // end of define_snapshot
