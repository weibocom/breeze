#[macro_export]
macro_rules! define_metrics {
    ($($name:ident, $in:ty, $item:tt);+) => {

pub(crate) struct Snapshot {
    $(pub(crate) $name: SnapshotItem<$item>,)+
}

impl Default for Snapshot {
    fn default() -> Self {
        Self::new()
    }
}

impl Snapshot {
    pub fn new() -> Self {
        Self {
            $($name: SnapshotItem::<$item>::new(),)+
        }
    }
    $(
    #[inline(always)]
    pub fn $name(&mut self, key:&'static str, v: $in, service: usize) {
        self.$name.apply(service, key, v);
    }
    )+
    #[inline]
    pub(crate) fn take(&mut self) -> Self {
        Self {
            $($name: self.$name.take(),)+
        }
    }
    #[inline]
    pub(crate) fn reset(&mut self) {
        $(self.$name.reset();)+
    }
    #[inline]
    pub(crate) fn visit_item<P:KV>(&mut self, secs:f64, packet:&P) {
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


struct SnapshotTicker {
    tick: Instant,
    inner: Snapshot,
}

impl SnapshotTicker {
    fn new() -> Self {
        Self {
            tick:Instant::now(),
            inner: Snapshot::new(),
        }
    }
    #[inline]
    pub fn elapsed(&self) -> Duration {
        self.tick.elapsed()
    }
}

impl std::ops::Deref for SnapshotTicker {
    type Target = Snapshot;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for SnapshotTicker {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

// g下面定义Recorder
thread_local! {
    static SNAPSHOT: std::cell::RefCell<SnapshotTicker> = std::cell::RefCell::new(SnapshotTicker::new());
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
    fn try_flush(&self, ss: &mut SnapshotTicker) {
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
