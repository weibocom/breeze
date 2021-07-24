use std::cell::RefCell;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Sender;

const COMMIT_TICK: Duration = Duration::from_secs(1);

pub struct Recorder {
    sender: Sender<Snapshot>,
}

// 耗时的metrics，记录了
// 数量，平均耗时，数量，以及不同区间的耗时。
// [0,512us], [512us, 1ms], [1ms, 2ms], [2ms,4ms], [4ms, 8ms], [8ms..]
#[derive(Clone, Debug)]
pub(crate) struct DurationItem {
    pub(crate) count: usize,
    pub(crate) elapse_us: usize,
    pub(crate) intervals: [usize; 6],
}
impl DurationItem {
    fn new() -> Self {
        Self {
            count: 0,
            elapse_us: 0,
            intervals: [0; 6],
        }
    }
    fn reset(&mut self) {
        self.count = 0;
        self.elapse_us = 0;
        unsafe { std::ptr::write_bytes(&mut self.intervals, 0, 1) };
    }
    pub(crate) fn get_interval_name(&self, idx: usize) -> &'static str {
        match idx {
            0 => "interval0",
            1 => "interval1",
            2 => "interval2",
            3 => "interval3",
            4 => "interval4",
            5 => "interval5",
            _ => "interval_overflow",
        }
    }
}
thread_local! {
    // Could add pub to make it public to whatever Foo already is public to.
    static COUNTERS: RefCell<Vec<HashMap<&'static str, usize>>> = RefCell::new(Vec::new());
    static DURATIONS: RefCell<Vec<HashMap<&'static str, DurationItem>>> = RefCell::new(Vec::new());
    static LAST_COMMIT: RefCell<Instant> = RefCell::new(Instant::now());
}

impl Recorder {
    pub(crate) fn new(sender: Sender<Snapshot>) -> Self {
        Self { sender: sender }
    }
    pub fn counter(&self, key: &'static str, c: usize) {
        self.counter_with_service(key, c, 0)
    }
    pub(crate) fn counter_with_service(&self, key: &'static str, c: usize, service: usize) {
        COUNTERS.with(|l| {
            let mut data = l.borrow_mut();
            if data.len() <= service {
                let res = service - data.len();
                data.reserve(res);
                for _ in 0..=res {
                    data.push(HashMap::with_capacity(16));
                }
            }
            let one_data = unsafe { data.get_unchecked_mut(service) };
            if let Some(counter) = one_data.get_mut(key) {
                *counter += c;
            } else {
                one_data.insert(key, c);
            }
        });
        self.try_flush();
    }
    pub(crate) fn duration(&self, key: &'static str, d: Duration) {
        self.duration_with_service(key, d, 0);
    }
    pub(crate) fn duration_with_service(&self, key: &'static str, d: Duration, service: usize) {
        DURATIONS.with(|l| {
            let mut data = l.borrow_mut();
            if data.len() <= service {
                let res = service - data.len();
                data.reserve(res);
                for _ in 0..=res {
                    data.push(HashMap::with_capacity(16));
                }
            }
            let one_data = unsafe { data.get_unchecked_mut(service) };
            let us = d.as_micros() as usize;
            // 计算us在哪个interval
            // [0,256], [256,512], [512,1024],[1024,2048], [2048, 4096], [4096...],
            let max = INTERVAL_IDX.len() - 1;
            let idx = INTERVAL_IDX[(us / 256).min(max)] as usize;
            if let Some(item) = one_data.get_mut(key) {
                item.count += 1;
                item.elapse_us += us;
                item.intervals[idx] += 1;
            } else {
                let mut item = DurationItem::new();
                item.count = 1;
                item.elapse_us = us;
                item.intervals[idx] += 1;
                one_data.insert(key, item);
            }
        });
        self.try_flush();
    }
    // 每10秒钟，flush一次
    fn try_flush(&self) {
        LAST_COMMIT.with(|l| {
            let elapsed = l.borrow().elapsed();
            log::debug!("metrics-flush. elapsed:{:?}", elapsed);
            if elapsed >= COMMIT_TICK {
                *l.borrow_mut() = Instant::now();

                let ss = Snapshot::from_threadlocal();
                log::debug!("metrics-flushing: {:?}", ss);
                if let Err(e) = self.sender.try_send(ss) {
                    log::warn!("metrics-flush: failed to send. {:?}", e);
                }
            }
        })
    }
}

static INTERVAL_IDX: [u8; 16] = [0, 1, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4];
#[derive(Default, Debug)]
pub(crate) struct Snapshot {
    pub(crate) counters: Vec<HashMap<&'static str, usize>>,
    pub(crate) durations: Vec<HashMap<&'static str, DurationItem>>,
}

impl Snapshot {
    fn from_threadlocal() -> Self {
        let counters = COUNTERS.with(|f| {
            let mut data = f.borrow_mut();
            let c = data.clone();
            Self::clear_counters(&mut data);
            c
        });
        let durations = DURATIONS.with(|f| {
            let mut data = f.borrow_mut();
            let c = data.clone();
            Self::clear_durations(&mut data);
            c
        });
        Self {
            counters: counters,
            durations: durations,
        }
    }
    fn clear_durations(data: &mut Vec<HashMap<&'static str, DurationItem>>) {
        for grp in data.iter_mut() {
            for (_, item) in grp {
                item.reset();
            }
        }
    }

    fn clear_counters(data: &mut Vec<HashMap<&'static str, usize>>) {
        for grp in data.iter_mut() {
            for (_, v) in grp.iter_mut() {
                *v = 0;
            }
        }
    }

    pub(crate) fn reset(&mut self) {
        Self::clear_counters(&mut self.counters);
        Self::clear_durations(&mut self.durations);
    }

    pub(crate) fn merge(&mut self, other: &Self) {
        // 合并
        for i in 0..self.counters.len().min(other.counters.len()) {
            let me = &mut self.counters[i];
            for (k, v) in other.counters[i].iter() {
                if let Some(mv) = me.get_mut(k) {
                    *mv += v;
                } else {
                    me.insert(k, *v);
                }
            }
        }
        // 新增
        for i in self.counters.len()..other.counters.len() {
            self.counters.push(other.counters[i].clone());
        }

        // 处理duration
        for i in 0..self.durations.len().min(other.durations.len()) {
            let me = &mut self.durations[i];

            for (k, item) in other.durations[i].iter() {
                if let Some(mut mv) = me.get_mut(k) {
                    mv.count += item.count;
                    mv.elapse_us += item.elapse_us;
                    for i in 0..mv.intervals.len() {
                        mv.intervals[i] += item.intervals[i];
                    }
                } else {
                    me.insert(k, item.clone());
                }
            }
        }
        for i in self.durations.len()..other.durations.len() {
            self.durations.push(other.durations[i].clone());
        }
    }
}
