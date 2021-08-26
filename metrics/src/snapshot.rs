use super::duration::*;
use std::collections::HashMap;
use std::time::{Duration, Instant};

pub(crate) struct Snapshot {
    pub(crate) last_commit: Instant,
    pub(crate) counters: SnapshotItem<usize>,
    pub(crate) durations: SnapshotItem<DurationItem>,
}

impl Snapshot {
    pub fn new() -> Self {
        Self {
            last_commit: Instant::now(),
            counters: SnapshotItem::new(),
            durations: SnapshotItem::new(),
        }
    }
    #[inline]
    pub fn elapsed(&self) -> Duration {
        self.last_commit.elapsed()
    }
    #[inline(always)]
    pub fn count(&mut self, key: &'static str, c: usize, service: usize) {
        self.counters.apply(service, key, c);
    }
    #[inline(always)]
    pub fn duration(&mut self, key: &'static str, d: Duration, service: usize) {
        self.durations.apply(service, key, d);
    }
    pub(crate) fn take(&mut self) -> Self {
        //log::info!("metrics taken. thread id:{}", thread_id::get());
        let counters = std::mem::take(&mut self.counters);
        let durations = std::mem::take(&mut self.durations);
        let last = self.last_commit;
        self.last_commit = Instant::now();
        Self {
            last_commit: last,
            counters: counters,
            durations: durations,
        }
    }
    pub(crate) fn reset(&mut self) {
        self.last_commit = Instant::now();
        let _counters = std::mem::take(&mut self.counters);
        let _durations = std::mem::take(&mut self.durations);
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.counters += other.counters;
        self.durations += other.durations;
    }
}

pub(crate) struct SnapshotItem<E> {
    pub(crate) inner: Vec<HashMap<&'static str, E>>,
}

impl<E> SnapshotItem<E> {
    pub fn new() -> Self {
        Self {
            inner: Vec::with_capacity(32),
        }
    }
    pub fn apply<V>(&mut self, pos: usize, key: &'static str, val: V)
    where
        E: AddAssign<V>,
        V: Into<E>,
    {
        for _ in self.inner.len()..=pos {
            self.inner.push(HashMap::with_capacity(16));
        }
        let ele = unsafe { self.inner.get_unchecked_mut(pos) };
        if let Some(e_val) = ele.get_mut(key) {
            *e_val += val;
        } else {
            let new = val.into();
            ele.insert(key, new);
        }
    }
}

use std::ops::AddAssign;
impl<E> AddAssign for SnapshotItem<E>
where
    E: AddAssign,
{
    fn add_assign(&mut self, other: Self) {
        for (i, group) in other.inner.into_iter().enumerate() {
            for (k, v) in group {
                self.apply(i, k, v);
            }
        }
    }
}

impl<E> Default for SnapshotItem<E> {
    fn default() -> Self {
        Self::new()
    }
}
