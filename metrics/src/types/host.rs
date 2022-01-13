use psutil::process::Process;

use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
static CPU_PERCENT: AtomicUsize = AtomicUsize::new(0);
static MEMORY: AtomicUsize = AtomicUsize::new(0);

static TASK_NUM: AtomicIsize = AtomicIsize::new(0);

pub struct Host {
    process: Process,
}

impl Host {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            process: Process::current().expect("cannot get current process"),
        }
    }
    #[inline]
    pub(crate) fn snapshot<W: crate::ItemWriter>(&mut self, w: &mut W, _secs: f64) {
        self.refresh();
        let percent = CPU_PERCENT.load(Ordering::Relaxed) as f64 / 100.0;
        w.write("mesh", "host", "cpu", percent as f64);
        w.write("mesh", "host", "mem", MEMORY.load(Ordering::Relaxed) as f64);

        let tasks = TASK_NUM.load(Ordering::Relaxed) as f64;
        w.write("mesh", "task", "num", tasks);
    }
    pub(crate) fn refresh(&mut self) {
        if let Ok(percent) = self.process.cpu_percent() {
            CPU_PERCENT.store((percent * 100.0) as usize, Ordering::Relaxed);
        }
        if let Ok(mem) = self.process.memory_info() {
            MEMORY.store(mem.rss() as usize, Ordering::Relaxed);
        }
    }
}

#[inline]
pub fn incr_task() {
    TASK_NUM.fetch_add(1, Ordering::Relaxed);
}
#[inline]
pub fn decr_task() {
    TASK_NUM.fetch_sub(1, Ordering::Relaxed);
}
