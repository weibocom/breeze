use psutil::process::Process;

use crate::BASE_PATH;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
use std::time::Instant;

static CPU_PERCENT: AtomicUsize = AtomicUsize::new(0);
static MEMORY: AtomicUsize = AtomicUsize::new(0);

static TASK_NUM: AtomicIsize = AtomicIsize::new(0);
static SOCKFILE_FAILED: AtomicIsize = AtomicIsize::new(0);

pub struct Host {
    start: Instant,
    process: Process,
    version: &'static str,
}

impl Host {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            start: Instant::now(),
            process: Process::current().expect("cannot get current process"),
            version: context::get_short_version(),
        }
    }
    #[inline]
    pub(crate) fn snapshot<W: crate::ItemWriter>(&mut self, w: &mut W, _secs: f64) {
        self.refresh();
        let percent = CPU_PERCENT.load(Ordering::Relaxed) as f64 / 100.0;
        w.write(BASE_PATH, "host", "cpu", percent as f64);
        w.write(
            BASE_PATH,
            "host",
            "mem",
            MEMORY.load(Ordering::Relaxed) as f64,
        );

        let tasks = TASK_NUM.load(Ordering::Relaxed) as f64;
        w.write(BASE_PATH, "task", "num", tasks);
        w.write(BASE_PATH, "version", self.version, 1.0);
        w.write(
            BASE_PATH,
            "host",
            "uptime_sec",
            self.start.elapsed().as_secs() as f64,
        );

        let sockfile_failed = SOCKFILE_FAILED.load(Ordering::Relaxed) as f64;
        w.write(BASE_PATH, "sockfile", "failed", sockfile_failed);
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
#[inline]
pub fn set_sockfile_failed(failed_count: usize) {
    SOCKFILE_FAILED.store(failed_count as isize, Ordering::Relaxed);
}
