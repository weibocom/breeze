use psutil::process::Process;

use crate::BASE_PATH;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::Instant;

static CPU_PERCENT: AtomicUsize = AtomicUsize::new(0);
static MEMORY: AtomicI64 = AtomicI64::new(0);

static TASK_NUM: AtomicI64 = AtomicI64::new(0);
static SOCKFILE_FAILED: AtomicI64 = AtomicI64::new(0);

pub struct Host {
    heap: Option<ds::HeapStats>, // 累积分配的堆内存
    start: Instant,
    process: Process,
    version: &'static str,
}

impl Host {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            heap: None,
            start: Instant::now(),
            process: Process::current().expect("cannot get current process"),
            version: &context::get().version,
        }
    }
    #[inline]
    pub(crate) fn snapshot<W: crate::ItemWriter>(&mut self, w: &mut W, secs: f64) {
        self.refresh();
        let uptime = self.start.elapsed().as_secs() as i64;
        w.write(BASE_PATH, "host", "uptime_sec", uptime);
        let percent = CPU_PERCENT.load(Ordering::Relaxed) as f64 / 100.0;
        w.write(BASE_PATH, "host", "cpu", percent);
        w.write(BASE_PATH, "host", "mem", MEMORY.load(Ordering::Relaxed));
        self.snapshot_heap(w, secs);

        let tasks = TASK_NUM.load(Ordering::Relaxed);
        w.write(BASE_PATH, "task", "num", tasks);
        w.write_opts(BASE_PATH, "version", "", 1, vec![("git", self.version)]);

        let sockfile_failed = SOCKFILE_FAILED.load(Ordering::Relaxed);
        w.write(BASE_PATH, "sockfile", "failed", sockfile_failed);
    }
    pub(crate) fn snapshot_heap<W: crate::ItemWriter>(&mut self, w: &mut W, _secs: f64) {
        if let Some(heap_stats) = ds::heap() {
            // 已使用堆内存
            w.write(BASE_PATH, "host", "heap", heap_stats.used as i64);
            // 已分配的对象的数量
            w.write(BASE_PATH, "host", "heap_o", heap_stats.used_objects as i64);
            if let Some(prev) = self.heap.take() {
                // 堆内存分配速率
                let bps = ((heap_stats.total - prev.total) as f64 / _secs) as i64;
                w.write(BASE_PATH, "host", "heap_bps", bps);
                // 堆分配对象速率
                let ops = ((heap_stats.total_objects - prev.total_objects) as f64 / _secs) as i64;
                w.write(BASE_PATH, "host", "heap_ops", ops);
            }
            self.heap = Some(heap_stats);
        }
    }
    pub(crate) fn refresh(&mut self) {
        if let Ok(percent) = self.process.cpu_percent() {
            CPU_PERCENT.store((percent * 100.0) as usize, Ordering::Relaxed);
        }
        if let Ok(mem) = self.process.memory_info() {
            MEMORY.store(mem.rss() as i64, Ordering::Relaxed);
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
    SOCKFILE_FAILED.store(failed_count as i64, Ordering::Relaxed);
}
