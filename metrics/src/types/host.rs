use psutil::process::Process;

use super::base::*;
use crate::{ItemWriter, BASE_PATH};
use ds::{time::Instant, Buffers, BUF_RX, BUF_TX};
use std::sync::atomic::{AtomicI64, Ordering::*};

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
        let uptime = self.start.elapsed().as_secs() as i64;
        w.write(BASE_PATH, "host", "uptime_sec", uptime);
        if let Ok(percent) = self.process.cpu_percent() {
            w.write(BASE_PATH, "host", "cpu", percent.min(400f32) as f64);
        }
        if let Ok(mem) = self.process.memory_info() {
            w.write(BASE_PATH, "host", "mem", mem.rss() as i64);
        }
        self.snapshot_heap(w, secs);

        let tasks = TASK_NUM.load(Relaxed);
        w.write(BASE_PATH, "task", "num", tasks);
        w.write_opts(BASE_PATH, "version", "", 1, vec![("git", self.version)]);

        let sockfile_failed = SOCKFILE_FAILED.load(Relaxed);
        w.write(BASE_PATH, "sockfile", "failed", sockfile_failed);

        self.snapshot_base(w, secs);
    }
    fn snapshot_heap<W: crate::ItemWriter>(&mut self, w: &mut W, _secs: f64) {
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
    fn snapshot_base<W: crate::ItemWriter>(&mut self, w: &mut W, secs: f64) {
        self.snapshot_buf(&BUF_TX, w, "mem_buf_tx", secs);
        self.snapshot_buf(&BUF_RX, w, "mem_buf_rx", secs);

        w.write(BASE_PATH, "leak_conn", "num", LEAKED_CONN.take());

        self.qps(w, secs, &P_W_CACHE, "poll_write_cache");
        self.qps(w, secs, &POLL_READ, "poll_read");
        self.qps(w, secs, &POLL_WRITE, "poll_write");
        self.qps(w, secs, &POLL_PENDING_R, "r_pending");
        self.qps(w, secs, &POLL_PENDING_W, "w_pending");
        self.qps(w, secs, &REENTER_10MS, "reenter10ms");

        self.qps(w, secs, &ds::CACHE_ALLOC_NUM, "heap_cache_num");
        self.qps(w, secs, &ds::CACHE_MISS_ALLOC_NUM, "heap_cache_miss_num");
    }
    pub(super) fn snapshot_buf<W: ItemWriter>(
        &mut self,
        b: &Buffers,
        w: &mut W,
        key: &str,
        secs: f64,
    ) {
        w.write(crate::BASE_PATH, key, "num", b.num.get());
        w.write(crate::BASE_PATH, key, "cnt", b.cnt.get());
        for (i, l) in b.layouts.iter().enumerate() {
            let v = l.get();
            // i == 0 对应于0字节，不统计。
            if i > 0 && v > 0 {
                let sub_key = format!("layout_{}", i);
                w.write(crate::BASE_PATH, key, &sub_key, v);
            }
        }
        self.qps(w, secs, &b.num_alloc, &(key.to_string() + "_num"));
        self.qps(w, secs, &b.bytes_alloc, &(key.to_string() + "_bytes"));
    }
    #[inline]
    fn qps<W: crate::ItemWriter>(&mut self, w: &mut W, secs: f64, m: &AtomicI64, key: &str) {
        use super::base::*;
        let v = m.take();
        if v > 0 {
            w.write(BASE_PATH, key, "qps", v as f64 / secs);
        }
    }
}

#[inline]
pub fn incr_task() {
    TASK_NUM.fetch_add(1, Relaxed);
}
#[inline]
pub fn decr_task() {
    TASK_NUM.fetch_sub(1, Relaxed);
}
#[inline]
pub fn set_sockfile_failed(failed_count: usize) {
    SOCKFILE_FAILED.store(failed_count as i64, Relaxed);
}

// fn unchange_number_metric(region_enable:bool,len_region: u16, port: &str, rsname: &str, region: &str) {
pub fn resource_num_metric(source: &str, namespace: &str, bip: &str, n: u16) {
    let path = crate::Path::new(vec![source, namespace, bip]);
    let mut metric = path.num("region_resource");
    if metric.inited() {
        metric.zero_num()
    };

    metric += n;
}
