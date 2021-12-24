use lazy_static::*;
use psutil::process::Process;
use std::sync::RwLock;
lazy_static! {
    static ref PROCESS: RwLock<Process> =
        RwLock::new(Process::current().expect("cannot get current process"));
}
// 采集宿主机的基本信息

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
static CPU_PERCENT: AtomicUsize = AtomicUsize::new(0);
static MEMORY: AtomicUsize = AtomicUsize::new(0);

pub struct Host;

impl Host {
    #[inline]
    pub(crate) fn snapshot<W: crate::ItemWriter>(w: &mut W, _secs: f64) {
        let percent = CPU_PERCENT.load(Ordering::Relaxed) as f64 / 100.0;
        w.write("host", "cpu", "", percent as f64);
        w.write("host", "mem", "", MEMORY.load(Ordering::Relaxed) as f64);
    }
}

fn loop_refresh(cycle: Duration) {
    let mut process = Process::current().expect("cannot get current process");
    loop {
        if let Ok(percent) = process.cpu_percent() {
            CPU_PERCENT.store((percent * 100.0) as usize, Ordering::Relaxed);
        }
        if let Ok(mem) = process.memory_info() {
            MEMORY.store(mem.rss() as usize, Ordering::Relaxed);
        }
        std::thread::sleep(cycle);
    }
}

// 获取cpu等信息时，需要读取/proc文件，为避免阻塞操作，将这个task独立于runtime，放在一个独立的系统线程中运行。
pub(crate) fn start_host_refresher(cycle: Duration) {
    std::thread::spawn(move || {
        loop_refresh(cycle);
    });
}
