use lazy_static::*;
use psutil::process::Process;
use std::sync::RwLock;
lazy_static! {
    static ref PROCESS: RwLock<Process> =
        RwLock::new(Process::current().expect("cannot get current process"));
}
// 采集宿主机的基本信息

pub struct Host;

impl Host {
    #[inline]
    pub(crate) fn snapshot<W: crate::ItemWriter>(w: &mut W, _secs: f64) {
        if let Ok(mut p) = PROCESS.try_write() {
            // cpu消耗
            if let Ok(percent) = p.cpu_percent() {
                w.write("host", "cpu", "", percent as f64);
            }
            // 内存占用
            if let Ok(mem) = p.memory_info() {
                w.write("host", "mem", "", mem.rss() as f64);
            }
        }
    }
}
