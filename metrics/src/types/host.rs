use lazy_static::*;
use psutil::process::Process;
use std::sync::RwLock;
lazy_static! {
    static ref PROCESS: RwLock<Process> =
        RwLock::new(Process::current().expect("cannot get current process"));
}
// 采集宿主机的基本信息

#[derive(Clone, Default, Debug)]
pub struct Host;

use std::ops::AddAssign;
impl AddAssign for Host {
    #[inline(always)]
    fn add_assign(&mut self, _other: Host) {}
}

impl crate::kv::KvItem for Host {
    #[inline]
    fn with_item<F: Fn(&'static str, f64)>(&self, _secs: f64, f: F) {
        if let Ok(mut p) = PROCESS.try_write() {
            // cpu消耗
            if let Ok(percent) = p.cpu_percent() {
                f("cpu", percent as f64);
            }
            // 内存占用
            if let Ok(mem) = p.memory_info() {
                f("mem", mem.rss() as f64);
            }
        }
    }
    #[inline]
    fn clear() -> bool {
        false
    }
}
