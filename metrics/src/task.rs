use tokio::runtime::Builder;
use tokio::task::LocalSet;

use crate::{MetricRegister, Sender};
// metrics统计在数量过多时，会存在重型的cpu操作。因此，将相关的操作放在独立的runtime中运行。
// 运行在一个专用的线程中。
pub(crate) fn start_in_dedicated_task(register: MetricRegister, sender: Sender) {
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    std::thread::spawn(move || {
        log::info!("metric task started in a dedicated thread");
        let local = LocalSet::new();
        local.spawn_local(register);
        local.spawn_local(sender);
        rt.block_on(local);
    });
}
