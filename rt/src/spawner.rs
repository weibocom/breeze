use std::future::Future;
use std::pin::Pin;

use tokio::runtime::Builder;
use tokio::task::LocalSet;
// 在独立的线程运行的spawner。
// 主要用于那些定时任务。
pub struct DedicatedSpawner {
    tasks: Vec<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
}

impl DedicatedSpawner {
    pub fn new() -> Self {
        Self { tasks: Vec::new() }
    }
    pub fn spawn(&mut self, f: impl Future<Output = ()> + 'static + Send) {
        self.tasks.push(Box::pin(f));
    }
    pub fn start_on_dedicated_thread(self) {
        std::thread::spawn(move || {
            let local = LocalSet::new();
            for task in self.tasks.into_iter() {
                local.spawn_local(task);
            }
            let rt = Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(local);
        });
    }
}
