#[macro_use]
extern crate lazy_static;

mod id;
pub use id::*;

mod ip;
pub use ip::*;

mod sender;
use sender::*;

mod register;
pub use register::*;

mod task;

pub fn start_metric_sender(addr: &str) {
    let cycle = std::time::Duration::from_secs(10);
    //types::start_host_refresher(cycle);
    //start_register_metrics();
    //let send = Sender::new(addr, cycle);
    //send.start_sending();
    let register = build_register_task();
    let sender = Sender::new(addr, cycle);
    task::start_in_dedicated_task(register, sender);
    log::info!("metric inited. item size:{}", std::mem::size_of::<Item>());
}

mod packet;

mod item;
use item::*;

mod types;
pub use types::*;
