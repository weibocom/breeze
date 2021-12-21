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

pub fn start_metric_sender(addr: &str) {
    start_register_metrics();
    let send = Sender::new(addr);
    send.start_sending();
    log::info!("metric inited. item size:{}", std::mem::size_of::<Item>());
}

mod packet;

mod item;
use item::*;

mod types;
pub use types::*;
