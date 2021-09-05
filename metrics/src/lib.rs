#[macro_use]
extern crate lazy_static;

mod id;

mod duration;

pub use id::*;

mod ip;
pub use ip::*;

mod sender;
use sender::Sender;

mod count;
mod item;
mod kv;
mod packet;

use once_cell::sync::OnceCell;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::mpsc::channel;

pub static RECORDER: OnceCell<Recorder> = OnceCell::new();
static INITED: AtomicBool = AtomicBool::new(false);

pub fn init(addr: &str) {
    if addr.len() > 0 {
        match INITED.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => {
                let (tx, rx) = channel::<Snapshot>(256);
                let recorder = Recorder::new(tx);
                RECORDER.set(recorder).ok().expect("recorder init once");

                let send = Sender::new(rx, addr);
                send.start_sending();
            }
            Err(_) => {}
        }
    }
}

mod macros;
use count::Count;
use duration::DurationItem;
use item::*;
use kv::*;
use std::time::{Duration, Instant};
// 第一个参数是名字。
// 第二个是是metric的输入数据类型，通常是简单的数字类型
// 第三个是实现了特定接口用于处理的Item类型, 需要实现From<第二个参数>. AddAssign<Self>
// AddAssign<第二个参数> 三个接口
define_metrics!(
    count, usize, Count;
    duration, std::time::Duration, DurationItem
);
