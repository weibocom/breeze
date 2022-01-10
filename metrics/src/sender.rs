use super::packet::PacketBuffer;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::ready;
use tokio::time::{interval, Interval, MissedTickBehavior};

pub struct Sender {
    tick: Interval,
    packet: PacketBuffer,
    last: Instant,
    host: super::Host,
}

impl Sender {
    pub fn new(addr: &str, cycle: Duration) -> Self {
        let mut tick = interval(cycle);
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

        log::info!("task started ==> metric sender");

        Self {
            packet: PacketBuffer::new(addr.to_string()),
            last: Instant::now(),
            tick: interval(cycle),
            host: super::Host::new(),
        }
    }
}

// 这个Future包含了大量的cpu、io等重型操作。需要将该future放在spawn_local中操作。
impl Future for Sender {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        ready!(me.packet.poll_flush(cx));
        loop {
            ready!(me.tick.poll_tick(cx));
            // 判断是否可以flush
            let elapsed = me.last.elapsed().as_secs_f64().round();
            if elapsed as usize > 0 {
                // 这是一个block 操作，
                let metrics = crate::get_metrics();
                metrics.write(&mut me.packet, elapsed);
                // 写入task的总的数量
                crate::types::tasks::snapshot(&mut me.packet, elapsed);
                me.host.snapshot(&mut me.packet, elapsed);
                me.last = Instant::now();
            }
            ready!(me.packet.poll_flush(cx));
        }
    }
}
unsafe impl Send for Sender {}
unsafe impl Sync for Sender {}
