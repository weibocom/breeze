use super::packet::PacketBuffer;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::ready;
use tokio::time::{interval, Interval, MissedTickBehavior};

pub(crate) struct Sender {
    tick: Interval,
    packet: PacketBuffer,
    last: Instant,
}

impl Sender {
    pub(crate) fn new(addr: &str) -> Self {
        let mut tick = interval(Duration::from_secs(10));
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

        Self {
            packet: PacketBuffer::new(addr.to_string()),
            last: Instant::now(),
            tick: interval(Duration::from_secs(10)),
        }
    }
    pub fn start_sending(self) {
        tokio::spawn(async move {
            log::info!("metric-send: task started:{}", &self.packet.addr);
            self.await;
        });
    }
}

impl Future for Sender {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        ready!(me.packet.poll_flush(cx));
        loop {
            ready!(me.tick.poll_tick(cx));
            // 判断是否可以flush
            let elapsed = me.last.elapsed().as_secs_f64();
            let metrics = crate::get_metrics();
            metrics.write(&mut me.packet, elapsed);
            crate::Host::snapshot(&mut me.packet, elapsed);
            me.last = Instant::now();
            ready!(me.packet.poll_flush(cx));
        }
    }
}
unsafe impl Send for Sender {}
unsafe impl Sync for Sender {}
