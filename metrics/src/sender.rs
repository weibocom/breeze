use super::{packet::PacketBuffer, Snapshot};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use tokio::sync::mpsc::Receiver;
use tokio::time::{interval, Interval};

use futures::ready;

pub(crate) struct Sender {
    last: Instant,
    tick: Interval,
    rx: Receiver<Snapshot>,
    packet: PacketBuffer,
    snapshot: Snapshot,
}

impl Sender {
    pub(crate) fn new(rx: Receiver<Snapshot>, addr: &str) -> Self {
        Self {
            packet: PacketBuffer::new(addr.to_string()),
            rx: rx,
            last: Instant::now(),
            tick: interval(Duration::from_secs(1)),
            snapshot: Snapshot::new(),
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
        let mut me = &mut *self;
        ready!(me.packet.poll_flush(cx));
        loop {
            let mut pending = false;
            match me.rx.poll_recv(cx) {
                Poll::Ready(None) => {
                    break;
                }
                Poll::Ready(Some(s)) => {
                    me.snapshot += s;
                }
                Poll::Pending => {
                    pending = true;
                }
            };
            // 判断是否可以flush
            let elapsed = me.last.elapsed();
            static DURATION: Duration = Duration::from_secs(10);
            if elapsed >= DURATION {
                me.last = Instant::now();
                // 写入机器信息
                me.snapshot.host("host", crate::Host::default(), 0);
                me.snapshot
                    .visit_item(elapsed.as_secs_f64(), &mut me.packet);
                me.snapshot.reset();
                ready!(me.packet.poll_flush(cx));
                continue;
            }
            // sleep 1s
            if pending {
                ready!(me.tick.poll_tick(cx));
            }
        }
        log::warn!("sender task complete");
        Poll::Ready(())
    }
}
unsafe impl Send for Sender {}
unsafe impl Sync for Sender {}
