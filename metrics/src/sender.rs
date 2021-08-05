use super::Snapshot;
use std::io::{Result, Write};
use std::time::{Duration, Instant};

use tokio::net::UdpSocket;
use tokio::sync::mpsc::Receiver;
use tokio::time::{interval, Interval};

use futures::ready;

pub(crate) struct Sender {
    rx: Receiver<Snapshot>,
    addr: String,
    socket: Option<UdpSocket>,
    buff: Vec<u8>,
    last: Instant,
    snapshot: Option<Snapshot>,
    tick: Interval,
}

impl Sender {
    pub(crate) fn new(rx: Receiver<Snapshot>, addr: &str) -> Self {
        Self {
            rx: rx,
            addr: addr.to_string(),
            socket: None,
            buff: Vec::with_capacity(512),
            last: Instant::now(),
            tick: interval(Duration::from_secs(1)),
            snapshot: Some(Snapshot::default()),
        }
    }
    pub fn start_sending(self) {
        tokio::spawn(async move {
            log::info!("metric-send: task started:{}", self.addr);
            self.await;
        });
    }
    fn clear(&mut self) {
        unsafe {
            self.buff.set_len(0);
        }
    }
    fn poll_flush(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        if let Some(ref sock) = self.socket {
            ready!(sock.poll_send(cx, &self.buff))?;
        }
        self.clear();
        Poll::Ready(Ok(()))
    }
    fn get_service_name(&self, sid: usize) -> String {
        crate::get_name(sid)
    }
    fn poll_send_packet(
        &mut self,
        cx: &mut Context,
        sid: usize,
        key: &'static str,
        sub_key: &'static str,
        v: f64,
    ) -> Poll<Result<()>> {
        self.build_packet(sid, key, sub_key, v);
        self.poll_flush(cx)
    }
    fn build_packet(&mut self, sid: usize, key: &'static str, sub_key: &'static str, v: f64) {
        let v = (v * 100f64) as isize as f64 / 100f64;
        self.write("breeze.");
        let service = self.get_service_name(sid).to_string();
        self.write(&service);
        self.write(".byhost.");
        self.write(&super::ip::local_ip());
        self.write(".");
        self.write(key);
        if sub_key.len() > 0 {
            self.write(".");
            self.write(sub_key);
        }
        self.write(":");
        self.write(&v.to_string());
        self.write("|kv\n");
    }
    fn write(&mut self, s: &str) {
        self.buff.write(s.as_bytes()).unwrap();
    }

    fn check_connect(&mut self) -> Result<()> {
        if self.socket.is_none() {
            let sock = std::net::UdpSocket::bind("0.0.0.0:34254")?;
            sock.connect(&self.addr)?;
            self.socket = Some(UdpSocket::from_std(sock)?);
        }
        Ok(())
    }
    fn reconnect(&mut self) {
        self.socket.take();
        //self.check_connect()
    }
    // 不保证snapshot成功发送。一旦出现pending，数据会丢失
    fn poll_send(&mut self, cx: &mut Context) {
        let mut snapshot = self.snapshot.take().expect("metrics snapshot");
        match self._poll_send(cx, &snapshot) {
            Poll::Ready(Err(_e)) => {
                log::debug!("metrics-send: error:{:?}", _e);
                self.reconnect();
            }
            _ => {}
        };
        self.clear();
        snapshot.reset();
        self.snapshot = Some(snapshot);
    }
    fn _poll_send(&mut self, cx: &mut Context, ss: &Snapshot) -> Poll<Result<()>> {
        let secs = self.last.elapsed().as_secs_f64();
        self.check_connect()?;
        // 写counter
        for (sid, group) in ss.counters.iter().enumerate() {
            for (key, counter) in group.iter() {
                ready!(self.poll_send_packet(cx, sid, key, "", *counter as f64 / secs))?;
            }
        }

        for (sid, group) in ss.durations.iter().enumerate() {
            for (key, item) in group.iter() {
                // 平均耗时
                let avg_us = if item.count == 0 {
                    0f64
                } else {
                    item.elapse_us as f64 / item.count as f64
                };
                ready!(self.poll_send_packet(cx, sid, key, "avg_us", avg_us as f64))?;
                // 总的qps
                let qps = item.count as f64 / secs;
                ready!(self.poll_send_packet(cx, sid, key, "qps", qps))?;
                for i in 0..item.intervals.len() {
                    let sub_key = item.get_interval_name(i);
                    let interval_qps = item.intervals[i] as f64 / secs;
                    ready!(self.poll_send_packet(cx, sid, key, sub_key, interval_qps))?;
                }
            }
        }
        Poll::Ready(Ok(()))
    }
}

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
impl Future for Sender {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut me = &mut *self;
        loop {
            match me.rx.poll_recv(cx) {
                Poll::Ready(None) => {
                    break;
                }
                Poll::Ready(Some(s)) => {
                    match me.snapshot.as_mut() {
                        Some(ss) => ss.merge(&s),
                        None => me.snapshot = Some(s),
                    };
                }
                Poll::Pending => {
                    // 判断是否可以flush
                    if me.last.elapsed() >= Duration::from_secs(10) {
                        me.poll_send(cx);
                        me.last = Instant::now();
                    }
                    // sleep 1s
                    ready!(me.tick.poll_tick(cx));
                }
            }
        }
        Poll::Ready(())
    }
}
