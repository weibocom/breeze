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
    last: Instant,
    snapshot: Option<Snapshot>,
    buff: Vec<u8>,
    idx_buff: usize,
    idx_c: usize, // 当前处理counter的位置
    idx_d: usize, // 当前处理duration的位置
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
            snapshot: Some(Snapshot::new()),
            idx_c: 0,
            idx_d: 0,
            idx_buff: 0,
        }
    }
    pub fn start_sending(self) {
        tokio::spawn(async move {
            log::info!("metric-send: task started:{}", self.addr);
            self.await;
        });
    }
    fn poll_flush(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        if let Some(ref sock) = self.socket {
            while self.idx_buff < self.buff.len() {
                self.idx_buff += ready!(sock.poll_send(cx, &self.buff[self.idx_buff..]))?;
            }
        }
        self.idx_buff = 0;
        unsafe {
            self.buff.set_len(0);
        }
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
    fn poll_send(&mut self, cx: &mut Context) -> Poll<()> {
        let mut snapshot = self.snapshot.take().expect("metrics snapshot");
        match self._poll_send(cx, &snapshot) {
            Poll::Pending => {
                self.snapshot = Some(snapshot);
            }
            Poll::Ready(Err(e)) => {
                log::warn!("send: error:{:?}", e);
                self.reconnect();
            }
            _ => {
                // 处理完成。reset
                snapshot.reset();
                self.snapshot = Some(snapshot);
            }
        };
        Poll::Ready(())
    }
    fn _poll_send(&mut self, cx: &mut Context, ss: &Snapshot) -> Poll<Result<()>> {
        let secs = self.last.elapsed().as_secs_f64();
        self.check_connect()?;
        ready!(self.poll_flush(cx)?);
        // 写counter
        for i in self.idx_c..ss.counters.inner.len() {
            let group = unsafe { ss.counters.inner.get_unchecked(i) };
            let sid = i;
            for (key, counter) in group.iter() {
                ready!(self.poll_send_packet(cx, sid, key, "", *counter as f64 / secs))?;
            }
            self.idx_c += 1;
        }

        for i in self.idx_d..ss.durations.inner.len() {
            let sid = i;
            let group = unsafe { ss.durations.inner.get_unchecked(i) };
            for (key, item) in group.iter() {
                // 平均耗时
                let avg_us = if item.count == 0 {
                    0f64
                } else {
                    item.elapse_us as f64 / item.count as f64
                };
                self.build_packet(sid, key, "avg_us", avg_us as f64);
                // 总的qps
                let qps = item.count as f64 / secs;
                self.build_packet(sid, key, "qps", qps);
                for i in 0..item.intervals.len() {
                    let sub_key = item.get_interval_name(i);
                    let interval_qps = item.intervals[i] as f64 / secs;
                    self.build_packet(sid, key, sub_key, interval_qps);
                }
            }
            ready!(self.poll_flush(cx)?);
            self.idx_d += 1;
        }
        self.idx_c = 0;
        self.idx_d = 0;
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
                        Some(ss) => ss.merge(s),
                        None => me.snapshot = Some(s),
                    };
                }
                Poll::Pending => {
                    // 判断是否可以flush
                    if me.last.elapsed() >= Duration::from_secs(10) {
                        ready!(me.poll_send(cx));
                        me.last = Instant::now();
                    }
                    // sleep 1s
                    ready!(me.tick.poll_tick(cx));
                }
            }
        }
        log::warn!("sender task complete");
        Poll::Ready(())
    }
}
