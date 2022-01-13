use std::task::{Context, Poll};

use futures::ready;
use tokio::net::UdpSocket;

#[derive(Default)]
pub struct PacketBuffer {
    idx: usize,
    buff: Vec<u8>, // 没有在请求的关键路径上。是异步发送metrics
    pub(crate) addr: String,
    socket: Option<UdpSocket>,
}

impl PacketBuffer {
    pub fn new(addr: String) -> Self {
        Self {
            addr: addr,
            ..Default::default()
        }
    }
    #[inline]
    pub fn poll_flush(&mut self, cx: &mut Context) -> Poll<()> {
        if self.idx < self.buff.len() {
            if let Err(e) = ready!(self._poll_flush(cx)) {
                log::warn!("failed to flush metrics:{} ", e);
                self.socket.take();
            }
            if self.buff.len() >= 4 * 1024 * 1024 {
                log::info!("metric size:{}", self.buff.len());
            }
            self.idx = 0;
            self.buff.clear();
            unsafe { self.buff.set_len(0) };
        }
        Poll::Ready(())
    }
    #[inline]
    fn _poll_flush(&mut self, cx: &mut Context) -> Poll<std::io::Result<()>> {
        if self.socket.is_none() {
            let sock = std::net::UdpSocket::bind("0.0.0.0:34254")?;
            sock.connect(&self.addr)?;
            self.socket = Some(UdpSocket::from_std(sock)?);
        }
        if let Some(ref mut sock) = self.socket.as_mut() {
            // 一次最多发送4k。
            while self.idx < self.buff.len() {
                let packet_size = 1024;

                let mut end = (self.idx + packet_size).min(self.buff.len() - 1);
                // 找着下一行。避免换行
                while end < self.buff.len() {
                    if self.buff[end] == b'\n' {
                        break;
                    }
                    end += 1;
                }
                self.idx += ready!(sock.poll_send(cx, &self.buff[self.idx..=end]))?;
            }
        }
        Poll::Ready(Ok(()))
    }
}

impl crate::item::ItemWriter for PacketBuffer {
    #[inline]
    fn write(&mut self, name: &str, key: &str, sub_key: &str, v: f64) {
        use ds::Buffer;
        let buff = &mut self.buff;
        buff.write("breeze.");
        buff.write(name);
        buff.write(".byhost.");
        buff.write(super::ip::local_ip());
        buff.write(".");
        buff.write(key);
        if sub_key.len() > 0 {
            buff.write(".");
            buff.write(sub_key);
        }
        buff.write(":");
        //let v = (v * 100f64) as isize as f64 / 100f64;
        buff.write(v.to_string());
        buff.write("|kv\n");
    }
}
