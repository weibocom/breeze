use std::cell::RefCell;
use std::task::{Context, Poll};

use futures::ready;
use tokio::net::UdpSocket;

#[derive(Default)]
pub struct PacketBuffer {
    idx: usize,
    buff: RefCell<Vec<u8>>, // 没有在请求的关键路径上。是异步发送metrics
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
        if self.idx < self.buff.borrow().len() {
            if let Err(e) = ready!(self._poll_flush(cx)) {
                log::warn!("failed to flush metrics:{} ", e);
                self.socket.take();
            }
            if self.buff.borrow().len() >= 4 * 1024 * 1024 {
                log::info!(
                    "metric size({}) is great than 4mb",
                    self.buff.borrow().len()
                );
            }
            self.idx = 0;
            let mut buff = self.buff.borrow_mut();
            unsafe { buff.set_len(0) };
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
            let buff = self.buff.borrow();
            // 一次最多发送4k。
            while self.idx < buff.len() {
                let packet_size = 1024;

                let mut end = (self.idx + packet_size).min(buff.len() - 1);
                // 找着下一行。避免换行
                while end < buff.len() {
                    if buff[end] == b'\n' {
                        break;
                    }
                    end += 1;
                }
                let old = self.idx;
                self.idx += ready!(sock.poll_send(cx, &buff[self.idx..=end]))?;
            }
        }
        Poll::Ready(Ok(()))
    }
}

impl crate::kv::KV for PacketBuffer {
    #[inline]
    fn kv(&self, sid: usize, key: &str, sub_key: &str, v: f64) {
        use ds::Buffer;
        let mut buff = self.buff.borrow_mut();
        let v = (v * 100f64) as isize as f64 / 100f64;
        buff.write("breeze.");
        buff.write(crate::get_name(sid));
        buff.write(".byhost.");
        buff.write(super::ip::local_ip());
        buff.write(".");
        buff.write(key);
        if sub_key.len() > 0 {
            buff.write(".");
            buff.write(sub_key);
        }
        buff.write(":");
        buff.write(v.to_string());
        buff.write("|kv\n");
    }
}
