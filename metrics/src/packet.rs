use std::cell::RefCell;
use std::task::{Context, Poll};

use futures::ready;
use tokio::net::UdpSocket;

#[derive(Default)]
pub struct PacketBuffer {
    idx: usize,
    buff: RefCell<Vec<u8>>,
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
        if let Err(e) = ready!(self._poll_flush(cx)) {
            log::warn!("failed to flush metrics:{}", e);
            self.socket.take();
        }
        self.idx = 0;
        let mut buff = self.buff.borrow_mut();
        unsafe { buff.set_len(0) };
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
            let buff = self.buff.borrow_mut();
            while self.idx < buff.len() {
                self.idx += ready!(sock.poll_send(cx, &buff[self.idx..]))?;
            }
        }
        Poll::Pending
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
