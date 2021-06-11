use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use super::HEADER_LEN;
use crate::chan::AsyncWriteAll;

const NON_EXISTS: u8 = std::u8::MAX;

pub struct MemcacheBinaryMetaStream {
    op: u8,
    waker: Option<Waker>,
}

impl MemcacheBinaryMetaStream {
    pub fn from(_backends: &str) -> Self {
        Self {
            op: NON_EXISTS,
            waker: None,
        }
    }
}

impl AsyncWriteAll for MemcacheBinaryMetaStream {}

impl AsyncWrite for MemcacheBinaryMetaStream {
    // 调用方必须确保buf是一个完整的请求
    fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>> {
        let mut me = &mut *self;
        assert!(buf.len() >= HEADER_LEN);

        // https://github.com/memcached/memcached/wiki/BinaryProtocolRevamped#command-opcodes
        match buf[1] {
            0x7 | 0x8 | 0xb => me.op = buf[1],
            _ => panic!("not a valid meta version:{}", buf[1]),
        }
        if let Some(waker) = me.waker.take() {
            waker.wake();
        }
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
}

// 0.0.1
const VERSION: [u8; 29] = [
    0x81, 0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x2e, 0x30, 0x2e, 0x31,
];
impl AsyncRead for MemcacheBinaryMetaStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<Result<()>> {
        let mut me = &mut *self;
        let op = me.op;
        if op == NON_EXISTS {
            me.waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        me.op = NON_EXISTS;
        match op {
            0xb => {
                buf.put_slice(&VERSION);
            }
            _ => panic!("{} is not a valid meta operation", op),
        }
        Poll::Ready(Ok(()))
    }
}
