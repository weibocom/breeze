pub enum PacketPos {
    Magic = 0,
    Opcode = 1,
    Key = 2,
    ExtrasLength = 4,
    DataType = 5,
    Status = 6,
    TotalBodyLength = 8,
    Opaque = 12,
    Cas = 16,
}

//use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
//
//use std::io::Result;
//use std::pin::Pin;
//use std::task::{Context, Poll, Waker};
//
//use super::HEADER_LEN;
//use crate::{AsyncReadAll, AsyncWriteAll, ResponseItem};
//
//const NON_EXISTS: u8 = std::u8::MAX;
//
//pub struct MemcacheBinaryMetaStream {
//    op: u8,
//    waker: Option<Waker>,
//}
//
//impl MemcacheBinaryMetaStream {
//    pub fn from(_backends: &str) -> Self {
//        Self {
//            op: NON_EXISTS,
//            waker: None,
//        }
//    }
//}
//
//impl AsyncWriteAll for MemcacheBinaryMetaStream {}
//
//impl AsyncWrite for MemcacheBinaryMetaStream {
//    // 调用方必须确保buf是一个完整的请求
//    fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>> {
//        let mut me = &mut *self;
//        assert!(buf.len() >= HEADER_LEN);
//
//        // https://github.com/memcached/memcached/wiki/BinaryProtocolRevamped#command-opcodes
//        match buf[1] {
//            0x7 | 0x8 | 0xb => me.op = buf[1],
//            _ => panic!("not a valid meta version:{}", buf[1]),
//        }
//        if let Some(waker) = me.waker.take() {
//            waker.wake();
//        }
//        Poll::Ready(Ok(buf.len()))
//    }
//    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<()>> {
//        Poll::Ready(Ok(()))
//    }
//    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<()>> {
//        Poll::Ready(Ok(()))
//    }
//}
//
//// 0.0.1
//const VERSION: [u8; 29] = [
//    0x81, 0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00,
//    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x2e, 0x30, 0x2e, 0x31,
//];
//impl AsyncReadAll for MemcacheBinaryMetaStream {
//    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<ResponseItem>> {
//        let mut me = &mut *self;
//        match me.op {
//            0xb => {
//                me.op = NON_EXISTS;
//                Poll::Ready(Ok(ResponseItem::from_slice(&VERSION)))
//            }
//            _ => panic!("{} is not a valid meta operation", me.op),
//        }
//    }
//    fn poll_done(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
//        Poll::Ready(Ok(()))
//    }
//}
