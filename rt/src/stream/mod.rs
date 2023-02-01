mod metric;
use ds::MemPolicy;
use metric::MetricStream;

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use std::task::ready;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

mod tx_buf;
pub use tx_buf::*;

// 1. read/write统计
// 2. 支持write buffer。
// 3. poll_write总是成功
pub struct Stream<S> {
    s: MetricStream<S>,
    buf: TxBuffer,
    write_to_buf: bool,
}
impl<S> From<S> for Stream<S> {
    #[inline]
    fn from(s: S) -> Self {
        Self {
            s: s.into(),
            buf: TxBuffer::new(),
            write_to_buf: false,
        }
    }
}
impl<S: AsyncRead + Unpin + std::fmt::Debug> AsyncRead for Stream<S> {
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.s).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin + std::fmt::Debug> AsyncWrite for Stream<S> {
    // 先将数据写入到io
    // 未写完的写入到buf
    // 不返回Pending
    #[inline]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        // 这个值应该要大于MSS，否则一个请求分多次返回，会触发delay ack。
        const LARGE_SIZE: usize = 4 * 1024;
        // 数据量比较大，尝试直接写入。写入之前要把buf flush掉。
        if self.buf.len() + data.len() >= LARGE_SIZE {
            let _ = self.as_mut().poll_flush(cx)?;
        }
        let mut oft = 0;
        // 1. buf.len()必须为0；
        // 2. 如果没有显示要求写入到buf, 或者数据量大，则直接写入
        if self.buf.len() == 0 && (!self.write_to_buf || data.len() >= LARGE_SIZE) {
            let _ = Pin::new(&mut self.s).poll_write(cx, data)?.map(|n| oft = n);
        }
        // 未写完的数据写入到buf。
        if oft < data.len() {
            self.buf.write(&data[oft..])
        }
        Poll::Ready(Ok(data.len()))
    }
    #[inline]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        if self.buf.avail() {
            let Self { s, buf, .. } = &mut *self;
            let mut w = Pin::new(s);
            loop {
                let data = buf.data();
                let n = ready!(w.as_mut().poll_write(cx, data))?;
                if buf.take(n) {
                    break;
                }
            }
            let flush = w.poll_flush(cx)?;
            ready!(flush);
        }
        Poll::Ready(Ok(()))
    }
    #[inline]
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let _ = self.as_mut().poll_flush(cx);
        Pin::new(&mut self.s).poll_shutdown(cx)
    }
}

#[ctor::ctor]
static NOOP: Waker = noop_waker::noop_waker();

impl<S: AsyncWrite + Unpin + std::fmt::Debug> protocol::Writer for Stream<S> {
    #[inline]
    fn cap(&self) -> usize {
        self.buf.cap()
    }
    #[inline]
    fn pending(&self) -> usize {
        self.buf.len()
    }
    #[inline]
    fn write(&mut self, data: &[u8]) -> protocol::Result<()> {
        if data.len() <= 4 {
            self.buf.write(data);
        } else {
            let mut ctx = Context::from_waker(&NOOP);
            let _ = Pin::new(self).poll_write(&mut ctx, data);
        }
        Ok(())
    }
    // hint: 提示可能优先写入到cache
    #[inline]
    fn cache(&mut self, hint: bool) {
        if self.write_to_buf != hint {
            self.write_to_buf = hint;
        }
    }
    #[inline]
    fn shrink(&mut self) {
        self.buf.shrink();
    }
}
impl<S: AsyncWrite + Unpin + std::fmt::Debug> ds::BufWriter for Stream<S> {
    #[inline]
    fn write_all(&mut self, data: &[u8]) -> std::io::Result<()> {
        if data.len() <= 4 {
            self.buf.write(data);
        } else {
            let mut ctx = Context::from_waker(&NOOP);
            let _ = Pin::new(self).poll_write(&mut ctx, data);
        }
        Ok(())
    }
    #[inline]
    fn write_seg_all(&mut self, buf0: &[u8], buf1: &[u8]) -> std::io::Result<()> {
        if self.write_to_buf != true {
            self.write_to_buf = true;
        }
        self.write_all(buf0)?;
        self.write_all(buf1)
    }
}
