mod metric;
use metric::MetricStream;

use std::io::{self};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

// 1. read/write统计
// 2. 支持write buffer。
// 3. poll_write总是成功
pub struct Stream<S> {
    s: MetricStream<S>,
    idx: usize,
    buf: Vec<u8>,
    write_to_buf: bool,
    cache: metrics::Metric,
    buf_tx: metrics::Metric,
}
impl<S> From<S> for Stream<S> {
    #[inline]
    fn from(s: S) -> Self {
        Self {
            s: s.into(),
            idx: 0,
            buf: Vec::new(),
            write_to_buf: false,
            cache: metrics::Path::base().qps("poll_write_cache"),
            buf_tx: metrics::Path::base().num("mem_buf_tx"),
        }
    }
}
impl<S: AsyncRead + Unpin> AsyncRead for Stream<S> {
    #[inline(always)]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.s).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Stream<S> {
    // 先将数据写入到io
    // 未写完的写入到buf
    // 不返回Pending
    #[inline(always)]
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
            self.reserve(data.len() - oft);
            self.cache += 1;
            use ds::Buffer;
            self.buf.write(&data[oft..])
        }
        Poll::Ready(Ok(data.len()))
    }
    #[inline(always)]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        if self.buf.len() > 0 {
            let Self { s, idx, buf, .. } = &mut *self;
            let mut w = Pin::new(s);
            while *idx < buf.len() {
                *idx += ready!(w.as_mut().poll_write(cx, &buf[*idx..]))?;
            }
            if *idx == buf.len() {
                *idx = 0;
                unsafe { buf.set_len(0) };
            }
            w.poll_flush(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }
    #[inline(always)]
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let _ = self.as_mut().poll_flush(cx);
        Pin::new(&mut self.s).poll_shutdown(cx)
    }
}

impl<S: AsyncWrite + Unpin> protocol::Writer for Stream<S> {
    #[inline]
    fn write(&mut self, data: &[u8]) -> protocol::Result<()> {
        if data.len() <= 4 {
            self.buf.write(data)
        } else {
            let noop = noop_waker::noop_waker();
            let mut ctx = Context::from_waker(&noop);
            let _ = Pin::new(self).poll_write(&mut ctx, data);
            Ok(())
        }
    }
    // hint: 提示可能优先写入到cache
    #[inline(always)]
    fn cache(&mut self, hint: bool) {
        if self.write_to_buf != hint {
            self.write_to_buf = hint;
        }
    }
}
impl<S> Stream<S> {
    #[inline(always)]
    fn reserve(&mut self, size: usize) {
        if self.buf.capacity() - self.buf.len() < size {
            let cap = (size + self.buf.len()).max(512).next_power_of_two();
            let grow = cap - self.buf.len();
            self.buf.reserve(grow);
            self.buf_tx += grow;
            debug_assert_eq!(cap, self.buf.capacity());
        }
    }
}
impl<S> Drop for Stream<S> {
    #[inline(always)]
    fn drop(&mut self) {
        self.buf_tx -= self.buf.capacity() as i64;
    }
}
