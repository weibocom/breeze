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
    buf_first: bool,
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
            buf_first: false,
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
    // 1. 先尝试一次直接写入；
    // 2. 把剩下的数据写入到buffer中;
    #[inline(always)]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let mut oft = 0;
        if self.buf.len() > 0 && buf.len() > 4 * 1024 {
            log::info!(
                "big data write to buffer. buf:{} {}",
                self.buf.len(),
                buf.len()
            );
        }
        if self.buf.len() == 0 && !self.buf_first {
            let _ = Pin::new(&mut self.s).poll_write(cx, buf)?.map(|n| oft = n);
        }
        if oft < buf.len() {
            self.reserve(buf.len() - oft);
            self.cache += 1;
            use ds::Buffer;
            self.buf.write(&buf[oft..])
        }
        Poll::Ready(Ok(buf.len()))
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
        let noop = noop_waker::noop_waker();
        let mut ctx = Context::from_waker(&noop);
        let _ = Pin::new(self).poll_write(&mut ctx, data);
        Ok(())
    }
    // hint: 提示可能优先写入到cache
    #[inline(always)]
    fn cache(&mut self, hint: bool) {
        if self.buf_first != hint {
            self.buf_first = hint;
        }
    }
}
impl<S> Stream<S> {
    #[inline(always)]
    fn reserve(&mut self, size: usize) {
        if self.buf.capacity() - self.buf.len() < size {
            let cap = (size + self.buf.len()).max(512).next_power_of_two();
            let grow = cap - self.buf.capacity();
            //log::info!(
            //    "reserved {} buffer resized from {} {} => {}",
            //    size,
            //    self.buf.len(),
            //    self.buf.capacity(),
            //    cap
            //);
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
