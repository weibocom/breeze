use metrics::{Metric, Path};
pub struct MetricStream<S> {
    write: Metric,
    w_pending: Metric,
    read_hit: Metric,
    read: Metric,
    r_pending: Metric,
    s: S,
}

use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

impl<S> From<S> for MetricStream<S> {
    #[inline]
    fn from(s: S) -> Self {
        let read = Path::base().qps("poll_read");
        let read_hit = Path::base().ratio("poll_read");
        let write = Path::base().qps("poll_write");
        let r_pending = Path::base().qps("r_pending");
        let w_pending = Path::base().qps("w_pending");

        Self {
            s,
            read,
            read_hit,
            write,
            r_pending,
            w_pending,
        }
    }
}

impl<S: AsyncRead + Unpin + std::fmt::Debug> AsyncRead for MetricStream<S> {
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        let pre = buf.remaining();
        let ret = Pin::new(&mut self.s).poll_read(cx, buf);
        self.read += 1;
        let hit = (buf.remaining() != pre) as i64;
        self.read_hit += (hit, 1);
        if ret.is_pending() {
            self.r_pending += 1;
        }
        //if hit > 0 {
        //    use protocol::Utf8;
        //    log::info!("poll_read-{:?} data:{:?}", self.s, buf.filled().utf8());
        //}
        ret
    }
}

impl<S: AsyncWrite + Unpin + std::fmt::Debug> AsyncWrite for MetricStream<S> {
    #[inline]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        self.write += 1;
        let r = Pin::new(&mut self.s).poll_write(cx, buf);
        if r.is_pending() {
            self.w_pending += 1;
        }
        //use protocol::Utf8;
        //log::info!("poll_write-{:?} data:{:?}", self.s, buf.utf8());
        r
    }
    #[inline]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.s).poll_flush(cx)
    }
    #[inline]
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.s).poll_shutdown(cx)
    }
}
