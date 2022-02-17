use metrics::{Metric, Path};
pub struct MetricStream<S> {
    write: Metric,
    read_hit: Metric,
    read: Metric,
    s: S,
}

use std::io::{self};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

impl<S> From<S> for MetricStream<S> {
    #[inline]
    fn from(s: S) -> Self {
        let read = Path::base().qps("poll_read");
        let read_hit = Path::base().ratio("poll_read");
        let write = Path::base().qps("poll_write");
        Self {
            s,
            read,
            read_hit,
            write,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for MetricStream<S> {
    #[inline(always)]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let pre = buf.remaining();
        let ret = Pin::new(&mut self.s).poll_read(cx, buf);
        self.read += 1;
        let hit = (buf.remaining() != pre) as i64;
        self.read_hit += (hit, 1);
        ret
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for MetricStream<S> {
    #[inline(always)]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        if buf.len() == 0 {
            log::info!("write zero len");
        }
        self.write += 1;
        Pin::new(&mut self.s).poll_write(cx, buf)
    }
    #[inline(always)]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.s).poll_flush(cx)
    }
    #[inline(always)]
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.s).poll_shutdown(cx)
    }
}
