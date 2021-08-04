use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use protocol::Resource;

pub(crate) struct Writer<W> {
    w: W,
    metric_id: usize,
}

impl<W> Writer<W> {
    pub(crate) fn from(w: W, addr: &str, resource: Resource, biz: &str) -> Self {
        let metric_id =
            metrics::register_names(vec![resource.name(), &metrics::encode_addr(biz), &metrics::encode_addr(addr)]);
        Self { w, metric_id }
    }
}

impl<W> AsyncWrite for Writer<W>
where
    W: AsyncWrite + Unpin,
{
    #[inline(always)]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        let instant = Instant::now();
        let ret = Pin::new(&mut self.w).poll_write(cx, buf);
        let duration = instant.elapsed();
        metrics::duration_with_service("tx", duration, self.metric_id);
        ret
    }
    #[inline(always)]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.w).poll_flush(cx)
    }
    #[inline(always)]
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.w).poll_shutdown(cx)
    }
}
impl<W> Drop for Writer<W> {
    fn drop(&mut self) {
        metrics::unregister_by_id(self.metric_id);
    }
}

pub(crate) struct Reader<R> {
    w: R,
    metric_id: usize,
}

impl<R> Reader<R> {
    pub(crate) fn from(w: R, addr: &str, resource: Resource, biz: &str) -> Self {
        let metric_id =
            metrics::register_names(vec![resource.name(), &metrics::encode_addr(biz), &metrics::encode_addr(addr)]);
        Self { w, metric_id }
    }
}

impl<R> Drop for Reader<R> {
    fn drop(&mut self) {
        metrics::unregister_by_id(self.metric_id);
    }
}

impl<R> AsyncRead for Reader<R>
where
    R: AsyncRead + Unpin,
{
    #[inline(always)]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        let instant = Instant::now();
        let ret = Pin::new(&mut self.w).poll_read(cx, buf);
        let duration = instant.elapsed();
        metrics::duration_with_service("rx", duration, self.metric_id);
        ret
    }
}
