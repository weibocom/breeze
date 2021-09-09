use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use crate::SLOW_DURATION;
use protocol::Resource;

use futures::ready;

pub(crate) struct Writer<W> {
    w: W,
    metric_id: usize,
}

impl<W> Writer<W> {
    pub(crate) fn from(w: W, addr: &str, resource: Resource, biz: &str) -> Self {
        let metric_id = metrics::register_names(vec![
            resource.name(),
            &metrics::encode_addr(biz),
            &metrics::encode_addr(addr),
        ]);
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
        let size = ready!(Pin::new(&mut self.w).poll_write(cx, buf))?;
        let duration = instant.elapsed();
        if duration >= SLOW_DURATION {
            log::info!(
                "{} write slow : {:?} size:{}",
                metrics::get_name(self.metric_id),
                duration,
                size
            );
        }
        metrics::duration("tx", duration, self.metric_id);
        Poll::Ready(Ok(size))
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
        let metric_id = metrics::register_names(vec![
            resource.name(),
            &metrics::encode_addr(biz),
            &metrics::encode_addr(addr),
        ]);
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
        ready!(Pin::new(&mut self.w).poll_read(cx, buf))?;
        let duration = instant.elapsed();
        if duration >= SLOW_DURATION {
            log::info!(
                "{} read slow : {:?} size:{}",
                metrics::get_name(self.metric_id),
                duration,
                buf.filled().len()
            );
        }
        metrics::duration("rx", duration, self.metric_id);
        Poll::Ready(Ok(()))
    }
}
