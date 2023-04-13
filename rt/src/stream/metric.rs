pub use inner::*;
#[cfg(feature = "poll-io-metrics")]
mod inner {
    use metrics::base::*;
    #[repr(transparent)]
    #[derive(Debug)]
    pub struct MetricStream<S> {
        s: S,
    }

    use std::io::Result;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

    impl<S> From<S> for MetricStream<S> {
        #[inline]
        fn from(s: S) -> Self {
            Self { s }
        }
    }

    impl<S: AsyncRead + Unpin + std::fmt::Debug> AsyncRead for MetricStream<S> {
        #[inline]
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<Result<()>> {
            let ret = Pin::new(&mut self.s).poll_read(cx, buf);
            POLL_READ.incr();
            if ret.is_pending() {
                POLL_PENDING_R.incr();
            }
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
            POLL_WRITE.incr();
            let r = Pin::new(&mut self.s).poll_write(cx, buf);
            log::debug!("+++ stream write rs:{:?}, ", r);
            if r.is_pending() {
                POLL_PENDING_W.incr();
            }
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
}
#[cfg(not(feature = "poll-io-metrics"))]
mod inner {
    pub type MetricStream<S> = S;
}
