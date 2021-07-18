use std::future::Future;
use std::io::Result;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::ready;
use tokio::io::{AsyncWrite, BufWriter};

use protocol::Request;

pub struct Snapshot {
    idx: usize,
    cids: Vec<usize>,
}
impl Snapshot {
    fn new() -> Self {
        Self {
            idx: 0,
            cids: Vec::with_capacity(64),
        }
    }
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.cids.len()
    }
    #[inline(always)]
    pub fn push(&mut self, cid: usize) {
        self.cids.push(cid);
    }
    // 调用方确保idx < cids.len()
    #[inline(always)]
    fn take(&mut self) -> usize {
        debug_assert!(self.idx < self.cids.len());
        let cid = unsafe { *self.cids.get_unchecked(self.idx) };
        self.idx += 1;
        cid
    }
    #[inline(always)]
    fn available(&self) -> usize {
        self.cids.len() - self.idx
    }
    #[inline(always)]
    fn reset(&mut self) {
        self.idx = 0;
        unsafe {
            self.cids.set_len(0);
        }
    }
}
pub trait RequestHandler {
    // 如果填充ss的长度为0，则说明handler没有要处理的数据流，提示到达eof。
    fn poll_fill_snapshot(&self, cx: &mut Context, ss: &mut Snapshot) -> Poll<()>;
    fn take(&self, cid: usize, seq: usize) -> Request;
}

pub struct BridgeRequestToBackend<H, W> {
    snapshot: Snapshot,
    // 当前处理的请求
    cache: Option<Request>,
    // 当前请求的data已经写入到writer的字节数量
    offset: usize,
    seq: usize,
    handler: H,
    w: BufWriter<W>,
    done: Arc<AtomicBool>,
}

impl<H, W> BridgeRequestToBackend<H, W> {
    pub fn from(buf: usize, handler: H, w: W, done: Arc<AtomicBool>) -> Self
    where
        W: AsyncWrite,
    {
        Self {
            done: done,
            seq: 0,
            handler: handler,
            w: BufWriter::with_capacity(buf.max(128 * 1024), w),
            snapshot: Snapshot::new(),
            cache: None,
            offset: 0,
        }
    }
}
impl<H, W> Future for BridgeRequestToBackend<H, W>
where
    H: Unpin + RequestHandler,
    W: AsyncWrite + Unpin,
{
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        log::debug!("task polling. request handler");
        let me = &mut *self;
        let mut w = Pin::new(&mut me.w);
        while !me.done.load(Ordering::Acquire) {
            if let Some(ref req) = me.cache {
                let data = req.data();
                log::debug!(
                    "req-handler: writing {:?} {} {}",
                    req.id(),
                    req.len(),
                    me.offset
                );
                while me.offset < data.len() {
                    let n = ready!(w.as_mut().poll_write(cx, &data[me.offset..]))?;
                    log::debug!(
                        "req-handler: {}/{} bytes written {:?}",
                        n,
                        req.len(),
                        req.id()
                    );
                    me.offset += n;
                }

                // 如果是noreply，则序号不需要增加。因为没有response
                if !req.noreply() {
                    me.seq += 1;
                }
            }
            me.offset = 0;
            if me.snapshot.available() > 0 {
                let cid = me.snapshot.take();
                let req = me.handler.take(cid, me.seq);
                me.cache.replace(req);
                continue;
            } else {
                me.cache.take();
            }
            me.snapshot.reset();
            log::debug!("req-handler: poll snapshot");
            match me.handler.poll_fill_snapshot(cx, &mut me.snapshot) {
                Poll::Ready(_) => {
                    log::debug!(
                        "req-handler: poll snapshot done. {} polled. {:?}",
                        me.snapshot.len(),
                        me.snapshot.cids
                    );
                    if me.snapshot.len() == 0 {
                        log::info!("req-handler: no request polled. eof. task complete");
                        break;
                    }
                }
                Poll::Pending => {
                    log::debug!("req-handler: flushing");
                    ready!(w.as_mut().poll_flush(cx))?;
                    return Poll::Pending;
                }
            }
        }
        Poll::Ready(Ok(()))
    }
}
