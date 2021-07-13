use std::future::Future;
use std::io::Result;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::ready;
use tokio::io::{AsyncWrite, BufWriter};
use tokio::sync::mpsc::Receiver;

use crate::BackendBuilder;
use ds::{RingBufferReader, RingBufferWriter};
use protocol::{Request, RequestId};

unsafe impl<W> Send for BridgeBufferToWriter<W> {}
unsafe impl<W> Sync for BridgeBufferToWriter<W> {}

pub struct RequestData {
    id: usize,
    data: Request,
}

impl RequestData {
    pub fn from(id: usize, b: Request) -> Self {
        Self { id: id, data: b }
    }
    fn data(&self) -> &[u8] {
        &self.data.data()
    }
    fn cid(&self) -> usize {
        self.id
    }
    fn rid(&self) -> &RequestId {
        &self.data.id()
    }
    fn noreply(&self) -> bool {
        self.data.noreply()
    }
}

pub trait RequestHandler {
    fn on_received(&self, id: usize, seq: usize);
}

pub struct BridgeBufferToWriter<W> {
    // 一次poll_write没有写完时，会暂存下来
    reader: RingBufferReader,
    w: BufWriter<W>,
    done: Arc<AtomicBool>,
    //cache: File,
    _builder: Arc<BackendBuilder>,
}

impl<W> BridgeBufferToWriter<W> {
    pub fn from(
        reader: RingBufferReader,
        w: W,
        done: Arc<AtomicBool>,
        builder: Arc<BackendBuilder>,
    ) -> Self
    where
        W: AsyncWrite,
    {
        //let cache = File::create("/tmp/cache.out").unwrap();
        Self {
            w: BufWriter::with_capacity(2048, w),
            reader: reader,
            done: done,
            //cache: cache,
            _builder: builder.clone(),
        }
    }
}

impl<W> Future for BridgeBufferToWriter<W>
where
    W: AsyncWrite + Unpin,
{
    type Output = Result<()>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        log::debug!("task polling. BridgeBufferToWriter");
        let me = &mut *self;
        let mut writer = Pin::new(&mut me.w);
        while !me.done.load(Ordering::Relaxed) {
            log::debug!("req-handler-writer: bridage buffer to backend.");
            let buff = match me.reader.poll_next(cx)? {
                Poll::Ready(buff) => buff,
                Poll::Pending => {
                    ready!(writer.as_mut().poll_flush(cx))?;
                    return Poll::Pending;
                }
            };
            debug_assert!(!buff.is_empty());
            log::debug!("req-handler-writer: send buffer {} ", buff.len());
            let num = ready!(writer.as_mut().poll_write(cx, buff))?;
            debug_assert!(num > 0);
            log::debug!("req-handler-writer: {} bytes sent", num);
            me.reader.consume(num);
        }
        log::debug!("task complete. bridge data from local buffer to backend server");
        Poll::Ready(Ok(()))
    }
}

pub struct BridgeRequestToBuffer<R> {
    cache: Option<RequestData>,
    done: Arc<AtomicBool>,
    seq: usize,
    r: R,
    receiver: Receiver<RequestData>,
    w: RingBufferWriter,
}

impl<R> BridgeRequestToBuffer<R> {
    pub fn from(
        receiver: Receiver<RequestData>,
        r: R,
        w: RingBufferWriter,
        done: Arc<AtomicBool>,
    ) -> Self {
        Self {
            done: done,
            seq: 0,
            receiver: receiver,
            r: r,
            w: w,
            cache: None,
        }
    }
}

impl<R> Future for BridgeRequestToBuffer<R>
where
    R: Unpin + RequestHandler,
{
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        log::debug!("task polling. BridgeRequestToBuffer");
        let me = &mut *self;
        let mut receiver = Pin::new(&mut me.receiver);
        while !me.done.load(Ordering::Relaxed) {
            if let Some(ref req) = me.cache {
                let data = req.data();
                log::debug!(
                    "req-handler-buffer: received. cid: {} len:{} rid:{:?}",
                    req.cid(),
                    req.data().len(),
                    req.rid()
                );
                ready!(me.w.poll_check_available(cx, data.len()));
                if !req.noreply() {
                    me.r.on_received(req.cid(), me.seq);
                }
                me.w.poll_put_no_check(data)?;
                log::debug!(
                    "req-handler-buffer: received and write to buffer. len:{} id:{} seq:{}, rid:{:?}",
                    req.data().len(),
                    req.cid(),
                    me.seq,
                    req.rid()
                );
                // 如果是noreply，则序号不需要增加。因为没有response
                if !req.noreply() {
                    me.seq += 1;
                }
            }
            me.cache.take();
            log::debug!("req-handler-buffer: bridge request to buffer: wating incomming data");
            let result = ready!(receiver.as_mut().poll_recv(cx));
            if result.is_none() {
                me.w.close();
                log::info!("req-handler-buffer: bridge request to buffer: channel closed, quit");
                break;
            }
            me.cache = result;
            log::debug!("req-handler-buffer: one request received from request channel");
        }
        Poll::Ready(Ok(()))
    }
}
pub struct BridgeRequestToBackend<H, W> {
    cache: Option<RequestData>,
    offset: usize, //  当前cache的数据已经写入的位置
    done: Arc<AtomicBool>,
    seq: usize,
    handler: H,
    receiver: Receiver<RequestData>,
    w: BufWriter<W>,
}

impl<H, W> BridgeRequestToBackend<H, W> {
    pub fn from(
        buf: usize,
        handler: H,
        receiver: Receiver<RequestData>,
        w: W,
        done: Arc<AtomicBool>,
    ) -> Self
    where
        W: AsyncWrite,
    {
        Self {
            done: done,
            seq: 0,
            receiver: receiver,
            handler: handler,
            w: BufWriter::with_capacity(buf.max(128 * 1024), w),
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
        let mut receiver = Pin::new(&mut me.receiver);
        let mut w = Pin::new(&mut me.w);
        while !me.done.load(Ordering::Relaxed) {
            if let Some(ref req) = me.cache {
                let data = req.data();
                log::debug!(
                    "req-handler: writeing to backend. cid: {} len:{} rid:{:?} offset:{}",
                    req.cid(),
                    req.data().len(),
                    req.rid(),
                    me.offset
                );
                while me.offset < data.len() {
                    let n = ready!(w.as_mut().poll_write(cx, &data[me.offset..]))?;
                    log::debug!(
                        "req-handler: {} bytes written . cid: {} len:{} rid:{:?}, {:?}",
                        n,
                        req.cid(),
                        req.data().len(),
                        req.rid(),
                        &data[me.offset..n]
                    );
                    me.offset += n;
                }

                // 如果是noreply，则序号不需要增加。因为没有response
                if !req.noreply() {
                    me.seq += 1;
                }
            }
            me.cache.take();
            me.offset = 0;
            log::debug!("req-handler: bridge request to buffer: wating incomming data");
            match receiver.as_mut().poll_recv(cx) {
                Poll::Ready(result) => {
                    me.cache = result;
                    match me.cache {
                        Some(ref req) => {
                            log::debug!("req-handler: request received. {:?}", req.rid());
                            if !req.noreply() {
                                me.handler.on_received(req.cid(), me.seq);
                            }
                        }
                        None => {
                            log::info!("req-handler: channel closed, quit");
                            break;
                        }
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
