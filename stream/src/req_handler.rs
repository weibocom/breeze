use std::future::Future;
use std::io::Result;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::ready;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc::Receiver;

use crate::BackendBuilder;
use ds::{RingBufferReader, RingBufferWriter, Slice};

unsafe impl<W> Send for BridgeBufferToWriter<W> {}
unsafe impl<W> Sync for BridgeBufferToWriter<W> {}

pub struct RequestData {
    id: usize,
    data: Slice,
}

impl RequestData {
    pub fn from(id: usize, b: Slice) -> Self {
        Self { id: id, data: b }
    }
    fn data(&self) -> &[u8] {
        &self.data.data()
    }
    fn id(&self) -> usize {
        self.id
    }
}

pub trait RequestHandler {
    fn on_received(&self, id: usize, seq: usize);
}

pub struct BridgeBufferToWriter<W> {
    // 一次poll_write没有写完时，会暂存下来
    reader: RingBufferReader,
    w: W,
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
    ) -> Self {
        //let cache = File::create("/tmp/cache.out").unwrap();
        Self {
            w: w,
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
            log::debug!("bridage buffer to backend.");
            let result_buffer = ready!(me.reader.poll_next(cx))?;
            if result_buffer.is_empty() {
                log::debug!("bridage buffer to backend: received empty");
                continue;
            }
            log::debug!("bridage buffer to backend. len:{} ", result_buffer.len());
            let num = ready!(writer.as_mut().poll_write(cx, result_buffer))?;
            //me.cache.write_all(&b[..num]).unwrap();
            debug_assert!(num > 0);
            log::debug!("bridage buffer to backend: {} bytes sent ", num);
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
            if let Some(req) = me.cache.take() {
                let data = req.data();
                log::debug!(
                    "bridge request to buffer: write to buffer. cid: {} len:{}",
                    req.id(),
                    req.data().len()
                );
                ready!(me.w.poll_put_slice(cx, data))?;
                let seq = me.seq;
                me.seq += 1;
                me.r.on_received(req.id(), seq);
                log::debug!(
                    "received data from bridge. len:{} id:{} seq:{}",
                    req.data().len(),
                    req.id(),
                    seq
                );
            }
            log::debug!("bridge request to buffer: wating to get request from channel");
            let result = ready!(receiver.as_mut().poll_recv(cx));
            if result.is_none() {
                me.w.close();
                log::debug!("bridge request to buffer: channel closed, quit");
                break;
            }
            me.cache = result;
            log::debug!("bridge request to buffer. one request received from request channel");
        }
        Poll::Ready(Ok(()))
    }
}
