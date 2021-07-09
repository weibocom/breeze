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

#[derive(Clone)]
pub struct RequestData {
    id: usize,
    data: Request,
}

impl RequestData {
    pub fn from(id: usize, b: Request) -> Self {
        Self { id: id, data: b }
    }
    // TODO debug完毕后，去掉pub
    pub fn data(&self) -> &[u8] {
        &self.data.data()
    }
    fn cid(&self) -> usize {
        self.id
    }
    fn rid(&self) -> &RequestId {
        &self.data.id()
    }
}

pub trait RequestHandler {
    // 把更新seq与更新状态分开。避免response在执行on_received之前返回时，会依赖seq做判断
    fn on_received_seq(&self, id: usize, seq: usize);
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
            println!("=====222=== in req_handler buf-to-writer: req:{:?}", buff);
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
                println!("====11111==== in req_handler req-to-buff: req:{:?}", data);
                log::debug!(
                    "req-handler-buffer: received. cid: {} len:{} rid:{:?}",
                    req.cid(),
                    req.data().len(),
                    req.rid()
                );
                me.r.on_received_seq(req.cid(), me.seq);
                ready!(me.w.poll_put_slice(cx, data))?;
                println!("====22222==== in req_handler req-to-buff: req:{:?}", data);
                let seq = me.seq;
                me.seq += 1;
                me.r.on_received(req.cid(), seq);
                log::debug!(
                    "req-handler-buffer: received and write to buffer. len:{} id:{} seq:{}, rid:{:?}",
                    req.data().len(),
                    req.cid(),
                    seq,
                    req.rid()
                );
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

            // just for debug
            let tmp = me.cache.clone();
            println!(
                "====000000000==== in req_handler req-to-buff: req:{:?}",
                tmp.unwrap().data()
            );
            log::debug!("req-handler-buffer: one request received from request channel");
        }
        Poll::Ready(Ok(()))
    }
}
