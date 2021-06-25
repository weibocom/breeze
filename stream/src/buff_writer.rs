use std::future::Future;
use std::io::Result;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

use futures::ready;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc::Receiver;

use super::ring::spsc::{RingBufferReader, RingBufferWriter};
use crate::BackendBuilder;

unsafe impl<W> Send for BridgeBufferToWriter<W> {}
unsafe impl<W> Sync for BridgeBufferToWriter<W> {}

pub struct RequestData {
    id: usize,
    //ptr: usize,
    //len: usize,
    data: Vec<u8>,
}

impl RequestData {
    pub fn from(id: usize, b: &[u8]) -> Self {
        let data = b.clone().to_vec();
        //let ptr = b.as_ptr() as usize;
        Self {
            id: id,
            //ptr: ptr,
            //len: b.len(),
            data: data,
        }
    }
    fn data(&self) -> &[u8] {
        //let ptr = self.ptr as *const u8;
        //unsafe { std::slice::from_raw_parts(ptr, self.len) }
        &self.data
    }
    fn id(&self) -> usize {
        self.id
    }
}

pub trait Request {
    fn request_received(&self, id: usize, seq: usize);
}

pub struct BridgeBufferToWriter<W> {
    // 一次poll_write没有写完时，会暂存下来
    reader: RingBufferReader,
    w: W,
    done: Arc<AtomicBool>,
    //cache: File,
    builder: Arc<RwLock<BackendBuilder>>,
}

impl<W> BridgeBufferToWriter<W> {
    pub fn from(reader: RingBufferReader, w: W, done: Arc<AtomicBool>, builder: Arc<RwLock<BackendBuilder>>) -> Self {
        //let cache = File::create("/tmp/cache.out").unwrap();
        Self {
            w: w,
            reader: reader,
            done: done,
            //cache: cache,
            builder: builder.clone(),
        }
    }
}

impl<W> Future for BridgeBufferToWriter<W>
where
    W: AsyncWrite + Unpin,
{
    type Output = Result<()>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        println!("task polling. BridgeBufferToWriter");
        let me = &mut *self;
        let mut writer = Pin::new(&mut me.w);
        while !me.done.load(Ordering::Relaxed) {
            println!("bridage buffer to backend.");
            let b = ready!(me.reader.poll_next(cx));
            println!("bridage buffer to backend. len:{} ", b.len());
            let num = ready!(writer.as_mut().poll_write(cx, b))?;
            //me.cache.write_all(&b[..num]).unwrap();
            debug_assert!(num > 0);
            println!("bridage buffer to backend: {} bytes sent ", num);
            me.reader.consume(num);
        }
        println!("task complete. bridge data from local buffer to backend server");
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
    R: Unpin + Request,
{
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        println!("task polling. BridgeRequestToBuffer");
        let me = &mut *self;
        let mut receiver = Pin::new(&mut me.receiver);
        while !me.done.load(Ordering::Relaxed) {
            if let Some(req) = me.cache.take() {
                let data = req.data();
                if !data.len() <= 1 {
                    assert_eq!(data[0], 0x80);
                    println!(
                        "bridge request to buffer: write to buffer. cid: {} len:{}",
                        req.id(),
                        req.data().len()
                    );
                    ready!(me.w.poll_put_slice(cx, data));
                    let seq = me.seq;
                    me.seq += 1;
                    me.r.request_received(req.id(), seq);
                    println!(
                        "received data from bridge. len:{} id:{} seq:{}",
                        req.data().len(),
                        req.id(),
                        seq
                    );
                }
            }
            println!("bridge request to buffer: wating to get request from channel");
            //me.cache = Some(ready!(receiver.as_mut().poll_recv(cx)).expect("channel closed"));
            let result = receiver.as_mut().blocking_recv();
            if result.is_none() {
                println!("bridge request to buffer: channel closed, quit");
                break;
            }
            else {
                me.cache = result;
            }
            println!("bridge request to buffer. one request received from request channel");
        }
        Poll::Ready(Ok(()))
    }
}
