use std::cell::RefCell;
use std::future::Future;
use std::io::{Error, ErrorKind, Result};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use super::status::*;
use super::{
    BridgeBufferToWriter, BridgeRequestToBuffer, BridgeResponseToLocal, IdAsyncRead, IdAsyncWrite,
    Request, Response, RingBuffer, RingSlice, SeqOffset,
};

use protocol::Protocol;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use tokio::sync::mpsc::{channel, Receiver};
use tokio_util::sync::PollSender;

use futures::ready;

use cache_line_size::CacheAligned;

unsafe impl Send for MpmcRingBufferStream {}
unsafe impl Sync for MpmcRingBufferStream {}

// 支持并发读取的stream
pub struct MpmcRingBufferStream {
    items: Vec<Item>,

    senders: Vec<RefCell<(bool, PollSender<RequestData>)>>,
    receiver: RefCell<Option<Receiver<RequestData>>>,

    // idx: 是seq % seq_cids.len()。因为seq是自增的，而且seq_cids.len() == items.len()
    // 用来当cache用。会通过item status进行double check
    seq_cids: Vec<CacheAligned<AtomicUsize>>,

    // 已经成功读取response的最小的offset，在ReadRrom里面使用
    offset: CacheAligned<SeqOffset>,

    // 读取response时，返回最后一个包的最小长度
    min: usize,

    done: Arc<AtomicBool>,
}

impl MpmcRingBufferStream {
    // id必须小于parallel
    pub fn with_capacity(min: usize, parallel: usize, done: Arc<AtomicBool>) -> Self {
        let parallel = parallel.next_power_of_two();
        assert!(parallel <= 32);
        let items = (0..parallel).map(|id| Item::new(id)).collect();
        let seq_cids = (0..parallel)
            .map(|_| CacheAligned(AtomicUsize::new(0)))
            .collect();

        let (sender, receiver) = channel(parallel * 2);
        let sender = PollSender::new(sender);
        let senders = (0..parallel)
            .map(|_| RefCell::new((true, sender.clone())))
            .collect();

        Self {
            items: items,
            seq_cids: seq_cids,
            offset: CacheAligned(SeqOffset::from(parallel)),
            receiver: RefCell::new(Some(receiver)),
            senders: senders,
            min: min,
            done: done,
        }
    }
    // 如果complete为true，则快速失败
    #[inline(always)]
    fn poll_check(&self) -> Poll<Result<()>> {
        if self.done.load(Ordering::Relaxed) {
            Poll::Ready(Err(Error::new(ErrorKind::NotConnected, "mpmc is done")))
        } else {
            Poll::Ready(Ok(()))
        }
    }
    pub fn poll_read(&self, cid: usize, cx: &mut Context, buf: &mut ReadBuf) -> Poll<Result<()>> {
        ready!(self.poll_check())?;
        //println!("poll read cid:{} ", cid);
        let item = unsafe { self.items.get_unchecked(cid) };
        if ready!(item.poll_read(cx, buf, self.min)) {
            let (start, end) = item.response_slice();
            self.offset.0.insert(start, end);
        }
        println!("mpmc poll read complete. data:{:?}", buf.filled());
        Poll::Ready(Ok(()))
    }
    // 释放cid的资源
    pub fn poll_shutdown(&self, cid: usize, _cx: &mut Context) -> Poll<Result<()>> {
        println!("mpmc: poll shutdown. cid:{}", cid);
        debug_assert!(self.get_item(cid).status_init());
        Poll::Ready(Ok(()))
    }
    pub fn poll_write(&self, cid: usize, cx: &mut Context, buf: &[u8]) -> Poll<Result<()>> {
        ready!(self.poll_check())?;
        println!("stream: poll write cid:{} len:{} ", cid, buf.len(),);
        let mut sender = unsafe { self.senders.get_unchecked(cid) }.borrow_mut();
        if sender.0 {
            self.get_item(cid).place_request();
            sender.0 = false;
            let req = RequestData::from(cid, buf);
            sender.1.start_send(req).ok().expect("channel closed");
        }
        ready!(sender.1.poll_send_done(cx))
            .ok()
            .expect("channel send failed");
        sender.0 = true;
        println!("stream: poll write complete cid:{} len:{} ", cid, buf.len());
        Poll::Ready(Ok(()))
    }
    #[inline]
    fn get_item(&self, cid: usize) -> &Item {
        debug_assert!(cid < self.items.len());
        unsafe { self.items.get_unchecked(cid) }
    }
    #[inline]
    fn mask_seq(&self, seq: usize) -> usize {
        (self.seq_cids.len() - 1) & seq
    }
    // bind_seq在reorder_req_offsets中被调用。
    // 生成一个seq，并且与cid绑定。在读取response时，直接使用cid即可快速获取。
    #[inline]
    fn bind_seq(&self, cid: usize, seq: usize) {
        let seq_idx = self.mask_seq(seq);
        unsafe {
            // 绑定
            self.seq_cids
                .get_unchecked(seq_idx)
                .0
                .store(cid, Ordering::Relaxed);
            self.get_item(cid).bind_seq(seq)
        };
    }
    fn place_response(&self, seq: usize, response: RingSlice) {
        unsafe {
            let seq_idx = self.mask_seq(seq);
            let cid = self
                .seq_cids
                .get_unchecked(seq_idx)
                .0
                .load(Ordering::Acquire) as usize;
            let mut item = self.get_item(cid);
            if seq != item.seq() {
                for it in self.items.iter() {
                    if it.seq() == seq {
                        item = it;
                    }
                }
            }
            item.place_response(response);
        }
    }

    fn check_bridge(&self) {
        // 必须是已经complete才能重新bridage
        self.done
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .expect("bridge an uncompleted stream");
    }

    // 构建一个ring buffer.
    // 一共3个线程。
    // 线程A: 把request data数据从item写入到ring buffer.
    // 线程B：把ring buffer的数据flush到server
    // 线程C：把response数据从server中读取，并且place到item的response中
    pub fn bridge<R, W, P>(
        self: Arc<Self>,
        parser: P,
        req_buffer: usize,
        resp_buffer: usize,
        r: R,
        w: W,
    ) where
        W: AsyncWrite + Unpin + Send + Sync + 'static,
        R: AsyncRead + Unpin + Send + 'static,
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        self.check_bridge();
        println!("request buffer size:{}", req_buffer);
        let (req_rb_writer, req_rb_reader) = RingBuffer::with_capacity(req_buffer).into_split();
        // 把数据从request同步到buffer
        let receiver = self.receiver.borrow_mut().take().expect("receiver exists");
        self.start_bridge(
            "bridge-request-to-buffer",
            BridgeRequestToBuffer::from(receiver, self.clone(), req_rb_writer, self.done.clone()),
        );
        //// 把数据从buffer发送数据到,server
        self.start_bridge(
            "bridge-buffer-to-backend",
            BridgeBufferToWriter::from(req_rb_reader, w, self.done.clone()),
        );

        //// 从response读取数据写入items
        self.start_bridge(
            "bridge-backend-to-local",
            BridgeResponseToLocal::from(
                r,
                self.clone(),
                parser.clone(),
                resp_buffer,
                self.done.clone(),
            ),
        );
    }
    pub fn bridge_no_reply<R, W>(self: Arc<Self>, req_buffer: usize, mut r: R, w: W)
    where
        W: AsyncWrite + Unpin + Send + Sync + 'static,
        R: AsyncRead + Unpin + Send + 'static,
    {
        println!("noreply bridaged");
        self.check_bridge();
        let (req_rb_writer, req_rb_reader) = RingBuffer::with_capacity(req_buffer).into_split();
        // 把数据从request同步到buffer
        let receiver = self.receiver.borrow_mut().take().expect("receiver exists");
        tokio::spawn(super::BridgeRequestToBuffer::from(
            receiver,
            self.clone(),
            req_rb_writer,
            self.done.clone(),
        ));
        // 把数据从buffer发送数据到,server
        tokio::spawn(super::BridgeBufferToWriter::from(
            req_rb_reader,
            w,
            self.done.clone(),
        ));

        tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            loop {
                match r.read(&mut buf).await {
                    Ok(n) => {
                        if n > 0 {
                            continue;
                        } else {
                            // EOF
                            break;
                        }
                    }
                    // TODO
                    Err(_e) => break,
                }
            }
        });
    }
    fn start_bridge<F>(&self, name: &'static str, future: F)
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        tokio::spawn(async move {
            println!("{} bridge task started", name);
            match future.await {
                Ok(_) => {
                    println!("{} bridge task complete", name);
                }
                Err(e) => {
                    println!("{} bridge task complete with error:{:?}", name, e);
                }
            };
        });
    }

    pub fn is_complete(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }
    // 在complete从true变成false的时候，需要将mpmc进行初始化。
    // 满足以下所有条件之后，初始化返回成功
    // 1. 所有item没有在处于waker在等待数据读取
    // 2. 所有的状态都处于Init状态
    // 3. 关闭senders的channel
    // 4. 重新初始化senders与receivers
    pub fn reset(&self) -> bool {
        false
    }
}

use super::RequestData;
impl Request for Arc<MpmcRingBufferStream> {
    fn on_received(&self, id: usize, seq: usize) {
        self.bind_seq(id, seq);
    }
}
impl Response for Arc<MpmcRingBufferStream> {
    // 获取已经被全部读取的字节的位置
    #[inline]
    fn load_offset(&self) -> usize {
        self.offset.0.load()
    }
    // 在从response读取的数据后调用。
    fn on_received(&self, seq: usize, first: RingSlice) {
        self.place_response(seq, first);
    }
}

impl IdAsyncRead for MpmcRingBufferStream {
    fn poll_read(&self, id: usize, cx: &mut Context, buf: &mut ReadBuf) -> Poll<Result<()>> {
        self.poll_read(id, cx, buf)
    }
}
impl IdAsyncWrite for MpmcRingBufferStream {
    fn poll_write(&self, id: usize, cx: &mut Context, buf: &[u8]) -> Poll<Result<()>> {
        self.poll_write(id, cx, buf)
    }
    fn poll_shutdown(&self, id: usize, cx: &mut Context) -> Poll<Result<()>> {
        self.poll_shutdown(id, cx)
    }
}
impl IdAsyncRead for Arc<MpmcRingBufferStream> {
    fn poll_read(&self, id: usize, cx: &mut Context, buf: &mut ReadBuf) -> Poll<Result<()>> {
        (**self).poll_read(id, cx, buf)
    }
}
impl IdAsyncWrite for Arc<MpmcRingBufferStream> {
    fn poll_write(&self, id: usize, cx: &mut Context, buf: &[u8]) -> Poll<Result<()>> {
        (**self).poll_write(id, cx, buf)
    }
    fn poll_shutdown(&self, id: usize, cx: &mut Context) -> Poll<Result<()>> {
        (**self).poll_shutdown(id, cx)
    }
}
