use std::cell::RefCell;
use std::future::Future;
use std::io::{Error, ErrorKind, Result};
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use ds::{RingBuffer, RingSlice, SeqOffset};

use super::status::*;
use super::RequestData;
use crate::{
    BridgeBufferToWriter, BridgeRequestToBuffer, BridgeResponseToLocal, Request, RequestHandler,
    ResponseData, ResponseHandler,
};

use protocol::Protocol;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
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

    // 在运行当中的线程数。一共会有三个
    // 1. BridgeRequestToBuffer: 把request数据从receiver读取到本地的buffer
    // 2. BridgeBufferToWriter:  把request数据从本地的buffer写入到backend server
    // 3. BridgeResponseToLocal: 把response数据从backend server读取到items
    runnings: Arc<AtomicIsize>,

    // chan是否处理reset状态, 在reset_chan与bridge方法中使用
    chan_reset: AtomicBool,
    done: Arc<AtomicBool>,
}

impl MpmcRingBufferStream {
    // id必须小于parallel
    pub fn with_capacity(parallel: usize, done: Arc<AtomicBool>) -> Self {
        let parallel = parallel.next_power_of_two();
        assert!(parallel <= 64);
        let items = (0..parallel).map(|id| Item::new(id)).collect();
        let seq_cids = (0..parallel)
            .map(|_| CacheAligned(AtomicUsize::new(0)))
            .collect();

        let (sender, receiver) = channel(parallel * 2);
        let mut sender = PollSender::new(sender);
        let senders = (0..parallel)
            .map(|_| RefCell::new((true, sender.clone())))
            .collect();
        sender.close_this_sender();
        drop(sender);

        Self {
            items: items,
            seq_cids: seq_cids,
            offset: CacheAligned(SeqOffset::from(parallel)),
            receiver: RefCell::new(Some(receiver)),
            senders: senders,
            done: done,
            runnings: Arc::new(AtomicIsize::new(0)),
            chan_reset: AtomicBool::new(false),
        }
    }
    // 如果complete为true，则快速失败
    #[inline(always)]
    fn poll_check(&self, cid: usize) -> Poll<Result<()>> {
        if self.done.load(Ordering::Acquire) {
            self.get_item(cid).shutdown();
            Poll::Ready(Err(Error::new(ErrorKind::NotConnected, "mpmc is done")))
        } else {
            Poll::Ready(Ok(()))
        }
    }
    pub fn poll_next(&self, cid: usize, cx: &mut Context) -> Poll<Result<ResponseData>> {
        ready!(self.poll_check(cid))?;
        let item = unsafe { self.items.get_unchecked(cid) };
        let data = ready!(item.poll_read(cx));
        log::debug!(
            "mpmc: data read out. cid:{} len:{} rid:{:?} ",
            cid,
            data.data().len(),
            data.rid()
        );
        Poll::Ready(Ok(data))
    }
    pub fn response_done(&self, cid: usize, response: &ResponseData) {
        let item = unsafe { self.items.get_unchecked(cid) };
        item.response_done(response.seq());
        let (start, end) = response.data().location();
        self.offset.0.insert(start, end);
        log::debug!(
            "mpmc done. cid:{} start:{} end:{} rid:{:?}",
            cid,
            start,
            end,
            response.rid()
        );
    }
    // 释放cid的资源
    pub fn poll_shutdown(&self, cid: usize, _cx: &mut Context) -> Poll<Result<()>> {
        log::debug!("mpmc: poll shutdown. cid:{}", cid);
        debug_assert!(self.get_item(cid).status_init());
        Poll::Ready(Ok(()))
    }
    pub fn poll_write(&self, cid: usize, cx: &mut Context, buf: &Request) -> Poll<Result<()>> {
        ready!(self.poll_check(cid))?;
        log::debug!(
            "mpmc: write cid:{} len:{} id:{:?}",
            cid,
            buf.len(),
            buf.id()
        );

        println!("++++++++ in mpmc req: {:?}", buf.clone().inner.data());
        let mut sender = unsafe { self.senders.get_unchecked(cid) }.borrow_mut();
        if sender.0 {
            self.get_item(cid).place_request(buf);
            sender.0 = false;
            let req = RequestData::from(cid, buf.clone());
            println!("++++++222++ in mpmc req: {:?}", req.clone().data());
            sender.1.start_send(req).ok().expect("channel closed");
        }
        ready!(sender.1.poll_send_done(cx))
            .ok()
            .expect("channel send failed");
        sender.0 = true;
        log::debug!("mpmc: write complete cid:{} len:{} ", cid, buf.len());

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
                .store(cid, Ordering::Release);
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
                        break;
                    }
                }
            }
            item.place_response(response, seq);
        }
    }

    fn check_bridge(&self) {
        // 必须是已经complete才能重新bridage
        self.done
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .expect("bridge an uncompleted stream");
        assert_eq!(self.runnings.load(Ordering::Acquire), 0);
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
        builder: Arc<BackendBuilder>,
    ) where
        W: AsyncWrite + Unpin + Send + Sync + 'static,
        R: AsyncRead + Unpin + Send + 'static,
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        self.check_bridge();
        log::debug!("request buffer size:{}", req_buffer);
        let (req_rb_writer, req_rb_reader) = RingBuffer::with_capacity(req_buffer).into_split();
        // 把数据从request同步到buffer
        let receiver = self.receiver.borrow_mut().take().expect("receiver exists");
        Self::start_bridge(
            self.clone(),
            builder.clone(),
            "bridge-request-to-buffer",
            BridgeRequestToBuffer::from(receiver, self.clone(), req_rb_writer, self.done.clone()),
        );
        //// 把数据从buffer发送数据到,server
        Self::start_bridge(
            self.clone(),
            builder.clone(),
            "bridge-buffer-to-backend",
            BridgeBufferToWriter::from(req_rb_reader, w, self.done.clone(), builder.clone()),
        );

        //// 从response读取数据写入items
        Self::start_bridge(
            self.clone(),
            builder.clone(),
            "bridge-backend-to-local",
            BridgeResponseToLocal::from(
                r,
                self.clone(),
                parser,
                resp_buffer,
                self.done.clone(),
                builder.clone(),
            ),
        );
    }
    pub fn bridge_no_reply<R, W>(
        self: Arc<Self>,
        req_buffer: usize,
        mut r: R,
        w: W,
        builder: Arc<BackendBuilder>,
    ) where
        W: AsyncWrite + Unpin + Send + Sync + 'static,
        R: AsyncRead + Unpin + Send + 'static,
    {
        log::debug!("noreply bridaged");
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
            builder.clone(),
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
    fn start_bridge<F>(
        self: Arc<Self>,
        _builder: Arc<BackendBuilder>,
        name: &'static str,
        future: F,
    ) where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        let runnings = self.runnings.clone();
        tokio::spawn(async move {
            runnings.fetch_add(1, Ordering::AcqRel);
            log::debug!("{} bridge task started", name);
            match future.await {
                Ok(_) => {
                    log::debug!("{} bridge task complete", name);
                }
                Err(e) => {
                    log::debug!("{} bridge task complete with error:{:?}", name, e);
                }
            };
            runnings.fetch_add(-1, Ordering::AcqRel);
            log::debug!("{} bridge task completed", name);
        });
    }

    fn do_close(self: Arc<Self>) {
        self.reset();
    }
    pub fn is_complete(self: Arc<Self>) -> bool {
        self.done.load(Ordering::Acquire) && self.runnings.load(Ordering::Acquire) == 0
    }
    pub fn try_complete(self: Arc<Self>) {
        if self.clone().done.load(Ordering::Acquire) {
            return;
        }
        self.clone().do_close();
        while self.clone().runnings.load(Ordering::Acquire) != 0 {
            log::debug!(
                "running threads: {}",
                self.clone().runnings.load(Ordering::Acquire)
            );
            sleep(Duration::from_secs(1));
        }
        log::debug!("all threads completed");
    }
    // done == true。所以新到达的poll_write都会直接返回
    // 不会再操作senders, 可以重置senders
    fn reset_chann(&self) {
        if !self.chan_reset.load(Ordering::Acquire) {
            let (sender, receiver) = channel(self.senders.len());
            let sender = PollSender::new(sender);
            // 删除所有的sender，则receiver会会接收到一个None，而不会阻塞
            for s in self.senders.iter() {
                let (_, mut old) = s.replace((true, sender.clone()));
                old.close_this_sender();
                drop(old);
            }
            let old = self.receiver.replace(Some(receiver));
            if let Some(o_r) = old {
                log::debug!(
                    "may be the old stream is not established, but the new one is reconnected"
                );
                drop(o_r);
            }
            self.chan_reset.store(true, Ordering::Release);
        }
    }
    // done == true
    // runnings == 0
    // 所有的线程都结束了
    // 不会再有额外的线程来更新items信息
    fn reset_item_status(&self) {
        for item in self.items.iter() {
            item.reset();
        }
    }
    // 在complete从true变成false的时候，需要将mpmc进行初始化。
    // 满足以下所有条件之后，初始化返回成功
    // 0. 三个线程全部结束
    // 1. 所有item没有在处于waker在等待数据读取
    // 2. 所有的状态都处于Init状态
    // 3. 关闭senders的channel
    // 4. 重新初始化senders与receivers
    pub fn reset(&self) -> bool {
        debug_assert!(self.done.load(Ordering::Acquire));
        let runnings = self.runnings.load(Ordering::Acquire);
        debug_assert!(runnings >= 0);

        self.reset_item_status();

        self.reset_chann();

        if runnings > 0 {
            return false;
        }

        true
    }
}

use crate::BackendBuilder;
use std::thread::sleep;
use std::time::Duration;

impl RequestHandler for Arc<MpmcRingBufferStream> {
    //fn on_received_seq(&self, id: usize, seq: usize) {
    //    self.bind_seq(id, seq);
    //}
    fn on_received(&self, id: usize, seq: usize) {
        self.bind_seq(id, seq);
        self.get_item(id).on_sent(seq);
    }
}
impl ResponseHandler for Arc<MpmcRingBufferStream> {
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
