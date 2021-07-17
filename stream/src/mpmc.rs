use std::future::Future;
use std::io::{Error, ErrorKind, Result};
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use super::status::*;
use crate::{
    AtomicWaker, BridgeRequestToBackend, BridgeResponseToLocal, Request, RequestHandler,
    ResponseData, ResponseHandler,
};
use ds::{BitMap, RingSlice, SeqOffset};
use protocol::Protocol;

use cache_line_size::CacheAligned;
use futures::ready;
use tokio::io::{AsyncRead, AsyncWrite};

unsafe impl Send for MpmcRingBufferStream {}
unsafe impl Sync for MpmcRingBufferStream {}

// 支持并发读取的stream
pub struct MpmcRingBufferStream {
    items: Vec<CacheAligned<Item>>,
    waker: AtomicWaker,
    bits: BitMap,

    // idx: 是seq % seq_cids.len()。因为seq是自增的，而且seq_cids.len() == items.len()
    // 用来当cache用。会通过item status进行double check
    seq_cids: Vec<CacheAligned<AtomicUsize>>,
    seq_mask: usize,

    // 已经成功读取response的最小的offset，在ReadRrom里面使用
    offset: CacheAligned<SeqOffset>,

    // 在运行当中的线程数。一共会有三个
    // 1. BridgeRequestToBuffer: 把request数据从receiver读取到本地的buffer
    // 2. BridgeBufferToWriter:  把request数据从本地的buffer写入到backend server
    // 3. BridgeResponseToLocal: 把response数据从backend server读取到items
    runnings: Arc<AtomicIsize>,

    done: Arc<AtomicBool>,
}

impl MpmcRingBufferStream {
    // id必须小于parallel
    pub fn with_capacity(parallel: usize, done: Arc<AtomicBool>) -> Self {
        let parallel = parallel.next_power_of_two();
        assert!(parallel <= super::MAX_CONNECTIONS);
        let items = (0..parallel)
            .map(|id| CacheAligned(Item::new(id)))
            .collect();
        let seq_cids = (0..parallel)
            .map(|_| CacheAligned(AtomicUsize::new(0)))
            .collect();

        Self {
            items: items,
            waker: AtomicWaker::new(),
            bits: BitMap::with_capacity(parallel),
            seq_cids: seq_cids,
            seq_mask: parallel - 1,
            offset: CacheAligned(SeqOffset::with_capacity(parallel)),
            done: done,
            runnings: Arc::new(AtomicIsize::new(0)),
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
    #[inline(always)]
    pub fn poll_next(&self, cid: usize, cx: &mut Context) -> Poll<Result<ResponseData>> {
        ready!(self.poll_check(cid))?;
        let item = self.get_item(cid);
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
        let item = self.get_item(cid);
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
    #[inline]
    pub fn poll_write(&self, cid: usize, _cx: &mut Context, buf: &Request) -> Poll<Result<()>> {
        ready!(self.poll_check(cid))?;
        log::debug!(
            "mpmc: write cid:{} len:{} id:{:?}, noreply:{}",
            cid,
            buf.len(),
            buf.id(),
            buf.noreply()
        );

        self.get_item(cid).place_request(buf);
        self.bits.mark(cid);
        self.waker.wake();
        log::debug!("mpmc: write complete cid:{} len:{} ", cid, buf.len());
        Poll::Ready(Ok(()))
    }
    #[inline(always)]
    fn get_item(&self, cid: usize) -> &Item {
        debug_assert!(cid < self.items.len());
        unsafe { &self.items.get_unchecked(cid).0 }
    }
    #[inline(always)]
    fn mask_seq(&self, seq: usize) -> usize {
        //self.seq_cids.len() - 1) & seq
        self.seq_mask & seq
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
                    if it.0.seq() == seq {
                        item = &it.0;
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
        self.reset_item_status();
        Self::start_bridge(
            self.clone(),
            builder.clone(),
            "bridge-send-req",
            BridgeRequestToBackend::from(req_buffer, self.clone(), w, self.done.clone()),
        );

        //// 从response读取数据写入items
        Self::start_bridge(
            self.clone(),
            builder.clone(),
            "bridge-recv-response",
            BridgeResponseToLocal::from(r, self.clone(), parser, resp_buffer, self.done.clone()),
        );
    }
    fn start_bridge<F>(
        self: Arc<Self>,
        _builder: Arc<BackendBuilder>,
        _name: &'static str,
        future: F,
    ) where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        let runnings = self.runnings.clone();
        tokio::spawn(async move {
            runnings.fetch_add(1, Ordering::Release);
            log::debug!(
                "{} bridge task started, runnings = {}",
                _name,
                runnings.load(Ordering::Acquire)
            );
            match future.await {
                Ok(_) => {
                    log::debug!("{} bridge task complete", _name);
                }
                Err(_e) => {
                    log::debug!("{} bridge task complete with error:{:?}", _name, _e);
                }
            };
            runnings.fetch_add(-1, Ordering::Release);
            log::debug!(
                "{} bridge task completed, runnings = {}",
                _name,
                runnings.load(Ordering::Acquire)
            );
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
    fn shutdown_item_status(&self) {
        for item in self.items.iter() {
            item.0.shutdown();
        }
    }

    fn reset_item_status(&self) {
        for item in self.items.iter() {
            item.0.reset();
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
        debug_assert!(!self.done.load(Ordering::Acquire));
        let runnings = self.runnings.load(Ordering::Acquire);
        debug_assert!(runnings >= 0);
        self.done.store(true, Ordering::Release);

        self.shutdown_item_status();

        runnings == 0
    }
}

use crate::BackendBuilder;
use std::thread::sleep;
use std::time::Duration;

use crate::req_handler::Snapshot;
impl RequestHandler for Arc<MpmcRingBufferStream> {
    #[inline(always)]
    fn take(&self, cid: usize, seq: usize) -> Request {
        self.bind_seq(cid, seq);
        self.get_item(cid).take_request(seq)
    }
    #[inline]
    fn poll_fill_snapshot(&self, cx: &mut Context, ss: &mut Snapshot) -> Poll<()> {
        debug_assert_eq!(ss.len(), 0);
        let bits: Vec<usize> = self.bits.snapshot();
        for i in 0..bits.len() {
            let mut one = unsafe { *bits.get_unchecked(i) };
            while one > 0 {
                let zeros = one.trailing_zeros() as usize;
                let cid = i * std::mem::size_of::<usize>() + zeros;
                ss.push(cid);
                one = one & !(1 << zeros);
            }
        }
        self.bits.unmark_all(&bits);
        // 没有获取到数据的时候，并且done为false，返回pending.
        if ss.len() == 0 && !self.done.load(Ordering::Acquire) {
            self.waker.register_by_ref(&cx.waker());
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}
impl ResponseHandler for Arc<MpmcRingBufferStream> {
    // 获取已经被全部读取的字节的位置
    #[inline]
    fn load_offset(&self) -> usize {
        self.offset.0.load()
    }
    #[inline]
    // 在从response读取的数据后调用。
    fn on_received(&self, seq: usize, first: RingSlice) {
        self.place_response(seq, first);
    }
}
