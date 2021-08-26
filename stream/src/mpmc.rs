use std::future::Future;
use std::io::{Error, ErrorKind, Result};
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use super::status::*;
use crate::{
    AtomicWaker, BridgeRequestToBackend, BridgeResponseToLocal, Notify, Request, RequestHandler,
    ResponseData, ResponseHandler,
};
use ds::{BitMap, RingSlice, SeqOffset};
use protocol::Protocol;

use cache_line_size::CacheAligned;
use futures::ready;
use tokio::io::{AsyncRead, AsyncWrite};

//use tokio::sync::mpsc::{channel, Receiver, Sender};
use crossbeam_channel::{bounded, Receiver, Sender};

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
    // 1. BridgeRequestToBackend: 把请求发送到backend
    // 2. BridgeResponseToLocal: 把response数据从backend server读取到items
    runnings: AtomicIsize,

    // resp_num - req_num就是正在处理中的请求。用来检查timeout
    // 接入到的请求数量
    req_num: CacheAligned<AtomicUsize>,
    // 成功返回的请求数量
    resp_num: CacheAligned<AtomicUsize>,

    noreply_tx: Sender<Request>,
    noreply_rx: Receiver<Request>,

    done: Arc<AtomicBool>,
    closed: AtomicBool,
}

impl MpmcRingBufferStream {
    // id必须小于parallel
    pub fn with_capacity(parallel: usize) -> Self {
        let parallel = parallel.next_power_of_two();
        assert!(parallel <= super::MAX_CONNECTIONS);
        let items = (0..parallel)
            .map(|id| CacheAligned(Item::new(id)))
            .collect();
        let seq_cids = (0..parallel)
            .map(|_| CacheAligned(AtomicUsize::new(0)))
            .collect();

        let (tx, rx) = bounded(2048);

        Self {
            items: items,
            waker: AtomicWaker::new(),
            bits: BitMap::with_capacity(parallel),
            seq_cids: seq_cids,
            seq_mask: parallel - 1,
            offset: CacheAligned(SeqOffset::with_capacity(parallel)),
            done: Arc::new(AtomicBool::new(true)),
            closed: AtomicBool::new(false),
            runnings: AtomicIsize::new(0),
            req_num: CacheAligned(AtomicUsize::new(0)),
            resp_num: CacheAligned(AtomicUsize::new(0)),
            noreply_tx: tx,
            noreply_rx: rx,
        }
    }
    // 如果complete为true，则快速失败
    #[inline(always)]
    fn check(&self, _cid: usize) -> Result<()> {
        if self.done.load(Ordering::Acquire) {
            Err(Error::new(ErrorKind::NotConnected, "mpmc is done"))
        } else {
            Ok(())
        }
    }
    #[inline(always)]
    pub fn poll_next(&self, cid: usize, cx: &mut Context) -> Poll<Result<ResponseData>> {
        self.check(cid)?;
        let item = self.get_item(cid);
        let data = ready!(item.poll_read(cx))?;
        log::debug!(
            "next. cid:{} len:{} rid:{} ",
            cid,
            data.data().len(),
            data.rid()
        );
        self.resp_num.0.fetch_add(1, Ordering::Relaxed);
        Poll::Ready(Ok(data))
    }
    pub fn response_done(&self, cid: usize, response: &ResponseData) {
        let item = self.get_item(cid);
        item.response_done(response.seq());
        let (start, end) = response.data().location();
        self.offset.0.insert(start, end);
        log::debug!(
            "done. cid:{} loc:({}->{}). {}",
            cid,
            start,
            end,
            response.rid()
        );
    }
    // 释放cid的资源
    pub fn shutdown(&self, cid: usize) {
        log::debug!("mpmc: poll shutdown. cid:{}", cid);
        self.get_item(cid).reset();
    }
    #[inline]
    pub fn poll_write(&self, cid: usize, _cx: &mut Context, buf: &Request) -> Poll<Result<()>> {
        self.check(cid)?;
        log::debug!(
            "write cid:{} len:{} id:{}, noreply:{}",
            cid,
            buf.len(),
            buf.id(),
            buf.noreply(),
        );
        if buf.noreply() {
            self.noreply_tx.try_send(buf.clone()).map_err(|e| {
                Error::new(
                    ErrorKind::Interrupted,
                    format!("noreply data chan full:{:?}", e.to_string()),
                )
            })?;
        } else {
            let item = self.get_item(cid);
            item.place_request(buf);
            self.bits.mark(cid);
        }
        self.waker.wake();
        self.req_num.0.fetch_add(1, Ordering::Relaxed);
        log::debug!("write complete cid:{} len:{} ", cid, buf.len());
        // noreply请求要等request sent之后才能返回。
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
        assert_eq!(self.runnings.load(Ordering::Acquire), 0);
        assert!(self.done.load(Ordering::Acquire));
    }

    // 构建一个ring buffer.
    // 一共2个线程。
    // 线程A: 把request data数据从item写入backend
    // 线程B：把response数据从server中读取，并且place到item的response中
    pub fn bridge<R, W, P, N>(self: Arc<Self>, parser: P, r: R, w: W, notify: N)
    where
        W: AsyncWrite + Unpin + Send + Sync + 'static,
        R: AsyncRead + Unpin + Send + 'static,
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
        N: Notify + Clone + Send + 'static,
    {
        self.check_bridge();
        self.done.store(false, Ordering::Release);
        std::sync::atomic::fence(Ordering::AcqRel);
        self.req_num.0.store(0, Ordering::Relaxed);
        self.resp_num.0.store(0, Ordering::Relaxed);
        Self::start_bridge(
            self.clone(),
            notify.clone(),
            "bridge-send-req",
            BridgeRequestToBackend::from(self.clone(), w, self.done.clone()),
        );

        //// 从response读取数据写入items
        Self::start_bridge(
            self.clone(),
            notify.clone(),
            "bridge-recv-response",
            BridgeResponseToLocal::from(r, self.clone(), parser, self.done.clone()),
        );
    }
    fn start_bridge<F, N>(self: Arc<Self>, notify: N, _name: &'static str, future: F)
    where
        F: Future<Output = Result<()>> + Send + 'static,
        N: Notify + Send + 'static,
    {
        tokio::spawn(async move {
            let runnings = self.runnings.fetch_add(1, Ordering::Release) + 1;
            log::info!("{} bridge task started, runnings = {}", _name, runnings);
            match future.await {
                Ok(_) => {
                    log::info!("mpmc-task: {} complete", _name);
                }
                Err(_e) => {
                    log::error!("mpmc-task: {} complete with error:{:?}", _name, _e);
                }
            };
            self.done.store(true, Ordering::Release);
            let runnings = self.runnings.fetch_add(-1, Ordering::Release) - 1;
            log::info!(
                "mpmc-task: {} task completed, runnings = {} done:{}",
                _name,
                runnings,
                self.done.load(Ordering::Acquire)
            );
            self.waker.wake();
            if runnings == 0 {
                log::info!("mpmc-task: all threads attached completed. reset item status");
                self.reset_item_status();
                if !self.closed.load(Ordering::Acquire) {
                    notify.notify();
                    log::info!("mpmc-task: stream inited, try to connect");
                }
            }
        });
    }

    fn reset_item_status(&self) {
        for item in self.items.iter() {
            item.0.reset();
        }
    }
    // 通常是某个资源被注释时调用。
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.done.store(true, Ordering::Release);
        self.waker.wake();
        log::warn!("close: closing called");
    }

    pub(crate) fn load_ping_ping(&self) -> (usize, usize) {
        (
            self.req_num.0.load(Ordering::Relaxed),
            self.resp_num.0.load(Ordering::Relaxed),
        )
    }
    pub(crate) fn done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }

    pub(crate) fn mark_done(&self) {
        self.done.store(true, Ordering::Release);
        self.waker.wake();
    }
}

impl Drop for MpmcRingBufferStream {
    fn drop(&mut self) {
        self.close();
    }
}

use crate::req_handler::Snapshot;
impl RequestHandler for Arc<MpmcRingBufferStream> {
    #[inline(always)]
    fn take(&self, cid: usize, seq: usize) -> Request {
        self.bind_seq(cid, seq);
        self.get_item(cid).take_request(seq)
    }
    #[inline(always)]
    fn sent(&self, _cid: usize, _seq: usize, req: &Request) {
        if req.noreply() {
            // noreply的请求，发送即接收
            self.resp_num.0.fetch_add(1, Ordering::Relaxed);
        }
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
        for _ in 0..8 {
            match self.noreply_rx.try_recv() {
                Ok(req) => ss.push_one(req),
                Err(_) => break,
            }
        }
        // 没有获取到数据的时候，并且done为false，返回pending.
        if ss.len() == 0 && !self.done.load(Ordering::Acquire) {
            self.waker.register(&cx.waker());
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
    fn wake(&self) {
        for item in self.items.iter() {
            item.0.try_wake();
        }
    }
}
