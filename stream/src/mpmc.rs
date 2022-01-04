use std::future::Future;
use std::io::{Error, ErrorKind::*, Result};
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use super::status::Status;
use crate::{
    Address, Addressed, AtomicWaker, Notify, Request, RequestHandler, ResponseHandler, Snapshot,
};
use ds::{BitMap, CacheAligned, SeqOffset};
use metrics::MetricId;
use protocol::{Protocol, RequestId, Response};

use futures::ready;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::Instant;

use crate::{bounded, Receiver, Sender};

unsafe impl Send for MpmcStream {}
unsafe impl Sync for MpmcStream {}

// 支持并发读取的stream
pub struct MpmcStream {
    items: Vec<CacheAligned<Status>>,
    waker: AtomicWaker,
    bits: BitMap,

    // idx: 是seq % seq_cids.len()。因为seq是自增的，而且seq_cids.len() == items.len()
    // 用来当cache用。会通过item status进行double check
    seq_cids: Vec<CacheAligned<AtomicUsize>>,
    seq_mask: usize,

    // 已经成功读取response的最小的offset，在ReadRrom里面使用
    offset: CacheAligned<SeqOffset>,

    // 1. RequestHandler
    // 2. ResponseHandler
    runnings: AtomicIsize,

    // resp_num - req_num就是正在处理中的请求。用来检查timeout
    // 接入到的请求数量
    req_num: CacheAligned<AtomicUsize>,
    // 成功返回的请求数量
    resp_num: CacheAligned<AtomicUsize>,

    noreply_tx: Sender<Request>,
    noreply_rx: Receiver<Request>,

    done: AtomicBool,
    closed: AtomicBool,

    // 持有的远端资源地址（ip:port）
    addr: String,
    metric_id: MetricId,
}

impl MpmcStream {
    // id必须小于parallel
    pub fn with_capacity(parallel: usize, biz: &str, addr: &str, rsrc: protocol::Resource) -> Self {
        let parallel = parallel.next_power_of_two();
        assert!(parallel <= super::MAX_CONNECTIONS);
        let items = (0..parallel)
            .map(|id| CacheAligned::new(Status::new(id)))
            .collect();
        let seq_cids = (0..parallel)
            .map(|_| CacheAligned::new(AtomicUsize::new(0)))
            .collect();

        let (tx, rx) = bounded(32);

        Self {
            items: items,
            waker: AtomicWaker::new(),
            bits: BitMap::with_capacity(parallel),
            seq_cids: seq_cids,
            seq_mask: parallel - 1,
            offset: CacheAligned::new(SeqOffset::with_capacity(parallel)),
            done: AtomicBool::new(true),
            closed: AtomicBool::new(false),
            runnings: AtomicIsize::new(0),
            req_num: CacheAligned::new(AtomicUsize::new(0)),
            resp_num: CacheAligned::new(AtomicUsize::new(0)),
            noreply_tx: tx,
            noreply_rx: rx,

            addr: addr.to_string(),
            metric_id: metrics::register!(rsrc.name(), biz, addr),
        }
    }
    // 如果complete为true，则快速失败
    #[inline(always)]
    fn check(&self, _cid: usize) -> Result<()> {
        if self.done.load(Ordering::Acquire) {
            Err(Error::new(NotConnected, "mpmc is done"))
        } else {
            Ok(())
        }
    }
    #[inline(always)]
    pub fn poll_next(&self, cid: usize, cx: &mut Context) -> Poll<Result<(RequestId, Response)>> {
        self.check(cid)?;
        let item = self.get_item(cid);
        let data = ready!(item.poll_read(cx))?;
        log::debug!("next. cid:{} rid:{:?} ", cid, data);
        self.resp_num.0.fetch_add(1, Ordering::Relaxed);
        Poll::Ready(Ok(data))
    }
    #[inline]
    pub fn response_done(&self, cid: usize, response: &Response) {
        if let Ok(_) = self.check(cid) {
            let item = self.get_item(cid);
            item.response_done();
            let (start, end) = response.location();
            self.offset.0.insert(start, end);
        }
        log::debug!("done. cid:{} response: {}", cid, response);
    }
    // 释放cid的资源
    #[inline]
    pub fn shutdown(&self, cid: usize) {
        log::debug!("mpmc: poll shutdown. cid:{}", cid);
        self.bits.unmark(cid);
        self.get_item(cid).reset();
    }
    #[inline]
    pub fn poll_write(&self, cid: usize, _cx: &mut Context, buf: &Request) -> Poll<Result<()>> {
        // 连接关闭后，只停写，现有请求可以继续处理。
        let write_begin = Instant::now();
        if self.closed.load(Ordering::Relaxed) {
            return Poll::Ready(Err(Error::new(NotFound, "mpmc closed")));
        }
        self.check(cid)?;
        log::debug!("write cid:{} req:{}", cid, buf);
        if buf.noreply() {
            self.noreply_tx
                .try_send(buf.clone())
                .map_err(|e| Error::new(Interrupted, format!("noreply data chan full:{:?}", e)))?;
        } else {
            let item = self.get_item(cid);
            item.place_request(buf);
            self.bits.mark(cid);
        }
        self.waker.wake();
        self.req_num.0.fetch_add(1, Ordering::Relaxed);
        log::debug!(
            "write complete cid:{} len:{} cost: {:?}",
            cid,
            buf.len(),
            Instant::now().duration_since(write_begin)
        );
        Poll::Ready(Ok(()))
    }
    #[inline(always)]
    fn get_item(&self, cid: usize) -> &Status {
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
    #[inline(always)]
    fn place_response(&self, seq: usize, response: protocol::Response) {
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
        // 上一个线程已经结束。所以reset不会有date race
        self.offset.0.reset();
        Self::start_bridge(
            self.clone(),
            notify.clone(),
            RequestHandler::from(self.clone(), w, self.metric_id.id()),
        );

        //// 从response读取数据写入items
        Self::start_bridge(
            self.clone(),
            notify.clone(),
            ResponseHandler::from(r, self.clone(), parser, self.metric_id.id()),
        );
    }
    fn start_bridge<F, N>(self: Arc<Self>, notify: N, future: F)
    where
        F: Future<Output = Result<()>> + Send + 'static,
        N: Notify + Send + 'static,
    {
        rt::spawn(async move {
            let runnings = self.runnings.fetch_add(1, Ordering::Release) + 1;
            log::debug!("{}-th task started, {} ", runnings, self.metric_id.name());
            if let Err(e) = future.await {
                log::error!("task complete. error:{:?} {}", e, self.metric_id.name());
            };
            self.done.store(true, Ordering::Release);
            let runnings = self.runnings.fetch_add(-1, Ordering::Release) - 1;
            self.waker.wake();
            if runnings == 0 {
                log::info!("all handler completed. {}", self.metric_id.name());
                // sleep一段时间，减少因为reset导致status在cas时的状态冲突。
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                self.reset_item_status();
                if !self.closed.load(Ordering::Acquire) {
                    notify.notify();
                }
            }
        });
    }

    fn reset_item_status(&self) {
        for item in self.items.iter() {
            item.0.reset();
        }
    }
    // 在资源被下线后，标识状态。当前请求继续，在poll_write时，判断closed状态，不再接收新后续请求。
    pub(crate) fn try_close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        log::info!("{} finished. stream will be closed later", self.addr);
    }
    // 通常是某个资源被注释时调用。
    pub(crate) fn shutdown_all(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.mark_done();
    }

    pub(crate) fn load_ping_pong(&self) -> (usize, usize) {
        (
            self.req_num.0.load(Ordering::Relaxed),
            self.resp_num.0.load(Ordering::Relaxed),
        )
    }
    #[inline(always)]
    pub(crate) fn done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }

    pub(crate) fn mark_done(&self) {
        self.done.store(true, Ordering::Release);
        self.waker.wake();
    }
    pub(crate) fn address(&self) -> &str {
        &self.addr
    }
    #[inline(always)]
    pub(crate) fn metric_id(&self) -> usize {
        self.metric_id.id()
    }
}

impl Drop for MpmcStream {
    fn drop(&mut self) {
        log::info!("{} closed.", self.addr);
    }
}

impl crate::handler::request::Handler for Arc<MpmcStream> {
    #[inline(always)]
    fn take(&self, cid: usize, seq: usize) -> Option<(usize, Request)> {
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
        let snapshot: Vec<usize> = self.bits.take();
        ss.push_all(snapshot);
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
    #[inline(always)]
    fn running(&self) -> bool {
        !self.done()
    }
}
impl crate::handler::response::Handler for Arc<MpmcStream> {
    // 获取已经被全部读取的字节的位置
    #[inline]
    fn load_read(&self) -> usize {
        self.offset.0.span()
    }
    // 在从response读取的数据后调用。
    #[inline]
    fn on_received(&self, seq: usize, first: protocol::Response) {
        self.place_response(seq, first);
    }
    #[inline(always)]
    fn running(&self) -> bool {
        !self.done()
    }
}

impl Addressed for Arc<MpmcStream> {
    fn addr(&self) -> Address {
        Address::from(self.addr.clone())
    }
}
