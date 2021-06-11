use std::io::{Error, ErrorKind, Result};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use super::ring::MonoRingBuffer;
use super::status::*;
use super::waker::MyWaker;
use super::{BuffCopyTo, BuffReadFrom, IdAsyncRead, IdAsyncWrite, Request, Response, RingSlice};

use protocol::ResponseParser;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};

use futures::ready;

unsafe impl Send for MpscRingBufferStream {}
unsafe impl Sync for MpscRingBufferStream {}

// 支持并发读取的stream
pub struct MpscRingBufferStream {
    req_buff: MonoRingBuffer,
    resp_buff: usize,
    items: Vec<Item>,

    // 所有的字节都已经读取完成，等待新的写入。
    // 在BuffCopyTo的poll时会阻塞在该waker中，每次有写入时，调用wakeup
    req_read_waker: MyWaker,
    // 写入空间不够，在write_req时，可能会等待，在consume时会释放空间，调用wakeup
    req_write_waker: MyWaker,

    // idx: 是seq % seq_cids.len()。因为seq是自增的，而且seq_cids.len() == items.len()
    // 用来当cache用。会通过item status进行double check
    seq_cids: Vec<AtomicU32>,

    // 在从stream读取response字节时，如果空间不够，则进行park
    response_write_waker: MyWaker,

    req_seq: AtomicUsize,
    // 已经成功读取response的最小的offset，在ReadRrom里面使用
    resp_read_offset: AtomicUsize,
    resp_read_offset_ext: lockfree::map::Map<usize, usize>,

    copy_to: AtomicBool,
    copy_from: AtomicBool,

    running: AtomicBool,
    complete: AtomicBool,
}

impl MpscRingBufferStream {
    // id必须小于parallel
    pub fn with_capacity(req_buff: usize, resp_buff: usize, parallel: usize) -> Self {
        let req_buff = req_buff.next_power_of_two();
        let resp_buff = resp_buff.next_power_of_two();
        assert!(req_buff <= 8 << 20 && resp_buff <= 8 << 20);

        let parallel = parallel.next_power_of_two();
        assert!(parallel <= 32);
        let items = (0..parallel).map(|id| Item::new(id)).collect();
        let seq_cids = (0..parallel).map(|_| AtomicU32::new(0)).collect();

        Self {
            req_buff: MonoRingBuffer::with_capacity(req_buff),
            resp_buff: resp_buff,
            items: items,
            req_read_waker: MyWaker::with_capacity(parallel),
            req_write_waker: MyWaker::with_capacity(parallel),
            response_write_waker: MyWaker::with_capacity(parallel),
            req_seq: AtomicUsize::new(0),
            seq_cids: seq_cids,
            resp_read_offset: AtomicUsize::new(0),
            resp_read_offset_ext: Default::default(),
            copy_to: AtomicBool::new(false),
            copy_from: AtomicBool::new(false),
            running: AtomicBool::new(false),
            complete: AtomicBool::new(true),
        }
    }
    // 如果complete为true，则快速失败
    #[inline(always)]
    fn poll_check(&self) -> Poll<Result<()>> {
        if self.complete.load(Ordering::Relaxed) {
            Poll::Ready(Err(Error::from(ErrorKind::NotConnected)))
        } else {
            Poll::Ready(Ok(()))
        }
    }
    fn update_response_read_offset(&self, start: usize, end: usize) {
        for _ in 0..8 {
            match self.resp_read_offset.compare_exchange(
                start,
                end,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return;
                }
                Err(_offset) => {}
            }
        }
        // slow
        self.resp_read_offset_ext.insert(start, end);
    }
    pub fn poll_read(&self, cid: usize, cx: &mut Context, buf: &mut ReadBuf) -> Poll<Result<()>> {
        ready!(self.poll_check())?;
        //println!("poll read cid:{} ", cid);
        let item = unsafe { self.items.get_unchecked(cid) };
        if ready!(item.poll_read(cx, buf)) {
            let (start, end) = item.response_slice();
            self.update_response_read_offset(start, end);
        }
        Poll::Ready(Ok(()))
    }
    // 释放cid的资源
    pub fn poll_shutdown(&self, cid: usize, _cx: &mut Context) -> Poll<Result<()>> {
        debug_assert!(self.get_item(cid).is_status_init());
        Poll::Ready(Ok(()))
    }
    pub fn poll_write(&self, cid: usize, cx: &mut Context, buf: &[u8]) -> Poll<Result<()>> {
        ready!(self.poll_check())?;
        let (mut offset, len) = self.get_item(cid).offset_len();
        debug_assert!(len == 0 || len == buf.len());
        if len == 0 {
            offset = self.req_reserve(buf.len());
        }
        //println!("poll write cid:{} len:{} offset:{}", cid, buf.len(), offset);
        if self.req_buff.write(offset, buf) {
            self.on_request_write_success(cid, offset, buf.len());
            Poll::Ready(Ok(()))
        } else {
            self.on_request_write_failed(cx.waker().clone());
            Poll::Pending
        }
    }
    #[inline]
    pub fn req_reserve(&self, len: usize) -> usize {
        self.req_buff.reserve(len)
    }
    #[inline]
    fn mask_seq(&self, seq: usize) -> usize {
        seq & (self.seq_cids.len() - 1)
    }
    #[inline]
    fn get_item(&self, cid: usize) -> &Item {
        debug_assert!(cid < self.items.len());
        unsafe { self.items.get_unchecked(cid) }
    }
    // bind_seq在reorder_req_offsets中被调用。
    // 生成一个seq，并且与cid绑定。在读取response时，直接使用cid即可快速获取。
    #[inline]
    fn bind_seq(&self, cid: usize) {
        // 只有一个线程在更新，不需要做cas操作。
        let seq = self.req_seq.fetch_add(1, Ordering::AcqRel);
        let seq_idx = self.mask_seq(seq);
        unsafe {
            // 绑定
            self.seq_cids
                .get_unchecked(seq_idx)
                .store(cid as u32, Ordering::Release);
            self.get_item(cid).bind_seq(seq)
        };
    }
    // req写入时，并不是连续的，会造成空洞，所以获取数据时，只能获取连接的数据。
    // 因为把所有的offset按照顺序排列
    // 排列的过程会把offset、cid、与seq进行绑定。
    fn reorder_req_offsets(&self, current: usize) -> usize {
        use std::collections::HashMap;
        let mut offsets = HashMap::with_capacity(32);
        for p in self.items.iter() {
            if p.is_status_received() {
                let (offset, len) = p.offset_len();
                offsets.insert(offset, (p.cid, len));
            }
        }
        let mut next = current;
        while let Some(&(cid, len)) = offsets.get(&next) {
            self.bind_seq(cid);
            next += len;
        }
        next
    }
    // 预读取已经成功写入的字节流, 及其对应的id。
    pub fn fetch_req_stream(&self) -> Option<&[u8]> {
        self.req_buff.fetch(self)
    }
    fn place_response(&self, seq: usize, response: RingSlice) {
        unsafe {
            let seq_idx = self.mask_seq(seq);
            let cid = self.seq_cids.get_unchecked(seq_idx).load(Ordering::Acquire) as usize;
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

    // 写入失败，说明req buf不足。需要等等读取成功的通知
    pub fn on_request_write_failed(&self, waker: Waker) {
        self.req_read_waker.wait_on(waker);
    }
    // 通知buf，n个字节已经读取并且消费完成。n的大小通常是 as_bytes返回的大小
    // 写失败时，会等待读取成功通知。
    pub fn on_request_read_success(&self, len: usize) {
        self.req_buff.consume(len);
        self.req_read_waker.notify();
    }

    // 写入成功
    // 1. 更新状态。需要更新offset、len
    // 2. 通知request reader，有数据可以读取
    // 3. 不用更新w_offset。因为写入是先预留再写入
    pub fn on_request_write_success(&self, cid: usize, offset: usize, len: usize) {
        unsafe {
            self.items.get_unchecked(cid).req_received(offset, len);
        }
        self.req_write_waker.notify();
    }
    // 读取失败，等待写入成功通知。
    pub fn on_request_read_failed(&self, waker: Waker) {
        //println!("read request failed");
        self.req_write_waker.wait_on(waker);
    }

    // response buffer已经满了。需要等待读取成功后，变更seq，调整相应的read
    pub fn on_response_write_failed(&self, _waker: Waker) {
        panic!("response full");
    }
    pub fn on_response_write_success(&self, seq: usize) {}
    //pub fn on_response_read_failed(&self, waker: Waker) {}
    //pub fn on_response_read_success(&self, len: usize) {}

    fn check_bridge(&self) {
        // 必须是已经complete才能重新bridage
        self.complete
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .expect("bridge an uncompleted stream");
    }

    pub fn bridge<R, W, P>(self: Arc<Self>, r: R, w: W, parser: P)
    where
        W: AsyncWrite + Unpin + Send + Sync + 'static,
        R: AsyncRead + Unpin + Send + 'static,
        P: Unpin + Send + Sync + ResponseParser + Default + 'static,
    {
        self.check_bridge();
        let req = self.clone();
        tokio::spawn(async move { req.copy_to(w).await });

        let resp = self.clone();
        tokio::spawn(async move { resp.copy_from(r, parser).await });
    }
    pub fn bridge_no_reply<R, W, P>(self: Arc<Self>, mut r: R, w: W, _parser: P)
    where
        W: AsyncWrite + Unpin + Send + Sync + 'static,
        R: AsyncRead + Unpin + Send + 'static,
        P: Unpin + Send + Sync + ResponseParser + Default + 'static,
    {
        self.check_bridge();
        let req = self.clone();
        tokio::spawn(async move { req.copy_to(w).await });

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

    // 把当前的req buf 写入到w
    pub fn copy_to<W>(&self, w: W) -> BuffCopyTo<Self, W> {
        self.copy_to
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .expect("copy_to executed more than once");
        BuffCopyTo::from(self, w)
    }

    // 从r读取数据到response buf
    pub fn copy_from<R, P>(&self, r: R, parser: P) -> BuffReadFrom<Self, R, P> {
        self.copy_from
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .expect("copy_from executed more than once");
        BuffReadFrom::from(self, r, parser, self.resp_buff)
    }

    fn on_io_error(&self, err: Error) {
        // TODO
        if !self.complete.load(Ordering::Acquire) {
            println!("io error:{:?}", err);
            self.complete.store(true, Ordering::Release);
        }
    }
    pub fn running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }
    pub fn is_complete(&self) -> bool {
        self.complete.load(Ordering::Acquire)
    }
    pub fn try_complete(&self) {}
}

impl super::ring::OffsetSequence for MpscRingBufferStream {
    fn next(&self, offset: usize) -> usize {
        self.reorder_req_offsets(offset)
    }
}

impl Request for MpscRingBufferStream {
    // request发送成功时调用. num是发送成功的字节数量。
    // 这个num并不对应于一个完整的request
    fn on_success(&self, num: usize) {
        self.on_request_read_success(num)
    }
    // 没有可发送的请求，后续由waker唤醒
    fn on_empty(&self, waker: Waker) {
        //println!("read empty from request buffer");
        self.on_request_read_failed(waker);
    }
    // 获取连续发送的一段request请求的内容。
    fn stream_fetch(&self) -> Option<&[u8]> {
        self.fetch_req_stream()
    }
    fn on_error(&self, err: Error) {
        self.on_io_error(err);
    }
}
impl Response for MpscRingBufferStream {
    // 获取已经被全部读取的字节的位置
    #[inline]
    fn load_read_offset(&self) -> usize {
        let mut offset = self.resp_read_offset.load(Ordering::Relaxed);
        while let Some(removed) = self.resp_read_offset_ext.remove(&offset) {
            offset = *removed.val();
        }
        offset
    }
    // 没有足够的空间存储response的返回值。
    fn on_full(&self, waker: Waker) {
        self.on_response_write_failed(waker);
    }
    // 在从response读取的数据后调用。
    fn on_response(&self, seq: usize, first: RingSlice) {
        self.place_response(seq, first);
        self.on_response_write_success(seq)
    }
    fn on_error(&self, err: Error) {
        self.on_io_error(err);
    }
}

impl IdAsyncRead for MpscRingBufferStream {
    fn poll_read(&self, id: usize, cx: &mut Context, buf: &mut ReadBuf) -> Poll<Result<()>> {
        self.poll_read(id, cx, buf)
    }
}
impl IdAsyncWrite for MpscRingBufferStream {
    fn poll_write(&self, id: usize, cx: &mut Context, buf: &[u8]) -> Poll<Result<()>> {
        self.poll_write(id, cx, buf)
    }
}
impl IdAsyncRead for Arc<MpscRingBufferStream> {
    fn poll_read(&self, id: usize, cx: &mut Context, buf: &mut ReadBuf) -> Poll<Result<()>> {
        (**self).poll_read(id, cx, buf)
    }
}
impl IdAsyncWrite for Arc<MpscRingBufferStream> {
    fn poll_write(&self, id: usize, cx: &mut Context, buf: &[u8]) -> Poll<Result<()>> {
        (**self).poll_write(id, cx, buf)
    }
}
