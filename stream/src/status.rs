use crate::AtomicWaker;
use std::cell::RefCell;
use std::io::{Error, ErrorKind, Result};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering::*};
use std::task::{Context, Poll};

use crate::ResponseData;
use ds::RingSlice;
use protocol::{Request, RequestId};

#[repr(u8)]
#[derive(Clone, Copy)]
pub enum ItemStatus {
    Init = 0u8,
    RequestReceived,
    RequestSent,
    ResponseReceived, // 数据已写入
    Read,             // 已读走
}
const STATUSES: [ItemStatus; 5] = [Init, RequestReceived, RequestSent, ResponseReceived, Read];
impl From<u8> for ItemStatus {
    #[inline(always)]
    fn from(s: u8) -> Self {
        STATUSES[s as usize]
    }
}
use ItemStatus::*;
impl PartialEq<u8> for ItemStatus {
    #[inline(always)]
    fn eq(&self, other: &u8) -> bool {
        *self as u8 == *other
    }
}
impl PartialEq<ItemStatus> for u8 {
    #[inline(always)]
    fn eq(&self, other: &ItemStatus) -> bool {
        *self == *other as u8
    }
}

unsafe impl Send for ItemStatus {}
#[derive(Default)]
pub struct Item {
    id: usize,
    seq: AtomicUsize, // 用来做request与response的同步
    status: AtomicU8, // 0: 待接收请求。

    rid: RefCell<RequestId>,
    request: RefCell<Option<Request>>,

    response: RefCell<RingSlice>,
    waker: AtomicWaker,
}

unsafe impl Send for Item {}

impl Item {
    pub fn new(cid: usize) -> Self {
        Self {
            id: cid,
            status: AtomicU8::new(Init as u8),
            ..Default::default()
        }
    }
    // 把buf的指针保存下来。
    // 上面的假设待验证
    #[inline(always)]
    pub fn place_request(&self, req: &Request) {
        debug_assert_eq!(self.status.load(Acquire), ItemStatus::Init as u8);
        self.status_cas(Init as u8, RequestReceived as u8);
        *self.request.borrow_mut() = Some(req.clone());
        log::debug!("place:{:?}", self.rid.replace(req.id()));
    }
    #[inline(always)]
    pub fn take_request(&self, seq: usize) -> Request {
        self.status_cas(RequestReceived as u8, RequestSent as u8);
        log::debug!("take request. {} seq:{} ", self.rid.borrow(), seq);
        let req = self.request.borrow_mut().take().expect("take request");
        // noreply的请求不需要seq来进行request与response之间的协调
        if !req.noreply() {
            self.seq_cas(0, seq);
        }
        req
    }

    #[inline(always)]
    pub fn seq(&self) -> usize {
        self.seq.load(Acquire)
    }

    #[inline(always)]
    fn status(&self) -> u8 {
        self.status.load(Acquire)
    }

    #[inline(always)]
    fn seq_cas(&self, old: usize, new: usize) {
        match self.seq.compare_exchange(old, new, AcqRel, Acquire) {
            Ok(_) => {}
            Err(cur) => panic!("item status seq cas. {} => {} but found {}", old, new, cur),
        }
    }
    // 有两种可能的状态。
    #[inline]
    pub fn poll_read(&self, cx: &mut Context) -> Poll<Result<ResponseData>> {
        let mut status = self.status();
        if status != ResponseReceived {
            self.waker.register(cx.waker());
            // 再取一次状态。避免AtomicWaker因为memory order的问题，在与place_response出现race而
            // 丢失最后一次notify
            status = self.status();
        }
        match status.into() {
            ResponseReceived => Poll::Ready(Ok(self.take_response())),
            // 如果是Init，则有一种情况：因为stream异常，状态被重置了。直接返回错误，让client进行容错处理
            Init | Read => Poll::Ready(Err(Error::new(
                ErrorKind::ConnectionReset,
                format!("read in status:{}", status),
            ))),
            RequestReceived | RequestSent => Poll::Pending,
        }
    }
    #[inline(always)]
    fn take_response(&self) -> ResponseData {
        let response = self.response.take();
        let rid = self.rid.take();
        self.status_cas(ResponseReceived as u8, Read as u8);
        log::debug!("take response {:?}", response.location());
        ResponseData::from(response, rid, self.seq())
    }
    #[inline]
    pub fn place_response(&self, response: RingSlice, seq: usize) {
        debug_assert_eq!(seq, self.seq());
        log::debug!("place response:{:?} ", response.location());
        self.response.replace(response);

        // 1. response到达之前的状态是 RequestSent. 即请求已发出。 这是大多数场景
        // 2. 因为在req_handler中，是先发送请求，再调用
        //    bind_req来更新状态为RequestSent，有可能在这中间，response已经接收到了。此时的状态是RequestReceived。
        self.status_cas(RequestSent as u8, ResponseReceived as u8);
        self.waker.wake();
    }
    #[inline(always)]
    fn status_cas(&self, old: u8, new: u8) {
        match self.status.compare_exchange(old, new, AcqRel, Acquire) {
            Ok(_) => {}
            Err(cur) => {
                let rid = self.rid.borrow();
                log::error!(
                    "cas {} => {}, {} found. seq:{} {}",
                    old,
                    new,
                    cur,
                    self.seq(),
                    rid
                );
                panic!("cas {} => {}, {} found. {}", old, new, cur, rid);
            }
        }
    }
    // 在在sender把Response发送给client后，在Drop中会调用response_done，更新状态。
    #[inline]
    pub fn response_done(&self, seq: usize) {
        // 把状态调整为Init
        debug_assert_eq!(self.status(), Read as u8);
        self.status_cas(Read as u8, ItemStatus::Init as u8);
        // 如果seq为0，说明有一种情况，之前连接进行过reset，但response已获取。
        if self.seq() > 0 {
            self.seq_cas(seq, 0);
        }
    }
    // reset只会把状态从shutdown变更为init
    // 必须在done设置为true之后调用。否则会有data race
    pub(crate) fn reset(&self) {
        if self.status() != Init {
            let loc = self.response.take().location();
            log::warn!(
                "reset. id:{} {} seq:{} loc:{:?}",
                self.id,
                self.status(),
                self.seq(),
                loc,
            );
        }
        self.status.store(Init as u8, Release);
        self.seq.store(0, Release);
        //self.request.take();
        self.waker.wake();
    }
    pub(crate) fn try_wake(&self) {
        if self.status() == ResponseReceived {
            let loc = self.response.borrow().location();
            log::info!("id:{} loc:{:?} seq:{}", self.id, loc, self.seq());
        }
        self.waker.wake();
    }
}
