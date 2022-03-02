use crate::AtomicWaker;
use std::cell::RefCell;
use std::io::{Error, ErrorKind, Result};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering::*};
use std::task::{Context, Poll};

use protocol::{Request, RequestId, Response};

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
    #[inline]
    fn from(s: u8) -> Self {
        STATUSES[s as usize]
    }
}
use ItemStatus::*;
impl PartialEq<u8> for ItemStatus {
    #[inline]
    fn eq(&self, other: &u8) -> bool {
        *self as u8 == *other
    }
}
impl PartialEq<ItemStatus> for u8 {
    #[inline]
    fn eq(&self, other: &ItemStatus) -> bool {
        *self == *other as u8
    }
}

unsafe impl Send for ItemStatus {}
#[derive(Default)]
pub struct Status {
    id: usize,
    seq: AtomicUsize, // 用来做request与response的同步
    status: AtomicU8, // 0: 待接收请求。

    rid: RefCell<RequestId>,
    request: RefCell<Option<Request>>,

    response: RefCell<protocol::Response>,
    waker: AtomicWaker,
}

unsafe impl Send for Status {}

impl Status {
    pub fn new(cid: usize) -> Self {
        Self {
            id: cid,
            status: AtomicU8::new(Init as u8),
            ..Default::default()
        }
    }
    // 把buf的指针保存下来。
    // 上面的假设待验证
    #[inline]
    pub fn place_request(&self, req: &Request) {
        assert_eq!(self.status.load(Acquire), ItemStatus::Init as u8);
        self.status_cas(Init as u8, RequestReceived as u8);
        *self.request.borrow_mut() = Some(req.clone());
        self.rid.replace(req.id());
    }
    // 如果状态不是RequestReceived, 则返回None.
    // 有可能是连接被重置导致。
    #[inline]
    pub fn take_request(&self, seq: usize) -> Option<(usize, Request)> {
        if self.try_status_cas(RequestReceived as u8, RequestSent as u8) {
            log::debug!("take request. {} seq:{} ", self.rid.borrow(), seq);
            if let Some(req) = self.request.take() {
                // noreply的请求不需要seq来进行request与response之间的协调
                if !req.noreply() {
                    self.seq_cas(0, seq);
                }
                return Some((self.id, req));
            }
        }
        log::warn!("take failed. status({}) may be reset before", self.status());
        None
    }

    #[inline]
    pub fn seq(&self) -> usize {
        self.seq.load(Acquire)
    }

    #[inline]
    fn status(&self) -> u8 {
        self.status.load(Acquire)
    }

    #[inline]
    fn seq_cas(&self, old: usize, new: usize) {
        match self.seq.compare_exchange(old, new, AcqRel, Acquire) {
            Ok(_) => {}
            Err(cur) => panic!("item status seq cas. {} => {} but found {}", old, new, cur),
        }
    }
    // 有两种可能的状态。
    #[inline]
    pub fn poll_read(&self, cx: &mut Context) -> Poll<Result<(RequestId, Response)>> {
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
    #[inline]
    fn take_response(&self) -> (RequestId, Response) {
        let response = self.response.take();
        let rid = self.rid.take();
        self.status_cas(ResponseReceived as u8, Read as u8);
        log::debug!("take response {:?}", response.location());
        (rid, response)
    }
    #[inline]
    pub fn place_response(&self, response: protocol::Response, seq: usize) {
        assert_eq!(seq, self.seq());
        log::debug!("place response:{:?} ", response.location());
        self.response.replace(response);

        // 1. response到达之前的状态是 RequestSent. 即请求已发出。 这是大多数场景
        // 2. 因为在req_handler中，是先发送请求，再调用
        //    bind_req来更新状态为RequestSent，有可能在这中间，response已经接收到了。此时的状态是RequestReceived。
        self.try_status_cas(RequestSent as u8, ResponseReceived as u8);
        self.waker.wake();
    }
    #[inline]
    fn try_status_cas(&self, old: u8, new: u8) -> bool {
        match self.status.compare_exchange(old, new, AcqRel, Acquire) {
            Ok(_) => true,
            Err(_cur) => {
                log::error!("cas {} => {}, {} found. ", old, new, self);
                false
            }
        }
    }
    #[inline]
    fn status_cas(&self, old: u8, new: u8) {
        match self.status.compare_exchange(old, new, AcqRel, Acquire) {
            Ok(_) => {}
            Err(cur) => {
                let rid = self.rid.borrow();
                log::error!("cas {} => {}, {} found. ", old, new, self);
                panic!("cas {} => {}, {} found. {}", old, new, cur, rid);
            }
        }
    }
    // 在在sender把Response发送给client后，在Drop中会调用response_done，更新状态。
    #[inline]
    pub fn response_done(&self) {
        assert_eq!(self.status(), Read as u8);
        //assert_eq!(self.seq(), seq);
        // 把状态调整为Init
        // 如果seq为0，说明有一种情况，之前连接进行过reset，但response已获取。
        if self.try_status_cas(Read as u8, ItemStatus::Init as u8) {
            self.seq.store(0, Release);
        }
    }
    // reset只会把状态从shutdown变更为init
    // 必须在done设置为true之后调用。否则会有data race
    pub(crate) fn reset(&self) {
        if self.status() != Init {
            let loc = self.response.take().location();
            log::warn!("reset. {} loc:{:?}", self, loc);
        }
        self.status.store(Init as u8, Release);
        self.seq.store(0, Release);
        self.request.take();
        self.waker.wake();
    }
}

use std::fmt::{self, Display, Formatter};
impl Display for Status {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "(item - id:{} seq:{} status:{} )",
            self.id,
            self.seq(),
            self.status(),
        )
    }
}
