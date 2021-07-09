use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

//use super::RequestData;
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
    Shutdown,         // 当前状态隶属的stream已结束。
}
use ItemStatus::*;
impl PartialEq<u8> for ItemStatus {
    fn eq(&self, other: &u8) -> bool {
        *self as u8 == *other
    }
}
impl PartialEq<ItemStatus> for u8 {
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

    request_id: RefCell<RequestId>,

    // 下面的数据要加锁才能访问
    waker_lock: AtomicBool,
    waker: RefCell<Option<Waker>>,
    response: RefCell<RingSlice>,
}

unsafe impl Send for Item {}

impl Item {
    pub fn new(cid: usize) -> Self {
        Self {
            id: cid,
            status: AtomicU8::new(ItemStatus::Init as u8),
            ..Default::default()
        }
    }
    // 把buf的指针保存下来。
    // 上面的假设待验证
    pub fn place_request(&self, req: &Request) {
        debug_assert_eq!(self.status.load(Ordering::Acquire), ItemStatus::Init as u8);
        //self.request.replace(RequestData::from(self.id, buf));
        match self.status_cas(ItemStatus::Init as u8, ItemStatus::RequestReceived as u8) {
            Ok(_) => {}
            Err(status) => {
                panic!("place request must be in Init status but {} found", status);
            }
        }

        self.request_id.replace(req.id().clone());
    }
    pub fn seq(&self) -> usize {
        self.seq.load(Ordering::Acquire)
    }

    fn status(&self) -> u8 {
        self.status.load(Ordering::Acquire)
    }

    fn check_seq(&self, seq: usize, method: &str) {
        let old_seq = self.seq.load(Ordering::Acquire);
        if old_seq != seq {
            panic!("item status: {} {} != {}", method, old_seq, seq);
        }
    }
    fn seq_cas(&self, old: usize, new: usize, m: &str) {
        match self
            .seq
            .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {}
            Err(cur) => panic!(
                "item status seq cas. {} => {} but found {} m:{}",
                old, new, cur, m
            ),
        }
    }

    // seq: 是SPSC中请求的seq
    // req_handler中，将请求写入到buffer前调用bind_req，更新seq;
    // 之后，会调用on_sent更新状态
    // 在调用on_sent之前，
    // 1. response有可能没返回，此时的前置状态是RequestReceived. (这是大部分情况)
    // 2. 也有可能response返回了，并且成功调用了place_response，此时的状态是ResponseReceived
    // 3. 甚至有可能response已经返回了。
    // 只能是以上两种状态
    pub fn on_sent(&self, seq: usize) {
        self.seq_cas(0, seq, "on_sent");
        log::debug!(
            "item status: on_sent. cid:{} seq:{} rid:{:?}",
            self.id,
            seq,
            self.request_id
        );
        // 说明是同一个req请求
        // 只把状态从RequestReceived改为RequestSent. 其他状态则忽略
        match self.status_cas(RequestReceived as u8, RequestSent as u8) {
            Ok(_) => {}
            Err(status) => panic!("item status: status mut be req-received. found:{}", status),
        }
    }

    // 有两种可能的状态。
    // Received, 说明之前从来没有poll过
    // Reponded: 有数据并且成功返回
    // 调用方确保当前status不为shutdown
    pub fn poll_read(&self, cx: &mut Context) -> Poll<ResponseData> {
        self.lock_waker();
        let status = self.status();
        if status == ItemStatus::ResponseReceived {
            let response = self.response.take();
            log::debug!("item status: read {:?}", response.location());
            let rid = self.request_id.take();

            self.unlock_waker();
            Poll::Ready(ResponseData::from(response, rid, self.seq()))
        } else {
            *self.waker.borrow_mut() = Some(cx.waker().clone());
            self.unlock_waker();
            Poll::Pending
        }

        //loop {
        //    let status = self.status();
        //    debug_assert_ne!(status, ItemStatus::Shutdown as u8);
        //    if status == ItemStatus::ResponseReceived {
        //        break;
        //    }
        //    // 进入waiting状态
        //    if self.try_wake_lock() {
        //        // double check
        //        if status == ItemStatus::ResponseReceived {
        //            self.unlock_waker();
        //            break;
        //        }
        //        *self.waker.borrow_mut() = Some(cx.waker().clone());
        //        self.unlock_waker();
        //        return Poll::Pending;
        //    }
        //}
        //// 响应已返回，开始读取。状态一旦进入Responsed状态，没有其他任何请求会变更。只有poll_read会变更
        //// place_response先更新数据，后更新状态。不会有并发问题
        //// 读数据
        //let response = self.response.take();
        //log::debug!("item status: read {:?}", response.location());
        //let rid = self.request_id.take();

        //Poll::Ready(ResponseData::from(response, rid, self.seq()))
    }
    pub fn place_response(&self, response: RingSlice, seq: usize) {
        self.check_seq(seq, "place_response");
        log::debug!("item status: write :{:?} ", response.location());
        self.response.replace(response);

        // 1. response到达之前的状态是 RequestSent. 即请求已发出。 这是大多数场景
        // 2. 因为在req_handler中，是先发送请求，再调用
        //    bind_req来更新状态为RequestSent，有可能在这中间，response已经接收到了。此时的状态是RequestReceived。
        match self.status_cas(RequestSent as u8, ResponseReceived as u8) {
            Ok(_) => {
                self.try_wake();
            }
            // 状态，状态可能是Received状态, 但这个状态可能会有data race
            Err(_status) => {
                panic!("item status. response.cur: {}", _status);
            }
        }
    }
    #[inline(always)]
    fn status_cas(&self, old: u8, new: u8) -> std::result::Result<u8, u8> {
        log::debug!(
            "item status cas. expected: {} current:{} update to:{}",
            old,
            self.status.load(Ordering::Acquire),
            new
        );
        self.status
            .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
    }
    // 在在sender把Response发送给client后，在Drop中会调用response_done，更新状态。
    #[inline]
    pub fn response_done(&self, seq: usize) {
        // 把状态调整为Init
        let status = self.status();
        debug_assert_eq!(status, ItemStatus::ResponseReceived as u8);
        if let Err(status) = self.status_cas(status, ItemStatus::Init as u8) {
            // 0会有qi歧义。因为seq是从0开始的。
            panic!("data race: responded status expected, but {} found", status);
        }
        self.seq_cas(seq, 0, "done");
    }
    #[inline]
    fn try_wake(&self) -> bool {
        self.lock_waker();
        let waker = self.waker.borrow_mut().take();
        self.unlock_waker();
        if let Some(waker) = waker {
            log::debug!(
                "item status: wakeup. id:{} status:{}",
                self.id,
                self.status()
            );
            waker.wake();
            true
        } else {
            log::debug!("item status: not waker found. {}", self.id);
            false
        }
    }
    // TODO ReadGuard
    fn try_wake_lock(&self) -> bool {
        match self
            .waker_lock
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => true,
            Err(_) => {
                log::debug!("item status: waker lock failed");
                false
            }
        }
    }
    fn unlock_waker(&self) {
        match self
            .waker_lock
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {}
            Err(_) => panic!("item status: waker unlock failed"),
        }
    }
    #[inline(always)]
    fn lock_waker(&self) {
        while !self.try_wake_lock() {
            std::hint::spin_loop();
        }
    }
    pub(crate) fn status_init(&self) -> bool {
        self.status.load(Ordering::Acquire) == ItemStatus::Init as u8
    }
    // 把所有状态设置为shutdown
    // 状态一旦变更为Shutdown，只有reset都会把状态从Shutdown变更为Init
    pub(crate) fn shutdown(&self) {
        self.status
            .store(ItemStatus::Shutdown as u8, Ordering::Release);
    }

    // reset只会把状态从shutdown变更为init
    // 必须在shutdown之后调用
    pub(crate) fn reset(&self) {
        match self.status_cas(ItemStatus::Shutdown as u8, ItemStatus::Init as u8) {
            Ok(_) => {}
            Err(status) => assert_eq!(status, ItemStatus::Init as u8),
        }
    }
}
