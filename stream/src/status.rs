use std::cell::RefCell;
use std::sync::atomic::fence;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

//use super::RequestData;
use ds::RingSlice;

#[repr(u8)]
pub enum ItemStatus {
    Init = 0u8,
    RequestReceived,
    RequestSent,
    ReadPending,      // 增加一个中间状态来协调poll_read与place_response
    ResponseReceived, // 数据已写入
    Shutdown,         // 当前状态隶属的stream已结束。
}

unsafe impl Send for ItemStatus {}
#[derive(Default)]
pub struct Item {
    // connection id
    id: usize,
    seq: AtomicUsize, // 用来做request与response的同步
    status: AtomicU8, // 0: 待接收请求。

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
    // TODO 在最上层调用时，进行了ping-pong处理，在ping-pong返回之前，buf不会被释放
    // 上面的假设待验证
    pub fn place_request(&self) {
        debug_assert_eq!(self.status.load(Ordering::Acquire), ItemStatus::Init as u8);
        //self.request.replace(RequestData::from(self.id, buf));
        match self.status_cas(ItemStatus::Init as u8, ItemStatus::RequestReceived as u8) {
            Ok(_) => {}
            Err(status) => {
                panic!("place request must be in Init status but {} found", status);
            }
        }
    }
    pub fn seq(&self) -> usize {
        self.seq.load(Ordering::Acquire)
    }

    // 调用方确保状态正确性
    pub fn bind_seq(&self, seq: usize) {
        match self.status_cas(
            ItemStatus::RequestReceived as u8,
            ItemStatus::RequestSent as u8,
        ) {
            Ok(_) => {}
            Err(status) => panic!(
                "bind seq must be after request received. but found:{}",
                status
            ),
        }
        self.seq.store(seq, Ordering::Release);
    }
    // 有两种可能的状态。
    // Received, 说明之前从来没有poll过
    // Reponded: 有数据并且成功返回
    // last_min: 最后一次获取最少要保障last_min个字节。
    // 调用方确保当前status不为shutdown
    pub fn poll_read(&self, cx: &mut Context) -> Poll<RingSlice> {
        let status = self.status.load(Ordering::Acquire);
        debug_assert_ne!(status, ItemStatus::Shutdown as u8);
        if status != ItemStatus::ResponseReceived as u8 {
            // 进入waiting状态
            self.waiting(cx.waker().clone());
            return Poll::Pending;
        }
        // 响应已返回，开始读取。状态一旦进入Responsed状态，没有其他任何请求会变更。只有poll_read会变更
        // place_response先更新数据，后更新状态。不会有并发问题
        // 读数据
        println!("poll read id:{}", self.id);
        let response = self.response.take();

        Poll::Ready(response)
    }
    pub fn place_response(&self, response: RingSlice) {
        //println!("response received len:{}", response.available());
        self.response.replace(response);
        // Ok poll_read更新状态时， 会把状态从RequestReceived变更为Pending，所以可能会失败。

        match self.status_cas(
            ItemStatus::RequestSent as u8,
            ItemStatus::ResponseReceived as u8,
        ) {
            Ok(_) => {
                self.try_wake();
                return;
            }
            // 状态，状态可能是Received状态, 但这个状态可能会有data race
            Err(status) => {
                debug_assert_eq!(status, ItemStatus::ReadPending as u8);
            }
        }
    }
    #[inline(always)]
    fn status_cas(&self, old: u8, new: u8) -> std::result::Result<u8, u8> {
        println!(
            "status cas. expected: {} current:{} update to:{}",
            old,
            self.status.load(Ordering::Relaxed),
            new
        );
        self.status
            .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
    }
    #[inline]
    pub fn response_done(&self) {
        // 把状态调整为Init
        let status = self.status.load(Ordering::Acquire);
        debug_assert_eq!(status, ItemStatus::ResponseReceived as u8);
        if let Err(status) = self.status_cas(status, ItemStatus::Init as u8) {
            panic!("data race: responded status expected, but {} found", status);
        }
    }
    #[inline]
    fn waiting(&self, waker: Waker) {
        println!("entering waiting status:{}", self.id);
        self.lock_waker();
        *self.waker.borrow_mut() = Some(waker);
        self.unlock_waker();
    }
    #[inline]
    fn try_wake(&self) -> bool {
        self.lock_waker();
        let waker = self.waker.borrow_mut().take();
        self.unlock_waker();
        if let Some(waker) = waker {
            waker.wake();
            true
        } else {
            false
        }
    }
    #[inline(always)]
    fn lock_waker(&self) {
        loop {
            match self
                .waker_lock
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(_) => {
                    println!("lock failed");
                    continue;
                }
            }
        }
        fence(Ordering::Acquire);
    }
    #[inline(always)]
    fn unlock_waker(&self) {
        self.waker_lock.store(false, Ordering::Release);
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
