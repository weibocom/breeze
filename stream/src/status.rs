use std::cell::RefCell;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use tokio::io::ReadBuf;

use super::RingSlice;
use std::hint::spin_loop;

#[repr(u8)]
pub enum ItemStatus {
    Init = 0u8,
    Received,
    WaitingPending, // 增加一个中间状态来协调poll_read与place_response
    Waiting,        // 在等待response数据写入
    Responsed,      // 数据已写入
}

unsafe impl Send for ItemStatus {}
#[derive(Default)]
pub struct Item {
    // connection id
    pub(crate) cid: usize,
    pub(crate) status: AtomicU8, // 0: 待接收请求。
    offset: AtomicUsize,
    len: AtomicUsize,
    seq: AtomicUsize, // 用来做request与response的同步

    // 下面的数据要加锁才能访问
    waker: RefCell<Option<Waker>>,
    response: RefCell<RingSlice>,
}

unsafe impl Send for Item {}

impl Item {
    pub fn new(cid: usize) -> Self {
        Self {
            cid: cid,
            status: AtomicU8::new(ItemStatus::Init as u8),
            ..Default::default()
        }
    }
    pub fn is_status_init(&self) -> bool {
        self.status.load(Ordering::Acquire) == ItemStatus::Init as u8
    }
    #[inline]
    pub fn req_received(&self, offset: usize, len: usize) {
        let status = self.status.load(Ordering::Acquire);
        debug_assert!(status == ItemStatus::Init as u8 || status == ItemStatus::Waiting as u8);
        match self.status_cas(status, ItemStatus::Received as u8) {
            Ok(_) => {
                if status == ItemStatus::Waiting as u8 {
                    self.wakeup();
                }
            }
            Err(status) => panic!("data race. status Init expected, but found:{:?}", status),
        };
        self.offset.store(offset, Ordering::Release);
        self.len.store(len, Ordering::Release);
    }
    #[inline]
    pub fn is_status_received(&self) -> bool {
        let status = self.status.load(Ordering::Acquire);
        status == ItemStatus::Received as u8 || status == ItemStatus::Waiting as u8
    }
    #[inline]
    pub fn offset_len(&self) -> (usize, usize) {
        (
            self.offset.load(Ordering::Acquire),
            self.len.load(Ordering::Acquire),
        )
    }
    pub fn seq(&self) -> usize {
        self.seq.load(Ordering::Acquire)
    }
    pub fn bind_seq(&self, seq: usize) {
        self.seq.store(seq, Ordering::Release);
    }
    // 有两种可能的状态。
    // Received, 说明之前从来没有poll过
    // Reponded: 有数据并且成功返回
    pub fn poll_read(&self, cx: &mut Context, buf: &mut ReadBuf) -> Poll<bool> {
        loop {
            let status = self.status.load(Ordering::Acquire);
            // read请求可能会先于write请求到达，所以有可能初始状态是Init状态。
            if status == ItemStatus::Received as u8
                || status == ItemStatus::Init as u8
                || status == ItemStatus::Waiting as u8
            {
                // 进入waiting状态
                // 需要用一个waiting的中间状态来协调poll_read与place_response
                match self.status_cas(status, ItemStatus::WaitingPending as u8) {
                    Ok(_) => {
                        *self.waker.borrow_mut() = Some(cx.waker().clone());
                        // 再把状态改成pending
                        match self.status_cas(ItemStatus::WaitingPending as u8, ItemStatus::Waiting as u8) {
                        Ok(_) => return Poll::Pending,
                        // 代码有bug。
                        Err(status) => panic!("unexpected data race found. waiting pending status compare and swap failed. {:?} found.", status),
                    }
                    }
                    Err(_status) => continue,
                }
            }
            if status == ItemStatus::Responsed as u8 {
                // 读数据
                if self.response.borrow_mut().read(buf) {
                    // 标识为0. 在stream.poll_write时，依赖该值为0重新获取offset
                    self.len.store(0, Ordering::Release);
                    // 把状态调整为Init
                    match self.status_cas(status, ItemStatus::Init as u8) {
                        Ok(_) => return Poll::Ready(true),
                        Err(status) => {
                            panic!("data race: responded status expected, but {} found", status)
                        }
                    }
                } else {
                    return Poll::Ready(false);
                }
            }
            panic!("not a valid status:{:?}", status);
        }
    }
    pub fn place_response(&self, response: RingSlice) {
        //println!("response received len:{}", response.available());
        self.response.replace(response);
        loop {
            // 状态通常是waiting状态。如果是waiting状态，则进行wakeup
            match self.status_cas(ItemStatus::Waiting as u8, ItemStatus::Responsed as u8) {
                // Ok 说明是waiting状态，一定有waker
                Ok(_) => {
                    self.wakeup();
                    return;
                }
                // 状态，状态可能是Received状态, 但这个状态可能会有data race
                Err(_status) => {
                    match self.status_cas(ItemStatus::Received as u8, ItemStatus::Responsed as u8) {
                        Ok(_) => return,
                        // 即不是Wating, 也不是Received，说明有data
                        // race。poll_read尝试在更新状态。进入spin
                        Err(_) => {
                            spin_loop();
                        }
                    }
                }
            }
        }
    }
    #[inline(always)]
    fn status_cas(&self, old: u8, new: u8) -> std::result::Result<u8, u8> {
        //println!(
        //    "status cas. expected: {} current:{} update to:{}",
        //    old,
        //    self.status.load(Ordering::Relaxed),
        //    new
        //);
        self.status
            .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
    }
    #[inline(always)]
    fn wakeup(&self) {
        self.waker
            .borrow_mut()
            .take()
            .expect("waiting status must contains waker")
            .wake();
    }
    #[inline(always)]
    fn try_wakeup(&self) {
        if let Some(waker) = self.waker.borrow_mut().take() {
            waker.wake();
        }
    }
    pub fn response_slice(&self) -> (usize, usize) {
        self.response.borrow().location()
    }
}
