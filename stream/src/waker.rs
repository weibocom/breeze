use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError};
use std::sync::atomic::{AtomicIsize, Ordering};
use std::task::Waker;

pub struct MyWaker {
    len: AtomicIsize,
    receiver: Receiver<Waker>,
    sender: Sender<Waker>,
}

// 用在一个通常不会出现阻塞的场景。所以通知的时候，是进行全量通知。
impl MyWaker {
    pub fn with_capacity(cap: usize) -> Self {
        let (sender, receiver) = bounded(cap);
        let len = AtomicIsize::new(0);
        MyWaker {
            receiver,
            sender,
            len,
        }
    }
    pub fn wait_on(&self, waker: Waker) {
        self.sender.send(waker).expect("send waker");
        self.len.fetch_add(1, Ordering::AcqRel);
    }
    pub fn notify(&self) {
        let len = self.len.load(Ordering::Acquire);
        // 大部分情况是走这个逻辑。
        if len == 0 {
            return;
        }
        // 一次最多notify 4个，避免notify阻塞过多
        for _i in 0..4 {
            match self.receiver.try_recv() {
                Ok(waker) => {
                    waker.wake();
                    if self.len.fetch_add(-1, Ordering::AcqRel) - 1 == 0 {
                        return;
                    }
                }
                Err(TryRecvError::Empty) => {
                    return;
                }
                Err(e) => panic!("waker error:{:?}", e),
            }
        }
        // 说明超过4次的notify
        // TODO
        todo!("for test")
    }
}

unsafe impl Send for MyWaker {}
