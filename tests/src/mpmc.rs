#[cfg(test)]
mod mpmc_test {
    use super::*;
    use std::pin::Pin;
    use thread_id;

    struct TestMpmc {
        senders: Vec<RefCell<(bool, PollSender<RequestData>)>>,
        receiver: RefCell<Option<Receiver<RequestData>>>,
    }

    pub struct ReceiverTester {
        cache: Option<RequestData>,
        done: Arc<AtomicBool>,
        seq: usize,
        receiver: Receiver<RequestData>,
    }

    impl ReceiverTester {
        pub fn from(receiver: Receiver<RequestData>, done: Arc<AtomicBool>) -> Self {
            Self {
                done: done,
                seq: 0,
                receiver: receiver,
                cache: None,
            }
        }
    }

    impl Future for ReceiverTester {
        type Output = Result<()>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            log::debug!("thread {}: task polling. ReceiverTester", thread_id::get());
            let me = &mut *self;
            let mut receiver = Pin::new(&mut me.receiver);
            while !me.done.load(Ordering::Relaxed) {
                log::debug!("thread {}: come into poll loop", thread_id::get());
                if let Some(req) = me.cache.take() {
                    let data = req.data();
                    if !data.len() <= 1 {
                        assert_eq!(data[0], 0x80);
                        log::debug!("request  received");
                        let seq = me.seq;
                        me.seq += 1;
                    }
                }
                let result = ready!(receiver.as_mut().poll_recv(cx));
                if result.is_none() {
                    log::debug!("thread {}: channel closed, quit", thread_id::get());
                    break;
                }
                me.cache = result;
            }
            log::debug!("poll done");
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn test_mpmc() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let test_mpmc = {
                let (origin_sender, receiver) = channel(100);
                let mut sender = PollSender::new(origin_sender);
                let senders = (0..50)
                    .map(|_| RefCell::new((true, sender.clone())))
                    .collect();
                /*
                sender.close_this_sender();
                drop(sender);
                */
                log::debug!("thread {}: new testMpmc", thread_id::get());

                TestMpmc {
                    senders: senders,
                    receiver: RefCell::new(Some(receiver)),
                }
            };

            let done = Arc::new(AtomicBool::new(false));

            let receiver = test_mpmc
                .receiver
                .borrow_mut()
                .take()
                .expect("receiver exists");
            log::debug!("thread {}: goto new thread", thread_id::get());
            tokio::spawn(ReceiverTester::from(receiver, done.clone()));

            std::thread::sleep(Duration::from_secs(5));
            log::debug!("thread {}: sleep 5 seconds, begin drop", thread_id::get());

            let (sender, receiver) = channel(100);
            let sender = PollSender::new(sender);
            // 删除所有的sender，则receiver会会接收到一个None，而不会阻塞
            for s in test_mpmc.senders.iter() {
                let (_, mut old) = s.replace((true, sender.clone()));
                old.close_this_sender();
                drop(old);
            }
            log::debug!("thread {}: drop done", thread_id::get());
            let old = test_mpmc.receiver.replace(Some(receiver));
            assert!(old.is_none());
            std::thread::sleep(Duration::from_secs(5));
        });
    }
}
