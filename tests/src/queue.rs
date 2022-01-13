#[cfg(test)]
mod queue {
    use ds::PinnedQueue;
    use std::sync::atomic::{AtomicIsize, Ordering::*};

    static EXISTS: AtomicIsize = AtomicIsize::new(0);

    struct Data {
        v: u32,
    }
    impl Drop for Data {
        fn drop(&mut self) {
            EXISTS.fetch_sub(1, Relaxed);
        }
    }

    impl From<u32> for Data {
        fn from(v: u32) -> Self {
            EXISTS.fetch_add(1, Relaxed);
            Self { v }
        }
    }

    #[test]
    fn pinned_queue() {
        let mut q: PinnedQueue<Data> = PinnedQueue::with_capacity(1);
        q.push_back(0.into());
        assert_eq!(q.len(), 1);
        q.pop_front().expect("pop front").v = 0;
        assert_eq!(q.len(), 0);

        q.push_back(1.into());
        q.push_back(2.into());
        q.push_back(3.into());

        assert_eq!(q.len(), 3);
        unsafe { assert_eq!(q.pop_front_unchecked().v, 1) };
        unsafe { assert_eq!(q.pop_front_unchecked().v, 2) };
        unsafe { assert_eq!(q.pop_front_unchecked().v, 3) };
        assert!(q.pop_front().is_none());
        assert!(q.pop_front().is_none());
        assert!(q.pop_front().is_none());

        assert_eq!(q.len(), 0);

        // 随机验证。
        // 从0开始，随机写入m条，阿布读取n条。进行数据一致性验证。
        let MAX_CAP = 64;
        let MAX_COUNT = 1000000;
        // i 是插入过的数据条数量。
        // j 是pop出来的数据数量
        let mut i = 0;
        let mut j = 0;
        use rand::Rng;
        let mut rng = rand::thread_rng();
        while i < MAX_COUNT {
            let m = rng.gen::<u32>() % MAX_CAP;
            for v in i..i + m {
                q.push_back(v.into());
            }
            i += m;
            let n = rng.gen::<u32>() % MAX_CAP;
            for _ in 0..n {
                let v = q.pop_front();
                if j >= i {
                    assert_eq!(q.len(), 0);
                    assert!(v.is_none());
                } else {
                    assert_eq!(j, v.expect("take front").v);
                    j += 1;
                }
            }
        }

        drop(q);
        std::sync::atomic::fence(AcqRel);
        let exists = EXISTS.load(Acquire);
        assert_eq!(exists, 0);
    }
}
