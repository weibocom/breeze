use rand::Rng;
use std::sync::atomic::{AtomicIsize, Ordering::*};

use ds::PinnedQueue;
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
fn pinned_queue_push_pop() {
    let cap = 4;
    let mut q: PinnedQueue<Data> = PinnedQueue::with_fix(cap);
    q.push_back(0.into());
    assert_eq!(q.len(), 1);
    q.pop_front().expect("pop front").v = 0;
    assert_eq!(q.len(), 0);

    for i in 1..=cap * 2 {
        q.push_back(i.into());
    }

    assert_eq!(q.len(), cap as usize * 2);
    for i in 1..=cap * 2 {
        unsafe { assert_eq!(q.pop_front_unchecked().v, i) };
    }
    assert!(q.pop_front().is_none());
    assert!(q.pop_front().is_none());

    assert_eq!(q.len(), 0);

    // 随机验证。
    // 从0开始，随机写入m条，阿布读取n条。进行数据一致性验证。
    const MAX_CAP: u32 = 64;
    const MAX_COUNT: u32 = 1_000_000;
    // i 是插入过的数据条数量。
    // j 是pop出来的数据数量
    let (mut i, mut j) = (0, 0);
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

// 场景：
// 1. 不从堆上创建数据，直接让queue返回数据；
// 2. 数据返回queue的时候也不释放。
#[test]
fn pinned_queue_push_forget_u32() {
    let cap = 1;
    use std::sync::atomic::{AtomicU32, Ordering::*};
    let mut q: PinnedQueue<AtomicU32> = PinnedQueue::with_fix(4);
    assert_eq!(q.len(), 0);
    let d0 = unsafe { q.push_back_mut() };
    *d0.get_mut() = 5;
    assert_eq!(q.len(), 1);
    assert!(q.front_mut().is_some());
    assert_eq!(q.front_mut().expect("is some").load(Acquire), 5);
    unsafe { q.forget_front() };
    assert_eq!(q.len(), 0);

    for i in 0..cap * 2 {
        unsafe { *q.push_back_mut().get_mut() = i };
    }
    assert_eq!(q.len(), cap as usize * 2);

    for i in 0..cap * 2 {
        assert!(q.front_mut().is_some());
        assert_eq!(q.front_mut().expect("is some").load(Acquire), i);
        unsafe { q.forget_front() };
    }
    assert_eq!(q.len(), 0);

    // 随机验证。
    // 从0开始，随机写入m条，阿布读取n条。进行数据一致性验证。
    const MAX_CAP: u32 = 64;
    const MAX_COUNT: u32 = 1_000_000;
    // i 是插入过的数据条数量。
    // j 是pop出来的数据数量
    let (mut i, mut j) = (0, 0);
    let mut rng = rand::thread_rng();
    while i < MAX_COUNT {
        let m = rng.gen::<u32>() % MAX_CAP;
        for v in i..i + m {
            unsafe { *q.push_back_mut().get_mut() = v };
        }
        i += m;
        let n = rng.gen::<u32>() % MAX_CAP;
        for _ in 0..n {
            let v = q.front_mut();
            if j >= i {
                assert!(v.is_none());
                assert_eq!(q.len(), 0);
                continue;
            } else {
                assert_eq!(j, v.expect("take front").load(Acquire));
                j += 1;
                unsafe { q.forget_front() };
            }
        }
    }
}
