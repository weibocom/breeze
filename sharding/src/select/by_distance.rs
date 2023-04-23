use discovery::distance::Addr;
use rand::Rng;
use std::sync::atomic::{AtomicUsize, Ordering::*};
use std::sync::Arc;

#[repr(transparent)]
#[derive(Clone, Default)]
pub struct BackendQuota {
    used_us: Arc<AtomicUsize>, // 所有副本累计使用的时间
}
impl BackendQuota {
    #[inline]
    pub fn incr(&self, d: ds::time::Duration) {
        self.used_us.fetch_add(d.as_micros() as usize, Relaxed);
    }
    #[inline]
    pub fn err_incr(&self, d: ds::time::Duration) {
        // 一次错误请求，消耗500ms
        self.used_us
            .fetch_add(d.as_micros().max(500_000) as usize, Relaxed);
    }
    // 配置时间的微秒计数
    #[inline]
    fn us(&self) -> usize {
        self.used_us.load(Relaxed)
    }
}

// 按机器之间的距离来选择replica。
// 1. B类地址相同的最近与同一个zone的是近距离，优先访问。
// 2. 其他的只做错误处理时访问。
#[derive(Clone)]
pub struct Distance<T> {
    len_local: u16,
    backend_quota: bool,
    idx: Arc<AtomicUsize>,
    replicas: Vec<(T, BackendQuota)>,
}
impl<T: Addr> Distance<T> {
    pub fn new() -> Self {
        Self {
            len_local: 0,
            backend_quota: false,
            idx: Default::default(),
            replicas: Vec::new(),
        }
    }
    #[inline]
    fn idx(&self) -> usize {
        self.idx.load(Relaxed)
    }
    #[inline]
    pub fn quota(&self) -> BackendQuota {
        let idx = self.idx();
        debug_assert!(idx < self.len(), "{} < {}", idx, self.len());
        unsafe { self.replicas.get_unchecked(idx).1.clone() }
    }
    pub fn with_local(replicas: Vec<T>, local: bool) -> Self {
        assert_ne!(replicas.len(), 0);
        let mut me = Self::new();
        me.refresh(replicas);

        // 开启local，即开启后端使用quota
        me.backend_quota = local;
        me.topn(me.len());

        me
    }
    #[inline]
    pub fn from(replicas: Vec<T>) -> Self {
        Self::with_local(replicas, true)
    }
    // 同时更新配额
    fn refresh(&mut self, replicas: Vec<T>) {
        self.replicas = replicas
            .into_iter()
            .map(|r| (r, BackendQuota::default()))
            .collect();
    }
    pub fn update(&mut self, replicas: Vec<T>, topn: usize) {
        self.refresh(replicas);
        self.topn(topn);
    }
    // 只取前n个进行批量随机访问
    fn topn(&mut self, n: usize) {
        assert!(n > 0 && n <= self.len(), "n: {}, len:{}", n, self.len());

        // 开启local，即开启后端使用quota；
        if !self.backend_quota {
            self.backend_quota = n < self.len();
        }

        self.len_local = self.len() as u16;

        use rand::seq::SliceRandom;
        use rand::thread_rng;
        self.replicas.shuffle(&mut thread_rng());

        // 初始节点随机选择，避免第一个节点成为热点；使用quota所有后端均访问
        let idx: usize = rand::thread_rng().gen_range(0..self.len());
        self.idx.store(idx, Relaxed);
    }
    // // 前freeze个是local的，不参与排序
    // fn local(&mut self) {
    //     let local = self.replicas.sort(Vec::new());
    //     self.topn(local);
    // }
    #[inline]
    pub fn take(&mut self) -> Vec<T> {
        self.replicas
            .split_off(0)
            .into_iter()
            .map(|(r, _)| r)
            .collect()
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.replicas.len()
    }
    #[inline]
    pub fn local_len(&self) -> usize {
        self.len_local as usize
    }
    // 检查当前节点的配额
    // 如果配额用完，则idx+1
    // 返回idx
    #[inline]
    fn check_quota_get_idx(&self) -> usize {
        let mut idx = self.idx();
        debug_assert!(idx < self.len());
        let quota = unsafe { &self.replicas.get_unchecked(idx).1 };
        // 每个backend的配置为2秒
        if quota.us() >= 2_000_000 {
            let new = (idx + 1) % self.local_len();
            // 超过配额，则idx+1
            if let Ok(_) = self.idx.compare_exchange(idx, new, AcqRel, Relaxed) {
                quota.used_us.store(0, Relaxed);
            }
            idx = new;
        }
        idx
    }
    #[inline]
    pub unsafe fn get_unchecked(&self, idx: usize) -> &T {
        debug_assert!(idx < self.len());
        &self.replicas.get_unchecked(idx).0
    }
    // 从local选择一个实例
    #[inline]
    pub fn select_idx(&self) -> usize {
        assert_ne!(self.len(), 0);
        let idx = if self.len() == 1 {
            0
        } else {
            self.check_quota_get_idx()
        };
        debug_assert!(idx < self.local_len(), "idx:{} overflow {:?}", idx, self);
        idx
    }
    // 只从local获取
    #[inline]
    pub fn unsafe_select(&self) -> (usize, &T) {
        let idx = self.select_idx();
        (idx, unsafe { &self.replicas.get_unchecked(idx).0 })
    }
    // idx: 上一次获取到的idx
    // runs: 已经连续获取到的次数
    #[inline]
    pub fn select_next_idx(&self, idx: usize, runs: usize) -> usize {
        assert!(runs < self.len(), "{} {} {:?}", idx, runs, self);
        // 还可以从local中取
        let s_idx = if runs < self.local_len() {
            // 在sort时，相关的distance会进行一次random处理，在idx节点宕机时，不会让idx+1个节点成为热点
            (idx + 1) % self.local_len()
        } else {
            // 从remote中取. remote_len > 0
            assert_ne!(self.local_len(), self.len(), "{} {} {:?}", idx, runs, self);
            if idx < self.local_len() {
                // 第一次使用remote，为避免热点，从[len_local..len)随机取一个
                rand::thread_rng().gen_range(self.local_len()..self.len())
            } else {
                // 按顺序从remote中取. 走到最后一个时，从local_len开始
                ((idx + 1) % self.len()).max(self.local_len())
            }
        };
        assert!(s_idx < self.len(), "{},{} {} {:?}", idx, s_idx, runs, self);
        s_idx
    }
    #[inline]
    pub unsafe fn unsafe_next(&self, idx: usize, runs: usize) -> (usize, &T) {
        let idx = self.select_next_idx(idx, runs);
        (idx, &self.replicas.get_unchecked(idx).0)
    }
    pub fn into_inner(self) -> Vec<T> {
        self.replicas.into_iter().map(|(r, _)| r).collect()
    }
    #[inline]
    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            start: unsafe { NonNull::new_unchecked(self.replicas.as_ptr() as *mut _) },
            end: unsafe { self.replicas.as_ptr().add(self.replicas.len()) },
            _marker: std::marker::PhantomData,
        }
    }
}

use std::ptr::NonNull;
pub struct Iter<'a, T> {
    start: NonNull<(T, BackendQuota)>,
    end: *const (T, BackendQuota),
    _marker: std::marker::PhantomData<&'a T>,
}
impl<'a, T> std::iter::Iterator for Iter<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<Self::Item> {
        if self.start.as_ptr() as *const _ == self.end {
            None
        } else {
            let ret = unsafe { &self.start.as_ref().0 };
            self.start = unsafe { NonNull::new_unchecked(self.start.as_ptr().add(1)) };
            Some(ret)
        }
    }
}

impl<T: Addr> std::fmt::Debug for Distance<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "len: {}, local: {} backends:{:?}",
            self.len(),
            self.len_local,
            self.replicas.first().map(|s| s.addr())
        )
    }
}
