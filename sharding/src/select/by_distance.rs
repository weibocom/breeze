use discovery::distance::{Addr, ByDistance};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;

#[repr(transparent)]
#[derive(Clone)]
pub struct BackendQuota {
    used_us: Arc<AtomicUsize>, // 所有副本累计使用的时间
}
impl BackendQuota {
    #[inline]
    pub fn incr(&self, d: ds::time::Duration) {
        self.used_us.fetch_add(d.as_micros() as usize, Relaxed);
    }
    #[inline]
    fn idx(&self) -> usize {
        // 2*1024*1024 us 换backend
        self.used_us.load(Relaxed) >> 21
    }
    #[inline]
    fn val(&self) -> usize {
        self.used_us.load(Relaxed)
    }
}

// 按机器之间的距离来选择replica。
// 1. B类地址相同的最近与同一个zone的是近距离，优先访问。
// 2. 其他的只做错误处理时访问。
#[derive(Clone)]
pub struct Distance<T> {
    len_local: u16,
    quota: BackendQuota, // 所有副本累计使用的时间
    pub(super) replicas: Vec<T>,
}
impl<T: Addr> Distance<T> {
    pub fn new() -> Self {
        Self {
            len_local: 0,
            quota: BackendQuota {
                used_us: Arc::new(AtomicUsize::new(0)),
            },
            replicas: Vec::new(),
        }
    }
    pub fn quota(&self) -> BackendQuota {
        self.quota.clone()
    }
    pub fn with_local(replicas: Vec<T>, local: bool) -> Self {
        assert_ne!(replicas.len(), 0);
        let mut me = Self {
            len_local: 0,
            quota: BackendQuota {
                used_us: Arc::new(AtomicUsize::new(0)),
            },
            replicas,
        };
        if local {
            me.local();
        } else {
            use rand::seq::SliceRandom;
            use rand::thread_rng;
            me.replicas.shuffle(&mut thread_rng());
            me.topn(me.len());
        }
        me
    }
    #[inline]
    pub fn from(replicas: Vec<T>) -> Self {
        Self::with_local(replicas, true)
    }
    // 只取前n个进行批量随机访问
    pub fn topn(&mut self, n: usize) {
        assert!(n > 0 && n <= self.len(), "n: {}, len:{}", n, self.len());
        self.len_local = n as u16;
    }
    // 前freeze个是local的，不参与排序
    fn local(&mut self) {
        let local = self.replicas.sort(Vec::new());
        self.topn(local);
    }
    #[inline]
    pub fn take(&mut self) -> Vec<T> {
        self.replicas.split_off(0)
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.replicas.len()
    }
    #[inline]
    pub fn local_len(&self) -> usize {
        self.len_local as usize
    }
    #[inline]
    fn remote_len(&self) -> usize {
        self.len() - self.local_len()
    }
    #[inline]
    pub fn select_idx(&self) -> usize {
        assert_ne!(self.len(), 0);
        let idx = if self.len() == 1 {
            0
        } else {
            (self.quota.idx()) % self.local_len()
        };
        assert!(idx < self.len(), "{} >= {}", idx, self.len());
        idx
    }
    // 只从local获取
    #[inline]
    pub fn unsafe_select(&self) -> (usize, &T) {
        let idx = self.select_idx();
        (idx, unsafe { self.replicas.get_unchecked(idx) })
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
                // 第一次使用remote，为避免热点，从[idx_local..idx_remote)随机取一个
                self.quota.val() % self.remote_len() + self.local_len()
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
        (idx, self.replicas.get_unchecked(idx))
    }
    pub fn into_inner(self) -> Vec<T> {
        self.replicas
    }
}

impl<T> std::ops::Deref for Distance<T> {
    type Target = Vec<T>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.replicas
    }
}
impl<T> std::ops::DerefMut for Distance<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.replicas
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
