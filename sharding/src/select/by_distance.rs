use discovery::distance::{Addr, ByDistance};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;
// 按机器之间的距离来选择replica。
// 1. B类地址相同的最近与同一个zone的是近距离，优先访问。
// 2. 其他的只做错误处理时访问。
#[derive(Clone)]
pub struct Distance<T> {
    // batch: 至少在一个T上连续连续多少次
    batch_shift: u8,
    len_local: u16,
    seq: Arc<AtomicUsize>,
    pub(super) replicas: Vec<T>,
}
impl<T: Addr> Distance<T> {
    pub fn new() -> Self {
        Self {
            batch_shift: 0,
            len_local: 0,
            seq: Arc::new(AtomicUsize::new(0)),
            replicas: Vec::new(),
        }
    }
    #[inline]
    pub fn from(replicas: Vec<T>) -> Self {
        assert_ne!(replicas.len(), 0);
        let mut me = Self::new();
        me.replicas = replicas;
        me.refresh(0);
        me
    }
    // 前freeze个是local的，不参与排序
    pub fn refresh(&mut self, freeze: usize) {
        let local = self.replicas.sort_and_take(freeze);
        self.len_local = local as u16;
        let batch = 1024usize;
        // 最小是1，最大是65536
        let batch_shift = batch.max(1).next_power_of_two().min(65536).trailing_zeros() as u8;
        self.seq.store(rand::random::<u16>() as usize, Relaxed);
        self.batch_shift = batch_shift;
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
            (self.seq.fetch_add(1, Relaxed) >> self.batch_shift as usize) % self.local_len()
        };
        assert!(idx < self.replicas.len());
        idx
    }
    // 只从local获取
    #[inline]
    pub fn unsafe_select(&self) -> (usize, &T) {
        let idx = self.select_idx();
        (idx, unsafe { self.replicas.get_unchecked(idx) })
    }
    #[inline]
    pub fn select_next_idx(&self, idx: usize, runs: usize) -> usize {
        assert!(runs < self.len());
        // 还可以从local中取
        let idx = if runs < self.local_len() {
            // 在sort时，相关的distance会进行一次random处理，在idx节点宕机时，不会让idx+1个节点成为热点
            (idx + 1) % self.local_len()
        } else {
            assert_ne!(self.local_len(), self.len());
            if idx == self.local_len() {
                // 从[idx_local..idx_remote)随机取一个
                self.seq.fetch_add(1, Relaxed) % self.remote_len() + self.local_len()
            } else {
                if idx + 1 == self.len() {
                    self.local_len()
                } else {
                    idx + 1
                }
            }
        };
        assert!(idx < self.replicas.len());
        idx
    }
    #[inline]
    pub unsafe fn unsafe_next(&self, idx: usize, runs: usize) -> (usize, &T) {
        let idx = self.select_next_idx(idx, runs);
        (idx, self.replicas.get_unchecked(idx))
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
