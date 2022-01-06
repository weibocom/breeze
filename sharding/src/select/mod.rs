// 从多个副本中，选择一个副本

mod random;
pub use random::*;

#[derive(Clone)]
pub enum ReplicaSelect<T> {
    // 随机选择
    Random(Random<T>),
    // 本区域优先
    Distance,
}

impl<T> ReplicaSelect<T> {
    #[inline(always)]
    pub fn random(replicas: Vec<T>) -> Self {
        Self::Random(Random::from(replicas))
    }
    #[inline(always)]
    pub fn len(&self) -> usize {
        match self {
            Self::Random(r) => r.replicas.len(),
            _ => todo!(),
        }
    }
    #[inline(always)]
    pub fn into_inner(self) -> Vec<T> {
        match self {
            Self::Random(r) => r.replicas,
            _ => todo!(),
        }
    }
    #[inline(always)]
    pub fn as_ref(&self) -> &[T] {
        match self {
            Self::Random(r) => &r.replicas,
            _ => todo!(),
        }
    }
    #[inline(always)]
    pub unsafe fn unsafe_select(&self) -> (usize, &T) {
        match self {
            Self::Random(r) => r.unsafe_select(),
            _ => todo!(),
        }
    }
    #[inline(always)]
    pub unsafe fn unsafe_next(&self, idx: usize) -> (usize, &T) {
        match self {
            Self::Random(r) => r.unsafe_next(idx),
            _ => todo!(),
        }
    }
}
