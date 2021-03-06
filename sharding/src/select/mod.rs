// 从多个副本中，选择一个副本

mod random;
pub use random::*;

mod by_distance;
pub use by_distance::*;

#[derive(Clone)]
pub enum ReplicaSelect<T> {
    // 随机选择
    Random(Random<T>),
    // 本区域优先
    Distance(Distance<T>),
}

impl<T: Addr> ReplicaSelect<T> {
    #[inline]
    pub fn from(name: &str, replicas: Vec<T>) -> Self {
        match name {
            "random" => Self::random(replicas),
            _ => Self::distance(replicas),
        }
    }
    #[inline]
    pub fn random(replicas: Vec<T>) -> Self {
        Self::Random(Random::from(replicas))
    }
    #[inline]
    pub fn distance(replicas: Vec<T>) -> Self {
        Self::Distance(Distance::from(replicas))
    }
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Random(r) => r.replicas.len(),
            Self::Distance(r) => r.len(),
        }
    }
    #[inline]
    pub fn into_inner(self) -> Vec<T> {
        match self {
            Self::Random(r) => r.replicas,
            Self::Distance(r) => r.replicas,
        }
    }
    #[inline]
    pub fn as_ref(&self) -> &[T] {
        match self {
            Self::Random(r) => &r.replicas,
            Self::Distance(r) => &r.replicas[0..self.len()],
        }
    }
    #[inline]
    pub unsafe fn unsafe_select(&self) -> (usize, &T) {
        match self {
            Self::Random(r) => r.unsafe_select(),
            Self::Distance(r) => r.unsafe_select(),
        }
    }
    #[inline]
    pub unsafe fn unsafe_next(&self, idx: usize, runs: usize) -> (usize, &T) {
        match self {
            Self::Random(r) => r.unsafe_next(idx),
            Self::Distance(r) => r.unsafe_next(idx, runs),
        }
    }
}
