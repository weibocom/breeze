#[derive(Clone)]
pub enum Modula {
    Pow2(Pow2),
    Other(Other),
}

impl Modula {
    pub fn from(sharding_num: usize) -> Self {
        if sharding_num == 0 || sharding_num & (sharding_num - 1) == 0 {
            Modula::Pow2(Pow2::from(sharding_num))
        } else {
            Modula::Other(Other::from(sharding_num))
        }
    }

    #[inline(always)]
    pub fn index(&self, hash: u64) -> usize {
        match self {
            Self::Pow2(m) => m.index(hash),
            Self::Other(m) => m.index(hash),
        }
    }
}

#[derive(Clone)]
pub struct Pow2 {
    mask: usize,
}

impl Pow2 {
    pub fn from(shard_num: usize) -> Self {
        let mask = if shard_num == 0 { 0 } else { shard_num - 1 };
        Self { mask }
    }
    #[inline(always)]
    pub fn index(&self, hash: u64) -> usize {
        hash as usize & self.mask
    }
}

#[derive(Clone)]
pub struct Other {
    len: usize,
}
impl Other {
    pub fn from(shard_num: usize) -> Self {
        Self { len: shard_num }
    }
    #[inline(always)]
    pub fn index(&self, hash: u64) -> usize {
        hash as usize % self.len
    }
}
