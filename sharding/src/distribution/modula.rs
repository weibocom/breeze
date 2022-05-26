#[derive(Clone, Debug)]
pub enum Modula {
    Pow2(Pow2),
    Other(Other),
}

impl Modula {
    // absolut_hash 用于兼容hc业务算法：google commons crc32 + java 求余并absolute
    pub fn from(sharding_num: usize, absolute_hash: bool) -> Self {
        if sharding_num == 0 || sharding_num & (sharding_num - 1) == 0 {
            Modula::Pow2(Pow2::from(sharding_num, absolute_hash))
        } else {
            Modula::Other(Other::from(sharding_num, absolute_hash))
        }
    }

    #[inline]
    pub fn index(&self, hash: i64) -> usize {
        match self {
            Self::Pow2(m) => m.index(hash),
            Self::Other(m) => m.index(hash),
        }
    }
}

// 实际hash计算一般都是int，但对于负数，各个语言、算法处理不同，为考虑兼容，hash用i64，在必需的场景，才转成i32 fishermen 2020.5.26
#[derive(Clone, Debug, Default)]
pub struct Pow2 {
    mask: usize,
    absolute_hash: bool,
}

impl Pow2 {
    pub fn from(shard_num: usize, absolute_hash: bool) -> Self {
        let mask = if shard_num == 0 { 0 } else { shard_num - 1 };
        Self {
            mask,
            absolute_hash,
        }
    }
    #[inline]
    pub fn index(&self, hash: i64) -> usize {
        if self.absolute_hash {
            (hash as i32).abs() as usize & self.mask
        } else {
            hash as usize & self.mask
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Other {
    len: usize,
    absolute_hash: bool,
}
impl Other {
    pub fn from(shard_num: usize, absolute_hash: bool) -> Self {
        Self {
            len: shard_num,
            absolute_hash,
        }
    }
    #[inline]
    pub fn index(&self, hash: i64) -> usize {
        if self.absolute_hash {
            return (hash as i32).abs() as usize % self.len;
        } else {
            hash as usize % self.len
        }
    }
}
